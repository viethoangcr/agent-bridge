package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestLogger returns a JSON slog logger writing into buf.
func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, nil))
}

// logRecords decodes every JSON record captured in buf.
func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

// onlyLogRecord requires exactly one captured record and returns it.
func onlyLogRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	records := logRecords(t, buf)
	if len(records) != 1 {
		t.Fatalf("log records = %d, want 1 (payload %q)", len(records), buf.String())
	}
	return records[0]
}

// assertLogStatus asserts the numeric status field of a captured record.
func assertLogStatus(t *testing.T, rec map[string]any, want int) {
	t.Helper()
	got, ok := rec["status"].(float64)
	if !ok {
		t.Fatalf("status = %v (%T), want %d", rec["status"], rec["status"], want)
	}
	if int(got) != want {
		t.Errorf("status = %v, want %d", got, want)
	}
}

// assertLatencyMs asserts latency_ms is a non-negative integer.
func assertLatencyMs(t *testing.T, rec map[string]any) {
	t.Helper()
	got, ok := rec["latency_ms"].(float64)
	if !ok {
		t.Fatalf("latency_ms = %v (%T), want number", rec["latency_ms"], rec["latency_ms"])
	}
	if got < 0 {
		t.Errorf("latency_ms = %v, want >= 0", got)
	}
	if math.Trunc(got) != got {
		t.Errorf("latency_ms = %v, want integer milliseconds", got)
	}
}

func TestRequestLoggerRecordsFields(t *testing.T) {
	const target = "/v1/health?verbose=1"

	cases := []struct {
		name       string
		handler    http.Handler
		wantStatus int
	}{
		{
			name: "success",
			handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}),
			wantStatus: http.StatusCreated,
		},
		{
			name: "problem",
			handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				WriteProblem(w, Problem{
					Type:   "about:blank",
					Title:  "Not Found",
					Status: http.StatusNotFound,
					Detail: "not found",
				})
			}),
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			handler := requestLogger(newTestLogger(&buf), tc.handler)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))

			entry := onlyLogRecord(t, &buf)
			if got := entry["method"]; got != http.MethodGet {
				t.Errorf("method = %v, want %q", got, http.MethodGet)
			}
			if got := entry["uri"]; got != target {
				t.Errorf("uri = %v, want %q", got, target)
			}
			assertLogStatus(t, entry, tc.wantStatus)
			assertLatencyMs(t, entry)
		})
	}
}

func TestRequestLoggerNeverLogsSecrets(t *testing.T) {
	const (
		secret      = "distinctive-authorization-secret-9f3a"
		requestBody = "distinctive-request-body-7d1e"
		headerValue = "distinctive-header-value-4b2c"
	)

	var buf bytes.Buffer
	handler := requestLogger(newTestLogger(&buf), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Response-Marker", headerValue)
		_, _ = w.Write([]byte(requestBody))
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/health", strings.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("X-Request-Marker", headerValue)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	payload := buf.String()
	if payload == "" {
		t.Fatal("no log output captured")
	}
	for _, forbidden := range []string{secret, "Authorization", requestBody, headerValue} {
		if strings.Contains(payload, forbidden) {
			t.Errorf("log payload contains %q: %s", forbidden, payload)
		}
	}
	onlyLogRecord(t, &buf)
}

func TestRequestLoggerImplicitStatus(t *testing.T) {
	var buf bytes.Buffer
	handler := requestLogger(newTestLogger(&buf), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("no explicit status"))
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	entry := onlyLogRecord(t, &buf)
	assertLogStatus(t, entry, http.StatusOK)
}

func TestRequestLoggerPreservesFlusher(t *testing.T) {
	var buf bytes.Buffer
	var flushed bool
	handler := requestLogger(newTestLogger(&buf), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Error("wrapped writer does not implement http.Flusher")
			return
		}
		f.Flush()
		flushed = true
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if !flushed {
		t.Error("handler did not flush through the wrapped writer")
	}
	if !rec.Flushed {
		t.Error("underlying ResponseRecorder was not flushed")
	}
	_ = onlyLogRecord(t, &buf)
}

func TestRequestLoggerServerWiring(t *testing.T) {
	const token = "wiring-token"

	var buf bytes.Buffer
	server := NewServer(Dependencies{Token: token, Log: newTestLogger(&buf)})

	// An unauthenticated /v1 request must still be logged, proving logging is
	// the outermost middleware (outside authenticate).
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/health", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	entry := onlyLogRecord(t, &buf)
	assertLogStatus(t, entry, http.StatusUnauthorized)

	// A nil logger must not panic and must still serve requests.
	nilServer := NewServer(Dependencies{Token: token})
	nilRec := httptest.NewRecorder()
	nilServer.Handler().ServeHTTP(nilRec, httptest.NewRequest(http.MethodGet, "/", nil))
	if nilRec.Code != http.StatusOK {
		t.Fatalf("nil-log root status = %d, want %d", nilRec.Code, http.StatusOK)
	}
}

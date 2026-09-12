package httpapi

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

func TestProcessValidationDecodeJSON(t *testing.T) {
	type payload struct {
		A int `json:"a"`
	}
	tests := []struct {
		name        string
		body        string
		contentType string
		limit       int64
		wantOK      bool
		wantStatus  int
	}{
		{"valid", `{"a":1}`, "application/json", 1024, true, http.StatusOK},
		{"unknown field", `{"a":1,"b":2}`, "application/json", 1024, false, http.StatusBadRequest},
		{"trailing value", `{"a":1}{}`, "application/json", 1024, false, http.StatusBadRequest},
		{"malformed", `{"a":`, "application/json", 1024, false, http.StatusBadRequest},
		{"missing content type", `{"a":1}`, "", 1024, false, http.StatusUnsupportedMediaType},
		{"over limit", `{"a":123456789}`, "application/json", 8, false, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/processes/config", strings.NewReader(tc.body))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			var dst payload
			ok := decodeProcessJSON(rec, req, tc.limit, &dst)
			if ok != tc.wantOK {
				t.Fatalf("decodeProcessJSON() = %v, want %v (status %d)", ok, tc.wantOK, rec.Code)
			}
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestProcessValidationInputEncodedCeiling(t *testing.T) {
	tests := []struct {
		name   string
		active int
		want   int64
	}{
		{"one byte", 1, 4 + 1024},
		{"exact block", 3, 4 + 1024},
		{"partial block", 4, 8 + 1024},
		{"production maximum", 7340032, 9787736},
		{"above production maximum", 7340033, 9787736},
		{"above global ceiling clamps", 1 << 30, maxProcessJSONBytes},
		{"near-maximum clamps", math.MaxInt - 1, maxProcessJSONBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := inputEncodedBodyLimit(tc.active); got != tc.want {
				t.Fatalf("inputEncodedBodyLimit(%d) = %d, want %d", tc.active, got, tc.want)
			}
		})
	}

	t.Run("overflow clamps to global ceiling", func(t *testing.T) {
		if got := inputEncodedBodyLimit(math.MaxInt); got != maxProcessJSONBytes {
			t.Fatalf("inputEncodedBodyLimit(MaxInt) = %d, want %d", got, maxProcessJSONBytes)
		}
	})
	t.Run("global ceiling is 10MiB", func(t *testing.T) {
		if maxProcessJSONBytes != 10<<20 {
			t.Fatalf("maxProcessJSONBytes = %d, want 10MiB", maxProcessJSONBytes)
		}
	})
}

func TestProcessValidationLogsQuery(t *testing.T) {
	zero := 0
	seven := 7
	tests := []struct {
		name      string
		target    string
		want      process.LogQuery
		wantError bool
	}{
		{"empty defaults to combined", "", process.LogQuery{}, false},
		{"stdout", "?stream=stdout", process.LogQuery{Stream: "stdout"}, false},
		{"stderr", "?stream=stderr", process.LogQuery{Stream: "stderr"}, false},
		{"combined", "?stream=combined", process.LogQuery{Stream: "combined"}, false},
		{"since", "?since=5", process.LogQuery{Since: 5}, false},
		{"tail zero", "?tail=0", process.LogQuery{Tail: &zero}, false},
		{"tail positive", "?tail=7", process.LogQuery{Tail: &seven}, false},
		{"unknown key", "?foo=1", process.LogQuery{}, true},
		{"repeated key", "?tail=1&tail=2", process.LogQuery{}, true},
		{"empty value", "?tail=", process.LogQuery{}, true},
		{"empty stream", "?stream=", process.LogQuery{}, true},
		{"malformed tail", "?tail=x", process.LogQuery{}, true},
		{"negative since", "?since=-1", process.LogQuery{}, true},
		{"unknown stream", "?stream=bogus", process.LogQuery{}, true},
		{"fractional tail", "?tail=1.5", process.LogQuery{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/processes/proc_1/logs"+tc.target, nil)
			got, err := parseLogsQuery(req.URL.Query())
			if (err != nil) != tc.wantError {
				t.Fatalf("parseLogsQuery() error = %v, wantError %v", err, tc.wantError)
			}
			if err == nil && !equalLogQuery(got, tc.want) {
				t.Fatalf("parseLogsQuery() = %+v, want %+v", got, tc.want)
			}
		})
	}

	t.Run("all keys accepted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/processes/proc_1/logs?stream=stdout&since=3&tail=2", nil)
		got, err := parseLogsQuery(req.URL.Query())
		if err != nil {
			t.Fatalf("parseLogsQuery() error = %v", err)
		}
		two := 2
		if !equalLogQuery(got, process.LogQuery{Stream: "stdout", Since: 3, Tail: &two}) {
			t.Fatalf("parseLogsQuery() = %+v", got)
		}
	})
}

func equalLogQuery(a, b process.LogQuery) bool {
	if a.Since != b.Since || a.Stream != b.Stream {
		return false
	}
	switch {
	case a.Tail == nil && b.Tail == nil:
		return true
	case a.Tail == nil || b.Tail == nil:
		return false
	default:
		return *a.Tail == *b.Tail
	}
}

func TestProcessValidationMapError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"validation", process.ErrValidation, http.StatusBadRequest},
		{"wrapped validation", errors.Join(errors.New("x"), process.ErrValidation), http.StatusBadRequest},
		{"payload too large", process.ErrPayloadTooLarge, http.StatusRequestEntityTooLarge},
		{"not found", process.ErrNotFound, http.StatusNotFound},
		{"conflict", process.ErrConflict, http.StatusConflict},
		{"capacity", process.ErrCapacity, http.StatusConflict},
		{"start", process.ErrStart, http.StatusBadGateway},
		{"gateway", process.ErrGateway, http.StatusBadGateway},
		{"unknown", errors.New("boom"), http.StatusBadGateway},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			problem := mapProcessError(tc.err)
			if problem.Status != tc.wantStatus {
				t.Fatalf("status = %d, want %d", problem.Status, tc.wantStatus)
			}
			if problem.Type != "about:blank" || problem.Title == "" || problem.Detail == "" {
				t.Fatalf("incomplete problem: %+v", problem)
			}
		})
	}
}

func TestProcessValidationRouteFallback(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
	}{
		{"config wrong method", http.MethodPut, "/v1/processes/config", http.StatusMethodNotAllowed},
		{"config delete", http.MethodDelete, "/v1/processes/config", http.StatusMethodNotAllowed},
		{"unknown process path", http.MethodGet, "/v1/processes/nope", http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newProcessServer(t)
			rec := doProcessRequest(t, s, tc.method, tc.target, "", "")
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("Content-Type = %q, want application/problem+json", ct)
			}
			var problem struct {
				Status int `json:"status"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
				t.Fatalf("decode problem: %v", err)
			}
			if problem.Status != tc.wantStatus {
				t.Fatalf("problem status = %d, want %d", problem.Status, tc.wantStatus)
			}
		})
	}

	t.Run("allow header lists both config methods", func(t *testing.T) {
		s, _ := newProcessServer(t)
		rec := doProcessRequest(t, s, http.MethodPut, "/v1/processes/config", "", "")
		allow := rec.Header().Get("Allow")
		if !strings.Contains(allow, http.MethodGet) || !strings.Contains(allow, http.MethodPost) {
			t.Fatalf("Allow = %q, want GET and POST", allow)
		}
	})
}

// TestProcessEndpointsRejectQueryStrings proves every process endpoint except
// logs documents no query parameters: any supplied query string is the
// invalid-query 400, while logs keeps its documented allowlist.
func TestProcessEndpointsRejectQueryStrings(t *testing.T) {
	endpoints := []struct {
		name        string
		method      string
		target      string
		contentType string
		body        string
	}{
		{"start", http.MethodPost, "/v1/processes", "application/json", `{"command":"/bin/true"}`},
		{"run", http.MethodPost, "/v1/processes/run", "application/json", `{"command":"/bin/true"}`},
		{"config get", http.MethodGet, "/v1/processes/config", "", ""},
		{"config post", http.MethodPost, "/v1/processes/config", "application/json", configJSON(t, nil)},
		{"list", http.MethodGet, "/v1/processes", "", ""},
		{"get", http.MethodGet, "/v1/processes/proc_1", "", ""},
		{"stop", http.MethodPost, "/v1/processes/proc_1/stop", "", ""},
		{"kill", http.MethodPost, "/v1/processes/proc_1/kill", "", ""},
		{"delete", http.MethodDelete, "/v1/processes/proc_1", "", ""},
		{"input", http.MethodPost, "/v1/processes/proc_1/input", "application/json", `{"data":"aGk=","encoding":"base64"}`},
	}
	for _, endpoint := range endpoints {
		for _, query := range []string{"?foo=1", "?foo=1&foo=2", "?stream=stdout"} {
			t.Run(endpoint.name+" "+query, func(t *testing.T) {
				s, _ := newProcessServer(t)
				rec := doProcessRequest(t, s, endpoint.method, endpoint.target+query, endpoint.contentType, endpoint.body)
				assertProblem(t, rec, http.StatusBadRequest)
			})
		}
	}

	t.Run("logs keeps its documented allowlist", func(t *testing.T) {
		s, _ := newProcessServer(t)
		rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/proc_missing/logs?stream=stdout", "", "")
		assertProblem(t, rec, http.StatusNotFound)
	})
}

// TestProcessLogsMalformedQueryRejected proves a percent-decoding error in the
// logs query is rejected rather than silently dropped.
func TestProcessLogsMalformedQueryRejected(t *testing.T) {
	s, _ := newProcessServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/processes/proc_missing/logs", nil)
	req.URL.RawQuery = "stream=stdout&evil=%zz"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assertProblem(t, rec, http.StatusBadRequest)
}

// TestProcessReservedLiteralRoutes proves the config and run literals never
// fall into the /{id} wildcard handlers: every unsupported method gets an
// exact 405 with a deterministic Allow, while unknown IDs keep 404 semantics.
func TestProcessReservedLiteralRoutes(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
		allow  string
	}{
		{"run HEAD", http.MethodHead, "/v1/processes/run", "POST"},
		{"run GET", http.MethodGet, "/v1/processes/run", "POST"},
		{"run PUT", http.MethodPut, "/v1/processes/run", "POST"},
		{"run PATCH", http.MethodPatch, "/v1/processes/run", "POST"},
		{"run DELETE", http.MethodDelete, "/v1/processes/run", "POST"},
		{"run OPTIONS", http.MethodOptions, "/v1/processes/run", "POST"},
		{"run TRACE", http.MethodTrace, "/v1/processes/run", "POST"},
		{"config HEAD", http.MethodHead, "/v1/processes/config", "GET, POST"},
		{"config PUT", http.MethodPut, "/v1/processes/config", "GET, POST"},
		{"config PATCH", http.MethodPatch, "/v1/processes/config", "GET, POST"},
		{"config DELETE", http.MethodDelete, "/v1/processes/config", "GET, POST"},
		{"config OPTIONS", http.MethodOptions, "/v1/processes/config", "GET, POST"},
		{"config TRACE", http.MethodTrace, "/v1/processes/config", "GET, POST"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newProcessServer(t)
			rec := doProcessRequest(t, s, tc.method, tc.target, "", "")
			assertProblem(t, rec, http.StatusMethodNotAllowed)
			if got := rec.Header().Get("Allow"); got != tc.allow {
				t.Fatalf("Allow = %q, want %q", got, tc.allow)
			}
		})
	}

	t.Run("unknown ids keep 404", func(t *testing.T) {
		s, _ := newProcessServer(t)
		if rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/proc_missing", "", ""); rec.Code != http.StatusNotFound {
			t.Fatalf("GET unknown id status = %d, want 404: %s", rec.Code, rec.Body.String())
		}
		if rec := doProcessRequest(t, s, http.MethodDelete, "/v1/processes/proc_missing", "", ""); rec.Code != http.StatusNotFound {
			t.Fatalf("DELETE unknown id status = %d, want 404: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("supported methods still work", func(t *testing.T) {
		s, _ := newProcessServer(t)
		if rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/config", "", ""); rec.Code != http.StatusOK {
			t.Fatalf("GET config status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		if rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/run", "application/json", `{"command":"/bin/true"}`); rec.Code != http.StatusOK {
			t.Fatalf("POST run status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
	})
}

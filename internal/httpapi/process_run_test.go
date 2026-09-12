package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// newProcessHTTPServer serves the composed handler over a real HTTP server so
// the one-shot run endpoint is exercised across the wire, not only through a
// ResponseRecorder.
func newProcessHTTPServer(t *testing.T) (*httptest.Server, *process.Manager) {
	t.Helper()
	manager := process.NewManager(nil, t.TempDir())
	srv := httptest.NewServer(NewServer(Dependencies{Processes: manager}).Handler())
	t.Cleanup(func() {
		srv.Close()
		for _, snap := range manager.List() {
			if snap.Status == process.StatusRunning {
				_, _ = manager.Kill(snap.ID)
			}
			_ = manager.Delete(snap.ID)
		}
	})
	return srv, manager
}

// roundTrip performs one request over a real HTTP server and returns the
// status, raw body, and headers.
func roundTrip(t *testing.T, srv *httptest.Server, method, path, contentType, body string) (int, []byte, http.Header) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s body: %v", method, path, err)
	}
	return resp.StatusCode, data, resp.Header
}

// TestProcessRun covers POST /v1/processes/run through a real HTTP server with
// exact result serialization, timeout exit-code omission, independent stream
// truncation, validation, capacity conflicts, and spawn failures.
func TestProcessRun(t *testing.T) {
	t.Run("success and non-zero exit", func(t *testing.T) {
		srv, _ := newProcessHTTPServer(t)
		code, body, _ := roundTrip(t, srv, http.MethodPost, "/v1/processes/run", "application/json",
			`{"command":"/bin/sh","args":["-c","printf out; printf err >&2; exit 3"]}`)
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", code, body)
		}
		assertExactKeys(t, decodeObject(t, body), "exitCode", "timedOut", "stdout", "stderr", "stdoutTruncated", "stderrTruncated", "durationMs")

		var res process.RunResult
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decode run result: %v", err)
		}
		if res.ExitCode == nil || *res.ExitCode != 3 {
			t.Fatalf("exitCode = %v, want 3", res.ExitCode)
		}
		if res.TimedOut {
			t.Fatalf("timedOut = true, want false")
		}
		if res.Stdout != "out" || res.Stderr != "err" {
			t.Fatalf("stdout/stderr = %q/%q, want out/err", res.Stdout, res.Stderr)
		}
		if res.StdoutTruncated || res.StderrTruncated {
			t.Fatalf("truncation flags = %v/%v, want false", res.StdoutTruncated, res.StderrTruncated)
		}
		if res.DurationMs < 0 {
			t.Fatalf("durationMs = %d, want non-negative", res.DurationMs)
		}

		code, body, _ = roundTrip(t, srv, http.MethodPost, "/v1/processes/run", "application/json",
			`{"command":"/bin/sh","args":["-c","printf hello"]}`)
		if code != http.StatusOK {
			t.Fatalf("success status = %d, want 200: %s", code, body)
		}
		var ok process.RunResult
		if err := json.Unmarshal(body, &ok); err != nil {
			t.Fatalf("decode success result: %v", err)
		}
		if ok.ExitCode == nil || *ok.ExitCode != 0 || ok.Stdout != "hello" {
			t.Fatalf("success result = %+v, want exit 0 and stdout hello", ok)
		}
	})

	t.Run("timeout omits exitCode", func(t *testing.T) {
		srv, _ := newProcessHTTPServer(t)
		code, body, _ := roundTrip(t, srv, http.MethodPost, "/v1/processes/run", "application/json",
			`{"command":"/bin/sleep","args":["30"],"timeoutMs":50}`)
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", code, body)
		}
		assertExactKeys(t, decodeObject(t, body), "timedOut", "stdout", "stderr", "stdoutTruncated", "stderrTruncated", "durationMs")
		var res process.RunResult
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decode timeout result: %v", err)
		}
		if !res.TimedOut {
			t.Fatalf("timedOut = false, want true")
		}
	})

	t.Run("independent truncation flags", func(t *testing.T) {
		srv, _ := newProcessHTTPServer(t)
		code, body, _ := roundTrip(t, srv, http.MethodPost, "/v1/processes/run", "application/json",
			`{"command":"/bin/sh","args":["-c","printf 1234567890; printf ab >&2"],"maxOutputBytes":4}`)
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", code, body)
		}
		var res process.RunResult
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decode truncation result: %v", err)
		}
		if res.Stdout != "1234" || !res.StdoutTruncated {
			t.Fatalf("stdout = %q truncated=%v, want %q truncated=true", res.Stdout, res.StdoutTruncated, "1234")
		}
		if res.Stderr != "ab" || res.StderrTruncated {
			t.Fatalf("stderr = %q truncated=%v, want %q truncated=false", res.Stderr, res.StderrTruncated, "ab")
		}
	})

	t.Run("validation", func(t *testing.T) {
		srv, _ := newProcessHTTPServer(t)
		tests := []struct {
			name        string
			contentType string
			body        string
			wantStatus  int
		}{
			{"empty command", "application/json", `{"command":""}`, http.StatusBadRequest},
			{"unknown field", "application/json", `{"command":"/bin/true","owner":"me"}`, http.StatusBadRequest},
			{"timeout above maximum", "application/json", `{"command":"/bin/true","timeoutMs":300001}`, http.StatusBadRequest},
			{"output above maximum", "application/json", `{"command":"/bin/true","maxOutputBytes":1048577}`, http.StatusBadRequest},
			{"malformed", "application/json", `{`, http.StatusBadRequest},
			{"trailing value", "application/json", `{"command":"/bin/true"} {}`, http.StatusBadRequest},
			{"missing content type", "", `{"command":"/bin/true"}`, http.StatusUnsupportedMediaType},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				code, body, header := roundTrip(t, srv, http.MethodPost, "/v1/processes/run", tc.contentType, tc.body)
				if code != tc.wantStatus {
					t.Fatalf("status = %d, want %d: %s", code, tc.wantStatus, body)
				}
				if ct := header.Get("Content-Type"); ct != "application/problem+json" {
					t.Fatalf("Content-Type = %q, want application/problem+json", ct)
				}
			})
		}
	})

	t.Run("capacity conflict", func(t *testing.T) {
		srv, manager := newProcessHTTPServer(t)
		cfg := manager.Config()
		cfg.MaxConcurrentProcesses = 1
		if err := manager.UpdateConfig(cfg); err != nil {
			t.Fatalf("UpdateConfig: %v", err)
		}
		code, body, _ := roundTrip(t, srv, http.MethodPost, "/v1/processes", "application/json", `{"command":"/bin/sleep","args":["30"]}`)
		if code != http.StatusOK {
			t.Fatalf("start status = %d, want 200: %s", code, body)
		}
		code, body, _ = roundTrip(t, srv, http.MethodPost, "/v1/processes/run", "application/json", `{"command":"/bin/true"}`)
		if code != http.StatusConflict {
			t.Fatalf("run status = %d, want 409: %s", code, body)
		}
	})

	t.Run("spawn failure is 502 without env leak", func(t *testing.T) {
		srv, _ := newProcessHTTPServer(t)
		code, body, _ := roundTrip(t, srv, http.MethodPost, "/v1/processes/run", "application/json",
			`{"command":"/no/such/agent-bridge-binary","env":{"AGENT_BRIDGE_TEST_SECRET":"hunter2"}}`)
		if code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502: %s", code, body)
		}
		if strings.Contains(string(body), "hunter2") {
			t.Fatalf("problem leaked environment value: %s", body)
		}
	})
}

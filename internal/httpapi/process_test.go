package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// newProcessServer builds the public handler around a real process manager.
// Cleanup kills and deletes every record so no helper process leaks on failure.
func newProcessServer(t *testing.T) (*Server, *process.Manager) {
	t.Helper()
	manager := process.NewManager(nil, t.TempDir())
	t.Cleanup(func() {
		for _, snap := range manager.List() {
			if snap.Status == process.StatusRunning {
				_, _ = manager.Kill(snap.ID)
			}
			_ = manager.Delete(snap.ID)
		}
	})
	return NewServer(Dependencies{Processes: manager}), manager
}

// doProcessRequest drives one request through the public assembly path.
func doProcessRequest(t *testing.T, s *Server, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// configJSON marshals a full replacement config after applying mutate.
func configJSON(t *testing.T, mutate func(*process.Config)) string {
	t.Helper()
	cfg := process.DefaultConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return string(encoded)
}

const wantDefaultConfigJSON = `{"maxConcurrentProcesses":64,"defaultRunTimeoutMs":30000,"maxRunTimeoutMs":300000,"maxOutputBytes":1048576,"maxLogBytesPerProcess":10485760,"maxInputBytesPerRequest":65536}`

func TestProcessConfigGetDefaults(t *testing.T) {
	s, _ := newProcessServer(t)
	rec := doProcessRequest(t, s, http.MethodGet, "/v1/processes/config", "", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != wantDefaultConfigJSON {
		t.Fatalf("body = %s, want %s", got, wantDefaultConfigJSON)
	}
}

func TestProcessConfigPostReplacesAllFields(t *testing.T) {
	s, manager := newProcessServer(t)
	next := process.Config{
		MaxConcurrentProcesses:  8,
		DefaultRunTimeoutMs:     100,
		MaxRunTimeoutMs:         200,
		MaxOutputBytes:          1024,
		MaxLogBytesPerProcess:   2048,
		MaxInputBytesPerRequest: 512,
	}
	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/config", "application/json", configJSON(t, func(c *process.Config) { *c = next }))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := manager.Config(); got != next {
		t.Fatalf("active config = %+v, want %+v", got, next)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != configJSON(t, func(c *process.Config) { *c = next }) {
		t.Fatalf("body = %s, want replacement config", got)
	}

	get := doProcessRequest(t, s, http.MethodGet, "/v1/processes/config", "", "")
	if got := strings.TrimSpace(get.Body.String()); got != configJSON(t, func(c *process.Config) { *c = next }) {
		t.Fatalf("GET body = %s, want replacement config", got)
	}
}

func TestProcessConfigPostAcceptsOutputAtMaximum(t *testing.T) {
	s, manager := newProcessServer(t)
	body := configJSON(t, func(c *process.Config) { c.MaxOutputBytes = 16777216 })
	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/config", "application/json", body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := manager.Config().MaxOutputBytes; got != 16777216 {
		t.Fatalf("maxOutputBytes = %d, want 16777216", got)
	}
}

func TestProcessConfigPostAcceptsEveryMaximum(t *testing.T) {
	s, manager := newProcessServer(t)
	atMax := process.Config{
		MaxConcurrentProcesses:  1024,
		DefaultRunTimeoutMs:     86400000,
		MaxRunTimeoutMs:         86400000,
		MaxOutputBytes:          16777216,
		MaxLogBytesPerProcess:   268435456,
		MaxInputBytesPerRequest: 7340032,
	}
	body := configJSON(t, func(c *process.Config) { *c = atMax })
	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/config", "application/json", body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := manager.Config(); got != atMax {
		t.Fatalf("active config = %+v, want every maximum", got)
	}
}

func TestProcessConfigPostRejectsInvalidBodies(t *testing.T) {
	intFields := []struct {
		name string
		set  func(*process.Config, int)
	}{
		{"maxConcurrentProcesses", func(c *process.Config, v int) { c.MaxConcurrentProcesses = v }},
		{"defaultRunTimeoutMs", func(c *process.Config, v int) { c.DefaultRunTimeoutMs = v }},
		{"maxRunTimeoutMs", func(c *process.Config, v int) { c.MaxRunTimeoutMs = v }},
		{"maxOutputBytes", func(c *process.Config, v int) { c.MaxOutputBytes = v }},
		{"maxLogBytesPerProcess", func(c *process.Config, v int) { c.MaxLogBytesPerProcess = v }},
		{"maxInputBytesPerRequest", func(c *process.Config, v int) { c.MaxInputBytesPerRequest = v }},
	}
	maxima := map[string]int{
		"maxConcurrentProcesses":  1024,
		"defaultRunTimeoutMs":     86400000,
		"maxRunTimeoutMs":         86400000,
		"maxOutputBytes":          16777216,
		"maxLogBytesPerProcess":   268435456,
		"maxInputBytesPerRequest": 7340032,
	}

	cases := map[string]string{
		"missing field":    `{"maxConcurrentProcesses":8}`,
		"unknown field":    strings.TrimSuffix(configJSON(t, nil), "}") + `,"owner":"me"}`,
		"pty field":        strings.TrimSuffix(configJSON(t, nil), "}") + `,"pty":true}`,
		"restart field":    strings.TrimSuffix(configJSON(t, nil), "}") + `,"restart":"always"}`,
		"malformed json":   `{`,
		"trailing value":   configJSON(t, nil) + ` {}`,
		"wrong type":       `{"maxConcurrentProcesses":"8","defaultRunTimeoutMs":100,"maxRunTimeoutMs":200,"maxOutputBytes":1024,"maxLogBytesPerProcess":2048,"maxInputBytesPerRequest":512}`,
		"fractional value": `{"maxConcurrentProcesses":8.5,"defaultRunTimeoutMs":100,"maxRunTimeoutMs":200,"maxOutputBytes":1024,"maxLogBytesPerProcess":2048,"maxInputBytesPerRequest":512}`,
		"integer overflow": `{"maxConcurrentProcesses":9223372036854775808,"defaultRunTimeoutMs":100,"maxRunTimeoutMs":200,"maxOutputBytes":1024,"maxLogBytesPerProcess":2048,"maxInputBytesPerRequest":512}`,
		"invalid ordering": configJSON(t, func(c *process.Config) {
			c.DefaultRunTimeoutMs = 200
			c.MaxRunTimeoutMs = 100
		}),
		"output above maximum": configJSON(t, func(c *process.Config) { c.MaxOutputBytes = 16777217 }),
	}
	for _, field := range intFields {
		cases[field.name+" zero"] = configJSON(t, func(c *process.Config) { field.set(c, 0) })
		cases[field.name+" negative"] = configJSON(t, func(c *process.Config) { field.set(c, -1) })
		cases[field.name+" above maximum"] = configJSON(t, func(c *process.Config) { field.set(c, maxima[field.name]+1) })
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s, manager := newProcessServer(t)
			before := manager.Config()
			rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/config", "application/json", body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("Content-Type = %q, want application/problem+json", ct)
			}
			if got := manager.Config(); got != before {
				t.Fatalf("active config changed to %+v after rejected update, want %+v", got, before)
			}
		})
	}
}

func TestProcessConfigContentTypeStrictness(t *testing.T) {
	body := configJSON(t, nil)
	tests := []struct {
		name        string
		contentType string
		wantStatus  int
	}{
		{"missing", "", http.StatusUnsupportedMediaType},
		{"plain text", "text/plain", http.StatusUnsupportedMediaType},
		{"malformed", "application", http.StatusUnsupportedMediaType},
		{"case insensitive", "Application/JSON", http.StatusOK},
		{"parameters allowed", "application/json; charset=utf-8", http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newProcessServer(t)
			rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes/config", tc.contentType, body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

// startManagedProcess starts a process through the public endpoint and returns
// its snapshot.
func startManagedProcess(t *testing.T, s *Server, body string) process.Snapshot {
	t.Helper()
	rec := doProcessRequest(t, s, http.MethodPost, "/v1/processes", "application/json", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("start status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var snap process.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snap.ID == "" {
		t.Fatalf("start returned empty id")
	}
	return snap
}

// waitProcessExited polls the manager until id reports exited. HTTP handlers do
// not expose the record's exit channel.
func waitProcessExited(t *testing.T, manager *process.Manager, id string) process.Snapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := manager.Get(id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if snap.Status == process.StatusExited {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %s did not exit", id)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// decodeObject decodes a JSON object into its raw members.
func decodeObject(t *testing.T, body []byte) map[string]json.RawMessage {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		t.Fatalf("decode object: %v (body %s)", err, body)
	}
	return obj
}

// assertExactKeys fails unless obj has exactly the wanted keys.
func assertExactKeys(t *testing.T, obj map[string]json.RawMessage, want ...string) {
	t.Helper()
	if len(obj) != len(want) {
		t.Fatalf("keys = %v, want %v", sortedKeys(obj), want)
	}
	for _, key := range want {
		if _, ok := obj[key]; !ok {
			t.Fatalf("keys = %v, want %v", sortedKeys(obj), want)
		}
	}
}

func sortedKeys(obj map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// assertProblem fails unless rec is an RFC 9457 response with status.
func assertProblem(t *testing.T, rec *httptest.ResponseRecorder, status int) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d: %s", rec.Code, status, rec.Body.String())
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
	if problem.Status != status {
		t.Fatalf("problem status = %d, want %d", problem.Status, status)
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

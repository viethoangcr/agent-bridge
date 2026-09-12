package app

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a concurrency-safe log sink: the HTTP logger writes from request
// goroutines while the test reads it after cancellation.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForHealthAuth polls the authenticated health endpoint with the bridge
// bearer token so a token-configured server is not mistaken for unhealthy.
func waitForHealthAuth(t *testing.T, url, token string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("new health request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("authenticated health endpoint never served at %s", url)
}

// TestRunComposesAllPhaseRoutes proves every Phase 02-05 route is reachable
// through the single httpapi.Server that app.Run composes, that every /v1
// route shares the one bearer-auth middleware, and that dispatch reaches the
// intended non-nil dependency rather than the 503 "unavailable" fallback.
func TestRunComposesAllPhaseRoutes(t *testing.T) {
	restore := setShutdownGrace(t, 3*time.Second)
	defer restore()

	const token = "route-integration-token"
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	home := t.TempDir()
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST":        host,
		"AGENT_BRIDGE_PORT":        port,
		"AGENT_BRIDGE_DB":          filepath.Join(home, "bridge.db"),
		"AGENT_BRIDGE_PID_FILE":    filepath.Join(home, "bridge.pid"),
		"AGENT_BRIDGE_TOKEN":       token,
		"AGENT_BRIDGE_IDLE_TTL_MS": "0",
		"HOME":                     home,
	})

	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, getenv, IO{
			Stdin:  strings.NewReader(""),
			Stdout: io.Discard,
			Stderr: &logs,
		})
	}()
	waitForHealthAuth(t, "http://"+addr+"/v1/health", token)

	base := "http://" + addr
	client := &http.Client{Timeout: 5 * time.Second}

	enum := []struct {
		name   string
		method string
		path   string
		body   string
		wrong  string
	}{
		{name: "fs entries", method: http.MethodGet, path: "/v1/fs/entries?directory=" + home, wrong: http.MethodPost},
		{name: "fs file get", method: http.MethodGet, path: "/v1/fs/file?path=" + filepath.Join(home, "missing"), wrong: http.MethodPost},
		{name: "fs file put", method: http.MethodPut, path: "/v1/fs/file?path=" + filepath.Join(home, "new.txt"), body: "x", wrong: http.MethodPost},
		{name: "fs entry delete", method: http.MethodDelete, path: "/v1/fs/entry?path=" + filepath.Join(home, "missing"), wrong: http.MethodGet},
		{name: "fs mkdir", method: http.MethodPost, path: "/v1/fs/mkdir", body: "{}", wrong: http.MethodGet},
		{name: "fs move", method: http.MethodPost, path: "/v1/fs/move", body: "{}", wrong: http.MethodGet},
		{name: "fs stat", method: http.MethodGet, path: "/v1/fs/stat?path=" + home, wrong: http.MethodPost},
		{name: "fs upload batch", method: http.MethodPost, path: "/v1/fs/upload-batch?directory=" + home, wrong: http.MethodGet},
		{name: "config mcp get", method: http.MethodGet, path: "/v1/config/mcp?directory=" + home, wrong: http.MethodPost},
		{name: "config mcp put", method: http.MethodPut, path: "/v1/config/mcp?directory=" + home, body: "{}", wrong: http.MethodPost},
		{name: "config mcp delete", method: http.MethodDelete, path: "/v1/config/mcp?directory=" + home, wrong: http.MethodPost},
		{name: "config skills get", method: http.MethodGet, path: "/v1/config/skills?directory=" + home, wrong: http.MethodPost},
		{name: "config skills put", method: http.MethodPut, path: "/v1/config/skills?directory=" + home, body: "{}", wrong: http.MethodPost},
		{name: "config skills delete", method: http.MethodDelete, path: "/v1/config/skills?directory=" + home, wrong: http.MethodPost},
		{name: "process list", method: http.MethodGet, path: "/v1/processes", wrong: http.MethodPatch},
		{name: "process start", method: http.MethodPost, path: "/v1/processes", body: "{}", wrong: http.MethodPatch},
		{name: "process get", method: http.MethodGet, path: "/v1/processes/p1", wrong: http.MethodPatch},
		{name: "process delete", method: http.MethodDelete, path: "/v1/processes/p1", wrong: http.MethodPatch},
		{name: "process stop", method: http.MethodPost, path: "/v1/processes/p1/stop", wrong: http.MethodGet},
		{name: "process kill", method: http.MethodPost, path: "/v1/processes/p1/kill", wrong: http.MethodGet},
		{name: "process logs", method: http.MethodGet, path: "/v1/processes/p1/logs", wrong: http.MethodPatch},
		{name: "process input", method: http.MethodPost, path: "/v1/processes/p1/input", body: "{}", wrong: http.MethodGet},
		{name: "process config get", method: http.MethodGet, path: "/v1/processes/config", wrong: http.MethodPatch},
		{name: "process config post", method: http.MethodPost, path: "/v1/processes/config", body: "{}", wrong: http.MethodPatch},
		{name: "process run", method: http.MethodPost, path: "/v1/processes/run", body: "{}", wrong: http.MethodGet},
		{name: "acp list", method: http.MethodGet, path: "/v1/acp", wrong: http.MethodPatch},
		{name: "acp post", method: http.MethodPost, path: "/v1/acp/s1", body: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`, wrong: http.MethodPatch},
		{name: "acp sse", method: http.MethodGet, path: "/v1/acp/s1", wrong: http.MethodPatch},
		{name: "acp delete", method: http.MethodDelete, path: "/v1/acp/s1", wrong: http.MethodPatch},
		{name: "acp status", method: http.MethodGet, path: "/v1/acp/s1/status", wrong: http.MethodPatch},
		{name: "acp events", method: http.MethodGet, path: "/v1/acp/s1/events", wrong: http.MethodPatch},
		{name: "health", method: http.MethodGet, path: "/v1/health", wrong: http.MethodPatch},
	}

	for _, tc := range enum {
		t.Run(tc.name, func(t *testing.T) {
			// Unauthenticated: every /v1 route is behind exactly one auth gate.
			unauth := doRoute(t, client, http.MethodGet, base+tc.path, "", "")
			if unauth.StatusCode != http.StatusUnauthorized {
				t.Errorf("unauthenticated status = %d, want 401", unauth.StatusCode)
			}

			// Wrong method on a registered path: the shared fallback proves the
			// path is registered (405 with an Allow set) instead of a 404.
			wrong := doRoute(t, client, tc.wrong, base+tc.path, "", token)
			if wrong.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("wrong-method status = %d, want 405 (path not registered)", wrong.StatusCode)
			}
			if allow := wrong.Header.Get("Allow"); allow == "" {
				t.Error("wrong-method response has no Allow header")
			}

			// Intended dependency is present: a nil service yields 503, and a
			// missing route yields 405/404 at the fallback.
			got := doRoute(t, client, tc.method, base+tc.path, tc.body, token)
			if got.StatusCode == http.StatusServiceUnavailable {
				t.Errorf("authenticated status = 503, want the composed dependency to be present")
			}
			if got.StatusCode == http.StatusMethodNotAllowed {
				t.Errorf("authenticated status = 405, want the registered method to dispatch")
			}
		})
	}

	// The root document stays public while every /v1 route is guarded.
	root := doRoute(t, client, http.MethodGet, base+"/", "", "")
	if root.StatusCode != http.StatusOK {
		t.Errorf("root status = %d, want 200 public", root.StatusCode)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// Every request above went through the shared logging middleware, and the
	// logger never records the bearer credential.
	logged := logs.String()
	if !strings.Contains(logged, `"msg":"request"`) {
		t.Fatalf("shared request logger produced no records; logs = %q", logged)
	}
	if strings.Contains(logged, token) {
		t.Fatal("request log leaked the bearer token")
	}
}

// doRoute issues one request and returns the response with its body drained.
func doRoute(t *testing.T, client *http.Client, method, url, body, token string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, url, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

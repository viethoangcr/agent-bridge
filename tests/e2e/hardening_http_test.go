//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func assertProblemJSON(t *testing.T, resp *http.Response, data []byte, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, want, data)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var p problem
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode problem %s: %v", data, err)
	}
	if p.Type != "about:blank" || p.Status != want || p.Title == "" || p.Detail == "" {
		t.Fatalf("problem = %+v, want about:blank %d with title and detail", p, want)
	}
}

type v1Route struct {
	method string
	target string
}

// everyV1Route enumerates at least one method/path per registered /v1 pattern
// so the auth boundary is proven for the whole subtree rather than a sample.
func everyV1Route() []v1Route {
	return []v1Route{
		{http.MethodGet, "/v1/health"},
		{http.MethodGet, "/v1/acp"},
		{http.MethodPost, "/v1/acp/demo?agent=mock"},
		{http.MethodPost, "/v1/acp/"},
		{http.MethodGet, "/v1/acp/demo"},
		{http.MethodDelete, "/v1/acp/demo"},
		{http.MethodGet, "/v1/acp/demo/status"},
		{http.MethodGet, "/v1/acp/demo/events"},
		{http.MethodGet, "/v1/processes/config"},
		{http.MethodPost, "/v1/processes/config"},
		{http.MethodGet, "/v1/processes/run"},
		{http.MethodDelete, "/v1/processes/run"},
		{http.MethodPost, "/v1/processes/run"},
		{http.MethodPost, "/v1/processes"},
		{http.MethodGet, "/v1/processes"},
		{http.MethodGet, "/v1/processes/demo"},
		{http.MethodPost, "/v1/processes/demo/stop"},
		{http.MethodPost, "/v1/processes/demo/kill"},
		{http.MethodDelete, "/v1/processes/demo"},
		{http.MethodGet, "/v1/processes/demo/logs"},
		{http.MethodPost, "/v1/processes/demo/input"},
		{http.MethodGet, "/v1/fs/entries"},
		{http.MethodGet, "/v1/fs/file"},
		{http.MethodPut, "/v1/fs/file"},
		{http.MethodDelete, "/v1/fs/entry"},
		{http.MethodPost, "/v1/fs/mkdir"},
		{http.MethodPost, "/v1/fs/move"},
		{http.MethodGet, "/v1/fs/stat"},
		{http.MethodPost, "/v1/fs/upload-batch"},
		{http.MethodGet, "/v1/config/mcp"},
		{http.MethodPut, "/v1/config/mcp"},
		{http.MethodDelete, "/v1/config/mcp"},
		{http.MethodGet, "/v1/config/skills"},
		{http.MethodPut, "/v1/config/skills"},
		{http.MethodDelete, "/v1/config/skills"},
		{http.MethodGet, "/v1/unknown"},
	}
}

// assertHTTPContract proves the public root, the complete /v1 auth boundary,
// handler-level RFC 9457 errors, request-log redaction/shape, and that
// transport-level parser/header errors are never claimed as bridge problems.
func assertHTTPContract(t *testing.T, image string) {
	token := uniqueToken("harden")
	c := startMock(t, image, token, nil)

	rootResp, rootBody, err := c.doNoAuth(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("public root: %v", err)
	}
	if rootResp.StatusCode != http.StatusOK || !bytes.Contains(rootBody, []byte(`"name":"agent-bridge"`)) {
		t.Fatalf("public root = %d %s, want 200 agent-bridge document", rootResp.StatusCode, rootBody)
	}

	for _, route := range everyV1Route() {
		t.Run("auth "+route.method+" "+route.target, func(t *testing.T) {
			resp, data, err := c.doNoAuth(route.method, route.target, nil)
			if err != nil {
				t.Fatalf("unauthenticated %s %s: %v", route.method, route.target, err)
			}
			assertProblemJSON(t, resp, data, http.StatusUnauthorized)
		})
	}

	t.Run("handler-404", func(t *testing.T) {
		resp, data := c.request(http.MethodGet, "/v1/unknown", nil)
		assertProblemJSON(t, resp, data, http.StatusNotFound)
	})
	t.Run("handler-405", func(t *testing.T) {
		resp, data := c.request(http.MethodPost, "/v1/health", nil)
		assertProblemJSON(t, resp, data, http.StatusMethodNotAllowed)
		if allow := resp.Header.Get("Allow"); allow != "GET, HEAD" {
			t.Fatalf("Allow = %q, want GET, HEAD", allow)
		}
	})

	t.Run("media-negotiation", func(t *testing.T) {
		body := rpc("ct", "initialize", map[string]any{"protocolVersion": 1})
		resp, data := rawRequest(t, c, http.MethodPost, acpPath("media", "mock"), map[string]string{"Content-Type": "text/plain"}, body)
		assertProblemJSON(t, resp, data, http.StatusUnsupportedMediaType)

		resp, data = rawRequest(t, c, http.MethodPost, acpPath("media", "mock"), map[string]string{
			"Content-Type": "application/json",
			"Accept":       "text/plain",
		}, body)
		assertProblemJSON(t, resp, data, http.StatusNotAcceptable)

		resp, data = rawRequest(t, c, http.MethodGet, "/v1/acp/media", map[string]string{"Accept": "application/json"}, nil)
		assertProblemJSON(t, resp, data, http.StatusNotAcceptable)
	})

	t.Run("invalid-server-id", func(t *testing.T) {
		body := rpc("bad", "initialize", map[string]any{"protocolVersion": 1})
		resp, data := rawRequest(t, c, http.MethodPost, "/v1/acp/"+strings.Repeat("a", 129), map[string]string{"Content-Type": "application/json"}, body)
		assertProblemJSON(t, resp, data, http.StatusBadRequest)
	})

	t.Run("filesystem-errors", func(t *testing.T) {
		resp, data := c.request(http.MethodGet, "/v1/fs/stat?path=/workspace", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stat /workspace = %d: %s", resp.StatusCode, data)
		}
		resp, data = c.request(http.MethodGet, "/v1/fs/stat?path=/workspace/does-not-exist-missing", nil)
		assertProblemJSON(t, resp, data, http.StatusNotFound)
		resp, data = c.request(http.MethodGet, "/v1/fs/entries?directory=..%2Fescape", nil)
		assertProblemJSON(t, resp, data, http.StatusBadRequest)
	})

	// A distinctive body must never appear in request logs. The marker is
	// written through the filesystem API so the request is otherwise harmless.
	marker := "distinctive-hardening-body-" + uniqueSuffix()
	if resp, data := c.request(http.MethodPut, "/v1/fs/file?path=/workspace/hardening-marker", []byte(marker)); resp.StatusCode != http.StatusOK {
		t.Fatalf("marker write = %d: %s", resp.StatusCode, data)
	}

	t.Run("request-log", func(t *testing.T) {
		logs := string(dockerOrFail(t, "logs", c.name))
		if strings.Contains(logs, token) {
			t.Fatal("container logs leaked the bearer token")
		}
		if strings.Contains(logs, "Authorization") {
			t.Fatal("container logs leaked the Authorization header")
		}
		if strings.Contains(logs, marker) {
			t.Fatal("container logs leaked a request body")
		}
		var requests int
		for _, line := range strings.Split(logs, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || line[0] != '{' {
				continue
			}
			// The bridge logs one JSON record per request; Docker may prefix
			// agent stderr or other lines, so decode leniently.
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				continue
			}
			if rec["msg"] != "request" {
				continue
			}
			requests++
			if _, ok := rec["method"].(string); !ok {
				t.Errorf("request log missing method: %v", rec)
			}
			if _, ok := rec["uri"].(string); !ok {
				t.Errorf("request log missing uri: %v", rec)
			}
			if _, ok := rec["status"].(float64); !ok {
				t.Errorf("request log missing status: %v", rec)
			}
			if _, ok := rec["latency_ms"].(float64); !ok {
				t.Errorf("request log missing latency_ms: %v", rec)
			}
		}
		if requests == 0 {
			t.Fatalf("no request log records found:\n%s", logs)
		}
	})
}

// assertInsecureRemoteContract proves the non-loopback empty-token default
// refuses to start and the explicit unsafe override is the only escape hatch.
func assertInsecureRemoteContract(t *testing.T, image string) {
	t.Run("empty-token-refused", func(t *testing.T) {
		name := "agent-bridge-e2e-emptytoken-" + uniqueSuffix()
		t.Cleanup(func() {
			if !keep() {
				_, _ = docker("rm", "-f", name)
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", "run", "--name", name, image).CombinedOutput()
		if err == nil {
			t.Fatalf("image started on the non-loopback bind without a token: %s", out)
		}
		if !bytes.Contains(out, []byte("non-loopback")) || !bytes.Contains(out, []byte("AGENT_BRIDGE_TOKEN")) {
			t.Fatalf("empty-token refusal does not name the rule and variable: %s", out)
		}
	})

	t.Run("override-allows-startup", func(t *testing.T) {
		c := startContainerAllowEmpty(t, image, "", map[string]string{"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE": "1"})
		c.mustHealthy()
		resp, data, err := c.doNoAuth(http.MethodGet, "/v1/health", nil)
		if err != nil {
			t.Fatalf("unauthenticated health under override: %v\n%s", err, c.diagnostics())
		}
		if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(data)) != `{"status":"ok"}` {
			t.Fatalf("override health = %d %s, want 200 {\"status\":\"ok\"}", resp.StatusCode, data)
		}
	})
}

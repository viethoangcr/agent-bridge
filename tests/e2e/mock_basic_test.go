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

// TestDockerMockProtocol proves the raw-byte ACP mock contract: health/token
// gating, ACP lifecycle, the failure matrix, SSE, and REST behavior. Each
// subtest owns the containers it creates.
func TestDockerMockProtocol(t *testing.T) {
	image := buildImage(t)

	t.Run("smoke-health-and-token", func(t *testing.T) { testMockSmokeHealth(t, image) })
	t.Run("smoke-missing-token-exits", func(t *testing.T) { testMockMissingToken(t, image) })
	t.Run("acp-lifecycle", func(t *testing.T) { testMockACPLifecycle(t, image) })
	t.Run("failure-matrix", func(t *testing.T) { testMockFailureMatrix(t, image) })
	t.Run("sse", func(t *testing.T) { testMockSSE(t, image) })
	t.Run("rest", func(t *testing.T) { testMockREST(t, image) })
}

// testMockSmokeHealth proves authenticated health succeeds and the same route
// without a token is an authenticated problem document.
func testMockSmokeHealth(t *testing.T, image string) {
	token := uniqueToken("smoke")
	c := startMock(t, image, token, nil)

	resp, body := c.request(http.MethodGet, "/v1/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated health = %d: %s", resp.StatusCode, body)
	}
	if strings.TrimSpace(string(body)) != `{"status":"ok"}` {
		t.Fatalf("health body = %s, want {\"status\":\"ok\"}", body)
	}

	noAuth, data := c.getNoAuth("/v1/health")
	if noAuth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated health = %d: %s, want 401", noAuth.StatusCode, data)
	}
	if ct := noAuth.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("unauthenticated health content-type = %q", ct)
	}
}

// testMockMissingToken proves the image refuses to start without a token on the
// non-loopback bind and names both the rule and the variable.
func testMockMissingToken(t *testing.T, image string) {
	name := "agent-bridge-e2e-notoken-" + uniqueSuffix()
	t.Cleanup(func() {
		if !keep() {
			_, _ = docker("rm", "-f", name)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "run", "--name", name, image).CombinedOutput()
	if err == nil {
		t.Fatalf("image started without AGENT_BRIDGE_TOKEN: %s", out)
	}
	var exitErr *exec.ExitError
	if !asExitError(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("expected a non-zero exit without a token, got %v: %s", err, out)
	}
	if !bytes.Contains(out, []byte("non-loopback")) {
		t.Fatalf("missing-token output does not name the non-loopback rule: %s", out)
	}
	if !bytes.Contains(out, []byte("AGENT_BRIDGE_TOKEN")) {
		t.Fatalf("missing-token output does not name AGENT_BRIDGE_TOKEN: %s", out)
	}
}

func asExitError(err error, target **exec.ExitError) bool {
	exitErr, ok := err.(*exec.ExitError)
	if ok {
		*target = exitErr
	}
	return ok
}

// testMockACPLifecycle covers the ordered raw-byte mock flow: agent
// requirement, initialize, session/new cwd persistence, id correlation,
// notification 202, reverse-call completion, and raw payload equality.
func testMockACPLifecycle(t *testing.T, image string) {
	c := startMock(t, image, uniqueToken("life"), nil)

	// A first POST without ?agent= is the documented 400.
	code, body := postACP(t, c, "life", "", rpc("initialize", "initialize", map[string]any{
		"protocolVersion": 1,
	}))
	if code != http.StatusBadRequest {
		t.Fatalf("first POST without agent = %d: %s, want 400", code, body)
	}
	var missingAgent problem
	if err := json.Unmarshal(body, &missingAgent); err != nil || missingAgent.Status != http.StatusBadRequest {
		t.Fatalf("missing-agent problem = %s (%v)", body, err)
	}

	// Initialize and capture the exact raw response bytes.
	initBody := rpc("initialize", "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{},
	})
	code, initData := postACP(t, c, "life", "mock", initBody)
	if code != http.StatusOK {
		t.Fatalf("initialize = %d: %s", code, initData)
	}
	initEnv := decodeEnvelope(t, initData)
	if initEnv.Error != nil || !bytes.Contains(initEnv.Result, []byte(`"protocolVersion":1`)) {
		t.Fatalf("initialize envelope = %+v", initEnv)
	}

	// session/new persists a session with a container-writable cwd.
	sessionID, gotCWD := sessionNewResult(t, c, "life", mockCWD)
	if gotCWD != mockCWD {
		t.Fatalf("session/new cwd = %q, want %q", gotCWD, mockCWD)
	}
	view, code := status(t, c, "life")
	if code != http.StatusOK || view.Status != "idle" {
		t.Fatalf("status = %d %+v, want idle", code, view)
	}
	if len(view.SessionIDs) != 1 || view.SessionIDs[0] != sessionID {
		t.Fatalf("sessionIds = %v, want [%s]", view.SessionIDs, sessionID)
	}

	// Numeric and string JSON-RPC ids correlate and are preserved exactly.
	code, data := postACP(t, c, "life", "", rpc(42, "_mock/delay", map[string]any{"ms": 0}))
	if code != http.StatusOK {
		t.Fatalf("numeric id request = %d: %s", code, data)
	}
	if got := string(decodeEnvelope(t, data).ID); got != "42" {
		t.Fatalf("numeric id echoed %q, want 42", got)
	}
	code, data = postACP(t, c, "life", "",
		[]byte(`{"jsonrpc":"2.0","id":1.0,"method":"_mock/delay","params":{"ms":0}}`))
	if code != http.StatusOK {
		t.Fatalf("float id request = %d: %s", code, data)
	}
	if got := string(decodeEnvelope(t, data).ID); got != "1.0" {
		t.Fatalf("float id echoed %q, want 1.0", got)
	}
	code, data = postACP(t, c, "life", "", rpc("str-id", "_mock/delay", map[string]any{"ms": 0}))
	if code != http.StatusOK {
		t.Fatalf("string id request = %d: %s", code, data)
	}
	if got := string(decodeEnvelope(t, data).ID); got != `"str-id"` {
		t.Fatalf("string id echoed %q, want \"str-id\"", got)
	}

	// Notifications are forwarded and acknowledged with an empty 202.
	resp, notifBody := c.request(http.MethodPost, acpPath("life", ""),
		[]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	if resp.StatusCode != http.StatusAccepted || len(notifBody) != 0 {
		t.Fatalf("notification = %d %q, want 202 empty", resp.StatusCode, notifBody)
	}

	// Reverse-call: the prompt blocks until the client answers the
	// permission request.
	done := make(chan struct {
		code int
		body []byte
	}, 1)
	go func() {
		resp, data, err := c.do(http.MethodPost, acpPath("life", ""),
			rpc("prompt", "session/prompt", map[string]any{
				"sessionId": sessionID,
				"prompt":    []map[string]any{{"type": "text", "text": "[mock:request_permission]"}},
			}))
		if err != nil {
			done <- struct {
				code int
				body []byte
			}{code: 0, body: []byte(err.Error())}
			return
		}
		done <- struct {
			code int
			body []byte
		}{code: resp.StatusCode, body: data}
	}()

	request := waitEventMethod(t, c, "life", "session/request_permission", 15*time.Second)
	perm := decodeEnvelope(t, request.Payload)
	if len(perm.ID) == 0 {
		t.Fatalf("permission request has no id: %s", request.Payload)
	}
	clientResponse := append([]byte(`{"jsonrpc":"2.0","id":`), perm.ID...)
	clientResponse = append(clientResponse, []byte(`,"result":{"outcome":{"outcome":"selected","optionId":"allow"}}}`)...)
	resp, respBody := c.request(http.MethodPost, acpPath("life", ""), clientResponse)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("client response = %d: %s", resp.StatusCode, respBody)
	}
	select {
	case got := <-done:
		if got.code != http.StatusOK {
			t.Fatalf("reverse-call prompt = %d: %s", got.code, got.body)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("prompt did not complete after the client response")
	}

	// Raw payload byte equality: the persisted response event is the exact
	// bytes returned over HTTP.
	list, code := events(t, c, "life", "limit=1000")
	if code != http.StatusOK {
		t.Fatalf("events = %d", code)
	}
	var matched bool
	for _, event := range list {
		if event.Kind != "response" {
			continue
		}
		if string(decodeEnvelope(t, event.Payload).ID) == `"initialize"` {
			matched = true
			if !bytes.Equal(event.Payload, initData) {
				t.Fatalf("persisted initialize payload = %s, want exact HTTP bytes %s", event.Payload, initData)
			}
		}
	}
	if !matched {
		t.Fatal("no persisted initialize response event found")
	}
}

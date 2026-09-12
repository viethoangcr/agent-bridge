//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// mockCWD is a path that is writable by the image's non-root runtime user.
const mockCWD = "/workspace"

// startMock starts and health-checks a mock-capable bridge container.
func startMock(t *testing.T, image, token string, env map[string]string) *container {
	t.Helper()
	c := startContainer(t, image, token, env, "")
	c.mustHealthy()
	return c
}

// sessionNewResult posts session/new and returns its sessionId and echoed cwd.
func sessionNewResult(t *testing.T, c *container, serverID, cwd string) (string, string) {
	t.Helper()
	code, data := postACP(t, c, serverID, "", rpc("session-new", "session/new", map[string]any{"cwd": cwd}))
	if code != http.StatusOK {
		t.Fatalf("session/new %s = %d: %s\n%s", serverID, code, data, c.diagnostics())
	}
	env := decodeEnvelope(t, data)
	if env.Error != nil {
		t.Fatalf("session/new returned error: %+v", env.Error)
	}
	var result struct {
		SessionID string `json:"sessionId"`
		CWD       string `json:"cwd"`
	}
	if err := json.Unmarshal(env.Result, &result); err != nil || result.SessionID == "" {
		t.Fatalf("session/new result = %s (%v)", env.Result, err)
	}
	return result.SessionID, result.CWD
}

// prompt posts session/prompt synchronously and returns the status and body.
func prompt(t *testing.T, c *container, serverID, sessionID, text string) (int, []byte) {
	t.Helper()
	return postACP(t, c, serverID, "", rpc("prompt", "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []map[string]any{{"type": "text", "text": text}},
	}))
}

// TestDockerMockProtocol is the focused raw-byte mock fixture. Subtests run in
// order and each owns the containers it creates.
func TestDockerMockProtocol(t *testing.T) {
	image := buildImage(t)

	t.Run("smoke-health-and-token", func(t *testing.T) {
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
	})

	t.Run("smoke-missing-token-exits", func(t *testing.T) {
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
	})

	t.Run("acp-lifecycle", func(t *testing.T) {
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
	})

	t.Run("failure-matrix", func(t *testing.T) {
		t.Run("duplicate-inflight-id", func(t *testing.T) {
			c := startMock(t, image, uniqueToken("dup"), nil)
			initialize(t, c, "dup", "mock")

			go func() {
				_, _, _ = c.do(http.MethodPost, acpPath("dup", ""),
					[]byte(`{"jsonrpc":"2.0","id":1,"method":"_mock/delay","params":{"ms":3000}}`))
			}()
			waitFor(t, c, 5*time.Second, "busy after first delay", func() bool {
				view, code := status(t, c, "dup")
				return code == http.StatusOK && view.Status == "busy"
			})
			code, body := postACP(t, c, "dup", "",
				[]byte(`{"jsonrpc":"2.0","id":1.0,"method":"_mock/delay","params":{"ms":3000}}`))
			if code != http.StatusConflict {
				t.Fatalf("lexically duplicate id = %d: %s, want 409", code, body)
			}
		})

		t.Run("timeout-late-response-persisted", func(t *testing.T) {
			c := startMock(t, image, uniqueToken("slow"),
				map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "300"})
			initialize(t, c, "slow", "mock")

			go func() {
				_, _, _ = c.do(http.MethodPost, acpPath("slow", ""),
					[]byte(`{"jsonrpc":"2.0","id":"delay","method":"_mock/delay","params":{"ms":900}}`))
			}()
			waitFor(t, c, 5*time.Second, "mock loop busy", func() bool {
				view, code := status(t, c, "slow")
				return code == http.StatusOK && view.Status == "busy"
			})

			code, body := postACP(t, c, "slow", "", rpc("new", "session/new", map[string]any{"cwd": "/late"}))
			if code != http.StatusGatewayTimeout {
				t.Fatalf("lifecycle timeout = %d: %s, want 504", code, body)
			}

			var lateSession string
			waitFor(t, c, 10*time.Second, "late session/new response event", func() bool {
				list, code := events(t, c, "slow", "limit=1000")
				if code != http.StatusOK {
					return false
				}
				for _, event := range list {
					if event.Kind == "response" && event.SessionID != nil && *event.SessionID != "" {
						lateSession = *event.SessionID
						return true
					}
				}
				return false
			})
			if lateSession == "" {
				t.Fatal("late response did not carry a persisted session")
			}
		})

		t.Run("invalid-stdout-synthetic-event", func(t *testing.T) {
			c := startMock(t, image, uniqueToken("badout"), nil)
			initialize(t, c, "badout", "mock")
			code, body := postACP(t, c, "badout", "",
				rpc("bad", "_mock/invalid_stdout", map[string]any{"line": "{not json"}))
			if code != http.StatusOK {
				t.Fatalf("invalid_stdout = %d: %s", code, body)
			}
			waitEventMethod(t, c, "badout", "_adapter/invalid_stdout", 10*time.Second)
		})

		t.Run("agent-exit-synthetic-and-reinitialize", func(t *testing.T) {
			c := startMock(t, image, uniqueToken("exit"), nil)
			initialize(t, c, "exit", "mock")
			code, body := postACP(t, c, "exit", "", rpc("bye", "_mock/exit", map[string]any{}))
			if code != http.StatusOK {
				t.Fatalf("_mock/exit = %d: %s", code, body)
			}
			waitEventMethod(t, c, "exit", "_adapter/agent_exited", 10*time.Second)
			waitFor(t, c, 10*time.Second, "exited status without a PID", func() bool {
				view, code := status(t, c, "exit")
				return code == http.StatusOK && view.Status == "exited" && view.PID == nil
			})
			before, code := events(t, c, "exit", "limit=1000")
			if code != http.StatusOK || len(before) == 0 {
				t.Fatalf("events before reinitialize = %d %d", code, len(before))
			}

			code, body = postACP(t, c, "exit", "", rpc("again", "_mock/delay", map[string]any{"ms": 0}))
			if code != http.StatusConflict {
				t.Fatalf("non-initialize after exit = %d: %s, want 409", code, body)
			}
			initialize(t, c, "exit", "mock")

			after, code := events(t, c, "exit", "limit=1000")
			if code != http.StatusOK || len(after) <= len(before) {
				t.Fatalf("events after reinitialize = %d, want more than %d", len(after), len(before))
			}
			if after[0].Seq != before[0].Seq {
				t.Fatalf("reinitialize dropped old events: first seq %d, want %d", after[0].Seq, before[0].Seq)
			}
		})

		t.Run("stderr-redaction", func(t *testing.T) {
			script := `printf 'password=hunter2\n' 1>&2; sleep 0.3; exit 1`
			c := startMock(t, image, uniqueToken("redact"), claudeShellEnv(script))
			assertSafeStderr(t, c, "redact", "hunter2", false)
		})

		t.Run("stderr-cap", func(t *testing.T) {
			script := `node -e 'process.stderr.write(Buffer.alloc(9000,120).toString()+"\n")' 1>&2; sleep 0.3; exit 1`
			c := startMock(t, image, uniqueToken("cap"), claudeShellEnv(script))
			assertSafeStderr(t, c, "cap", "", true)
		})
	})

	t.Run("sse", func(t *testing.T) {
		c := startMock(t, image, uniqueToken("sse"), nil)
		initialize(t, c, "sse", "mock")
		sessionID, _ := sessionNewResult(t, c, "sse", mockCWD)

		ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
		defer cancel()
		stream, err := c.subscribe(ctx, "sse", "0")
		if err != nil {
			t.Fatalf("subscribe: %v\n%s", err, c.diagnostics())
		}
		defer stream.close()

		var ids []int64
		readIDs := func(want int) {
			t.Helper()
			for len(ids) < want {
				event, err := stream.nextMessage(c)
				if err != nil {
					t.Fatalf("read sse event %d: %v\n%s", len(ids)+1, err, c.diagnostics())
				}
				if !json.Valid(event.Data) {
					t.Fatalf("sse data %d is not raw JSON: %q", event.ID, event.Data)
				}
				ids = append(ids, event.ID)
			}
		}

		// Replay from Last-Event-ID: 0 delivers the initialize response first.
		readIDs(1)

		// Live events produced after subscription must continue with no gap.
		if code, body := prompt(t, c, "sse", sessionID, "hello"); code != http.StatusOK {
			t.Fatalf("prompt = %d: %s", code, body)
		}
		readIDs(5)

		for i, id := range ids {
			if id != int64(i+1) {
				t.Fatalf("SSE ids not strict monotonic from 1: %v", ids)
			}
		}

		// Heartbeat framing is the `: heartbeat` comment line.
		comment, err := stream.waitComment(20 * time.Second)
		if err != nil {
			t.Fatalf("heartbeat: %v\n%s", err, c.diagnostics())
		}
		if !strings.Contains(comment.Text, "heartbeat") {
			t.Fatalf("comment frame = %q, want heartbeat", comment.Text)
		}

		// Reconnect and lag catch-up: events created while disconnected are
		// replayed exactly once after the prior watermark.
		stream.close()
		last := ids[len(ids)-1]
		if code, body := prompt(t, c, "sse", sessionID, "lag"); code != http.StatusOK {
			t.Fatalf("lag prompt = %d: %s", code, body)
		}
		reconnected, err := c.subscribe(ctx, "sse", strconv.FormatInt(last, 10))
		if err != nil {
			t.Fatalf("reconnect: %v", err)
		}
		defer reconnected.close()
		for i := 0; i < 3; i++ {
			event, err := reconnected.nextMessage(c)
			if err != nil {
				t.Fatalf("lag read %d: %v", i, err)
			}
			want := last + int64(i) + 1
			if event.ID != want {
				t.Fatalf("lag event id = %d, want %d", event.ID, want)
			}
		}

		// DELETE closes the live SSE stream.
		initialize(t, c, "sse-del", "mock")
		delStream, err := c.subscribe(ctx, "sse-del", "0")
		if err != nil {
			t.Fatalf("delete subscribe: %v", err)
		}
		defer delStream.close()
		if _, err := delStream.nextMessage(c); err != nil {
			t.Fatalf("delete replay: %v", err)
		}
		closed := make(chan error, 1)
		go func() {
			// Read until the hard close terminates the stream.
			for {
				event, err := delStream.next()
				if err != nil {
					closed <- err
					return
				}
				_ = event
			}
		}()
		if resp, body := c.request(http.MethodDelete, "/v1/acp/sse-del", nil); resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE = %d: %s", resp.StatusCode, body)
		}
		select {
		case err := <-closed:
			if err == nil {
				t.Fatal("SSE stream did not close on DELETE")
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("SSE stream did not close on DELETE\n%s", c.diagnostics())
		}
	})

	t.Run("rest", func(t *testing.T) {
		c := startMock(t, image, uniqueToken("rest"), nil)
		initialize(t, c, "zeta", "mock")
		initialize(t, c, "alpha", "mock")

		var list serverListView
		if code, body := getJSON(t, c, "/v1/acp", &list); code != http.StatusOK {
			t.Fatalf("list = %d: %s", code, body)
		}
		if len(list.Servers) != 2 || list.Servers[0].ServerID != "alpha" || list.Servers[1].ServerID != "zeta" {
			t.Fatalf("server list order = %+v, want alpha then zeta", list.Servers)
		}

		s1, _ := sessionNewResult(t, c, "alpha", "/s1")
		s2, _ := sessionNewResult(t, c, "alpha", "/s2")
		view, code := status(t, c, "alpha")
		if code != http.StatusOK {
			t.Fatalf("alpha status = %d", code)
		}
		if len(view.SessionIDs) != 2 || view.SessionIDs[0] != s1 || view.SessionIDs[1] != s2 {
			t.Fatalf("sessionIds = %v, want [%s %s] sorted", view.SessionIDs, s1, s2)
		}

		// Attribute events to s1 only.
		if code, body := prompt(t, c, "alpha", s1, "hello"); code != http.StatusOK {
			t.Fatalf("prompt = %d: %s", code, body)
		}
		filtered, code := events(t, c, "alpha", "sessionId="+s1)
		if code != http.StatusOK || len(filtered) != 4 {
			t.Fatalf("session-filtered events = %d, want 4", len(filtered))
		}
		for _, event := range filtered {
			if event.SessionID == nil || *event.SessionID != s1 {
				t.Fatalf("filtered event has session %v, want %s", event.SessionID, s1)
			}
		}

		// Pagination and ordering.
		one, code := events(t, c, "alpha", "limit=1")
		if code != http.StatusOK || len(one) != 1 {
			t.Fatalf("limit=1 events = %d, want 1", len(one))
		}
		after, code := events(t, c, "alpha", "after=1")
		if code != http.StatusOK {
			t.Fatalf("after=1 = %d", code)
		}
		for _, event := range after {
			if event.Seq <= 1 {
				t.Fatalf("after=1 returned seq %d", event.Seq)
			}
		}
		desc, code := events(t, c, "alpha", "order=desc")
		if code != http.StatusOK || len(desc) == 0 {
			t.Fatalf("order=desc = %d", code)
		}
		for i := 1; i < len(desc); i++ {
			if desc[i].Seq >= desc[i-1].Seq {
				t.Fatalf("desc order not strict: %v", desc)
			}
		}

		// Unknown session and unknown server are 404.
		if _, code := events(t, c, "alpha", "sessionId=nope"); code != http.StatusNotFound {
			t.Fatalf("unknown session events = %d, want 404", code)
		}
		if _, code := events(t, c, "ghost", ""); code != http.StatusNotFound {
			t.Fatalf("unknown server events = %d, want 404", code)
		}

		// DELETE prunes durable state.
		if resp, body := c.request(http.MethodDelete, "/v1/acp/alpha", nil); resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE = %d: %s", resp.StatusCode, body)
		}
		if _, code := status(t, c, "alpha"); code != http.StatusNotFound {
			t.Fatalf("status after DELETE = %d, want 404", code)
		}
		if _, code := events(t, c, "alpha", ""); code != http.StatusNotFound {
			t.Fatalf("events after DELETE = %d, want 404", code)
		}
		if code, body := getJSON(t, c, "/v1/acp", &list); code != http.StatusOK {
			t.Fatalf("list after DELETE = %d: %s", code, body)
		}
		for _, server := range list.Servers {
			if server.ServerID == "alpha" {
				t.Fatalf("alpha still listed after DELETE: %+v", list.Servers)
			}
		}
	})
}

// keep reports whether the operator asked to retain Docker state.
func keep() bool { return os.Getenv("AGENT_BRIDGE_E2E_KEEP") == "1" }

// asExitError unwraps an *exec.ExitError without importing errors in the test.
func asExitError(err error, target **exec.ExitError) bool {
	exitErr, ok := err.(*exec.ExitError)
	if ok {
		*target = exitErr
	}
	return ok
}

// claudeShellEnv points the claude agent at a shell script for deterministic
// process-failure and stderr assertions.
func claudeShellEnv(script string) map[string]string {
	args, err := json.Marshal([]string{"-c", script})
	if err != nil {
		panic(err)
	}
	return map[string]string{
		"AGENT_BRIDGE_CLAUDE_BIN":  "/bin/sh",
		"AGENT_BRIDGE_CLAUDE_ARGS": string(args),
	}
}

// assertSafeStderr posts initialize against a failing claude agent and asserts
// the capped, redacted agentStderr problem extension.
func assertSafeStderr(t *testing.T, c *container, serverID, secret string, capped bool) {
	t.Helper()
	body := rpc("initialize", "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
	resp, data := c.request(http.MethodPost, acpPath(serverID, "claude"), body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("failing agent = %d: %s, want 502\n%s", resp.StatusCode, data, c.diagnostics())
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("502 content-type = %q", ct)
	}
	var p problem
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("502 body = %s (%v)", data, err)
	}
	if p.AgentStderr == "" {
		t.Fatalf("agentStderr omitted: %s", data)
	}
	if secret != "" && strings.Contains(p.AgentStderr, secret) {
		t.Fatalf("agentStderr leaked the secret: %q", p.AgentStderr)
	}
	if !capped && !strings.Contains(p.AgentStderr, "[REDACTED]") {
		t.Fatalf("agentStderr not redacted: %q", p.AgentStderr)
	}
	if capped && len(p.AgentStderr) > 8*1024 {
		t.Fatalf("agentStderr length = %d, want <= 8KiB", len(p.AgentStderr))
	}
}

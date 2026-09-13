//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// testMockFailureMatrix covers duplicate in-flight IDs, bounded timeouts with a
// persisted late response, synthetic invalid-output and agent-exit events, and
// stderr redaction/capping.
func testMockFailureMatrix(t *testing.T, image string) {
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
}

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

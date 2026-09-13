//go:build e2e

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// assertInjectedLimits exercises ACP, process input, process output, and
// process log boundaries through small runtime-injected limits. The real
// 10MiB/512MiB/8KiB production boundaries stay in their owning unit and mock
// tests and are intentionally not repeated here.
func assertInjectedLimits(t *testing.T, image string) {
	c := startMock(t, image, uniqueToken("limits"), nil)

	const smallConfig = `{"maxConcurrentProcesses":4,"defaultRunTimeoutMs":1000,"maxRunTimeoutMs":5000,"maxOutputBytes":64,"maxLogBytesPerProcess":16384,"maxInputBytesPerRequest":16}`
	resp, data := c.request(http.MethodPost, "/v1/processes/config", []byte(smallConfig))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("install small process config = %d: %s", resp.StatusCode, data)
	}

	t.Run("process-input-decoded", func(t *testing.T) {
		startBody := []byte(`{"command":"/bin/sleep","args":["300"]}`)
		resp, data := c.request(http.MethodPost, "/v1/processes", startBody)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("managed start = %d: %s", resp.StatusCode, data)
		}
		var view processView
		if err := json.Unmarshal(data, &view); err != nil || view.ID == "" {
			t.Fatalf("managed snapshot = %s (%v)", data, err)
		}
		inputBody := []byte(`{"data":"` + strings.Repeat("A", 24) + `","encoding":"utf8"}`)
		resp, data = c.request(http.MethodPost, "/v1/processes/"+view.ID+"/input", inputBody)
		assertProblemJSON(t, resp, data, http.StatusRequestEntityTooLarge)
	})

	t.Run("process-output-truncated", func(t *testing.T) {
		payload := strings.Repeat("B", 256)
		body, err := json.Marshal(map[string]any{
			"command":   "/bin/sh",
			"args":      []string{"-c", "printf '" + payload + "'"},
			"timeoutMs": 4000,
		})
		if err != nil {
			t.Fatalf("marshal run: %v", err)
		}
		resp, data := c.request(http.MethodPost, "/v1/processes/run", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("run = %d: %s", resp.StatusCode, data)
		}
		var result struct {
			Stdout          string `json:"stdout"`
			StdoutTruncated bool   `json:"stdoutTruncated"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("decode run result %s: %v", data, err)
		}
		if !result.StdoutTruncated {
			t.Fatalf("stdout not truncated at the injected 64-byte cap: %s", data)
		}
		if len(result.Stdout) > 64 {
			t.Fatalf("stdout length = %d, want <= 64", len(result.Stdout))
		}
	})

	t.Run("process-log-cap", func(t *testing.T) {
		body, err := json.Marshal(map[string]any{
			"command": "/bin/sh",
			"args":    []string{"-c", "dd if=/dev/zero bs=1000 count=40 2>/dev/null | tr '\\0' C; sleep 0.2"},
		})
		if err != nil {
			t.Fatalf("marshal managed start: %v", err)
		}
		resp, data := c.request(http.MethodPost, "/v1/processes", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("managed start = %d: %s", resp.StatusCode, data)
		}
		var view processView
		if err := json.Unmarshal(data, &view); err != nil || view.ID == "" {
			t.Fatalf("managed snapshot = %s (%v)", data, err)
		}
		waitFor(t, c, 10*time.Second, "managed log writer exit", func() bool {
			resp, data := c.request(http.MethodGet, "/v1/processes/"+view.ID, nil)
			if resp.StatusCode != http.StatusOK {
				return false
			}
			var got processView
			return json.Unmarshal(data, &got) == nil && got.Status == "exited"
		})
		resp, data = c.request(http.MethodGet, "/v1/processes/"+view.ID+"/logs", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("logs = %d: %s", resp.StatusCode, data)
		}
		var logs struct {
			Entries []struct {
				Data string `json:"data"`
			} `json:"entries"`
		}
		if err := json.Unmarshal(data, &logs); err != nil {
			t.Fatalf("decode logs %s: %v", data, err)
		}
		if len(logs.Entries) == 0 {
			t.Fatalf("expected retained log entries under the 16KiB cap: %s", data)
		}
		total := 0
		for _, entry := range logs.Entries {
			raw, err := base64.StdEncoding.DecodeString(entry.Data)
			if err != nil {
				t.Fatalf("decode log entry: %v", err)
			}
			total += len(raw)
		}
		if total > 16384 {
			t.Fatalf("retained log bytes = %d, want <= 16384", total)
		}
	})

	t.Run("acp-timeout-late-response", func(t *testing.T) {
		slow := startMock(t, image, uniqueToken("late"), map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "300"})
		initialize(t, slow, "late", "mock")

		go func() {
			_, _, _ = slow.do(http.MethodPost, acpPath("late", ""),
				[]byte(`{"jsonrpc":"2.0","id":"delay","method":"_mock/delay","params":{"ms":900}}`))
		}()
		waitFor(t, slow, 5*time.Second, "mock busy before late response", func() bool {
			view, code := status(t, slow, "late")
			return code == http.StatusOK && view.Status == "busy"
		})
		resp, body := slow.request(http.MethodPost, acpPath("late", ""), rpc("new", "session/new", map[string]any{"cwd": "/late"}))
		assertProblemJSON(t, resp, body, http.StatusGatewayTimeout)
		waitFor(t, slow, 10*time.Second, "late persisted response", func() bool {
			list, code := events(t, slow, "late", "limit=1000")
			if code != http.StatusOK {
				return false
			}
			for _, event := range list {
				if event.Kind == "response" && event.SessionID != nil && *event.SessionID != "" {
					return true
				}
			}
			return false
		})
	})

	t.Run("redacted-stderr", func(t *testing.T) {
		script := `printf 'secret=hunter2\n' 1>&2; sleep 0.3; exit 1`
		red := startMock(t, image, uniqueToken("redacth"), claudeShellEnv(script))
		body := rpc("r", "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
		resp, data := red.request(http.MethodPost, acpPath("redacth", "claude"), body)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("redacted stderr = %d: %s", resp.StatusCode, data)
		}
		var p problem
		if err := json.Unmarshal(data, &p); err != nil {
			t.Fatalf("decode 502 problem %s: %v", data, err)
		}
		if strings.Contains(p.AgentStderr, "hunter2") {
			t.Fatal("stderr problem leaked the secret value")
		}
		if !strings.Contains(p.AgentStderr, "[REDACTED]") {
			t.Fatalf("stderr problem was not redacted: %q", p.AgentStderr)
		}
	})
}

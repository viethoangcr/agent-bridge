//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// agentCWD is the container-writable cwd shared by the keyless lifecycle
// matrix. It matches the runtime workdir owned by the non-root user.
const agentCWD = "/workspace"

// agentOperationTimeout bounds each real-agent initialize/session/prompt
// operation independently. A cold adapter start can be slow, and one hung
// binary must be identifiable from the failing operation instead of stalling
// the whole matrix.
const agentOperationTimeout = 150 * time.Second

// forbiddenCredentialEnv names host credential variables that must never be
// baked into the image or forwarded into a keyless container. AGENT_BRIDGE_TOKEN
// is the bridge's own mandatory bearer token, not a host credential.
var forbiddenCredentialEnv = []string{
	"ANTHROPIC_API_KEY",
	"CLAUDE_CODE_OAUTH_TOKEN",
	"OPENAI_API_KEY",
	"CODEX_API_KEY",
	"GEMINI_API_KEY",
	"GOOGLE_API_KEY",
	"AZURE_OPENAI_API_KEY",
	"AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY",
	"AWS_SESSION_TOKEN",
	"GITHUB_TOKEN",
	"GH_TOKEN",
	"OPENCODE_API_KEY",
}

// browserProcessNames are browser or headless-browser executables that a
// keyless headless image must never spawn.
var browserProcessNames = []string{"chrome", "chromium", "firefox", "headless_shell", "playwright"}

// TestDockerKeylessAgents exercises every pinned real agent without
// credentials and asserts only the version-specific outcomes the
// specification allows for each adapter.
func TestDockerKeylessAgents(t *testing.T) {
	image := buildImage(t)

	cases := []struct {
		name string
		run  func(*testing.T, string)
	}{
		{"claude", testClaudeKeyless},
		{"codex", testCodexKeyless},
		{"opencode", testOpenCodeKeyless},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, image) })
	}
}

// startKeyless launches one container, completes ACP initialize, and proves
// the initialize response was persisted before returning.
func startKeyless(t *testing.T, image, agent string) (*container, string) {
	t.Helper()
	c := startContainer(t, image, uniqueToken("keyless-"+agent), nil, "")
	c.mustHealthy()
	c.requestTimeout = agentOperationTimeout

	serverID := "keyless-" + agent
	env := initialize(t, c, serverID, agent)
	if env.Error != nil {
		t.Fatalf("%s initialize returned error %+v\n%s", agent, env.Error, c.diagnostics())
	}
	if len(env.Result) == 0 || !json.Valid(env.Result) {
		t.Fatalf("%s initialize returned no JSON result: %s\n%s", agent, env.Result, c.diagnostics())
	}
	waitResponseEvent(t, c, serverID, "initialize", 30*time.Second)
	return c, serverID
}

// waitResponseEvent polls durable events until the response carrying wantID is
// present.
func waitResponseEvent(t *testing.T, c *container, serverID, wantID string, timeout time.Duration) eventView {
	t.Helper()
	var found eventView
	waitFor(t, c, timeout, "persisted response "+wantID, func() bool {
		list, code := events(t, c, serverID, "limit=1000")
		if code != http.StatusOK {
			return false
		}
		for _, event := range list {
			if event.Kind != "response" {
				continue
			}
			var decoded struct {
				ID *string `json:"id"`
			}
			if err := json.Unmarshal(event.Payload, &decoded); err != nil || decoded.ID == nil {
				continue
			}
			if *decoded.ID == wantID {
				found = event
				return true
			}
		}
		return false
	})
	return found
}

// assertACPAuthRequired asserts the structural JSON-RPC auth-required envelope
// (code -32000 with a message) without accepting arbitrary failures.
func assertACPAuthRequired(t *testing.T, c *container, env rpcEnvelope, operation string) {
	t.Helper()
	if env.Error == nil {
		t.Fatalf("%s: expected a JSON-RPC auth error, got result %s\n%s", operation, env.Result, c.diagnostics())
	}
	if env.Error.Code != -32000 {
		t.Fatalf("%s: error code = %d, want -32000 (message %q)\n%s", operation, env.Error.Code, env.Error.Message, c.diagnostics())
	}
	if strings.TrimSpace(env.Error.Message) == "" {
		t.Fatalf("%s: -32000 envelope carried no message\n%s", operation, c.diagnostics())
	}
	if len(env.Result) > 0 && string(env.Result) != "null" {
		t.Fatalf("%s: -32000 error also carried a result: %s\n%s", operation, env.Result, c.diagnostics())
	}
}

// assertKeylessContainerClean proves the container exposes no host credential
// environment, no host credential bind mount, and no browser process.
func assertKeylessContainerClean(t *testing.T, c *container) {
	t.Helper()

	envJSON := dockerOrFail(t, "inspect", "--format", "{{json .Config.Env}}", c.name)
	var env []string
	if err := json.Unmarshal(envJSON, &env); err != nil {
		t.Fatalf("decode container env: %v (%s)", err, envJSON)
	}
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		for _, forbidden := range forbiddenCredentialEnv {
			if key == forbidden {
				t.Fatalf("container config exposes credential variable %s\n%s", key, envJSON)
			}
		}
	}

	mountJSON := dockerOrFail(t, "inspect", "--format", "{{json .Mounts}}", c.name)
	var mounts []struct {
		Type        string `json:"Type"`
		Destination string `json:"Destination"`
	}
	if err := json.Unmarshal(mountJSON, &mounts); err != nil {
		t.Fatalf("decode container mounts: %v (%s)", err, mountJSON)
	}
	for _, mount := range mounts {
		if mount.Type != "volume" || mount.Destination != agentCWD {
			t.Fatalf("unexpected container mount %+v; want only a volume at %s\n%s", mount, agentCWD, mountJSON)
		}
	}

	top := strings.ToLower(string(dockerOrFail(t, "top", c.name)))
	for _, browser := range browserProcessNames {
		if strings.Contains(top, browser) {
			t.Fatalf("browser process matching %q is running:\n%s", browser, top)
		}
	}
}

// sessionIDFromResult decodes the required sessionId from a session/new result.
func sessionIDFromResult(t *testing.T, c *container, env rpcEnvelope, operation string) string {
	t.Helper()
	var result struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(env.Result, &result); err != nil {
		t.Fatalf("%s: decode result %s: %v\n%s", operation, env.Result, err, c.diagnostics())
	}
	return result.SessionID
}

// testClaudeKeyless covers the Claude keyless contract.
func testClaudeKeyless(t *testing.T, image string) { testLifecycleKeyless(t, image, "claude") }

// testCodexKeyless covers the Codex keyless contract.
func testCodexKeyless(t *testing.T, image string) { testLifecycleKeyless(t, image, "codex") }

// testLifecycleKeyless asserts the shared Claude/Codex contract: initialize
// persists, session/new is success or a valid auth-required envelope, and a
// reached prompt is success or a structural -32000 auth envelope.
func testLifecycleKeyless(t *testing.T, image, agent string) {
	c, serverID := startKeyless(t, image, agent)
	assertKeylessContainerClean(t, c)

	code, data := postACP(t, c, serverID, "", rpc("sess-new", "session/new", map[string]any{
		"cwd": agentCWD, "mcpServers": []any{},
	}))
	if code != http.StatusOK {
		t.Fatalf("%s session/new HTTP %d: %s\n%s", agent, code, data, c.diagnostics())
	}
	newEnv := decodeEnvelope(t, data)
	if newEnv.Error != nil {
		assertACPAuthRequired(t, c, newEnv, agent+" session/new")
		t.Logf("%s keyless outcome: session/new auth-required (-32000); prompt not reachable", agent)
		return
	}
	sessionID := sessionIDFromResult(t, c, newEnv, agent+" session/new")
	if sessionID == "" {
		t.Fatalf("%s session/new returned empty sessionId: %s\n%s", agent, data, c.diagnostics())
	}

	code, data = postACP(t, c, serverID, "", rpc("prompt", "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []map[string]any{{"type": "text", "text": "hi"}},
	}))
	if code != http.StatusOK {
		t.Fatalf("%s session/prompt HTTP %d: %s\n%s", agent, code, data, c.diagnostics())
	}
	promptEnv := decodeEnvelope(t, data)
	if promptEnv.Error != nil {
		assertACPAuthRequired(t, c, promptEnv, agent+" session/prompt")
		t.Logf("%s keyless outcome: session/new ok (sessionId=%s), session/prompt auth-required (-32000)", agent, sessionID)
		return
	}
	if len(promptEnv.Result) == 0 {
		t.Fatalf("%s session/prompt returned neither result nor error: %s\n%s", agent, data, c.diagnostics())
	}
	t.Logf("%s keyless outcome: session/new ok (sessionId=%s), session/prompt ok (%s)", agent, sessionID, promptEnv.Result)
}

// testOpenCodeKeyless asserts OpenCode session/new succeeds keyless with a
// non-empty session ID, the first prompt is success or a structural auth
// error, and the agent stays supervised.
func testOpenCodeKeyless(t *testing.T, image string) {
	c, serverID := startKeyless(t, image, "opencode")
	assertKeylessContainerClean(t, c)

	code, data := postACP(t, c, serverID, "", rpc("sess-new", "session/new", map[string]any{
		"cwd": agentCWD, "mcpServers": []any{},
	}))
	if code != http.StatusOK {
		t.Fatalf("opencode session/new HTTP %d: %s\n%s", code, data, c.diagnostics())
	}
	newEnv := decodeEnvelope(t, data)
	if newEnv.Error != nil {
		t.Fatalf("opencode session/new must succeed keyless, got error %+v\n%s", newEnv.Error, c.diagnostics())
	}
	sessionID := sessionIDFromResult(t, c, newEnv, "opencode session/new")
	if sessionID == "" {
		t.Fatalf("opencode session/new returned empty sessionId: %s\n%s", data, c.diagnostics())
	}

	// OpenCode 1.18.18 may omit sessionId from its session/load response. The
	// bridge must retain the requested ID instead of dropping or inventing one.
	code, data = postACP(t, c, serverID, "", rpc("sess-load", "session/load", map[string]any{
		"sessionId": sessionID, "cwd": agentCWD, "mcpServers": []any{},
	}))
	if code != http.StatusOK {
		t.Fatalf("opencode session/load HTTP %d: %s\n%s", code, data, c.diagnostics())
	}
	if loadEnv := decodeEnvelope(t, data); loadEnv.Error != nil {
		assertACPAuthRequired(t, c, loadEnv, "opencode session/load")
	} else {
		view, code := status(t, c, serverID)
		if code != http.StatusOK {
			t.Fatalf("opencode status after load HTTP %d\n%s", code, c.diagnostics())
		}
		if len(view.SessionIDs) != 1 || view.SessionIDs[0] != sessionID {
			t.Fatalf("session/load dropped bridge session state: %v, want [%s]\n%s", view.SessionIDs, sessionID, c.diagnostics())
		}
		t.Logf("opencode session/load ok (raw result keeps bridge sessionId=%s)", sessionID)
	}

	code, data = postACP(t, c, serverID, "", rpc("prompt", "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []map[string]any{{"type": "text", "text": "hi"}},
	}))
	if code != http.StatusOK {
		t.Fatalf("opencode session/prompt HTTP %d: %s\n%s", code, data, c.diagnostics())
	}
	promptEnv := decodeEnvelope(t, data)
	switch {
	case promptEnv.Error != nil:
		assertACPAuthRequired(t, c, promptEnv, "opencode session/prompt")
		t.Logf("opencode keyless outcome: session/new ok (sessionId=%s), session/prompt auth-required (-32000)", sessionID)
	case len(promptEnv.Result) == 0:
		t.Fatalf("opencode session/prompt returned neither result nor error: %s\n%s", data, c.diagnostics())
	default:
		t.Logf("opencode keyless outcome: session/new ok (sessionId=%s), session/prompt ok (%s)", sessionID, promptEnv.Result)
	}

	// The agent must remain supervised after the prompt, not failed or killed.
	waitFor(t, c, 15*time.Second, "opencode supervised after prompt", func() bool {
		view, code := status(t, c, serverID)
		return code == http.StatusOK && view.Status == "idle" && view.PID != nil &&
			len(view.SessionIDs) == 1 && view.SessionIDs[0] == sessionID
	})
}

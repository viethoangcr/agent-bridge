//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"
)

// assertChildEnvContract spawns a real agent child through the ACP surface and
// proves the four bridge-only variables are stripped while a benign
// credential-shaped variable survives. The child writes only variable names so
// no value, including the benign one, can ever be printed.
func assertChildEnvContract(t *testing.T, image string) {
	const (
		benignName  = "AGENTBRIDGE_BENIGN_CREDENTIAL_TOKEN"
		benignValue = "hardening-benign-credential-value-7f3a"
	)
	script := `env | cut -d= -f1 | sort > /workspace/agent-env-keys.txt; sleep 300`
	args, err := json.Marshal([]string{"-c", script})
	if err != nil {
		t.Fatalf("marshal agent args: %v", err)
	}
	env := map[string]string{
		"AGENT_BRIDGE_PID_FILE":               "/workspace/bridge.pid",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE":  "1",
		"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "1500",
		"AGENT_BRIDGE_CLAUDE_BIN":             "/bin/sh",
		"AGENT_BRIDGE_CLAUDE_ARGS":            string(args),
		benignName:                            benignValue,
	}
	c := startMock(t, image, uniqueToken("childenv"), env)

	// The shell agent never answers ACP, so the request times out while the
	// runtime stays live; only the file it wrote before sleeping matters.
	body := rpc("child", "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
	if resp, data := c.request(http.MethodPost, acpPath("childenv", "claude"), body); resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("shell agent initialize = %d: %s, want the bounded 504", resp.StatusCode, data)
	}

	keys := waitFileKeys(t, c, "/workspace/agent-env-keys.txt")
	for _, name := range []string{
		"AGENT_BRIDGE_TOKEN",
		"AGENT_BRIDGE_PID_FILE",
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
	} {
		if keys[name] {
			t.Fatalf("agent child environment leaked %s; child keys = %v", name, sortedKeys(keys))
		}
	}
	if !keys[benignName] {
		t.Fatalf("benign credential-shaped variable %s did not survive; child keys = %v", benignName, sortedKeys(keys))
	}
}

// waitFileKeys polls for a container file that holds one variable name per
// line and returns the set. Only names are read and reported.
func waitFileKeys(t *testing.T, c *container, path string) map[string]bool {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out, err := docker("exec", c.name, "cat", path)
		if err == nil {
			keys := map[string]bool{}
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if line = strings.TrimSpace(line); line != "" {
					keys[line] = true
				}
			}
			if len(keys) > 0 {
				return keys
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("agent environment keys file %s never appeared\n%s", path, c.diagnostics())
	return nil
}

// sortedKeys returns the key names in stable order for diagnostics. Only names
// are ever printed; values are never observed.
func sortedKeys(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

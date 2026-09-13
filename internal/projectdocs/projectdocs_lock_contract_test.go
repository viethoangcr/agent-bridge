package projectdocs

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeLockContract(t *testing.T) {
	manifest := readRepoFile(t, filepath.Join("docker", "runtime", "package.json"))
	lock := readRepoFile(t, filepath.Join("docker", "runtime", "package-lock.json"))
	for _, problem := range runtimeLockProblems([]byte(manifest), []byte(lock)) {
		t.Error(problem)
	}
}

// TestRuntimeLockContractRejectsIncompleteFixture proves the contract checker
// reports violations instead of silently accepting a wrong manifest or lock.
func TestRuntimeLockContractRejectsIncompleteFixture(t *testing.T) {
	manifest := `{"dependencies":{"@agentclientprotocol/claude-agent-acp":"^0.68.0","@agentclientprotocol/codex-acp":"1.3.0","opencode-ai":"1.18.18"}}`
	lock := `{"lockfileVersion":2,"packages":{"":{"dependencies":{"@agentclientprotocol/claude-agent-acp":"^0.68.0"}},"node_modules/@agentclientprotocol/claude-agent-acp":{"version":"0.68.0"}}}`

	problems := runtimeLockProblems([]byte(manifest), []byte(lock))
	if len(problems) == 0 {
		t.Fatal("incomplete fixture unexpectedly satisfied the runtime lock contract")
	}
	report := strings.Join(problems, "\n")
	for _, want := range []string{
		`package.json must set "private": true`,
		`package.json dependency @agentclientprotocol/claude-agent-acp = "^0.68.0", want exact "0.68.0"`,
		"lockfileVersion = 2, want 3",
		"lock missing entry node_modules/@agentclientprotocol/codex-acp",
		"lock missing entry node_modules/opencode-ai",
		"lock @agentclientprotocol/claude-agent-acp integrity is empty",
		`lock @agentclientprotocol/claude-agent-acp bin is missing required command "claude-agent-acp"`,
		"lock has no optional/platform package entries",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("fixture report is missing %q\n%s", want, report)
		}
	}
	t.Logf("fixture problems detected:\n%s", report)
}

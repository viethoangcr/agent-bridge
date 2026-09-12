// Package projectdocs verifies that repository documentation and developer
// commands stay in sync with the Phase 01 contract. It contains tests only.
package projectdocs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

// contains asserts that body contains fragment, reporting every missing
// fragment instead of stopping at the first one so one run lists all gaps.
func contains(t *testing.T, name, body, fragment string) {
	t.Helper()
	if !strings.Contains(body, fragment) {
		t.Errorf("%s is missing required content %q", name, fragment)
	}
}

func TestReadmeDocumentsPhase01Contract(t *testing.T) {
	readme := readRepoFile(t, "README.md")

	for _, fragment := range []string{
		"make check",
		"make build",
		"make test",
		"make lint",
		"make vuln",
		"Go 1.26.8",
		"AGENT_BRIDGE_HOST",
		"AGENT_BRIDGE_PORT",
		"AGENT_BRIDGE_LOG_LEVEL",
		"AGENT_BRIDGE_DB",
		"AGENT_BRIDGE_TOKEN",
		"AGENT_BRIDGE_PID_FILE",
		"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS",
		"AGENT_BRIDGE_IDLE_TTL_MS",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
		"non-loopback",
		"unsafe",
		"Authorization: Bearer",
		"docs/plans/20260815-agent-bridge.md",
		"docs/references/go-project-layout.md",
		"docs/references/go-coding-standards.md",
	} {
		contains(t, "README.md", readme, fragment)
	}
}

func TestAgentsDocumentedRules(t *testing.T) {
	agents := readRepoFile(t, "AGENTS.md")

	for _, fragment := range []string{
		"docs/plans/20260815-agent-bridge.md",
		"precedence",
		"numeric order",
		"modernc.org/sqlite",
		"test-first",
		"RED",
		"injected clocks",
		"CGO_ENABLED=0",
		"RFC 9457",
		"mock",
		"runtime install",
		"out of scope",
		"gofmt",
		"go mod tidy",
		"staticcheck@2026.2.1",
		"govulncheck@v1.7.0",
		"go run",
		"docs/references/go-project-layout.md",
		"docs/references/go-coding-standards.md",
	} {
		contains(t, "AGENTS.md", agents, fragment)
	}
}

func TestMakefileProvidesPhase01Commands(t *testing.T) {
	makefile := readRepoFile(t, "Makefile")

	for _, fragment := range []string{
		"check:",
		"test:",
		"lint:",
		"build:",
		"vuln:",
		"CGO_ENABLED=0",
		"go test ./...",
		"go vet ./...",
		"staticcheck@2026.2.1",
		"govulncheck@v1.7.0",
		"go env GOVERSION",
		"go1.26.8",
		"gofmt -l .",
		"go mod tidy",
	} {
		contains(t, "Makefile", makefile, fragment)
	}
}

// pinnedRuntimeAgents maps each committed runtime npm package to the exact
// version and the command its bin entry must expose. The version strings are
// deliberate pins; ranges or aliases fail the contract.
var pinnedRuntimeAgents = map[string]struct {
	version string
	command string
}{
	"@agentclientprotocol/claude-agent-acp": {"0.68.0", "claude-agent-acp"},
	"@agentclientprotocol/codex-acp":        {"1.3.0", "codex-acp"},
	"opencode-ai":                           {"1.18.18", "opencode"},
}

// runtimeLockProblems reports every manifest/lock contract violation so one
// run lists all gaps. An empty result means the committed runtime lock is
// exact, integrity-bearing, and exposes each required command.
func runtimeLockProblems(manifest, lock []byte) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	var pkg struct {
		Private      bool              `json:"private"`
		Dependencies map[string]string `json:"dependencies"`
	}
	if err := json.Unmarshal(manifest, &pkg); err != nil {
		return []string{fmt.Sprintf("parse docker/runtime/package.json: %v", err)}
	}
	if !pkg.Private {
		add("package.json must set \"private\": true")
	}
	for name, want := range pinnedRuntimeAgents {
		if got := pkg.Dependencies[name]; got != want.version {
			add("package.json dependency %s = %q, want exact %q", name, got, want.version)
		}
	}

	var lockFile struct {
		LockfileVersion int `json:"lockfileVersion"`
		Packages        map[string]struct {
			Version      string            `json:"version"`
			Integrity    string            `json:"integrity"`
			Bin          map[string]string `json:"bin"`
			Dependencies map[string]string `json:"dependencies"`
			Optional     bool              `json:"optional"`
			OS           []string          `json:"os"`
			CPU          []string          `json:"cpu"`
		} `json:"packages"`
	}
	if err := json.Unmarshal(lock, &lockFile); err != nil {
		return append(problems, fmt.Sprintf("parse docker/runtime/package-lock.json: %v", err))
	}
	if lockFile.LockfileVersion != 3 {
		add("lockfileVersion = %d, want 3", lockFile.LockfileVersion)
	}
	root := lockFile.Packages[""]
	for name, want := range pinnedRuntimeAgents {
		if got := root.Dependencies[name]; got != want.version {
			add("lock root dependency %s = %q, want exact %q", name, got, want.version)
		}
	}
	for name, want := range pinnedRuntimeAgents {
		entry, ok := lockFile.Packages["node_modules/"+name]
		if !ok {
			add("lock missing entry node_modules/%s", name)
			continue
		}
		if entry.Version != want.version {
			add("lock %s version = %q, want %q", name, entry.Version, want.version)
		}
		if strings.TrimSpace(entry.Integrity) == "" {
			add("lock %s integrity is empty", name)
		}
		if _, ok := entry.Bin[want.command]; !ok {
			add("lock %s bin is missing required command %q", name, want.command)
		}
	}
	optional := false
	for _, entry := range lockFile.Packages {
		if entry.Optional || len(entry.OS) > 0 || len(entry.CPU) > 0 {
			optional = true
			break
		}
	}
	if !optional {
		add("lock has no optional/platform package entries")
	}
	return problems
}

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
	dir := t.TempDir()
	manifest := `{"dependencies":{"@agentclientprotocol/claude-agent-acp":"^0.68.0","@agentclientprotocol/codex-acp":"1.3.0","opencode-ai":"1.18.18"}}`
	lock := `{"lockfileVersion":2,"packages":{"":{"dependencies":{"@agentclientprotocol/claude-agent-acp":"^0.68.0"}},"node_modules/@agentclientprotocol/claude-agent-acp":{"version":"0.68.0"}}}`
	manifestPath := filepath.Join(dir, "package.json")
	lockPath := filepath.Join(dir, "package-lock.json")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write fixture manifest: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte(lock), 0o600); err != nil {
		t.Fatalf("write fixture lock: %v", err)
	}
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read fixture manifest: %v", err)
	}
	lockData, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("read fixture lock: %v", err)
	}

	problems := runtimeLockProblems(manifestData, lockData)
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

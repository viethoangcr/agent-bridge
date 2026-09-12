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

// TestReadmeDocumentsFinalPublicContract locks the final Phase 06 public
// operator documentation: build/run, every public environment variable, agent
// pins, endpoint families and limits, persistence mounts, staged shutdown, and
// the keyless/headless auth limitations. It also requires the private mock
// agent to stay out of public text and asserts the GET / docs URL is wired.
func TestReadmeDocumentsFinalPublicContract(t *testing.T) {
	readme := readRepoFile(t, "README.md")

	for _, fragment := range []string{
		"## Build",
		"## Run",
		"## Configuration",
		"## Agents",
		"## ACP",
		"## Processes",
		"## Filesystem And Project Config",
		"## Persistence",
		"## Shutdown",
		"## Testing",
		"## Limitations",
		"docker buildx build",
		"linux/amd64,linux/arm64",
		"linux/amd64",
		"linux/arm64",
		"1 GB",
		"UID 10001",
		"non-root",
		"Tini 0.19.0",
		"PID 1",
		"subreaper",
		"2468",
		"immutable",
		"make build",
		"CGO_ENABLED=0",
		"-trimpath",
		"docker run",
		"0.0.0.0",
		"AGENT_BRIDGE_HOST",
		"AGENT_BRIDGE_PORT",
		"AGENT_BRIDGE_LOG_LEVEL",
		"AGENT_BRIDGE_DB",
		"AGENT_BRIDGE_TOKEN",
		"AGENT_BRIDGE_PID_FILE",
		"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS",
		"AGENT_BRIDGE_IDLE_TTL_MS",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
		"AGENT_BRIDGE_CLAUDE_BIN",
		"AGENT_BRIDGE_CLAUDE_ARGS",
		"AGENT_BRIDGE_CODEX_BIN",
		"AGENT_BRIDGE_CODEX_ARGS",
		"AGENT_BRIDGE_OPENCODE_BIN",
		"AGENT_BRIDGE_OPENCODE_ARGS",
		"127.0.0.1",
		"600000",
		"900000",
		"JSON array",
		"Authorization: Bearer",
		"unsafe",
		"claude-agent-acp",
		"codex-acp",
		"OpenCode 1.18.18",
		"0.68.0",
		"1.3.0",
		"Node 24",
		"credentials",
		"inherits",
		"/v1/health",
		"/v1/acp/",
		"initialize",
		"session/load",
		"session/resume",
		"raw passthrough",
		"one process per server ID",
		"DELETE",
		"exited",
		"SSE",
		"Last-Event-ID",
		"-32000",
		"clientCapabilities.auth.terminal",
		"10MiB",
		"429",
		"504",
		"unbounded",
		"quota",
		"curl",
		"/v1/processes",
		"/v1/processes/run",
		"300s",
		"1MiB",
		"64KiB",
		"409",
		"/v1/fs/entries",
		"/v1/fs/file",
		"/v1/fs/upload-batch",
		"/v1/config/mcp",
		"/v1/config/skills",
		"512MiB",
		"tar.gz",
		".agent-bridge/config",
		"~/.claude",
		"~/.codex",
		"OpenCode state",
		"WAL",
		"checkpoint",
		"SIGINT",
		"SIGTERM",
		"10-second",
		"PID file",
		"exit 0",
		"go test ./...",
		"go test -tags=e2e",
		"make check",
		"go test -race",
		"keyless",
		"deferred",
		"prompt/resume",
		"https://github.com/viethoangcr/agent-bridge#readme",
	} {
		contains(t, "README.md", readme, fragment)
	}

	if strings.Contains(strings.ToLower(readme), "mock") {
		t.Errorf("README.md must not mention the private mock agent")
	}

	server := readRepoFile(t, filepath.Join("internal", "httpapi", "server.go"))
	contains(t, "internal/httpapi/server.go", server, "https://github.com/viethoangcr/agent-bridge#readme")
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

// runtimeDockerfileStages lists every build stage the runtime image contract
// requires. A missing stage means target-aware cross-compilation, binary
// inspection, agent installation, or non-root hardening silently disappears.
var runtimeDockerfileStages = []string{"bridge-build", "binary-verify", "tini", "agent-deps", "runtime"}

// runtimeTiniSha256 pins the Tini 0.19.0 static release asset checksum per
// target architecture. A mismatch breaks reproducible, verified PID 1.
var runtimeTiniSha256 = map[string]string{
	"amd64": "c5b0666b4cb676901f90dfcb37106783c5fe2077b04590973b885950611b30ee",
	"arm64": "eae1d3aa50c48fb23b8cbdf4e369d0910dfc538566bfd09df89a774aa84a48b9",
}

// runtimeDockerfileProblems reports every runtime image contract violation so
// one run lists all gaps instead of stopping at the first one.
func runtimeDockerfileProblems(dockerfile []byte) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	body := string(dockerfile)

	stages := map[string]bool{}
	froms := 0
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) == 0 || !strings.EqualFold(fields[0], "FROM") {
			continue
		}
		froms++
		if !strings.Contains(line, "@sha256:") {
			add("FROM line is not digest-pinned: %q", strings.TrimSpace(line))
		}
		for i := 0; i+1 < len(fields); i++ {
			if strings.EqualFold(fields[i], "AS") {
				stages[fields[i+1]] = true
			}
		}
	}
	if froms == 0 {
		add("Dockerfile has no FROM instructions")
	}
	for _, stage := range runtimeDockerfileStages {
		if !stages[stage] {
			add("Dockerfile is missing stage %q", stage)
		}
	}

	for _, fragment := range []string{
		"ARG TARGETOS",
		"ARG TARGETARCH",
		"CGO_ENABLED=0",
		"GOOS=$TARGETOS",
		"GOARCH=$TARGETARCH",
		"-trimpath",
		"./cmd/agent-bridge",
		"sha256sum -c",
		"tini-static-amd64",
		"tini-static-arm64",
		"USER 10001:10001",
		`ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/agent-bridge"]`,
		"HOME=/home/agentbridge",
		"AGENT_BRIDGE_HOST=0.0.0.0",
		"DISABLE_AUTOUPDATER=1",
		"NO_BROWSER=1",
		"PATH=/opt/agents/node_modules/.bin",
		"EXPOSE 2468",
		"npm ci --include=optional",
	} {
		if !strings.Contains(body, fragment) {
			add("Dockerfile is missing required content %q", fragment)
		}
	}
	for arch, sha := range runtimeTiniSha256 {
		if !strings.Contains(body, sha) {
			add("Dockerfile is missing Tini %s sha256 pin %q", arch, sha)
		}
	}

	for _, fragment := range []string{
		"HEALTHCHECK",
		"AGENT_BRIDGE_TOKEN",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
		"--ignore-scripts",
		"npm install -g",
	} {
		if strings.Contains(body, fragment) {
			add("Dockerfile must not contain %q", fragment)
		}
	}
	return problems
}

// TestRuntimeDockerfileContractRejectsIncompleteFixture proves the contract
// checker reports violations instead of silently accepting a wrong image.
func TestRuntimeDockerfileContractRejectsIncompleteFixture(t *testing.T) {
	fixture := strings.Join([]string{
		"FROM golang:1.26.8-bookworm AS bridge-build",
		"USER root",
		"HEALTHCHECK CMD true",
		"ENV AGENT_BRIDGE_TOKEN=leaked",
		"RUN npm ci --ignore-scripts",
	}, "\n")

	problems := runtimeDockerfileProblems([]byte(fixture))
	if len(problems) == 0 {
		t.Fatal("incomplete fixture unexpectedly satisfied the runtime Dockerfile contract")
	}
	report := strings.Join(problems, "\n")
	for _, want := range []string{
		`FROM line is not digest-pinned: "FROM golang:1.26.8-bookworm AS bridge-build"`,
		`Dockerfile is missing stage "binary-verify"`,
		`Dockerfile is missing required content "sha256sum -c"`,
		"Dockerfile is missing Tini amd64 sha256 pin",
		`Dockerfile must not contain "HEALTHCHECK"`,
		`Dockerfile must not contain "AGENT_BRIDGE_TOKEN"`,
		`Dockerfile must not contain "--ignore-scripts"`,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("fixture report is missing %q\n%s", want, report)
		}
	}
	t.Logf("fixture problems detected:\n%s", report)
}

// TestRuntimeDockerfileContract locks the digest-pinned, target-aware,
// non-root runtime image contract.
func TestRuntimeDockerfileContract(t *testing.T) {
	dockerfile := readRepoFile(t, filepath.Join("docker", "runtime", "Dockerfile"))
	for _, problem := range runtimeDockerfileProblems([]byte(dockerfile)) {
		t.Error(problem)
	}
}

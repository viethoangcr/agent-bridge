package projectdocs

import (
	"path/filepath"
	"strings"
	"testing"
)

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
		"go vet -tags=e2e ./...",
		"staticcheck@2026.2.1 -tags=e2e ./...",
		"govulncheck@v1.7.0 -tags=e2e ./...",
		"go env GOVERSION",
		"go1.26.8",
		"gofmt -l .",
		"go mod tidy",
	} {
		contains(t, "Makefile", makefile, fragment)
	}
}

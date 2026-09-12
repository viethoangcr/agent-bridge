// Package projectdocs verifies that repository documentation and developer
// commands stay in sync with the Phase 01 contract. It contains tests only.
package projectdocs

import (
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

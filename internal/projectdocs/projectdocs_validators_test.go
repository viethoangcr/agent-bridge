// Package projectdocs verifies that repository documentation and developer
// commands stay in sync with the Phase 01 contract. It contains tests only.
package projectdocs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

// ciTrunkGate and ciReleaseGate are the exact job conditions Phase 06 Task 6.11
// requires. The trunk gate runs checks and host-architecture Docker E2E only on
// pull requests and pushes to main; the release gate runs the non-publishing
// multiarch build on pushes to main, the weekly schedule, and manual dispatch,
// and never on pull requests.
const (
	ciTrunkGate   = "if: github.event_name == 'pull_request' || (github.event_name == 'push' && github.ref == 'refs/heads/main')"
	ciReleaseGate = "if: (github.event_name == 'push' && github.ref == 'refs/heads/main') || github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'"
)

// ciPinnedAction matches a non-local action reference pinned to a full
// 40-character lowercase hexadecimal commit SHA, optionally followed by a
// trailing comment naming the human-readable tag.
var ciPinnedAction = regexp.MustCompile(`^[^\s@]+@[0-9a-f]{40}(\s*#.*)?$`)

// ciWorkflowProblems reports every CI workflow contract violation so one run
// lists all gaps. The workflow must own fast checks and both Docker gates with
// exact triggers, least-privilege read permissions, full-SHA action pins, and
// read-only formatting drift detection.
func ciWorkflowProblems(body []byte) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return []string{"ci.yml is empty"}
	}
	workflow := string(body)

	for _, fragment := range []string{
		"on:",
		"pull_request:",
		"push:",
		"branches: [main]",
		"schedule:",
		"cron:",
		"workflow_dispatch:",
		"permissions:",
		"contents: read",
		"runs-on:",
	} {
		if !strings.Contains(workflow, fragment) {
			add("ci.yml is missing required content %q", fragment)
		}
	}
	if strings.Contains(workflow, "contents: write") || strings.Contains(workflow, "write-all") {
		add("ci.yml must use least-privilege read permissions")
	}

	if got := strings.Count(workflow, ciTrunkGate); got != 2 {
		add("ci.yml trunk-gated job condition appears %d times, want 2 (checks and docker-e2e): %q", got, ciTrunkGate)
	}
	if got := strings.Count(workflow, ciReleaseGate); got != 1 {
		add("ci.yml release-gated job condition appears %d times, want 1 (multiarch): %q", got, ciReleaseGate)
	}

	for _, fragment := range []string{
		`test "$(go env GOVERSION)" = go1.26.8`,
		`test -z "$(gofmt -l cmd internal tests)"`,
		"go vet -tags=e2e ./...",
		"staticcheck@2026.2.1 -tags=e2e ./...",
		"govulncheck@v1.7.0 -tags=e2e ./...",
		"go test ./... -count=1",
		"go test -race -skip 'TestRealHeartbeat15Seconds' ./... -count=1",
		"go mod tidy && git diff --exit-code -- go.mod go.sum",
		"CGO_ENABLED=0 go build -trimpath",
		"GOOS=linux GOARCH=arm64 go build -trimpath",
		"readelf -h",
		"AArch64",
	} {
		if !strings.Contains(workflow, fragment) {
			add("ci.yml is missing required content %q", fragment)
		}
	}
	if strings.Contains(workflow, "gofmt -w") {
		add("ci.yml must detect formatting drift without writing files")
	}

	for _, fragment := range []string{
		"actions/checkout@",
		"actions/setup-go@",
		"docker/setup-buildx-action@",
		"scripts/verify-agents.sh --image",
		"go test -tags=e2e ./tests/e2e",
		"AGENT_BRIDGE_TOKEN",
		"::add-mask::",
	} {
		if !strings.Contains(workflow, fragment) {
			add("ci.yml is missing required content %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
		"set -x",
	} {
		if strings.Contains(workflow, forbidden) {
			add("ci.yml must not contain %q", forbidden)
		}
	}

	for _, fragment := range []string{
		"docker/setup-qemu-action@",
		"--platform linux/amd64,linux/arm64",
		"--output type=oci",
		`tar -xOf /tmp/agent-bridge.oci index.json`,
		`/tmp/agent-bridge-index.json`,
		`{"amd64", "arm64"}`,
	} {
		if !strings.Contains(workflow, fragment) {
			add("ci.yml is missing required multiarch content %q", fragment)
		}
	}
	for _, forbidden := range []string{"--push", "docker/login-action", "docker buildx imagetools create"} {
		if strings.Contains(workflow, forbidden) {
			add("ci.yml multiarch job must not publish: found %q", forbidden)
		}
	}

	// Every non-local `uses:` reference must be pinned to a full 40-character
	// hexadecimal commit SHA. Tags, branches, and abbreviations are rejected.
	for _, line := range strings.Split(workflow, "\n") {
		trimmed := strings.TrimPrefix(strings.TrimSpace(line), "- ")
		if !strings.HasPrefix(trimmed, "uses:") {
			continue
		}
		ref := strings.TrimSpace(strings.TrimPrefix(trimmed, "uses:"))
		if strings.HasPrefix(ref, "./") || strings.HasPrefix(ref, "docker://") {
			continue
		}
		if !ciPinnedAction.MatchString(ref) {
			add("ci.yml action %q is not pinned to a full 40-character SHA", ref)
		}
	}
	return problems
}

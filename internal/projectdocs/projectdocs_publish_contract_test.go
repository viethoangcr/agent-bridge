package projectdocs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPublishWorkflowContract locks the gated GHCR publish contract: it must
// trigger only from the completed `ci` workflow or a manual dispatch, publish
// the multi-architecture edge image with least-privilege permissions, and never
// echo the registry token.
func TestPublishWorkflowContract(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "publish.yml"))
	if err != nil {
		t.Fatalf("read .github/workflows/publish.yml: %v", err)
	}
	for _, problem := range publishWorkflowProblems(workflow) {
		t.Error(problem)
	}
}

// TestPublishWorkflowContractRejectsWrongFixture proves the contract checker
// reports every violation instead of silently accepting a wrong workflow.
func TestPublishWorkflowContractRejectsWrongFixture(t *testing.T) {
	fixture := strings.Join([]string{
		"name: publish",
		"on:",
		"  pull_request:",
		"  workflow_dispatch:",
		"permissions: write-all",
		"jobs:",
		"  publish:",
		"    runs-on: ubuntu-latest",
		"    steps:",
		"      - uses: actions/checkout@v4",
		"      - run: echo $AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
		"      - run: set -x",
		"      - run: docker buildx build --load -f Dockerfile .",
	}, "\n")

	report := strings.Join(publishWorkflowProblems([]byte(fixture)), "\n")
	if report == "" {
		t.Fatal("deliberately wrong workflow unexpectedly satisfied the publish workflow contract")
	}
	for _, want := range []string{
		`publish.yml is missing required content "workflow_run:"`,
		`publish.yml is missing required content "workflows: [ci]"`,
		`publish.yml is missing required content "types: [completed]"`,
		`publish.yml is missing required content "branches: [main]"`,
		`publish.yml is missing required content "github.event.workflow_run.event == 'push'"`,
		`publish.yml is missing required content "github.event.workflow_run.conclusion == 'success'"`,
		`publish.yml must not contain "pull_request:"`,
		"publish.yml must use least-privilege read permissions",
		`publish.yml is missing required content "contents: read"`,
		`publish.yml is missing required content "packages: write"`,
		`publish.yml is missing required content "--platform linux/amd64,linux/arm64"`,
		`publish.yml is missing required content "--push"`,
		`publish.yml is missing required content ":main"`,
		`publish.yml is missing required content "sha-"`,
		`publish.yml is missing required content "type=gha"`,
		`publish.yml is missing required content "docker/runtime/Dockerfile"`,
		`publish.yml is missing required content "imagetools inspect"`,
		`publish.yml action "actions/checkout@v4" is not pinned to a full 40-character SHA`,
		`publish.yml must not contain "AGENT_BRIDGE_ALLOW_INSECURE_REMOTE"`,
		`publish.yml must not contain "set -x"`,
		`publish.yml must not contain "--load"`,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("fixture report is missing %q\n%s", want, report)
		}
	}
	t.Logf("fixture problems detected:\n%s", report)
}

// publishWorkflowProblems reports every publish workflow contract violation so
// one run lists all gaps. The workflow must trigger on the completed `ci`
// workflow (or manual dispatch), gate the merge path on a successful push, use
// least-privilege permissions with job-level packages: write, publish both
// architectures with a GHA cache, and verify the pushed index without ever
// echoing the token or loading the image locally.
func publishWorkflowProblems(body []byte) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return []string{"publish.yml is empty"}
	}
	workflow := string(body)

	for _, fragment := range []string{
		"workflow_run:",
		"workflows: [ci]",
		"types: [completed]",
		"branches: [main]",
		"github.event.workflow_run.event == 'push'",
		"github.event.workflow_run.conclusion == 'success'",
		"workflow_dispatch:",
		"permissions:",
		"contents: read",
		"packages: write",
		"--platform linux/amd64,linux/arm64",
		"--push",
		":main",
		"sha-",
		"type=gha",
		"docker/runtime/Dockerfile",
		"imagetools inspect",
	} {
		if !strings.Contains(workflow, fragment) {
			add("publish.yml is missing required content %q", fragment)
		}
	}
	if strings.Contains(workflow, "write-all") || strings.Contains(workflow, "contents: write") {
		add("publish.yml must use least-privilege read permissions")
	}
	for _, forbidden := range []string{
		"pull_request:",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
		"set -x",
		"--load",
	} {
		if strings.Contains(workflow, forbidden) {
			add("publish.yml must not contain %q", forbidden)
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
			add("publish.yml action %q is not pinned to a full 40-character SHA", ref)
		}
	}
	return problems
}

package projectdocs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReleaseWorkflowContract locks the tag-driven release contract: a v* tag
// builds verified static Linux binaries for both architectures, publishes a
// GitHub Release with checksums, and retags the already-published merge image
// for the tagged commit.
func TestReleaseWorkflowContract(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "release.yml"))
	if err != nil {
		t.Fatalf("read .github/workflows/release.yml: %v", err)
	}
	for _, problem := range releaseWorkflowProblems(workflow) {
		t.Error(problem)
	}
}

// TestReleaseNotesCategoriesContract locks the generated release-note
// categories: a catch-all group keeps unlabeled changes from disappearing.
func TestReleaseNotesCategoriesContract(t *testing.T) {
	notes := readRepoFile(t, filepath.Join(".github", "release.yml"))
	for _, fragment := range []string{
		"changelog:",
		"categories:",
		`labels: ["*"]`,
		"feature",
		"fix",
		"docs",
		"ci",
		"build",
	} {
		contains(t, ".github/release.yml", notes, fragment)
	}
}

// TestReleaseWorkflowContractRejectsWrongFixture proves the contract checker
// reports every violation instead of silently accepting a wrong workflow.
func TestReleaseWorkflowContractRejectsWrongFixture(t *testing.T) {
	fixture := strings.Join([]string{
		"name: release",
		"on:",
		"  push:",
		"    branches: [main]",
		"  pull_request:",
		"permissions: write-all",
		"jobs:",
		"  binaries:",
		"    runs-on: ubuntu-latest",
		"    steps:",
		"      - uses: actions/checkout@v4",
		"      - run: go build ./cmd/agent-bridge",
	}, "\n")

	report := strings.Join(releaseWorkflowProblems([]byte(fixture)), "\n")
	if report == "" {
		t.Fatal("deliberately wrong workflow unexpectedly satisfied the release workflow contract")
	}
	for _, want := range []string{
		`release.yml is missing required content "tags:"`,
		`release.yml is missing required content "\"v*\""`,
		`release.yml is missing required content "contents: read"`,
		`release.yml is missing required content "contents: write"`,
		`release.yml is missing required content "packages: write"`,
		`release.yml is missing required content "needs: binaries"`,
		`release.yml is missing required content "gh release create"`,
		`release.yml is missing required content "--generate-notes"`,
		`release.yml is missing required content "--verify-tag"`,
		`release.yml is missing required content "--prerelease"`,
		`release.yml is missing required content "CGO_ENABLED=0"`,
		`release.yml is missing required content "-trimpath"`,
		`release.yml is missing required content "GOOS=linux GOARCH=amd64"`,
		`release.yml is missing required content "GOOS=linux GOARCH=arm64"`,
		`release.yml is missing required content "readelf"`,
		`release.yml is missing required content "sha256sum"`,
		`release.yml is missing required content "SHA256SUMS"`,
		`release.yml is missing required content "imagetools create"`,
		`release.yml is missing required content ":sha-"`,
		`release.yml is missing required content ":latest"`,
		`release.yml is missing required content "--password-stdin"`,
		`release.yml is missing required content "gh workflow run publish.yml"`,
		"release.yml must use least-privilege permissions",
		`release.yml must not contain "pull_request:"`,
		`release.yml action "actions/checkout@v4" is not pinned to a full 40-character SHA`,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("fixture report is missing %q\n%s", want, report)
		}
	}
	t.Logf("fixture problems detected:\n%s", report)
}

// releaseWorkflowProblems reports every release workflow contract violation so
// one run lists all gaps. The workflow must trigger on v* tags, build verified
// static Linux binaries for amd64 and arm64, publish a GitHub Release with
// SHA256SUMS (prerelease for -rc tags), and retag the tagged commit's existing
// :sha- image index, never moving :latest for a prerelease.
func releaseWorkflowProblems(body []byte) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return []string{"release.yml is empty"}
	}
	workflow := string(body)

	for _, fragment := range []string{
		"push:",
		"tags:",
		`"v*"`,
		"permissions:",
		"contents: read",
		"contents: write",
		"packages: write",
		"needs: binaries",
		"gh release create",
		"--generate-notes",
		"--verify-tag",
		"--prerelease",
		"CGO_ENABLED=0",
		"-trimpath",
		"GOOS=linux GOARCH=amd64",
		"GOOS=linux GOARCH=arm64",
		"readelf",
		"sha256sum",
		"SHA256SUMS",
		"imagetools create",
		":sha-",
		":latest",
		"--password-stdin",
		"gh workflow run publish.yml",
	} {
		if !strings.Contains(workflow, fragment) {
			add("release.yml is missing required content %q", fragment)
		}
	}
	if strings.Contains(workflow, "write-all") {
		add("release.yml must use least-privilege permissions")
	}
	if strings.Contains(workflow, "pull_request:") {
		add(`release.yml must not contain "pull_request:"`)
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
			add("release.yml action %q is not pinned to a full 40-character SHA", ref)
		}
	}
	return problems
}

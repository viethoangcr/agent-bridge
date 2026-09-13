package projectdocs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCIWorkflowContract locks the Phase 06 Task 6.11 CI contract: exact
// triggers, least-privilege permissions, exact job gating, read-only formatting
// drift detection, pinned checks and Docker gates, and full-SHA action pins.
func TestCIWorkflowContract(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read .github/workflows/ci.yml: %v", err)
	}
	for _, problem := range ciWorkflowProblems(workflow) {
		t.Error(problem)
	}
}

// TestCIWorkflowContractRejectsWrongFixture proves the contract checker reports
// violations instead of silently accepting a wrong workflow.
func TestCIWorkflowContractRejectsWrongFixture(t *testing.T) {
	fixture := strings.Join([]string{
		"name: ci",
		"on:",
		"  pull_request:",
		"permissions: write-all",
		"jobs:",
		"  checks:",
		"    runs-on: ubuntu-latest",
		"    steps:",
		"      - uses: actions/checkout@v4",
		"      - run: gofmt -w cmd internal tests",
		"      - run: go run honnef.co/go/tools/cmd/staticcheck@latest ./...",
		"      - run: docker buildx build --platform linux/amd64,linux/arm64 --push -f docker/runtime/Dockerfile .",
	}, "\n")

	report := strings.Join(ciWorkflowProblems([]byte(fixture)), "\n")
	if report == "" {
		t.Fatal("deliberately wrong workflow unexpectedly satisfied the CI workflow contract")
	}
	for _, want := range []string{
		`ci.yml is missing required content "schedule:"`,
		"ci.yml must use least-privilege read permissions",
		"ci.yml trunk-gated job condition appears 0 times, want 2",
		"ci.yml release-gated job condition appears 0 times, want 1",
		`ci.yml is missing required content "go mod tidy && git diff --exit-code -- go.mod go.sum"`,
		"ci.yml must detect formatting drift without writing files",
		"ci.yml is missing required multiarch content",
		"ci.yml multiarch job must not publish",
		`ci.yml action "actions/checkout@v4" is not pinned to a full 40-character SHA`,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("fixture report is missing %q\n%s", want, report)
		}
	}
	t.Logf("fixture problems detected:\n%s", report)
}

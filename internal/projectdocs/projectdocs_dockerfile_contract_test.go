package projectdocs

import (
	"path/filepath"
	"strings"
	"testing"
)

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

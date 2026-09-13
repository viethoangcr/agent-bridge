package projectdocs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPRWorkflowContract locks the pull-request hygiene contract: a
// Conventional Commits title gate triggered by pull-request events, with
// least-privilege permissions and any external action pinned to a full SHA.
func TestPRWorkflowContract(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "pr.yml"))
	if err != nil {
		t.Fatalf("read .github/workflows/pr.yml: %v", err)
	}
	body := string(workflow)

	for _, fragment := range []string{
		"on:",
		"pull_request:",
		"types: [opened, edited, reopened, synchronize]",
		"permissions:",
		"contents: read",
		"Conventional Commits",
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("pr.yml is missing required content %q", fragment)
		}
	}
	if strings.Contains(body, "contents: write") || strings.Contains(body, "write-all") {
		t.Error("pr.yml must use least-privilege read permissions")
	}

	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimPrefix(strings.TrimSpace(line), "- ")
		if !strings.HasPrefix(trimmed, "uses:") {
			continue
		}
		ref := strings.TrimSpace(strings.TrimPrefix(trimmed, "uses:"))
		if strings.HasPrefix(ref, "./") || strings.HasPrefix(ref, "docker://") {
			continue
		}
		if !ciPinnedAction.MatchString(ref) {
			t.Errorf("pr.yml action %q is not pinned to a full 40-character SHA", ref)
		}
	}
}

// TestPullRequestTemplateAndClaudeDocs locks the human-facing PR conventions:
// the template carries the required sections, and CLAUDE.md imports AGENTS.md.
func TestPullRequestTemplateAndClaudeDocs(t *testing.T) {
	template := readRepoFile(t, filepath.Join(".github", "pull_request_template.md"))
	for _, section := range []string{
		"Conventional Commits",
		"## Summary",
		"## Context",
		"## Testing",
		"## Hygiene",
		"## Risk / rollback",
	} {
		if !strings.Contains(template, section) {
			t.Errorf("pull_request_template.md is missing required section %q", section)
		}
	}

	claude := strings.TrimSpace(readRepoFile(t, "CLAUDE.md"))
	if claude != "@AGENTS.md" {
		t.Errorf("CLAUDE.md = %q, want %q", claude, "@AGENTS.md")
	}
}

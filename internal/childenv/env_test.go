package childenv_test

import (
	"reflect"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/childenv"
)

func TestSanitized(t *testing.T) {
	tests := []struct {
		name    string
		environ []string
		want    []string
	}{
		{
			name: "removes every bridge-only variable including duplicates and empty values",
			environ: []string{
				"PATH=/usr/bin",
				"AGENT_BRIDGE_TOKEN=secret",
				"HOME=/root",
				"AGENT_BRIDGE_PID_FILE=",
				"AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1",
				"NO_EQUALS_ENTRY",
				"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1",
				"AGENT_BRIDGE_TOKEN=second-secret",
				"FOO=bar=baz=qux",
				"AGENT_BRIDGE_TOKENX=not-bridge-only",
			},
			want: []string{
				"PATH=/usr/bin",
				"HOME=/root",
				"NO_EQUALS_ENTRY",
				"FOO=bar=baz=qux",
				"AGENT_BRIDGE_TOKENX=not-bridge-only",
			},
		},
		{
			name: "preserves credential-shaped and unrelated variables in order",
			environ: []string{
				"AGENT_BRIDGE_TOKEN=removed",
				"AWS_SECRET_ACCESS_KEY=aws-secret",
				"GITHUB_TOKEN=ghp_abc123",
				"OPENAI_API_KEY=sk-xyz=tail",
				"ANTHROPIC_API_KEY=sk-ant-123",
				"DATABASE_URL=postgres://user:pass@host/db",
				"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=0",
			},
			want: []string{
				"AWS_SECRET_ACCESS_KEY=aws-secret",
				"GITHUB_TOKEN=ghp_abc123",
				"OPENAI_API_KEY=sk-xyz=tail",
				"ANTHROPIC_API_KEY=sk-ant-123",
				"DATABASE_URL=postgres://user:pass@host/db",
			},
		},
		{
			name:    "empty environment",
			environ: []string{},
			want:    []string{},
		},
		{
			name:    "nil environment",
			environ: nil,
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := childenv.Sanitized(tt.environ)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Sanitized(%q) = %q, want %q", tt.environ, got, tt.want)
			}
		})
	}
}

func TestSanitizedDoesNotAliasInput(t *testing.T) {
	t.Run("mutating output does not affect input", func(t *testing.T) {
		input := []string{"PATH=/usr/bin", "HOME=/root", "AGENT_BRIDGE_TOKEN=secret"}
		out := childenv.Sanitized(input)
		for i := range out {
			out[i] = "mutated"
		}
		want := []string{"PATH=/usr/bin", "HOME=/root", "AGENT_BRIDGE_TOKEN=secret"}
		if !reflect.DeepEqual(input, want) {
			t.Fatalf("input mutated by output write: got %q, want %q", input, want)
		}
	})

	t.Run("mutating input does not affect output", func(t *testing.T) {
		input := []string{"PATH=/usr/bin", "HOME=/root", "AGENT_BRIDGE_TOKEN=secret"}
		out := childenv.Sanitized(input)
		for i := range input {
			input[i] = "mutated"
		}
		want := []string{"PATH=/usr/bin", "HOME=/root"}
		if !reflect.DeepEqual(out, want) {
			t.Fatalf("output aliases input backing array: got %q, want %q", out, want)
		}
	})

	t.Run("copies even when nothing is removed", func(t *testing.T) {
		input := []string{"PATH=/usr/bin", "HOME=/root"}
		out := childenv.Sanitized(input)
		if len(out) != len(input) {
			t.Fatalf("len(out) = %d, want %d", len(out), len(input))
		}
		for i := range out {
			out[i] = "mutated"
		}
		want := []string{"PATH=/usr/bin", "HOME=/root"}
		if !reflect.DeepEqual(input, want) {
			t.Fatalf("input mutated by output write: got %q, want %q", input, want)
		}
	})
}

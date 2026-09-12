package acpruntime_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/config"
)

func defaultCommands() map[string]config.AgentCommand {
	return map[string]config.AgentCommand{
		"claude":   {Binary: "claude-agent-acp"},
		"codex":    {Binary: "codex-acp"},
		"opencode": {Binary: "opencode", Args: []string{"acp"}},
	}
}

// sanitizedEnviron is a representative bridge environment containing every
// bridge-only variable, duplicates, values containing '=', and credentials.
func sanitizedEnviron() []string {
	return []string{
		"PATH=/usr/bin",
		"AGENT_BRIDGE_TOKEN=secret",
		"HOME=/home/tester",
		"AGENT_BRIDGE_PID_FILE=/tmp/bridge.pid",
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT=0",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1",
		"ANTHROPIC_API_KEY=sk-ant-secret",
		"FOO=bar=baz",
		"AGENT_BRIDGE_TOKEN=second-secret",
		"NO_EQUALS_ENTRY",
	}
}

// countKey counts entries whose key (text before the first '=') equals want.
func countKey(environ []string, want string) int {
	count := 0
	for _, entry := range environ {
		key, _, _ := strings.Cut(entry, "=")
		if key == want {
			count++
		}
	}
	return count
}

func TestResolverDefaults(t *testing.T) {
	var lookedUp []string
	lookPath := func(name string) (string, error) {
		lookedUp = append(lookedUp, name)
		return "/usr/local/bin/" + name, nil
	}

	tests := []struct {
		agent       string
		wantProgram string
		wantArgs    []string
		wantLookup  string
	}{
		{"claude", "/usr/local/bin/claude-agent-acp", nil, "claude-agent-acp"},
		{"codex", "/usr/local/bin/codex-acp", nil, "codex-acp"},
		{"opencode", "/usr/local/bin/opencode", []string{"acp"}, "opencode"},
	}

	for _, tt := range tests {
		t.Run(tt.agent, func(t *testing.T) {
			lookedUp = nil
			r := acpruntime.Resolver{Commands: defaultCommands(), LookPath: lookPath}
			spec, err := r.Resolve(tt.agent)
			if err != nil {
				t.Fatalf("Resolve(%q) error = %v, want nil", tt.agent, err)
			}
			if spec.Program != tt.wantProgram {
				t.Errorf("Resolve(%q).Program = %q, want %q", tt.agent, spec.Program, tt.wantProgram)
			}
			if !reflect.DeepEqual(spec.Args, tt.wantArgs) {
				t.Errorf("Resolve(%q).Args = %q, want %q", tt.agent, spec.Args, tt.wantArgs)
			}
			if !reflect.DeepEqual(lookedUp, []string{tt.wantLookup}) {
				t.Errorf("LookPath calls = %q, want [%q]", lookedUp, tt.wantLookup)
			}
		})
	}
}

func TestResolverExplicitBinaryOverridesLookPath(t *testing.T) {
	commands := defaultCommands()
	commands["claude"] = config.AgentCommand{Binary: "/opt/agents/claude-acp"}

	lookPathCalled := false
	r := acpruntime.Resolver{
		Commands: commands,
		LookPath: func(string) (string, error) {
			lookPathCalled = true
			return "/should/not/be/used", nil
		},
	}

	spec, err := r.Resolve("claude")
	if err != nil {
		t.Fatalf("Resolve(claude) error = %v, want nil", err)
	}
	if spec.Program != "/opt/agents/claude-acp" {
		t.Errorf("Program = %q, want explicit override %q", spec.Program, "/opt/agents/claude-acp")
	}
	if lookPathCalled {
		t.Error("LookPath was called for an explicit binary override")
	}
}

func TestResolverBareNameOverrideUsesLookPath(t *testing.T) {
	commands := defaultCommands()
	commands["codex"] = config.AgentCommand{Binary: "custom-codex"}

	var gotName string
	r := acpruntime.Resolver{
		Commands: commands,
		LookPath: func(name string) (string, error) {
			gotName = name
			return "/custom/bin/custom-codex", nil
		},
	}

	spec, err := r.Resolve("codex")
	if err != nil {
		t.Fatalf("Resolve(codex) error = %v, want nil", err)
	}
	if gotName != "custom-codex" {
		t.Errorf("LookPath name = %q, want %q", gotName, "custom-codex")
	}
	if spec.Program != "/custom/bin/custom-codex" {
		t.Errorf("Program = %q, want %q", spec.Program, "/custom/bin/custom-codex")
	}
}

func TestResolverCopiesCommandArgs(t *testing.T) {
	commands := defaultCommands()
	commands["opencode"] = config.AgentCommand{Binary: "opencode", Args: []string{"acp", "--verbose"}}

	r := acpruntime.Resolver{
		Commands: commands,
		LookPath: func(name string) (string, error) { return "/usr/bin/" + name, nil },
	}

	spec, err := r.Resolve("opencode")
	if err != nil {
		t.Fatalf("Resolve(opencode) error = %v, want nil", err)
	}
	if len(spec.Args) != 2 {
		t.Fatalf("Args = %q, want 2 entries", spec.Args)
	}

	spec.Args[0] = "mutated-output"
	if commands["opencode"].Args[0] != "acp" {
		t.Errorf("Commands slice mutated through result: got %q, want %q", commands["opencode"].Args[0], "acp")
	}

	commands["opencode"].Args[1] = "mutated-source"
	if spec.Args[1] != "--verbose" {
		t.Errorf("result aliases Commands slice: got %q, want %q", spec.Args[1], "--verbose")
	}
}

func TestResolverUnknownAgentNamesAgentAndBinary(t *testing.T) {
	r := acpruntime.Resolver{
		Commands: defaultCommands(),
		LookPath: func(string) (string, error) { return "", errors.New("unused") },
	}

	_, err := r.Resolve("gemini")
	if err == nil {
		t.Fatal("Resolve(gemini) error = nil, want unknown-agent error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "gemini") {
		t.Errorf("error %q does not name the agent", msg)
	}
	if !strings.Contains(msg, "binary") {
		t.Errorf("error %q does not name the binary", msg)
	}
}

func TestResolverLookPathFailureNamesAgentAndBinary(t *testing.T) {
	lookErr := errors.New("executable file not found")
	r := acpruntime.Resolver{
		Commands: defaultCommands(),
		LookPath: func(string) (string, error) { return "", lookErr },
	}

	_, err := r.Resolve("codex")
	if err == nil {
		t.Fatal("Resolve(codex) error = nil, want resolution error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "codex") {
		t.Errorf("error %q does not name the agent", msg)
	}
	if !strings.Contains(msg, "codex-acp") {
		t.Errorf("error %q does not name the binary", msg)
	}
	if !errors.Is(err, lookErr) {
		t.Errorf("error %v does not wrap the LookPath failure", err)
	}
}

func TestResolverMockLaunchesCurrentBinaryWithControlledEnvironment(t *testing.T) {
	r := acpruntime.Resolver{Executable: "/opt/agent-bridge", Environ: sanitizedEnviron()}

	spec, err := r.Resolve("mock")
	if err != nil {
		t.Fatalf("Resolve(mock) error = %v, want nil", err)
	}
	if spec.Program != "/opt/agent-bridge" {
		t.Errorf("Program = %q, want %q", spec.Program, "/opt/agent-bridge")
	}
	if len(spec.Args) != 0 {
		t.Errorf("Args = %q, want no public arguments or subcommand", spec.Args)
	}

	want := []string{
		"PATH=/usr/bin",
		"HOME=/home/tester",
		"ANTHROPIC_API_KEY=sk-ant-secret",
		"FOO=bar=baz",
		"NO_EQUALS_ENTRY",
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1",
	}
	if !reflect.DeepEqual(spec.Env, want) {
		t.Fatalf("Env = %q, want %q", spec.Env, want)
	}

	if got := countKey(spec.Env, "AGENT_BRIDGE_INTERNAL_MOCK_AGENT"); got != 1 {
		t.Errorf("AGENT_BRIDGE_INTERNAL_MOCK_AGENT occurrences = %d, want exactly 1", got)
	}
	for _, key := range []string{"AGENT_BRIDGE_TOKEN", "AGENT_BRIDGE_PID_FILE", "AGENT_BRIDGE_ALLOW_INSECURE_REMOTE"} {
		if got := countKey(spec.Env, key); got != 0 {
			t.Errorf("%s occurrences = %d, want 0", key, got)
		}
	}
}

func TestResolverNonMockEnvironmentIsSanitized(t *testing.T) {
	r := acpruntime.Resolver{
		Commands: defaultCommands(),
		Environ:  sanitizedEnviron(),
		LookPath: func(name string) (string, error) { return "/usr/bin/" + name, nil },
	}

	spec, err := r.Resolve("claude")
	if err != nil {
		t.Fatalf("Resolve(claude) error = %v, want nil", err)
	}

	want := []string{
		"PATH=/usr/bin",
		"HOME=/home/tester",
		"ANTHROPIC_API_KEY=sk-ant-secret",
		"FOO=bar=baz",
		"NO_EQUALS_ENTRY",
	}
	if !reflect.DeepEqual(spec.Env, want) {
		t.Fatalf("Env = %q, want %q", spec.Env, want)
	}

	if got := countKey(spec.Env, "AGENT_BRIDGE_INTERNAL_MOCK_AGENT"); got != 0 {
		t.Errorf("mock variable added to non-mock agent: %d occurrences, want 0", got)
	}
	for _, key := range []string{"AGENT_BRIDGE_TOKEN", "AGENT_BRIDGE_PID_FILE", "AGENT_BRIDGE_ALLOW_INSECURE_REMOTE"} {
		if got := countKey(spec.Env, key); got != 0 {
			t.Errorf("%s occurrences = %d, want 0", key, got)
		}
	}
	if countKey(spec.Env, "ANTHROPIC_API_KEY") != 1 {
		t.Error("credentials were not preserved for the agent child")
	}
}

func TestResolverNilLookPathUsesExecLookPath(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-agent-acp")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	if err := os.Chmod(bin, 0o755); err != nil {
		t.Fatalf("chmod fake binary: %v", err)
	}
	t.Setenv("PATH", dir)

	r := acpruntime.Resolver{
		Commands: map[string]config.AgentCommand{"claude": {Binary: "fake-agent-acp"}},
	}

	spec, err := r.Resolve("claude")
	if err != nil {
		t.Fatalf("Resolve(claude) error = %v, want nil", err)
	}
	if spec.Program != bin {
		t.Errorf("Program = %q, want %q", spec.Program, bin)
	}
}

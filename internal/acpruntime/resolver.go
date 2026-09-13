// Package acpruntime launches and manages private stdio ACP agent
// subprocesses. It resolves configured agents into launch specifications,
// owns independent process groups, and correlates JSON-RPC traffic.
package acpruntime

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/viethoangcr/agent-bridge/internal/childenv"
	"github.com/viethoangcr/agent-bridge/internal/config"
)

// mockAgent is the private agent identifier that re-execs the current bridge
// binary in its internal JSONL mock mode.
const mockAgent = "mock"

// mockEnvVar is the private dispatch variable added only for the mock agent.
const mockEnvVar = "AGENT_BRIDGE_INTERNAL_MOCK_AGENT"

// LaunchSpec is the fully resolved program, argument vector, and environment
// for one agent subprocess.
type LaunchSpec struct {
	// Program is the executable path or bare name to launch.
	Program string
	// Args is the argument vector after the program; Start passes it to the
	// child unchanged.
	Args []string
	// Env is the fully sanitized child environment.
	Env []string
}

// Resolver turns a configured agent identifier into a LaunchSpec.
type Resolver struct {
	// Executable is the running bridge binary's path, used to re-exec the
	// private mock agent.
	Executable string
	// Commands maps configured agent names to their command definitions.
	Commands map[string]config.AgentCommand
	// Environ is the bridge environment captured at startup. Resolve sanitizes
	// it through childenv.Sanitized for every child; only the private mock
	// agent additionally receives the controlled
	// AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1 entry.
	Environ []string
	// LookPath resolves a bare binary name; a nil value defaults to exec.LookPath.
	LookPath func(string) (string, error)
}

// Resolve returns the launch specification for agent. Known agents use their
// configured binary: an explicit path is used as-is (the override wins over
// LookPath), while a bare name is resolved through LookPath. Unknown agents
// return an error naming both the agent and the binary. The mock agent runs
// the current executable with no public arguments.
func (r Resolver) Resolve(agent string) (LaunchSpec, error) {
	if agent == mockAgent {
		env := childenv.Sanitized(r.Environ)
		env = append(env, mockEnvVar+"=1")
		return LaunchSpec{Program: r.Executable, Env: env}, nil
	}

	cmd, ok := r.Commands[agent]
	if !ok {
		return LaunchSpec{}, fmt.Errorf("unknown agent %q (binary %q)", agent, agent)
	}

	program := cmd.Binary
	if strings.ContainsRune(program, os.PathSeparator) {
		return LaunchSpec{
			Program: program,
			Args:    append([]string(nil), cmd.Args...),
			Env:     childenv.Sanitized(r.Environ),
		}, nil
	}

	lookPath := r.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	resolved, err := lookPath(program)
	if err != nil {
		return LaunchSpec{}, fmt.Errorf("resolve agent %q binary %q: %w", agent, program, err)
	}

	return LaunchSpec{
		Program: resolved,
		Args:    append([]string(nil), cmd.Args...),
		Env:     childenv.Sanitized(r.Environ),
	}, nil
}

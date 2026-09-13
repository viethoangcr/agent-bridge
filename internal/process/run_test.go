package process

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func ptrI64(v int64) *int64 { return &v }

func newRunManager(t *testing.T, peakBudget, maxConcurrent int) *Manager {
	t.Helper()
	m, err := newManagerWithBudgets(nil, t.TempDir(), managerBudgets{
		retainedLogMemoryBytes: 1 << 20,
		activeRunPeakBytes:     peakBudget,
	})
	if err != nil {
		t.Fatalf("newManagerWithBudgets: %v", err)
	}
	cfg := DefaultConfig()
	cfg.MaxConcurrentProcesses = maxConcurrent
	if err := m.config.update(cfg); err != nil {
		t.Fatalf("install test config: %v", err)
	}
	t.Cleanup(func() { cleanupManager(t, m) })
	return m
}

func setRunConfig(t *testing.T, m *Manager, mutate func(*Config)) {
	t.Helper()
	cfg := m.config.load()
	mutate(&cfg)
	if err := m.config.update(cfg); err != nil {
		t.Fatalf("update config: %v", err)
	}
}

func activeRunPeak(m *Manager) int {
	m.reserveMu.Lock()
	defer m.reserveMu.Unlock()
	return m.activeRunPeak
}

func waitForActiveProcesses(t *testing.T, m *Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if activeProcesses(m) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("activeProcesses never reached %d (now %d)", want, activeProcesses(m))
}

func runEnvExit(code string) map[string]string {
	env := helperEnv(helperExitCode)
	env[processHelperExitEnv] = code
	return env
}

func TestManager_Run_SuccessAndNonZeroExit(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)

	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: runEnvExit("0")})
	if err != nil {
		t.Fatalf("Run(success): %v", err)
	}
	if res.TimedOut {
		t.Fatal("TimedOut = true, want false")
	}
	if res.ExitCode == nil || *res.ExitCode != 0 {
		t.Fatalf("exitCode = %v, want 0", res.ExitCode)
	}
	if res.DurationMs < 0 {
		t.Fatalf("durationMs = %d, want >= 0", res.DurationMs)
	}

	res, err = m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: runEnvExit("3")})
	if err != nil {
		t.Fatalf("Run(non-zero): %v", err)
	}
	if res.TimedOut {
		t.Fatal("TimedOut = true for a normal exit, want false")
	}
	if res.ExitCode == nil || *res.ExitCode != 3 {
		t.Fatalf("exitCode = %v, want 3", res.ExitCode)
	}

	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after runs, want 0", got)
	}
	if got := activeRunPeak(m); got != 0 {
		t.Fatalf("activeRunPeak = %d after runs, want 0", got)
	}
}

func TestManager_Run_EffectiveCwdAndEnv(t *testing.T) {
	requireLinuxProcess(t)

	reportPath := filepath.Join(t.TempDir(), "report.json")
	overrideCwd := t.TempDir()
	defaultCwd := t.TempDir()
	m, err := newManagerWithBudgets(nil, defaultCwd, managerBudgets{
		retainedLogMemoryBytes: 1 << 20,
		activeRunPeakBytes:     20 * MaxOutputBytes,
	})
	if err != nil {
		t.Fatalf("newManagerWithBudgets: %v", err)
	}
	t.Cleanup(func() { cleanupManager(t, m) })

	env := helperEnv(helperEnvCwd)
	env[processHelperReportEnv] = reportPath
	env["KEEP"] = "override"
	if _, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: env, Cwd: overrideCwd}); err != nil {
		t.Fatalf("Run(env-cwd): %v", err)
	}
	report := readHelperReport(t, reportPath)
	if report.Cwd != overrideCwd {
		t.Fatalf("cwd = %q, want override %q", report.Cwd, overrideCwd)
	}
	if got := envMap(report.Env)["KEEP"]; got != "override" {
		t.Fatalf("KEEP = %q, want override", got)
	}

	env = helperEnv(helperEnvCwd)
	env[processHelperReportEnv] = reportPath
	if _, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: env}); err != nil {
		t.Fatalf("Run(default cwd): %v", err)
	}
	if report = readHelperReport(t, reportPath); report.Cwd != defaultCwd {
		t.Fatalf("default cwd = %q, want %q", report.Cwd, defaultCwd)
	}
}

func TestManager_Run_TimeoutKillsGroup(t *testing.T) {
	requireLinuxProcess(t)

	pidPath := filepath.Join(t.TempDir(), "pids.json")
	t.Cleanup(func() {
		if pids, ok := tryReadHelperPIDs(pidPath); ok {
			killHelperPIDs(pids)
		}
	})
	env := helperEnv(helperChild)
	env[processHelperPIDEnv] = pidPath

	m := newRunManager(t, 20*MaxOutputBytes, 4)
	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: env, TimeoutMs: ptrI64(150)})
	if err != nil {
		t.Fatalf("Run(timeout): %v", err)
	}
	if !res.TimedOut {
		t.Fatal("TimedOut = false, want true")
	}
	if res.ExitCode != nil {
		t.Fatalf("exitCode = %v, want omitted when killed", *res.ExitCode)
	}
	if res.DurationMs < 150 {
		t.Fatalf("durationMs = %d, want >= requested 150", res.DurationMs)
	}

	pids := readHelperPIDs(t, pidPath)
	waitGoneOrZombie(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)
	if got := activeRunPeak(m); got != 0 {
		t.Fatalf("activeRunPeak = %d after timeout, want 0", got)
	}
	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after timeout, want 0", got)
	}
}

func TestManager_Run_DefaultAndExactTimeout(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)
	setRunConfig(t, m, func(c *Config) {
		c.DefaultRunTimeoutMs = 120
		c.MaxRunTimeoutMs = 120
	})

	// An omitted timeout uses the active default.
	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	if err != nil {
		t.Fatalf("Run(default timeout): %v", err)
	}
	if !res.TimedOut || res.DurationMs < 120 {
		t.Fatalf("default timeout: timedOut=%v duration=%d, want timed out at >=120ms", res.TimedOut, res.DurationMs)
	}

	// A requested timeout equal to the active max is accepted.
	res, err = m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperBlocking), TimeoutMs: ptrI64(120)})
	if err != nil {
		t.Fatalf("Run(exact timeout): %v", err)
	}
	if !res.TimedOut || res.DurationMs < 120 {
		t.Fatalf("exact timeout: timedOut=%v duration=%d, want timed out at >=120ms", res.TimedOut, res.DurationMs)
	}
}

func TestManager_Run_Validation(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)
	setRunConfig(t, m, func(c *Config) {
		c.DefaultRunTimeoutMs = 100
		c.MaxRunTimeoutMs = 100
		c.MaxOutputBytes = 64
	})

	base := RunRequest{Command: os.Args[0], Env: helperEnv(helperOutput)}
	cases := []struct {
		name string
		req  RunRequest
	}{
		{"empty command", RunRequest{Command: "   "}},
		{"zero timeout", RunRequest{Command: base.Command, Env: base.Env, TimeoutMs: ptrI64(0)}},
		{"negative timeout", RunRequest{Command: base.Command, Env: base.Env, TimeoutMs: ptrI64(-1)}},
		{"timeout above max", RunRequest{Command: base.Command, Env: base.Env, TimeoutMs: ptrI64(101)}},
		{"zero output cap", RunRequest{Command: base.Command, Env: base.Env, MaxOutputBytes: ptrI64(0)}},
		{"negative output cap", RunRequest{Command: base.Command, Env: base.Env, MaxOutputBytes: ptrI64(-1)}},
		{"output cap above max", RunRequest{Command: base.Command, Env: base.Env, MaxOutputBytes: ptrI64(65)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := m.Run(context.Background(), tc.req); !errors.Is(err, ErrValidation) {
				t.Fatalf("Run(%s) error = %v, want ErrValidation", tc.name, err)
			}
			if got := activeProcesses(m); got != 0 {
				t.Fatalf("activeProcesses = %d after %s, want 0", got, tc.name)
			}
			if got := activeRunPeak(m); got != 0 {
				t.Fatalf("activeRunPeak = %d after %s, want 0", got, tc.name)
			}
		})
	}

	// The exact maxima are accepted, not clamped.
	res, err := m.Run(context.Background(), RunRequest{
		Command:        base.Command,
		Env:            base.Env,
		TimeoutMs:      ptrI64(100),
		MaxOutputBytes: ptrI64(64),
	})
	if err != nil {
		t.Fatalf("Run(at maxima): %v", err)
	}
	if res.Stdout != "stdout-payload" {
		t.Fatalf("stdout = %q, want full payload below the cap", res.Stdout)
	}
}

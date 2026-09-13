package acpruntime

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// TestRuntimeKillAfterNaturalExitClearsPGID proves the captured pgid is cleared
// once the direct child is reaped and that a post-exit Kill confirms promptly.
func TestRuntimeKillAfterNaturalExitClearsPGID(t *testing.T) {
	requireLinux(t)
	r, _, pidPath := startHelperRuntime(t, helperChildExits, time.Minute)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	if err := r.Wait(); err != nil {
		t.Fatalf("Wait after natural exit = %v, want nil", err)
	}
	if got := r.pgid.Load(); got != 0 {
		t.Fatalf("captured pgid after Wait = %d, want 0", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Kill(ctx); err != nil {
		t.Fatalf("Kill after Wait = %v, want nil", err)
	}
}

// TestRuntimeConcurrentKillBeforeAndAfterExit exercises repeated concurrent
// Kill calls on both sides of terminal exit under the race detector.
func TestRuntimeConcurrentKillBeforeAndAfterExit(t *testing.T) {
	requireLinux(t)
	r, _, pidPath := startHelperRuntime(t, helperProcessGroup, time.Minute)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i, err := range concurrentKillers(ctx, r, 8) {
		if err != nil {
			t.Errorf("pre-exit concurrent Kill[%d] = %v, want nil", i, err)
		}
	}
	if err := r.Wait(); err != nil {
		t.Logf("Wait after Kill = %v", err)
	}
	for i, err := range concurrentKillers(ctx, r, 8) {
		if err != nil {
			t.Errorf("post-exit concurrent Kill[%d] = %v, want nil", i, err)
		}
	}
}

// TestRuntimePostExitKillDoesNotSignalReusedGroup simulates the kernel
// recycling the captured PGID for a live unrelated process group after the
// direct child was reaped; a post-exit Kill must not signal it.
func TestRuntimePostExitKillDoesNotSignalReusedGroup(t *testing.T) {
	requireLinux(t)
	r, _, pidPath := startHelperRuntime(t, helperChildExits, time.Minute)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	if err := r.Wait(); err != nil {
		t.Fatalf("Wait after natural exit = %v, want nil", err)
	}

	unrelated := startUnrelatedProcessGroup(t)
	r.pgid.Store(int64(unrelated))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Kill(ctx); err != nil {
		t.Fatalf("post-exit Kill = %v, want nil", err)
	}
	if state, ok := processState(unrelated); !ok || state == 'Z' {
		t.Fatalf("unrelated group process %d state = %q (present %v), want alive after post-exit Kill", unrelated, state, ok)
	}
}

// startUnrelatedProcessGroup starts this test binary in the blocking helper
// mode under its own process group and returns its PID.
func startUnrelatedProcessGroup(t *testing.T) int {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = withEnv(os.Environ(), helperEnv, helperGrandchild)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start unrelated process: %v", err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = cmd.Wait()
	})
	return pid
}

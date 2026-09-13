package acpruntime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// TestRuntimeKillInterleavedWithReapDoesNotSignalReusedGroup forces the exact
// in-wait interleaving window: the waiter is paused after reaping the direct
// child, the captured PGID is overwritten with a live unrelated group (as the
// kernel recycling the ID would), and a Kill then runs. The group's signal gate
// must already be closed, so the recycled group is never signaled.
func TestRuntimeKillInterleavedWithReapDoesNotSignalReusedGroup(t *testing.T) {
	requireLinux(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	r, _, pidPath := startHelperRuntimeHooked(t, helperProcessGroup, time.Minute, func() {
		close(entered)
		<-release
	}, nil)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	// Natural exit of only the direct child; the grandchild holds the inherited
	// pipes until the waiter's mandatory group kill.
	if err := syscall.Kill(pids.Direct, syscall.SIGKILL); err != nil {
		t.Fatalf("kill direct child: %v", err)
	}
	awaitSignal(t, entered, "waiter paused after reaping the direct child")

	// Simulate the kernel recycling the reaped child's PGID for a live group
	// before the waiter releases the captured id.
	unrelated := startUnrelatedProcessGroup(t)
	r.pgid.Store(int64(unrelated))

	// An already-cancelled context makes Kill signal (or no-op) and return
	// without waiting for the still-paused waiter.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Kill(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("interleaved Kill = %v, want context.Canceled", err)
	}

	// Let the waiter finish so a signal sent during the window is delivered
	// before the recycled group is inspected.
	close(release)
	_ = r.Wait()
	if state, ok := processState(unrelated); !ok || state == 'Z' {
		t.Fatalf("unrelated group process %d state = %q (present %v), want alive after the interleaved Kill", unrelated, state, ok)
	}
}

// TestRuntimeFallbackReapSerializesWithKill forces the fallback (no unreaped
// exit observation) waiter path and proves its reap is serialized with external
// signaling. The gate's captured PGID is pointed at a live unrelated group, so
// any signal that sneaks in between reap and the gate mark kills it. While the
// fallback waiter holds the gate across cmd.Wait, a concurrent Kill cannot
// signal: it blocks until reap and the mark complete and then observes exited
// and no-ops, so the unrelated group survives.
func TestRuntimeFallbackReapSerializesWithKill(t *testing.T) {
	requireLinux(t)
	unrelated := startUnrelatedProcessGroup(t)

	store := newRuntimeStore(t)
	pidPath := filepath.Join(t.TempDir(), "helper-pids.json")
	env := withEnv(os.Environ(), helperEnv, helperProcessGroup)
	env = withEnv(env, helperPIDFileEnv, pidPath)
	spec := LaunchSpec{Program: os.Args[0], Env: env}

	r, err := start(t.Context(), store, "srv", spec, time.Minute, testLogger(), nil, func(r *Runtime, _ int) bool {
		// Simulate the kernel recycling the reaped child's PGID for the live
		// unrelated group before the fallback wait begins.
		r.pgid.Store(int64(unrelated))
		return false
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Kill(ctx)
	})

	// Wait, bounded, until the fallback waiter holds the gate across its reap.
	// The fixed code acquires the gate before cmd.Wait; the old code never holds
	// it across the reap, so the deadline simply expires.
	deadline := time.Now().Add(time.Second)
	for r.pgid.mu.TryLock() {
		r.pgid.mu.Unlock()
		if time.Now().After(deadline) {
			break
		}
		runtime.Gosched()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	killDone := make(chan error, 1)
	go func() { killDone <- r.Kill(ctx) }()

	// Release the direct child so the fallback reap can finish; the fixed
	// waiter reaps, marks the gate exited, and only then lets Kill observe its
	// no-op.
	if err := syscall.Kill(pids.Direct, syscall.SIGKILL); err != nil {
		t.Fatalf("kill direct child: %v", err)
	}
	if err := r.Wait(); err != nil {
		t.Logf("Wait after fallback reap = %v", err)
	}
	if err := <-killDone; err != nil {
		t.Fatalf("fallback Kill = %v, want nil", err)
	}

	if state, ok := processState(unrelated); !ok || state == 'Z' {
		t.Fatalf("unrelated group process %d state = %q (present %v), want alive: fallback reap signaled a recycled PGID", unrelated, state, ok)
	}
}

// TestSignalGateExitNoSignalRejectsSignals proves the mark-only waiter
// transition closes the signal gate without signaling: once marked, an external
// signal is a no-op and the captured (live) process group is left untouched. It
// drives the gate in isolation so it does not depend on the Linux unreaped-exit
// observation or the reap-then-mark fallback ordering.
func TestSignalGateExitNoSignalRejectsSignals(t *testing.T) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = withEnv(os.Environ(), helperEnv, helperGrandchild)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start target process group: %v", err)
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	})

	var g signalGate
	g.Store(int64(cmd.Process.Pid))
	g.exitNoSignal()
	if got := g.Load(); got != 0 {
		t.Fatalf("pgid after exitNoSignal = %d, want 0 (gate not marked)", got)
	}
	if err := g.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("signal after exitNoSignal = %v, want nil", err)
	}
	select {
	case <-done:
		t.Fatal("external signal reached the captured group after exitNoSignal")
	case <-time.After(100 * time.Millisecond):
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

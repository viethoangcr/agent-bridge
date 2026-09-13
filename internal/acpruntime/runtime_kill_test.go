package acpruntime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestRuntimeKillAfterExitReturnsNil proves a post-exit Kill is an idempotent
// nil confirmation: the direct child has been reaped, so the stdlib process
// handle reports os.ErrProcessDone, which Kill treats as success.
func TestRuntimeKillAfterExitReturnsNil(t *testing.T) {
	requireLinux(t)
	r, _, pidPath := startHelperRuntime(t, helperChildExits, time.Minute)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	if err := r.Wait(); err != nil {
		t.Fatalf("Wait after natural exit = %v, want nil", err)
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

// TestRuntimeKillTargetsDirectChildBeforeWaiterKillsGroup proves the ownership
// split of the redesigned termination path: external Kill SIGKILLs only the
// direct child through its os.Process handle, never the negative PGID. The
// waiter, paused here before observing exit, is the sole group owner; the
// descendant survives Kill itself and dies only once the runtime completes.
func TestRuntimeKillTargetsDirectChildBeforeWaiterKillsGroup(t *testing.T) {
	requireLinux(t)
	release := make(chan struct{})
	r, _, pidPath := startHelperRuntimeHooked(t, helperProcessGroup, time.Minute, nil, func(*Runtime, int) bool {
		<-release
		return true
	})
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	// An already-cancelled context makes Kill signal (or no-op) and return
	// without waiting for the still-paused waiter.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Kill(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Kill with paused waiter = %v, want context.Canceled", err)
	}
	requireAliveFor(t, pids.Grandchild, 200*time.Millisecond)

	// Releasing the waiter lets the sole group owner kill the captured group
	// while the direct child is still unreaped.
	close(release)
	if err := r.Wait(); err != nil {
		t.Logf("Wait after group teardown = %v", err)
	}
	waitReaped(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)
}

// TestRuntimeConcurrentKillAndExitLeavesUnrelatedGroupAlive hammers Kill while
// the waiter transitions the child to exited. Kill never signals a process
// group, so a live unrelated group must survive every interleaving under the
// race detector.
func TestRuntimeConcurrentKillAndExitLeavesUnrelatedGroupAlive(t *testing.T) {
	requireLinux(t)
	unrelated := startUnrelatedProcessGroup(t)
	release := make(chan struct{})
	r, _, pidPath := startHelperRuntimeHooked(t, helperChildExits, time.Minute, nil, func(*Runtime, int) bool {
		<-release
		return true
	})
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const killers = 8
	done := make(chan error, killers)
	for i := 0; i < killers; i++ {
		go func() { done <- r.Kill(ctx) }()
	}
	close(release)
	for i := 0; i < killers; i++ {
		if err := <-done; err != nil {
			t.Errorf("concurrent Kill[%d] = %v, want nil", i, err)
		}
	}
	if state, ok := processState(unrelated); !ok || state == 'Z' {
		t.Fatalf("unrelated group process %d state = %q (present %v), want alive after Kill/exit interleaving", unrelated, state, ok)
	}
}

// TestRuntimeKillInterleavedWithReapDoesNotSignalLiveGroup pauses the waiter
// after it reaps the direct child, then runs Kill. The child's PID may be
// recycled by then; because Kill signals only the stdlib process handle, which
// Wait has already marked done, no signal reaches a live unrelated group.
func TestRuntimeKillInterleavedWithReapDoesNotSignalLiveGroup(t *testing.T) {
	requireLinux(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	r, _, pidPath := startHelperRuntimeHooked(t, helperChildExits, time.Minute, func() {
		close(entered)
		<-release
	}, nil)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	awaitSignal(t, entered, "waiter paused after reaping the direct child")
	unrelated := startUnrelatedProcessGroup(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Kill(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("interleaved Kill = %v, want context.Canceled", err)
	}

	close(release)
	_ = r.Wait()
	if state, ok := processState(unrelated); !ok || state == 'Z' {
		t.Fatalf("unrelated group process %d state = %q (present %v), want alive after the interleaved Kill", unrelated, state, ok)
	}
}

// TestRuntimeStartContextCancelKillsGroup proves a canceled start context tears
// down the whole group: the default direct-child cancel kills the leader and
// the sole waiter then kills the descendant before reaping.
func TestRuntimeStartContextCancelKillsGroup(t *testing.T) {
	requireLinux(t)
	store := newRuntimeStore(t)
	pidPath := filepath.Join(t.TempDir(), "helper-pids.json")
	env := withEnv(os.Environ(), helperEnv, helperProcessGroup)
	env = withEnv(env, helperPIDFileEnv, pidPath)
	spec := LaunchSpec{Program: os.Args[0], Env: env}

	ctx, cancel := context.WithCancel(context.Background())
	r, err := start(ctx, store, "srv", spec, time.Minute, testLogger(), nil, nil)
	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	cancel()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not terminate after start context cancellation")
	}
	waitReaped(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)
}

// TestRuntimeAbortAfterSpawnKillsGroupWithoutHanging forces the post-spawn
// failure path with a live child and asserts abortAfterSpawn kills the direct
// child before observing exit, then cleans the descendant group. Without the
// intervening direct-child kill, ObserveExit would block on a live child.
func TestRuntimeAbortAfterSpawnKillsGroupWithoutHanging(t *testing.T) {
	requireLinux(t)
	store := newRuntimeStore(t)
	pidPath := filepath.Join(t.TempDir(), "helper-pids.json")
	env := withEnv(os.Environ(), helperEnv, helperProcessGroup)
	env = withEnv(env, helperPIDFileEnv, pidPath)

	cmd := exec.Command(os.Args[0])
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	r := newRuntime(store, "srv", testLogger(), cmd, func() {}, stdin, time.Minute)
	done := make(chan error, 1)
	go func() { done <- r.abortAfterSpawn(errors.New("boom")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("abortAfterSpawn = nil, want the abort reason")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("abortAfterSpawn hung; it must kill the direct child before observing exit")
	}
	waitReaped(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)
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

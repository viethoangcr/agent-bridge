package process

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// countOpenFDs returns the number of open descriptors for the test process.
// The directory used to read /proc/self/fd is closed before ReadDir returns,
// so the count does not include itself.
func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(entries)
}

// TestManager_NaturalExit_DoesNotLeakFDs guards the retained stdin write end:
// every naturally-exited managed process must release the descriptor before its
// record is published exited. os/exec's Cmd.Wait closes the StdinPipe parent
// write end, so this fails if the manager ever stops reaching Wait.
func TestManager_NaturalExit_DoesNotLeakFDs(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 1)

	// Warm up runtime-owned descriptors (poll epoll/eventfd) before sampling.
	snap := startHelper(t, m, StartRequest{Command: "/bin/true"})
	waitProcessDone(t, m, snap.ID)
	before := countOpenFDs(t)

	const runs = 25
	for range runs {
		snap := startHelper(t, m, StartRequest{Command: "/bin/true"})
		waitProcessDone(t, m, snap.ID)
	}

	if after := countOpenFDs(t); after > before {
		t.Fatalf("open FDs grew from %d to %d after %d naturally-exited processes", before, after, runs)
	}
}

// TestManager_NaturalExit_WriteInputAndShutdownIdempotent pins the documented
// contract after a natural exit and that the already-closed retained stdin
// leaves shutdown's close idempotent.
func TestManager_NaturalExit_WriteInputAndShutdownIdempotent(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 1)
	env := helperEnv(helperExitCode)
	env[processHelperExitEnv] = "0"
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	waitProcessDone(t, m, snap.ID)

	_, err := m.WriteInput(context.Background(), snap.ID, []byte("x"))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("WriteInput after natural exit = %v, want ErrConflict", err)
	}
	if errors.Is(err, os.ErrClosed) {
		t.Fatalf("WriteInput after natural exit leaked os.ErrClosed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown after natural exit = %v, want nil", err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown = %v, want nil", err)
	}
}

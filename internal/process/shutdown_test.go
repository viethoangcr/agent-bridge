package process

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

// TestProcessShutdown is the Task 4.9 RED suite for the staged process
// shutdown. It covers the pre-drain blocker, the shutdown killer, reservation
// release, idempotency, and the rule that exited records are never signaled.
func TestProcessShutdown(t *testing.T) {
	requireLinuxProcess(t)

	t.Run("pre-drain-block-rejects-new-work", func(t *testing.T) {
		m := newTestManager(t, 4)
		if err := m.BlockNew(context.Background()); err != nil {
			t.Fatalf("BlockNew() = %v, want nil", err)
		}
		if _, err := m.Start(StartRequest{Command: "/bin/sleep", Args: []string{"30"}}); !errors.Is(err, ErrConflict) {
			t.Fatalf("Start after BlockNew error = %v, want ErrConflict", err)
		}
		if _, err := m.Run(context.Background(), RunRequest{Command: "/bin/true"}); !errors.Is(err, ErrConflict) {
			t.Fatalf("Run after BlockNew error = %v, want ErrConflict", err)
		}
	})

	t.Run("shutdown-kills-managed-and-one-shot-groups", func(t *testing.T) {
		m := newTestManager(t, 8)

		managedPIDFile := filepath.Join(t.TempDir(), "managed.json")
		startHelper(t, m, StartRequest{
			Command: os.Args[0],
			Env:     map[string]string{processHelperEnv: helperChild, processHelperPIDEnv: managedPIDFile},
		})
		managed := readHelperPIDs(t, managedPIDFile)

		runPIDFile := filepath.Join(t.TempDir(), "run.json")
		runDone := make(chan error, 1)
		go func() {
			_, err := m.Run(context.Background(), RunRequest{
				Command:        os.Args[0],
				Env:            map[string]string{processHelperEnv: helperChild, processHelperPIDEnv: runPIDFile},
				MaxOutputBytes: ptrI64(64),
			})
			runDone <- err
		}()
		ran := readHelperPIDs(t, runPIDFile)
		waitForActiveProcesses(t, m, 2)
		if got := activeRunPeak(m); got <= 0 {
			t.Fatalf("activeRunPeak = %d, want a positive peak reservation", got)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown() = %v, want nil", err)
		}

		if got := activeProcesses(m); got != 0 {
			t.Fatalf("activeProcesses = %d after Shutdown, want 0", got)
		}
		if got := activeRunPeak(m); got != 0 {
			t.Fatalf("activeRunPeak = %d after Shutdown, want 0", got)
		}
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Fatal("one-shot Run did not return after Shutdown")
		}

		waitGoneOrZombie(t, managed.Direct)
		waitGoneOrZombie(t, managed.Grandchild)
		waitGoneOrZombie(t, ran.Direct)
		waitGoneOrZombie(t, ran.Grandchild)

		// Shutdown is idempotent and the manager stays closed.
		if err := m.Shutdown(ctx); err != nil {
			t.Fatalf("second Shutdown() = %v, want nil", err)
		}
		if _, err := m.Start(StartRequest{Command: "/bin/sleep", Args: []string{"30"}}); !errors.Is(err, ErrConflict) {
			t.Fatalf("Start after Shutdown error = %v, want ErrConflict", err)
		}
		if _, err := m.Run(context.Background(), RunRequest{Command: "/bin/true"}); !errors.Is(err, ErrConflict) {
			t.Fatalf("Run after Shutdown error = %v, want ErrConflict", err)
		}
	})

	t.Run("exited-record-pid-is-not-signaled", func(t *testing.T) {
		m := newTestManager(t, 4)
		sentinel := exec.Command("/bin/sleep", "30")
		if err := sentinel.Start(); err != nil {
			t.Fatalf("start sentinel: %v", err)
		}
		t.Cleanup(func() {
			_ = sentinel.Process.Kill()
			_, _ = sentinel.Process.Wait()
		})

		stale := testManagedProcess("proc_stale", 1024)
		m.mu.Lock()
		stale.pid = sentinel.Process.Pid
		m.processes[stale.id] = stale
		m.mu.Unlock()

		if err := m.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() = %v, want nil", err)
		}
		if err := syscall.Kill(sentinel.Process.Pid, 0); err != nil {
			t.Fatalf("Shutdown signaled an exited record's stale pid: %v", err)
		}
	})
}

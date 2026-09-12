package acpstore

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// reconcileHelperEnv re-execs the test binary as a long-lived helper process so
// a reconciliation test can prove a live PID is never signaled.
const reconcileHelperEnv = "AGENT_BRIDGE_TEST_RECONCILE_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(reconcileHelperEnv) == "1" {
		for {
			time.Sleep(time.Hour)
		}
	}
	os.Exit(m.Run())
}

// seedServerRow inserts one server row with explicit status/live metadata so a
// test can model state persisted before a bridge restart.
func seedServerRow(t *testing.T, store *Store, id string, status Status, atMs int64, pid *int, idleSince, exitedAt *int64) {
	t.Helper()
	var pidArg, idleArg, exitedArg any
	if pid != nil {
		pidArg = *pid
	}
	if idleSince != nil {
		idleArg = *idleSince
	}
	if exitedAt != nil {
		exitedArg = *exitedAt
	}
	if _, err := store.db.ExecContext(t.Context(),
		`INSERT INTO servers (server_id, agent, status, created_at_ms, updated_at_ms,
			idle_since_ms, last_event_seq, pid, exited_at_ms)
		 VALUES (?, 'mock', ?, ?, ?, ?, 0, ?, ?)`,
		id, string(status), atMs, atMs, idleArg, pidArg, exitedArg); err != nil {
		t.Fatalf("seed server %q: %v", id, err)
	}
}

func TestReconcileStaleRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reconcile.db")

	seed, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("open seed store: %v", err)
	}
	seedServerRow(t, seed, "srv-creating", StatusCreating, 100, intPtr(4100), nil, nil)
	seedServerRow(t, seed, "srv-idle", StatusIdle, 100, intPtr(4200), int64Ptr(150), nil)
	seedServerRow(t, seed, "srv-busy", StatusBusy, 100, intPtr(4300), nil, nil)
	seedServerRow(t, seed, "srv-exited", StatusExited, 100, intPtr(4400), nil, int64Ptr(250))
	if err := seed.db.Close(); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	clock := newFakeClock(1_000)
	store := openWithClock(t, path, clock.Now)
	clock.advanceMs(1_000)

	if err := store.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	for _, id := range []string{"srv-creating", "srv-idle", "srv-busy"} {
		server, err := store.Server(t.Context(), id)
		if err != nil {
			t.Fatalf("Server(%s): %v", id, err)
		}
		if server.Status != StatusExited {
			t.Errorf("%s status = %q, want %q", id, server.Status, StatusExited)
		}
		if server.PID != nil {
			t.Errorf("%s PID = %d, want nil", id, *server.PID)
		}
		if server.IdleSinceMs != nil {
			t.Errorf("%s idle_since_ms = %d, want nil", id, *server.IdleSinceMs)
		}
		if server.ExitedAtMs == nil || *server.ExitedAtMs != 2_000 {
			t.Errorf("%s exited_at_ms = %v, want 2000", id, server.ExitedAtMs)
		}
		if server.UpdatedAtMs != 2_000 {
			t.Errorf("%s updated_at_ms = %d, want 2000", id, server.UpdatedAtMs)
		}
	}

	exited, err := store.Server(t.Context(), "srv-exited")
	if err != nil {
		t.Fatalf("Server(srv-exited): %v", err)
	}
	if exited.Status != StatusExited {
		t.Errorf("srv-exited status = %q, want %q", exited.Status, StatusExited)
	}
	if exited.ExitedAtMs == nil || *exited.ExitedAtMs != 250 {
		t.Errorf("srv-exited exited_at_ms = %v, want retained 250", exited.ExitedAtMs)
	}
	if exited.UpdatedAtMs != 100 {
		t.Errorf("srv-exited updated_at_ms = %d, want retained 100", exited.UpdatedAtMs)
	}
	if exited.PID == nil || *exited.PID != 4400 {
		t.Errorf("srv-exited PID = %v, want retained 4400", exited.PID)
	}
}

func TestReconcileNeverSignalsLivePID(t *testing.T) {
	helper := exec.Command(os.Args[0])
	helper.Env = append(os.Environ(), reconcileHelperEnv+"=1")
	if err := helper.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() {
		_ = helper.Process.Kill()
		_ = helper.Wait()
	})
	pid := helper.Process.Pid
	if !processRunning(pid) {
		t.Fatalf("helper %d is not running after start", pid)
	}

	store := openWithClock(t, filepath.Join(t.TempDir(), "live-pid.db"), newFakeClock(1_000).Now)
	seedServerRow(t, store, "srv-live", StatusIdle, 100, &pid, int64Ptr(150), nil)

	if err := store.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !processRunning(pid) {
		t.Fatalf("Reconcile signaled live helper PID %d", pid)
	}
}

func TestCloseCheckpointsAndSeals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close.db")
	store := openWithClock(t, path, newFakeClock(1_000).Now)

	if _, err := store.CreateServer(t.Context(), "srv", "mock"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}

	walPath := path + "-wal"
	if info, err := os.Stat(walPath); err != nil || info.Size() == 0 {
		t.Fatalf("WAL content before Close: stat err = %v, want non-empty %s", err, walPath)
	}

	if err := store.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if info, err := os.Stat(walPath); err == nil {
		if info.Size() != 0 {
			t.Errorf("WAL size after Close = %d, want 0 or absent", info.Size())
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat WAL after Close: %v", err)
	}

	if err := store.Close(t.Context()); err != nil {
		t.Errorf("second Close error = %v, want nil", err)
	}

	if _, err := store.Server(t.Context(), "srv"); err == nil {
		t.Error("Server after Close error = nil, want error")
	}
	if _, err := store.Servers(t.Context()); err == nil {
		t.Error("Servers after Close error = nil, want error")
	}
	if err := store.Reconcile(t.Context()); err == nil {
		t.Error("Reconcile after Close error = nil, want error")
	}
	if err := store.SetStatus(t.Context(), "srv", StatusIdle); err == nil {
		t.Error("SetStatus after Close error = nil, want error")
	}
	if _, err := store.CreateServer(t.Context(), "srv-2", "mock"); err == nil {
		t.Error("CreateServer after Close error = nil, want error")
	}

	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.db.Close() })
	server, err := reopened.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server after reopen: %v", err)
	}
	if server.Status != StatusCreating {
		t.Errorf("reopened status = %q, want %q", server.Status, StatusCreating)
	}
}

// processRunning reports whether pid is a live direct child. A WNOHANG wait
// returns 0 only while the child is still running; a signaled child would be
// reported as a reaped zombie instead.
func processRunning(pid int) bool {
	var status syscall.WaitStatus
	reaped, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
	return reaped == 0 && err == nil
}

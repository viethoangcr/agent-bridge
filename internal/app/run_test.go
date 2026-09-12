package app

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestRunServesHealthAndReleasesListener is the end-to-end lifecycle check: the
// server serves health, cancellation returns cleanly, the PID file is removed,
// and the same address can be rebound afterwards.
func TestRunServesHealthAndReleasesListener(t *testing.T) {
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "bridge.pid")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST":     host,
		"AGENT_BRIDGE_PORT":     port,
		"AGENT_BRIDGE_PID_FILE": pidPath,
		"AGENT_BRIDGE_DB":       filepath.Join(dir, "bridge.db"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- Run(ctx, getenv, testIO()) }()

	waitForHealth(t, "http://"+addr+"/v1/health")

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if _, statErr := os.Stat(pidPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pid file still present after shutdown: stat error = %v", statErr)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("rebind %s after shutdown: %v", addr, err)
	}
	_ = ln.Close()
}

// TestRunLeavesNoPIDFileAfterBindFailure asserts listener creation precedes PID
// file creation so a failed bind never leaves a stale PID file.
func TestRunLeavesNoPIDFileAfterBindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer ln.Close()

	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST":     host,
		"AGENT_BRIDGE_PORT":     port,
		"AGENT_BRIDGE_PID_FILE": pidPath,
		"AGENT_BRIDGE_DB":       filepath.Join(t.TempDir(), "bridge.db"),
	})

	if err := Run(context.Background(), getenv, testIO()); err == nil {
		t.Fatal("Run() = nil, want bind failure")
	}
	if _, statErr := os.Stat(pidPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pid file exists after bind failure: stat error = %v", statErr)
	}
}

// TestRunRefusesExistingPIDFile asserts startup fails without disturbing the
// existing file, and that the listener acquired before the PID write is
// released.
func TestRunRefusesExistingPIDFile(t *testing.T) {
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	if err := os.WriteFile(pidPath, []byte("existing"), 0o600); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST":     host,
		"AGENT_BRIDGE_PORT":     port,
		"AGENT_BRIDGE_PID_FILE": pidPath,
		"AGENT_BRIDGE_DB":       filepath.Join(t.TempDir(), "bridge.db"),
	})

	if err := Run(context.Background(), getenv, testIO()); err == nil {
		t.Fatal("Run() = nil, want refusal to replace existing pid file")
	}

	got, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	if string(got) != "existing" {
		t.Fatalf("pid file = %q, want original content preserved", got)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("rebind %s after pid refusal: %v", addr, err)
	}
	_ = ln.Close()
}

// TestRunDatabaseReconciliation proves normal startup opens the store and
// reconciles stale live rows before the health endpoint can serve, so a client
// can never observe pre-reconciliation state. After cancellation the
// checkpointed database reopens cleanly with the reconciled rows committed.
func TestRunDatabaseReconciliation(t *testing.T) {
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "bridge.db")

	seed, err := acpstore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open seed store: %v", err)
	}
	seedServer := func(id string, pid int, busy bool) {
		t.Helper()
		if _, err := seed.CreateServer(context.Background(), id, "mock"); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		if pid != 0 {
			if err := seed.SetLive(context.Background(), id, pid); err != nil {
				t.Fatalf("set %s live: %v", id, err)
			}
		}
		if busy {
			if err := seed.SetStatus(context.Background(), id, acpstore.StatusBusy); err != nil {
				t.Fatalf("set %s busy: %v", id, err)
			}
		}
	}
	seedServer("srv-creating", 0, false)
	seedServer("srv-idle", 4100, false)
	seedServer("srv-busy", 4200, true)
	if err := seed.Close(context.Background()); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST": host,
		"AGENT_BRIDGE_PORT": port,
		"AGENT_BRIDGE_DB":   dbPath,
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runErr := make(chan error, 1)
	go func() { runErr <- Run(ctx, getenv, testIO()) }()

	waitForHealth(t, "http://"+addr+"/v1/health")

	// Health is reachable only after startup reconciliation completed. A stale
	// row here means reconciliation ran after readiness instead of before it.
	inspect, err := acpstore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open inspect store: %v", err)
	}
	for _, id := range []string{"srv-creating", "srv-idle", "srv-busy"} {
		server, err := inspect.Server(context.Background(), id)
		if err != nil {
			t.Fatalf("Server(%s): %v", id, err)
		}
		if server.Status != acpstore.StatusExited {
			t.Errorf("%s status = %q before health, want %q", id, server.Status, acpstore.StatusExited)
		}
		if server.PID != nil {
			t.Errorf("%s pid = %d before health, want nil", id, *server.PID)
		}
	}
	if err := inspect.Close(context.Background()); err != nil {
		t.Fatalf("close inspect store: %v", err)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	reopened, err := acpstore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("reopen store after shutdown: %v", err)
	}
	defer func() { _ = reopened.Close(context.Background()) }()
	server, err := reopened.Server(context.Background(), "srv-idle")
	if err != nil {
		t.Fatalf("Server after reopen: %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Errorf("reopened status = %q, want %q", server.Status, acpstore.StatusExited)
	}
}

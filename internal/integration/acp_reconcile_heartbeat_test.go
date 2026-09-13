package integration

import (
	"bufio"
	"context"
	"net/http"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestACPReconcileStaleLiveRows asserts startup reconciliation rewrites stale
// live rows without ever signaling a persisted PID.
func TestACPReconcileStaleLiveRows(t *testing.T) {
	sleep := exec.Command("sleep", "60")
	if err := sleep.Start(); err != nil {
		t.Fatalf("start sentinel process: %v", err)
	}
	defer func() { _ = sleep.Process.Kill(); _, _ = sleep.Process.Wait() }()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bridge.db")
	store, err := acpstore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open seed store: %v", err)
	}
	if _, err := store.CreateServer(context.Background(), "stale", "mock"); err != nil {
		t.Fatalf("create seed server: %v", err)
	}
	if err := store.SetLive(context.Background(), "stale", sleep.Process.Pid); err != nil {
		t.Fatalf("set seed live: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	b := startBridge(t, bridgeOptions{env: map[string]string{"AGENT_BRIDGE_DB": dbPath}})
	defer b.stop()

	status, code := b.status("stale")
	if code != http.StatusOK || status.Status != string(acpstore.StatusExited) || status.PID != nil {
		t.Fatalf("reconciled status = %d %+v, want exited without a PID", code, status)
	}
	if err := syscall.Kill(sleep.Process.Pid, 0); err != nil {
		t.Fatalf("startup reconciliation signaled the persisted PID: %v", err)
	}
}

// TestRealHeartbeat15Seconds is the sole real-time test: it waits for exactly
// one production 15-second SSE heartbeat under a 20-second outer deadline.
// It is deliberately excluded from the focused `-race` run by its name and is
// run once, without -race, via the full `go test ./...` sweep.
func TestRealHeartbeat15Seconds(t *testing.T) {
	if testing.Short() {
		t.Skip("real-time heartbeat test skipped under -short")
	}
	b := startBridge(t, bridgeOptions{})
	defer b.stop()
	b.initialize("beat", "mock")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.base+"/v1/acp/beat", nil)
	if err != nil {
		t.Fatalf("new sse request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := b.client.Do(req)
	if err != nil {
		t.Fatalf("sse connect: %v", err)
	}
	defer resp.Body.Close()

	start := time.Now()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if scanner.Text() == ": heartbeat" {
			elapsed := time.Since(start)
			if elapsed < 10*time.Second || elapsed > 20*time.Second {
				t.Fatalf("heartbeat after %s, want one production 15s interval inside 20s", elapsed)
			}
			return
		}
	}
	t.Fatalf("no heartbeat received within the 20s deadline: %v", scanner.Err())
}

package integration

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"

	_ "modernc.org/sqlite"
)

// TestACPReplayAfterRestart covers SSE replay, bridge restart, and startup
// reconciliation of stale live rows.
func TestACPReplayAfterRestart(t *testing.T) {
	b := startBridge(t, bridgeOptions{})
	b.initialize("replay", "mock")
	sessionID := b.sessionNew("replay", "/replay")
	if code, body := b.prompt("replay", sessionID, "hello"); code != http.StatusOK {
		t.Fatalf("prompt = %d: %s", code, body)
	}
	events, _ := b.events("replay", "limit=1000")
	if len(events) < 3 {
		t.Fatalf("events = %d, want at least 3", len(events))
	}
	firstSeq := events[0].Seq
	dbPath := b.dbPath

	// Replay live through SSE from an exact int64 sequence.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	replayed, err := b.sse(ctx, "replay", strconv.FormatInt(firstSeq, 10), len(events)-1)
	cancel()
	if err != nil {
		t.Fatalf("live sse reconnect: %v", err)
	}
	if len(replayed) != len(events)-1 || replayed[0].ID != events[1].Seq {
		t.Fatalf("live replay IDs = %+v, want starting at %d", replayed, events[1].Seq)
	}

	b.stop()

	// Restart the bridge on the same database; persisted events replay again.
	restarted := startBridge(t, bridgeOptions{env: map[string]string{"AGENT_BRIDGE_DB": dbPath}})
	defer restarted.stop()
	replayed, err = restarted.sse(ctx2(t), "replay", "0", len(events))
	if err != nil {
		t.Fatalf("restart sse replay: %v", err)
	}
	if len(replayed) != len(events) {
		t.Fatalf("restart replay = %d events, want %d", len(replayed), len(events))
	}
	for i := 1; i < len(replayed); i++ {
		if replayed[i].ID <= replayed[i-1].ID {
			t.Fatalf("restart replay not strictly ascending: %+v", replayed)
		}
	}
	status, code := restarted.status("replay")
	if code != http.StatusOK || status.Status != string(acpstore.StatusExited) {
		t.Fatalf("restarted status = %d %+v, want exited", code, status)
	}
}

func ctx2(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestACPDeleteRace races DELETE with an active Post lease and a failed prune.
func TestACPDeleteRace(t *testing.T) {
	t.Run("delete-releases-active-post", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()
		b.initialize("del", "mock")
		b.sessionNew("del", "/del")
		if code, body := b.prompt("del", "mock-session-1", "hello"); code != http.StatusOK {
			t.Fatalf("prompt = %d: %s", code, body)
		}

		// Hold an activity lease with a request whose timeout is far away.
		blocked := make(chan int, 1)
		go func() {
			code, _ := b.postEnvelope("del", "", `{"jsonrpc":"2.0","id":"hold","method":"_mock/delay","params":{"ms":60000}}`)
			blocked <- code
		}()
		b.waitFor(5*time.Second, "active lease", func() bool {
			status, code := b.status("del")
			return code == http.StatusOK && status.Status == string(acpstore.StatusBusy)
		})

		start := time.Now()
		resp := b.do(http.MethodDelete, "/v1/acp/del", "", nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE = %d, want 204", resp.StatusCode)
		}
		if elapsed := time.Since(start); elapsed > 10*time.Second {
			t.Fatalf("DELETE waited %s, want immediate signal-and-wait kill", elapsed)
		}
		select {
		case code := <-blocked:
			if code == http.StatusOK {
				t.Fatalf("blocked post succeeded after DELETE")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("active post did not release after DELETE")
		}

		if code := b.getJSON("/v1/acp/del/status", nil); code != http.StatusNotFound {
			t.Fatalf("status after DELETE = %d, want 404", code)
		}
	})

	t.Run("delete-prune-failure-retry", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()
		b.initialize("prune", "mock")

		side, err := sql.Open("sqlite", "file:"+b.dbPath+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatalf("open side db: %v", err)
		}
		defer side.Close()
		if _, err := side.Exec(`CREATE TRIGGER block_prune BEFORE DELETE ON servers BEGIN SELECT RAISE(ABORT, 'prune blocked'); END`); err != nil {
			t.Fatalf("create prune trigger: %v", err)
		}

		resp := b.do(http.MethodDelete, "/v1/acp/prune", "", nil)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("DELETE with failed prune = %d: %s, want 500", resp.StatusCode, body)
		}
		status, code := b.status("prune")
		if code != http.StatusOK || status.Status != string(acpstore.StatusExited) {
			t.Fatalf("status after failed prune = %d %+v, want exited", code, status)
		}

		if _, err := side.Exec(`DROP TRIGGER block_prune`); err != nil {
			t.Fatalf("drop prune trigger: %v", err)
		}
		resp = b.do(http.MethodDelete, "/v1/acp/prune", "", nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE retry = %d, want 204", resp.StatusCode)
		}
		if code := b.getJSON("/v1/acp/prune/status", nil); code != http.StatusNotFound {
			t.Fatalf("status after retry DELETE = %d, want 404", code)
		}
	})
}

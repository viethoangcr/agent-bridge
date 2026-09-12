package acpstore

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock for exact millisecond assertions.
type fakeClock struct {
	now time.Time
}

func newFakeClock(ms int64) *fakeClock {
	return &fakeClock{now: time.UnixMilli(ms)}
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) advanceMs(ms int64) {
	c.now = c.now.Add(time.Duration(ms) * time.Millisecond)
}

// openWithClock opens a store whose timestamps come from now. The clock field
// is a private seam; production Open always injects time.Now.
func openWithClock(t *testing.T, path string, now func() time.Time) *Store {
	t.Helper()
	store := openTestStore(t, path)
	store.clock = now
	return store
}

func intPtr(v int) *int { return &v }

func int64Ptr(v int64) *int64 { return &v }

func TestServerCreate(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "create.db"), newFakeClock(1_000).Now)

	created, err := store.CreateServer(t.Context(), "srv-b", "claude")
	if err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	want := Server{
		ServerID:    "srv-b",
		Agent:       "claude",
		Status:      StatusCreating,
		CreatedAtMs: 1_000,
		UpdatedAtMs: 1_000,
	}
	if !reflect.DeepEqual(created, want) {
		t.Errorf("CreateServer = %+v, want %+v", created, want)
	}

	got, err := store.Server(t.Context(), "srv-b")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Server = %+v, want %+v", got, want)
	}
}

func TestServerDuplicateConflict(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "conflict.db"), newFakeClock(1_000).Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("first CreateServer: %v", err)
	}
	if _, err := store.CreateServer(t.Context(), "srv", "codex"); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate CreateServer error = %v, want ErrConflict", err)
	}

	got, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	if got.Agent != "claude" {
		t.Errorf("agent after duplicate create = %q, want %q", got.Agent, "claude")
	}
}

func TestServerListSorting(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "list.db"), newFakeClock(1_000).Now)

	for _, id := range []string{"srv-c", "srv-a", "srv-b"} {
		if _, err := store.CreateServer(t.Context(), id, "claude"); err != nil {
			t.Fatalf("CreateServer(%q): %v", id, err)
		}
	}

	servers, err := store.Servers(t.Context())
	if err != nil {
		t.Fatalf("Servers: %v", err)
	}
	got := make([]string, len(servers))
	for i, srv := range servers {
		got[i] = srv.ServerID
	}
	want := []string{"srv-a", "srv-b", "srv-c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Servers IDs = %v, want %v", got, want)
	}
}

func TestServerSetLive(t *testing.T) {
	clock := newFakeClock(1_000)
	store := openWithClock(t, filepath.Join(t.TempDir(), "live.db"), clock.Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	clock.advanceMs(50)

	if err := store.SetLive(t.Context(), "srv", 4242); err != nil {
		t.Fatalf("SetLive: %v", err)
	}

	got, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	want := Server{
		ServerID:    "srv",
		Agent:       "claude",
		Status:      StatusIdle,
		CreatedAtMs: 1_000,
		UpdatedAtMs: 1_050,
		IdleSinceMs: int64Ptr(1_050),
		PID:         intPtr(4242),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Server after SetLive = %+v, want %+v", got, want)
	}
}

func TestServerSetStatusIdleBusy(t *testing.T) {
	clock := newFakeClock(1_000)
	store := openWithClock(t, filepath.Join(t.TempDir(), "status.db"), clock.Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	clock.advanceMs(10)
	if err := store.SetLive(t.Context(), "srv", 7); err != nil {
		t.Fatalf("SetLive: %v", err)
	}

	clock.advanceMs(20)
	if err := store.SetStatus(t.Context(), "srv", StatusBusy); err != nil {
		t.Fatalf("SetStatus(busy): %v", err)
	}
	busy, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	if busy.Status != StatusBusy || busy.IdleSinceMs != nil || busy.UpdatedAtMs != 1_030 {
		t.Errorf("busy server = %+v, want status busy, nil idle_since_ms, updated 1030", busy)
	}

	clock.advanceMs(30)
	if err := store.SetStatus(t.Context(), "srv", StatusIdle); err != nil {
		t.Fatalf("SetStatus(idle): %v", err)
	}
	idle, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	if idle.Status != StatusIdle || idle.IdleSinceMs == nil || *idle.IdleSinceMs != 1_060 || idle.UpdatedAtMs != 1_060 {
		t.Errorf("idle server = %+v, want status idle, idle_since_ms 1060, updated 1060", idle)
	}

	clock.advanceMs(5)
	if err := store.SetStatus(t.Context(), "srv", StatusBusy); err != nil {
		t.Fatalf("SetStatus(busy again): %v", err)
	}
	again, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	if again.IdleSinceMs != nil || again.UpdatedAtMs != 1_065 {
		t.Errorf("busy again = %+v, want nil idle_since_ms and updated 1065", again)
	}
}

func TestServerMarkExited(t *testing.T) {
	clock := newFakeClock(1_000)
	store := openWithClock(t, filepath.Join(t.TempDir(), "exited.db"), clock.Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	clock.advanceMs(10)
	if err := store.SetLive(t.Context(), "srv", 99); err != nil {
		t.Fatalf("SetLive: %v", err)
	}

	clock.advanceMs(40)
	if err := store.MarkExited(t.Context(), "srv"); err != nil {
		t.Fatalf("MarkExited: %v", err)
	}
	exited, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	if exited.Status != StatusExited || exited.PID != nil || exited.IdleSinceMs != nil {
		t.Errorf("exited server = %+v, want status exited with nil pid and idle_since_ms", exited)
	}
	if exited.ExitedAtMs == nil || *exited.ExitedAtMs != 1_050 || exited.UpdatedAtMs != 1_050 {
		t.Errorf("exited server timestamps = %+v, want exited_at_ms 1050 and updated 1050", exited)
	}

	clock.advanceMs(100)
	if err := store.MarkExited(t.Context(), "srv"); err != nil {
		t.Fatalf("second MarkExited: %v", err)
	}
	again, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}
	if again.ExitedAtMs == nil || *again.ExitedAtMs != 1_050 {
		t.Errorf("second MarkExited overwrote exit timestamp = %v, want 1050", again.ExitedAtMs)
	}
	if again.UpdatedAtMs != 1_150 {
		t.Errorf("updated_at_ms after second exit = %d, want 1150", again.UpdatedAtMs)
	}
}

func TestServerUnknown(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "unknown.db"), newFakeClock(1_000).Now)

	if _, err := store.Server(t.Context(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Server(unknown) error = %v, want ErrNotFound", err)
	}
	if err := store.SetLive(t.Context(), "nope", 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetLive(unknown) error = %v, want ErrNotFound", err)
	}
	if err := store.SetStatus(t.Context(), "nope", StatusIdle); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetStatus(unknown) error = %v, want ErrNotFound", err)
	}
	if err := store.MarkExited(t.Context(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("MarkExited(unknown) error = %v, want ErrNotFound", err)
	}
	if err := store.DeleteServer(t.Context(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("DeleteServer(unknown) error = %v, want ErrNotFound", err)
	}
	if _, err := store.Session(t.Context(), "nope", "s"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Session(unknown) error = %v, want ErrNotFound", err)
	}
	if _, err := store.Sessions(t.Context(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Sessions(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestServerDeleteCascade(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "cascade.db"), newFakeClock(1_000).Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	seedSession(t, store, "srv", "sess-1", "/tmp", 1)
	seedEvent(t, store, "srv", 1)

	if err := store.DeleteServer(t.Context(), "srv"); err != nil {
		t.Fatalf("DeleteServer: %v", err)
	}
	if _, err := store.Server(t.Context(), "srv"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Server after delete error = %v, want ErrNotFound", err)
	}
	assertRowCount(t, store, "server_sessions", 0)
	assertRowCount(t, store, "events", 0)
}

func TestSessionListingAndLookup(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "sessions.db"), newFakeClock(1_000).Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	seedSession(t, store, "srv", "s-3", "/c", 3)
	seedSession(t, store, "srv", "s-1", "/a", 1)
	seedSession(t, store, "srv", "s-2", "/b", 2)

	sessions, err := store.Sessions(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	got := make([]string, len(sessions))
	for i, sess := range sessions {
		got[i] = sess.SessionID
	}
	if want := []string{"s-1", "s-2", "s-3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Sessions IDs = %v, want %v", got, want)
	}

	one, err := store.Session(t.Context(), "srv", "s-2")
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	want := Session{ServerID: "srv", SessionID: "s-2", CWD: "/b", CreatedAtMs: 2, UpdatedAtMs: 2}
	if !reflect.DeepEqual(one, want) {
		t.Errorf("Session = %+v, want %+v", one, want)
	}

	if _, err := store.Session(t.Context(), "srv", "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Session(missing) error = %v, want ErrNotFound", err)
	}
}

func TestSessionCascadeRemoval(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "session-cascade.db"), newFakeClock(1_000).Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	seedSession(t, store, "srv", "sess-1", "/tmp", 1)
	seedSession(t, store, "srv", "sess-2", "/tmp", 2)
	seedEvent(t, store, "srv", 1)

	if err := store.DeleteServer(t.Context(), "srv"); err != nil {
		t.Fatalf("DeleteServer: %v", err)
	}
	assertRowCount(t, store, "server_sessions", 0)
	assertRowCount(t, store, "events", 0)
}

func seedSession(t *testing.T, store *Store, serverID, sessionID, cwd string, createdMs int64) {
	t.Helper()
	if _, err := store.db.ExecContext(t.Context(),
		`INSERT INTO server_sessions (server_id, session_id, cwd, created_at_ms, updated_at_ms)
		 VALUES (?, ?, ?, ?, ?)`,
		serverID, sessionID, cwd, createdMs, createdMs); err != nil {
		t.Fatalf("seed session %q: %v", sessionID, err)
	}
}

func seedEvent(t *testing.T, store *Store, serverID string, seq int64) {
	t.Helper()
	if _, err := store.db.ExecContext(t.Context(),
		`INSERT INTO events (server_id, seq, kind, payload, created_at_ms)
		 VALUES (?, ?, 'notification', X'7B7D', 1)`,
		serverID, seq); err != nil {
		t.Fatalf("seed event %d: %v", seq, err)
	}
}

func assertRowCount(t *testing.T, store *Store, table string, want int) {
	t.Helper()
	var got int
	if err := store.db.QueryRowContext(context.Background(), "SELECT count(*) FROM "+table).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Errorf("%s count = %d, want %d", table, got, want)
	}
}

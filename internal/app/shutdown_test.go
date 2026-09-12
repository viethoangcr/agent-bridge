package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
)

// TestRunShutdownOrder proves the database is checkpointed/closed by the
// post-drain hook while the PID file still exists, that the PID file is removed
// last, and that normal Run shutdown closes a real store end to end.
func TestRunShutdownOrder(t *testing.T) {
	t.Run("database-closed-before-pid-removed", func(t *testing.T) {
		restore := setShutdownGrace(t, 2*time.Second)
		defer restore()

		dbPath := filepath.Join(t.TempDir(), "bridge.db")
		store, err := acpstore.Open(context.Background(), dbPath)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		if _, err := store.CreateServer(context.Background(), "srv", "mock"); err != nil {
			t.Fatalf("create server: %v", err)
		}
		walPath := dbPath + "-wal"
		if info, err := os.Stat(walPath); err != nil || info.Size() == 0 {
			t.Fatalf("WAL before shutdown: stat err = %v, want non-empty %s", err, walPath)
		}

		rec := &eventRecorder{}
		listener := &fakeCloser{rec: rec}
		server := &fakeShutdowner{rec: rec}
		pidPath := filepath.Join(t.TempDir(), "bridge.pid")
		if err := os.WriteFile(pidPath, []byte("123\n"), 0o600); err != nil {
			t.Fatalf("seed pid file: %v", err)
		}

		pre := &lifecycle.Registry{}
		if err := pre.Add("streams", func(context.Context) error {
			rec.add("pre")
			return nil
		}); err != nil {
			t.Fatalf("add pre hook: %v", err)
		}
		var pidExistedDuringClose atomic.Bool
		post := &lifecycle.Registry{}
		if err := post.Add("database", func(ctx context.Context) error {
			if _, err := os.Stat(pidPath); err == nil {
				pidExistedDuringClose.Store(true)
			}
			rec.add("database")
			return store.Close(ctx)
		}); err != nil {
			t.Fatalf("add post hook: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		drain(ctx, listener, server, pre, post, discardLogger(), pidPath)

		want := []string{"listener", "pre", "shutdown", "database"}
		if got := rec.snapshot(); !slices.Equal(got, want) {
			t.Fatalf("stage order = %v, want %v", got, want)
		}
		if !pidExistedDuringClose.Load() {
			t.Fatal("database was closed after the pid file was already removed")
		}
		if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pid file not removed last: stat error = %v", err)
		}
		if _, err := store.Server(context.Background(), "srv"); err == nil {
			t.Error("store usable after shutdown, want closed")
		}
		if info, err := os.Stat(walPath); err == nil && info.Size() != 0 {
			t.Errorf("WAL size after close = %d, want 0 or absent", info.Size())
		}
	})

	t.Run("run-checkpoints-and-removes-pid", func(t *testing.T) {
		addr := freeAddress(t)
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("split address: %v", err)
		}
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "bridge.db")
		pidPath := filepath.Join(dir, "bridge.pid")
		getenv := testGetenv(map[string]string{
			"AGENT_BRIDGE_HOST":     host,
			"AGENT_BRIDGE_PORT":     port,
			"AGENT_BRIDGE_DB":       dbPath,
			"AGENT_BRIDGE_PID_FILE": pidPath,
		})

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runErr := make(chan error, 1)
		go func() { runErr <- Run(ctx, getenv, testIO()) }()

		waitForHealth(t, "http://"+addr+"/v1/health")
		if _, err := os.Stat(dbPath); err != nil {
			t.Fatalf("database not created during startup: %v", err)
		}
		if _, err := os.Stat(pidPath); err != nil {
			t.Fatalf("pid file missing while running: %v", err)
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

		if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pid file still present after shutdown: stat error = %v", err)
		}
		if info, err := os.Stat(dbPath + "-wal"); err == nil {
			if info.Size() != 0 {
				t.Errorf("WAL size after shutdown = %d, want 0 or absent", info.Size())
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("stat WAL after shutdown: %v", err)
		}
	})
}

// TestDrainStageOrdering asserts the exact shutdown order: listener close,
// pre-drain, http Shutdown, post-drain, PID-file removal last. It also proves
// the pre-drain hook closed the streaming handler before Shutdown was entered.
func TestDrainStageOrdering(t *testing.T) {
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

	rec := &eventRecorder{}
	stream := make(chan struct{})
	listener := &fakeCloser{rec: rec}
	server := &fakeShutdowner{
		rec: rec,
		streamClosed: func() bool {
			select {
			case <-stream:
				return true
			default:
				return false
			}
		},
	}

	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	if err := os.WriteFile(pidPath, []byte("123\n"), 0o600); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}

	pre := &lifecycle.Registry{}
	if err := pre.Add("streams", func(context.Context) error {
		close(stream)
		rec.add("pre")
		return nil
	}); err != nil {
		t.Fatalf("add pre hook: %v", err)
	}

	var pidExistedDuringPost atomic.Bool
	post := &lifecycle.Registry{}
	if err := post.Add("database", func(context.Context) error {
		if _, err := os.Stat(pidPath); err == nil {
			pidExistedDuringPost.Store(true)
		}
		rec.add("post")
		return nil
	}); err != nil {
		t.Fatalf("add post hook: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drain(ctx, listener, server, pre, post, discardLogger(), pidPath)

	want := []string{"listener", "pre", "shutdown", "post"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("stage order = %v, want %v", got, want)
	}
	if !listener.closed.Load() {
		t.Fatal("listener was not closed")
	}
	if !pidExistedDuringPost.Load() {
		t.Fatal("pid file was removed before post-drain ran")
	}
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pid file not removed last: stat error = %v", err)
	}
}

// TestServeErrorRunsPostCleanupBeforePIDRemoval asserts the unexpected
// serve-error path runs the post-drain database cleanup before removing the
// PID file, matching the cancellation path's ordering. The PID file must still
// exist while the store is being closed and disappear only afterwards.
func TestServeErrorRunsPostCleanupBeforePIDRemoval(t *testing.T) {
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

	rec := &eventRecorder{}
	listener := &failingListener{rec: rec, err: errors.New("accept boom")}
	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	if err := os.WriteFile(pidPath, []byte("123\n"), 0o600); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}

	var pidExistedDuringClose atomic.Bool
	post := &lifecycle.Registry{}
	if err := post.Add("database", func(context.Context) error {
		if _, err := os.Stat(pidPath); err == nil {
			pidExistedDuringClose.Store(true)
		}
		rec.add("database")
		return nil
	}); err != nil {
		t.Fatalf("add post hook: %v", err)
	}

	err := serve(context.Background(), listener, &http.Server{Handler: http.NotFoundHandler()},
		&lifecycle.Registry{}, post, discardLogger(), pidPath)
	if err == nil {
		t.Fatal("serve() = nil, want the accept failure surfaced")
	}

	want := []string{"listener", "database"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("cleanup order = %v, want %v", got, want)
	}
	if !pidExistedDuringClose.Load() {
		t.Fatal("pid file was removed before the database cleanup ran")
	}
	if _, statErr := os.Stat(pidPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pid file still present after serve error: stat error = %v", statErr)
	}
}

// TestDrainSharesOneAbsoluteDeadline asserts every stage receives the same
// absolute deadline with a non-increasing remaining budget, never a reset.
func TestDrainSharesOneAbsoluteDeadline(t *testing.T) {
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

	var mu sync.Mutex
	var deadlines []time.Time
	var remainings []time.Duration
	record := func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("stage context has no deadline")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		deadlines = append(deadlines, deadline)
		remainings = append(remainings, time.Until(deadline))
	}

	rec := &eventRecorder{}
	listener := &fakeCloser{rec: rec}
	server := &fakeShutdowner{rec: rec, onShutdown: record}

	pre := &lifecycle.Registry{}
	if err := pre.Add("streams", func(ctx context.Context) error {
		record(ctx)
		return nil
	}); err != nil {
		t.Fatalf("add pre hook: %v", err)
	}
	post := &lifecycle.Registry{}
	if err := post.Add("database", func(ctx context.Context) error {
		record(ctx)
		return nil
	}); err != nil {
		t.Fatalf("add post hook: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drain(ctx, listener, server, pre, post, discardLogger(), "")

	if len(deadlines) != 3 {
		t.Fatalf("recorded %d deadlines, want 3", len(deadlines))
	}
	for i, d := range deadlines {
		if !d.Equal(deadlines[0]) {
			t.Fatalf("stage %d deadline = %v, want same absolute deadline %v", i, d, deadlines[0])
		}
	}
	for i := 1; i < len(remainings); i++ {
		if remainings[i] > remainings[i-1] {
			t.Fatalf("remaining budget reset at stage %d: %v > %v", i, remainings[i], remainings[i-1])
		}
	}
}

// TestDrainForceClosesWhenBudgetExpires asserts that when the absolute budget
// expires while http Shutdown is still draining, Close force-closes remaining
// connections and post-drain still runs.
func TestDrainForceClosesWhenBudgetExpires(t *testing.T) {
	restore := setShutdownGrace(t, 30*time.Millisecond)
	defer restore()

	rec := &eventRecorder{}
	listener := &fakeCloser{rec: rec}
	server := &fakeShutdowner{rec: rec, block: true}

	post := &lifecycle.Registry{}
	if err := post.Add("database", func(context.Context) error {
		rec.add("post")
		return nil
	}); err != nil {
		t.Fatalf("add post hook: %v", err)
	}

	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	if err := os.WriteFile(pidPath, []byte("1\n"), 0o600); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drain(ctx, listener, server, &lifecycle.Registry{}, post, discardLogger(), pidPath)

	if !server.closeCalled.Load() {
		t.Fatal("http Close was not called after budget expiry")
	}
	want := []string{"listener", "shutdown", "close", "post"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("stage order = %v, want %v", got, want)
	}
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pid file not removed: stat error = %v", err)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// TestDrainLogsHookErrorsAfterStageCompletes asserts a hook error is logged
// only after every hook in that stage ran. The registry runs reverse
// registration order, so the error hook runs before the healthy hook and the
// log record follows both.
func TestDrainLogsHookErrorsAfterStageCompletes(t *testing.T) {
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

	rec := &eventRecorder{}
	logWriter := writerFunc(func(p []byte) (int, error) {
		rec.add("log")
		return len(p), nil
	})
	logger := slog.New(slog.NewJSONHandler(logWriter, nil))

	listener := &fakeCloser{rec: rec}
	server := &fakeShutdowner{rec: rec}

	pre := &lifecycle.Registry{}
	if err := pre.Add("healthy", func(context.Context) error {
		rec.add("hook-healthy")
		return nil
	}); err != nil {
		t.Fatalf("add healthy hook: %v", err)
	}
	if err := pre.Add("failing", func(context.Context) error {
		rec.add("hook-failing")
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("add failing hook: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drain(ctx, listener, server, pre, &lifecycle.Registry{}, logger, "")

	got := rec.snapshot()
	idxHealthy := slices.Index(got, "hook-healthy")
	idxFailing := slices.Index(got, "hook-failing")
	idxLog := slices.Index(got, "log")
	if idxHealthy < 0 || idxFailing < 0 || idxLog < 0 {
		t.Fatalf("missing events: %v", got)
	}
	if idxLog < idxHealthy || idxLog < idxFailing {
		t.Fatalf("error logged before every hook in the stage ran: %v", got)
	}
}

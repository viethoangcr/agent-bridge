package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
)

// TestACPPreDrainOrder proves the pre-drain hook signal-and-waits the runtimes
// before http.Server.Shutdown begins draining, and that post-drain confirmation
// runs before the database close, all under one absolute deadline.
func TestACPPreDrainOrder(t *testing.T) {
	var mu sync.Mutex
	var deadlines []time.Time
	record := func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("stage context has no deadline")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		deadlines = append(deadlines, deadline)
	}

	rec := &eventRecorder{}
	var killed atomic.Bool
	proxy := &fakeACPLifecycle{rec: rec}
	proxy.onShutdown = func(ctx context.Context) error {
		record(ctx)
		killed.Store(true)
		return nil
	}
	proxy.onConfirm = func(ctx context.Context) error {
		record(ctx)
		return nil
	}

	pre := &lifecycle.Registry{}
	post := &lifecycle.Registry{}
	if err := registerShutdown(pre, post, func(ctx context.Context) error {
		record(ctx)
		rec.add("database")
		return nil
	}, proxy); err != nil {
		t.Fatalf("registerShutdown: %v", err)
	}

	listener := &fakeCloser{rec: rec}
	server := &fakeShutdowner{
		rec:          rec,
		onShutdown:   record,
		streamClosed: killed.Load,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drain(ctx, listener, server, pre, post, discardLogger(), "", 2*time.Second)

	want := []string{"listener", "acp-pre", "shutdown", "acp-confirm", "database"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("stage order = %v, want %v", got, want)
	}
	if !killed.Load() {
		t.Fatal("pre-drain hook did not finish killing before http shutdown")
	}
	if len(deadlines) != 4 {
		t.Fatalf("recorded %d stage deadlines, want 4", len(deadlines))
	}
	for i, d := range deadlines {
		if !d.Equal(deadlines[0]) {
			t.Fatalf("stage %d deadline = %v, want shared absolute deadline %v", i, d, deadlines[0])
		}
	}
}

// TestProcessLifecycleRegistration proves the process pre-drain blocker runs
// before the shutdown killer within the pre-drain stage.
func TestProcessLifecycleRegistration(t *testing.T) {
	rec := &eventRecorder{}
	process := &fakeProcessLifecycle{rec: rec}
	pre := &lifecycle.Registry{}
	if err := registerProcessShutdown(pre, process); err != nil {
		t.Fatalf("registerProcessShutdown: %v", err)
	}
	if err := pre.Shutdown(context.Background()); err != nil {
		t.Fatalf("pre.Shutdown: %v", err)
	}

	want := []string{"process-block", "process-shutdown"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("process hook order = %v, want %v", got, want)
	}
	if got := process.blockCalls.Load(); got != 1 {
		t.Fatalf("BlockNew calls = %d, want 1", got)
	}
	if got := process.shutdownCalls.Load(); got != 1 {
		t.Fatalf("Shutdown calls = %d, want 1", got)
	}
}

// TestServeErrorRunsACPLifecycle proves an unexpected Serve failure still runs
// the pre-drain ACP shutdown (stop reaper, close subscriptions, signal-and-wait
// runtimes) before the post-drain confirmation and database close, all under
// one absolute deadline, so live runtimes never outlive the store.
func TestServeErrorRunsACPLifecycle(t *testing.T) {
	rec := &eventRecorder{}
	proxy := &fakeACPLifecycle{rec: rec}
	var deadlines []time.Time
	recordDeadline := func(stage string) func(context.Context) error {
		return func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Errorf("%s context has no deadline", stage)
				return nil
			}
			deadlines = append(deadlines, deadline)
			return nil
		}
	}
	proxy.onShutdown = recordDeadline("pre-drain")
	proxy.onConfirm = recordDeadline("post-drain")

	pre := &lifecycle.Registry{}
	post := &lifecycle.Registry{}
	if err := registerShutdown(pre, post, func(context.Context) error {
		rec.add("database")
		return nil
	}, proxy); err != nil {
		t.Fatalf("registerShutdown: %v", err)
	}

	listener := &failingListener{rec: rec, err: errors.New("accept boom")}
	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	if err := os.WriteFile(pidPath, []byte("123\n"), 0o600); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}

	err := serve(context.Background(), listener, &http.Server{Handler: http.NotFoundHandler()},
		pre, post, discardLogger(), pidPath, 2*time.Second)
	if err == nil {
		t.Fatal("serve() = nil, want the accept failure surfaced")
	}

	want := []string{"listener", "acp-pre", "acp-confirm", "database"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("cleanup order = %v, want %v", got, want)
	}
	if got := proxy.shutdownCalls.Load(); got != 1 {
		t.Fatalf("pre-drain Shutdown calls = %d, want 1", got)
	}
	if got := proxy.confirmCalls.Load(); got != 1 {
		t.Fatalf("post-drain Confirm calls = %d, want 1", got)
	}
	if len(deadlines) != 2 {
		t.Fatalf("recorded %d stage deadlines, want 2", len(deadlines))
	}
	if !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("stage deadlines differ: pre %v, post %v", deadlines[0], deadlines[1])
	}
	if _, statErr := os.Stat(pidPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pid file still present after serve error: stat error = %v", statErr)
	}
}

// TestACPShutdown proves pre-drain signal-and-wait kill unblocks a long-lived
// ACP request before HTTP drain, and that an expired drain budget still runs
// the force-close fallback and post-drain cleanup.
func TestACPShutdown(t *testing.T) {
	t.Run("pre-drain-kills-blocked-runtime-before-http-drain", func(t *testing.T) {
		addr := freeAddress(t)
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("split address: %v", err)
		}
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "bridge.db")
		pidPath := filepath.Join(dir, "bridge.pid")
		getenv := testGetenv(map[string]string{
			"AGENT_BRIDGE_HOST":        host,
			"AGENT_BRIDGE_PORT":        port,
			"AGENT_BRIDGE_DB":          dbPath,
			"AGENT_BRIDGE_PID_FILE":    pidPath,
			"AGENT_BRIDGE_IDLE_TTL_MS": "0",
		})

		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- run(ctx, getenv, testIO(), testOptions(5*time.Second)) }()
		waitForHealth(t, "http://"+addr+"/v1/health")

		blocked := make(chan struct{}, 1)
		go func() {
			body := strings.NewReader(`{"jsonrpc":"2.0","id":"hold","method":"_mock/delay","params":{"ms":60000}}`)
			req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/acp/shutdown?agent=mock", body)
			if err != nil {
				blocked <- struct{}{}
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			blocked <- struct{}{}
		}()
		time.Sleep(200 * time.Millisecond)

		start := time.Now()
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Fatalf("Run() = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return within the shutdown budget")
		}
		if elapsed := time.Since(start); elapsed >= 4*time.Second {
			t.Fatalf("shutdown took %s; pre-drain did not kill the blocked runtime before HTTP drain", elapsed)
		}
		select {
		case <-blocked:
		case <-time.After(2 * time.Second):
			t.Fatal("blocked ACP request did not release after shutdown")
		}
		if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pid file not removed last: %v", err)
		}
		reopened, err := acpstore.Open(context.Background(), dbPath)
		if err != nil {
			t.Fatalf("reopen store after shutdown: %v", err)
		}
		server, err := reopened.Server(context.Background(), "shutdown")
		if err != nil {
			t.Fatalf("Server(shutdown): %v", err)
		}
		if server.Status != acpstore.StatusExited {
			t.Errorf("status = %q, want exited", server.Status)
		}
		_ = reopened.Close(context.Background())
	})

	t.Run("budget-expiry-close-fallback-runs-post-drain", func(t *testing.T) {
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

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		drain(ctx, listener, server, &lifecycle.Registry{}, post, discardLogger(), "", 30*time.Millisecond)

		if !server.closeCalled.Load() {
			t.Fatal("http Close was not called after budget expiry")
		}
		want := []string{"listener", "shutdown", "close", "post"}
		if got := rec.snapshot(); !slices.Equal(got, want) {
			t.Fatalf("stage order = %v, want %v", got, want)
		}
	})
}

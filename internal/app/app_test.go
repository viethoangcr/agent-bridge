package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/config"
	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
	"github.com/viethoangcr/agent-bridge/internal/mockagent"
)

// TestMain lets this test binary double as the private mock agent when the ACP
// resolver re-execs it with AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1.
func TestMain(m *testing.M) {
	if os.Getenv("AGENT_BRIDGE_INTERNAL_MOCK_AGENT") == "1" {
		if err := mockagent.Run(context.Background(), os.Stdin, os.Stdout, os.Stderr); err != nil {
			fmt.Fprintln(os.Stderr, "mock agent:", err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func testGetenv(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

func setShutdownGrace(t *testing.T, d time.Duration) func() {
	t.Helper()
	old := shutdownGrace
	shutdownGrace = d
	return func() { shutdownGrace = old }
}

func testIO() IO {
	return IO{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard}
}

// TestRunMockBypassesStartup proves the private mock dispatch runs the JSONL
// loop before config.Load, listener creation, and PID-file creation: the HTTP
// environment is deliberately invalid, yet the mock answers on stdio. The mock
// never loads HTTP config, binds, or creates a database or its WAL files.
func TestRunMockBypassesStartup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bridge.db")
	pidPath := filepath.Join(dir, "bridge.pid")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT": "1",
		"AGENT_BRIDGE_PORT":                "not-a-port",
		"AGENT_BRIDGE_HOST":                "not-a-real-host",
		"AGENT_BRIDGE_PID_FILE":            pidPath,
		"AGENT_BRIDGE_DB":                  dbPath,
	})
	request := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}` + "\n"
	var out strings.Builder
	appIO := IO{Stdin: strings.NewReader(request), Stdout: &out, Stderr: io.Discard}

	err := Run(context.Background(), getenv, appIO)
	if err != nil {
		t.Fatalf("Run() = %v, want nil from private mock execution", err)
	}
	if !strings.Contains(out.String(), `"protocolVersion":1`) {
		t.Fatalf("private mock did not execute: stdout = %q", out.String())
	}
	if _, statErr := os.Stat(pidPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pid file exists after mock run: stat error = %v", statErr)
	}
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("mock mode created %s: stat error = %v", path, statErr)
		}
	}
}

// TestWritePIDFileContentAndMode asserts the decimal PID plus newline and 0600
// permissions required by the specification.
func TestWritePIDFileContentAndMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.pid")
	if err := writePIDFile(path); err != nil {
		t.Fatalf("writePIDFile() = %v, want nil", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	want := strconv.Itoa(os.Getpid()) + "\n"
	if string(got) != want {
		t.Fatalf("pid file = %q, want %q", got, want)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat pid file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("pid file mode = %o, want 0600", perm)
	}
}

// TestWritePIDFileRefusesExisting asserts O_EXCL semantics: an existing file is
// never replaced and keeps its original content.
func TestWritePIDFileRefusesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.pid")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}

	if err := writePIDFile(path); err == nil {
		t.Fatal("writePIDFile() = nil, want refusal to replace existing file")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	if string(got) != "existing" {
		t.Fatalf("pid file = %q, want original content preserved", got)
	}
}

// TestRemovePIDFileIgnoresMissing asserts removing an already-absent file is
// success while other removal failures would still surface.
func TestRemovePIDFileIgnoresMissing(t *testing.T) {
	if err := removePIDFile(filepath.Join(t.TempDir(), "absent.pid")); err != nil {
		t.Fatalf("removePIDFile() = %v, want nil for missing file", err)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitForHealth(t *testing.T, url string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("health endpoint never served at %s", url)
}

type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *eventRecorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *eventRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

type fakeCloser struct {
	rec    *eventRecorder
	closed atomic.Bool
}

// failingListener accepts with a fixed unexpected error so http.Server.Serve
// returns that error and drive the serve-error cleanup path.
type failingListener struct {
	rec    *eventRecorder
	err    error
	closed atomic.Bool
}

func (l *failingListener) Accept() (net.Conn, error) { return nil, l.err }

func (l *failingListener) Close() error {
	if l.closed.CompareAndSwap(false, true) {
		l.rec.add("listener")
	}
	return nil
}

func (l *failingListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

func (c *fakeCloser) Close() error {
	c.closed.Store(true)
	c.rec.add("listener")
	return nil
}

// Accept and Addr make fakeCloser a net.Listener so it can stand in for a
// listener whose Serve is driven by an injected fake server.
func (c *fakeCloser) Accept() (net.Conn, error) { return nil, errors.New("fake listener") }

func (c *fakeCloser) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

type fakeShutdowner struct {
	rec          *eventRecorder
	onShutdown   func(context.Context)
	streamClosed func() bool
	block        bool
	serveErr     error
	closeCalled  atomic.Bool
}

// Serve returns the configured acceptance error so serve's unexpected-error
// path can be driven without a real listener.
func (s *fakeShutdowner) Serve(net.Listener) error { return s.serveErr }

func (s *fakeShutdowner) Shutdown(ctx context.Context) error {
	s.rec.add("shutdown")
	if s.onShutdown != nil {
		s.onShutdown(ctx)
	}
	if s.streamClosed != nil && !s.streamClosed() {
		s.rec.add("shutdown-before-stream-closed")
	}
	if s.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (s *fakeShutdowner) Close() error {
	s.closeCalled.Store(true)
	s.rec.add("close")
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// fakeACPLifecycle records the ACP staged-shutdown calls without spawning any
// process so app ordering can be asserted deterministically.
type fakeACPLifecycle struct {
	rec           *eventRecorder
	started       atomic.Bool
	shutdownCalls atomic.Int32
	confirmCalls  atomic.Int32
	onShutdown    func(context.Context) error
	onConfirm     func(context.Context) error
}

func (f *fakeACPLifecycle) StartReaper() { f.started.Store(true) }

func (f *fakeACPLifecycle) Shutdown(ctx context.Context) error {
	f.shutdownCalls.Add(1)
	if f.rec != nil {
		f.rec.add("acp-pre")
	}
	if f.onShutdown != nil {
		return f.onShutdown(ctx)
	}
	return nil
}

func (f *fakeACPLifecycle) Confirm(ctx context.Context) error {
	f.confirmCalls.Add(1)
	if f.rec != nil {
		f.rec.add("acp-confirm")
	}
	if f.onConfirm != nil {
		return f.onConfirm(ctx)
	}
	return nil
}

func (f *fakeACPLifecycle) Post(context.Context, string, *string, string, json.RawMessage) (acpruntime.PostResult, error) {
	return acpruntime.PostResult{}, nil
}

func (f *fakeACPLifecycle) LivePID(string) (int, bool) { return 0, false }

// TestACPRegistryOrder proves app.Run constructs and starts the ACP proxy and
// runs its pre-drain hook before its post-drain confirmation. The store is
// checkpoint-closed only after confirmation, which the reopen verifies.
func TestACPRegistryOrder(t *testing.T) {
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "bridge.db")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST": host,
		"AGENT_BRIDGE_PORT": port,
		"AGENT_BRIDGE_DB":   dbPath,
	})

	rec := &eventRecorder{}
	proxy := &fakeACPLifecycle{rec: rec}
	oldNew := newACPProxy
	newACPProxy = func(*acpstore.Store, config.Config, *slog.Logger) acpService { return proxy }
	t.Cleanup(func() { newACPProxy = oldNew })

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

	if !proxy.started.Load() {
		t.Fatal("Run did not start the ACP reaper")
	}
	if got := proxy.shutdownCalls.Load(); got != 1 {
		t.Fatalf("pre-drain Shutdown calls = %d, want 1", got)
	}
	if got := proxy.confirmCalls.Load(); got != 1 {
		t.Fatalf("post-drain Confirm calls = %d, want 1", got)
	}
	got := rec.snapshot()
	preIdx := slices.Index(got, "acp-pre")
	confirmIdx := slices.Index(got, "acp-confirm")
	if preIdx < 0 || confirmIdx < 0 || preIdx > confirmIdx {
		t.Fatalf("ACP stage order = %v, want pre-drain before post-drain confirm", got)
	}

	reopened, err := acpstore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("reopen store after shutdown: %v", err)
	}
	_ = reopened.Close(context.Background())
}

// TestACPPreDrainOrder proves the pre-drain hook signal-and-waits the runtimes
// before http.Server.Shutdown begins draining, and that post-drain confirmation
// runs before the database close, all under one absolute deadline.
func TestACPPreDrainOrder(t *testing.T) {
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

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
	drain(ctx, listener, server, pre, post, discardLogger(), "")

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

// fakeProcessLifecycle records the process staged-shutdown calls without
// spawning any group so app hook ordering can be asserted deterministically.
type fakeProcessLifecycle struct {
	rec           *eventRecorder
	blockCalls    atomic.Int32
	shutdownCalls atomic.Int32
}

func (f *fakeProcessLifecycle) BlockNew(context.Context) error {
	f.blockCalls.Add(1)
	f.rec.add("process-block")
	return nil
}

func (f *fakeProcessLifecycle) Shutdown(context.Context) error {
	f.shutdownCalls.Add(1)
	f.rec.add("process-shutdown")
	return nil
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
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

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
		pre, post, discardLogger(), pidPath)
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
		restore := setShutdownGrace(t, 5*time.Second)
		defer restore()

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
		go func() { runErr <- Run(ctx, getenv, testIO()) }()
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

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		drain(ctx, listener, server, &lifecycle.Registry{}, post, discardLogger(), "")

		if !server.closeCalled.Load() {
			t.Fatal("http Close was not called after budget expiry")
		}
		want := []string{"listener", "shutdown", "close", "post"}
		if got := rec.snapshot(); !slices.Equal(got, want) {
			t.Fatalf("stage order = %v, want %v", got, want)
		}
	})
}

// TestRunWiresConfigServices proves Run builds one shared filesystem and
// project-config stack from the injected HOME and serves the config routes
// through the public handler, writing mode-0600 files.
func TestRunWiresConfigServices(t *testing.T) {
	restore := setShutdownGrace(t, 2*time.Second)
	defer restore()

	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	home := t.TempDir()
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST": host,
		"AGENT_BRIDGE_PORT": port,
		"AGENT_BRIDGE_DB":   filepath.Join(home, "bridge.db"),
		"HOME":              home,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- Run(ctx, getenv, testIO()) }()
	waitForHealth(t, "http://"+addr+"/v1/health")

	client := &http.Client{Timeout: time.Second}
	target := "http://" + addr + "/v1/config/mcp?directory=project"
	req, err := http.NewRequest(http.MethodPut, target, strings.NewReader(`{"s":{"command":"run"}}`))
	if err != nil {
		t.Fatalf("build PUT: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	path := filepath.Join(home, "project", ".agent-bridge", "config", "mcp.json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %04o, want 0600", info.Mode().Perm())
	}

	getResp, err := client.Get(target)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getResp.StatusCode, http.StatusOK)
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
}

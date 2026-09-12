package app

import (
	"context"
	"errors"
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

	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
)

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

// TestRunInternalMockAgentBypassesStartup proves the private mock dispatch runs
// before config.Load, listener creation, and PID-file creation: the HTTP
// environment is deliberately invalid yet the mock error is returned and no
// PID file appears.
func TestRunInternalMockAgentBypassesStartup(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT": "1",
		"AGENT_BRIDGE_PORT":                "not-a-port",
		"AGENT_BRIDGE_HOST":                "not-a-real-host",
		"AGENT_BRIDGE_PID_FILE":            pidPath,
	})

	err := Run(context.Background(), getenv, testIO())
	if err == nil || !strings.Contains(err.Error(), "internal mock agent is not implemented") {
		t.Fatalf("Run() error = %v, want internal mock agent result", err)
	}
	if _, statErr := os.Stat(pidPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("pid file exists after mock run: stat error = %v", statErr)
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

// TestRunServesHealthAndReleasesListener is the end-to-end lifecycle check: the
// server serves health, cancellation returns cleanly, the PID file is removed,
// and the same address can be rebound afterwards.
func TestRunServesHealthAndReleasesListener(t *testing.T) {
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	pidPath := filepath.Join(t.TempDir(), "bridge.pid")
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST":     host,
		"AGENT_BRIDGE_PORT":     port,
		"AGENT_BRIDGE_PID_FILE": pidPath,
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

func (c *fakeCloser) Close() error {
	c.closed.Store(true)
	c.rec.add("listener")
	return nil
}

type fakeShutdowner struct {
	rec          *eventRecorder
	onShutdown   func(context.Context)
	streamClosed func() bool
	block        bool
	closeCalled  atomic.Bool
}

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

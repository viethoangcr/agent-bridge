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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

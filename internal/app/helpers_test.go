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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
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

// testOptions returns production composition seams with the shutdown budget
// overridden to d. Tests override newACPProxy to inject a fake lifecycle owner.
func testOptions(d time.Duration) options {
	opts := defaultOptions()
	opts.shutdownGrace = d
	return opts
}

func testIO() IO {
	return IO{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard}
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

func (f *fakeACPLifecycle) Subscribe(context.Context, string, int64) (acpproxy.Subscription, error) {
	return nil, nil
}

func (f *fakeACPLifecycle) Delete(context.Context, string) error { return nil }

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

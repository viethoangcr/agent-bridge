package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// fakeHeartbeatTicker is a manually advanced SSE keep-alive source.
type fakeHeartbeatTicker struct {
	ch      chan time.Time
	stopped atomic.Bool
}

func newFakeHeartbeatTicker() *fakeHeartbeatTicker {
	return &fakeHeartbeatTicker{ch: make(chan time.Time, 1)}
}

func (f *fakeHeartbeatTicker) C() <-chan time.Time { return f.ch }
func (f *fakeHeartbeatTicker) Stop()               { f.stopped.Store(true) }

func (f *fakeHeartbeatTicker) tick() {
	select {
	case f.ch <- time.Now():
	default:
	}
}

// channelSubscription is a manually driven acpproxy.Subscription: Next yields
// queued events, then blocks until Close or request cancellation.
type channelSubscription struct {
	events    chan acpstore.Event
	closed    chan struct{}
	entered   chan struct{}
	closeOnce sync.Once
	closeHits atomic.Int32
}

func newChannelSubscription() *channelSubscription {
	return &channelSubscription{
		events:  make(chan acpstore.Event),
		closed:  make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
}

func (s *channelSubscription) Next(ctx context.Context) (acpstore.Event, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return acpstore.Event{}, ctx.Err()
	case <-s.closed:
		return acpstore.Event{}, io.EOF
	case event := <-s.events:
		return event, nil
	}
}

func (s *channelSubscription) Close() {
	s.closeOnce.Do(func() {
		s.closeHits.Add(1)
		close(s.closed)
	})
}

// push delivers one event, failing fast if the handler never consumes it.
func (s *channelSubscription) push(t *testing.T, event acpstore.Event) {
	t.Helper()
	select {
	case s.events <- event:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription never consumed the queued event")
	}
}

// fakeSSEProxy satisfies the complete ACPProxy surface with deterministic,
// manually driven behavior.
type fakeSSEProxy struct {
	mu sync.Mutex

	sub          acpproxy.Subscription
	subscribeErr error
	subscribeHit int
	after        int64

	deleteErr     error
	deleteHit     int
	deleting      bool
	deleteStarted chan struct{}
	releaseDelete chan struct{}
}

func (f *fakeSSEProxy) Post(_ context.Context, _ string, _ *string, _ string, _ json.RawMessage) (acpruntime.PostResult, error) {
	f.mu.Lock()
	deleting := f.deleting
	f.mu.Unlock()
	if deleting {
		return acpruntime.PostResult{}, acpproxy.ErrDeleting
	}
	return acpruntime.PostResult{Accepted: true}, nil
}

func (f *fakeSSEProxy) LivePID(string) (int, bool) { return 0, false }

func (f *fakeSSEProxy) Subscribe(_ context.Context, _ string, after int64) (acpproxy.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribeHit++
	f.after = after
	return f.sub, f.subscribeErr
}

func (f *fakeSSEProxy) Delete(_ context.Context, _ string) error {
	f.mu.Lock()
	f.deleteHit++
	f.deleting = true
	started, release := f.deleteStarted, f.releaseDelete
	sub, err := f.sub, f.deleteErr
	f.mu.Unlock()

	if sub != nil {
		sub.Close()
	}
	if started != nil {
		close(started)
	}
	if release != nil {
		<-release
	}
	f.mu.Lock()
	f.deleting = false
	f.mu.Unlock()
	return err
}

func (f *fakeSSEProxy) setDeleteErr(err error) {
	f.mu.Lock()
	f.deleteErr = err
	f.mu.Unlock()
}

// flushRecorder is a synchronized http.ResponseWriter + http.Flusher that
// records frames and signals every flush so streaming tests never sleep.
type flushRecorder struct {
	mu        sync.Mutex
	header    http.Header
	body      bytes.Buffer
	status    int
	flushes   int
	failWrite bool
	flushCh   chan struct{}
}

func newFlushRecorder() *flushRecorder {
	return &flushRecorder{header: make(http.Header), flushCh: make(chan struct{}, 64)}
}

func (w *flushRecorder) Header() http.Header { return w.header }

func (w *flushRecorder) WriteHeader(code int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = code
	}
}

func (w *flushRecorder) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failWrite {
		return 0, errors.New("write failed")
	}
	return w.body.Write(b)
}

func (w *flushRecorder) Flush() {
	w.mu.Lock()
	w.flushes++
	w.mu.Unlock()
	select {
	case w.flushCh <- struct{}{}:
	default:
	}
}

func (w *flushRecorder) snapshot() (int, string, int, http.Header) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, w.body.String(), w.flushes, w.header.Clone()
}

func (w *flushRecorder) waitFlush(t *testing.T) {
	t.Helper()
	select {
	case <-w.flushCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an SSE flush")
	}
}

// newSSEServer builds the public handler with an injected fake heartbeat.
func newSSEServer(t *testing.T, proxy ACPProxy, ticker heartbeatTicker) *Server {
	t.Helper()
	server := NewServer(Dependencies{ACP: proxy})
	if ticker != nil {
		server.newHeartbeatTicker = func() heartbeatTicker { return ticker }
	}
	return server
}

func waitHandlerDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE handler did not return")
	}
}

func assertSSEProblem(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if got := rec.Result().Header.Get("Content-Type"); got != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", got)
	}
	var problem struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem %q: %v", rec.Body.String(), err)
	}
	if problem.Status != want {
		t.Errorf("problem status = %d, want %d", problem.Status, want)
	}
	if problem.Title == "" || problem.Detail == "" {
		t.Errorf("problem missing title/detail: %+v", problem)
	}
}

// sseFrame is the exact expected wire frame for one event.
func sseFrame(seq int64, payload string) string {
	return "event: message\nid: " + strconv.FormatInt(seq, 10) + "\ndata: " + payload + "\n\n"
}

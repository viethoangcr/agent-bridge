package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
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

// fakeSSEProxy satisfies ACPProxy, ACPSubscriber, and ACPDeleter with
// deterministic, manually driven behavior.
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

func TestACPSSEAccept(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  int
	}{
		{name: "missing", value: "", want: http.StatusOK},
		{name: "event stream", value: "text/event-stream", want: http.StatusOK},
		{name: "text wildcard", value: "text/*", want: http.StatusOK},
		{name: "full wildcard", value: "*/*", want: http.StatusOK},
		{name: "comma list", value: "application/json, text/event-stream", want: http.StatusOK},
		{name: "parameters", value: "text/event-stream; charset=utf-8", want: http.StatusOK},
		{name: "incompatible json", value: "application/json", want: http.StatusNotAcceptable},
		{name: "incompatible plain", value: "text/plain", want: http.StatusNotAcceptable},
		{name: "q zero", value: "text/event-stream; q=0", want: http.StatusNotAcceptable},
		{name: "q zero decimal", value: "text/event-stream; q=0.0", want: http.StatusNotAcceptable},
		{name: "malformed q", value: "text/event-stream; q=bogus", want: http.StatusNotAcceptable},
		{name: "q half", value: "text/event-stream; q=0.5", want: http.StatusOK},
		{name: "q one", value: "text/event-stream; q=1", want: http.StatusOK},
		{name: "q zero then acceptable", value: "text/event-stream; q=0, text/event-stream", want: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := newChannelSubscription()
			sub.Close()
			proxy := &fakeSSEProxy{sub: sub}
			server := newSSEServer(t, proxy, newFakeHeartbeatTicker())

			req := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil)
			if tc.value != "" {
				req.Header.Set("Accept", tc.value)
			}
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == http.StatusNotAcceptable {
				assertSSEProblem(t, rec, http.StatusNotAcceptable)
				if proxy.subscribeHit != 0 {
					t.Errorf("Subscribe calls = %d, want 0", proxy.subscribeHit)
				}
			}
		})
	}
}

func TestACPSSELastEventID(t *testing.T) {
	maxInt64 := strconv.FormatInt(math.MaxInt64, 10)
	cases := []struct {
		name    string
		set     bool
		value   string
		want    int
		wantSeq int64
	}{
		{name: "absent", set: false, want: http.StatusOK, wantSeq: 0},
		{name: "zero", set: true, value: "0", want: http.StatusOK, wantSeq: 0},
		{name: "one", set: true, value: "1", want: http.StatusOK, wantSeq: 1},
		{name: "max int64", set: true, value: maxInt64, want: http.StatusOK, wantSeq: math.MaxInt64},
		{name: "negative", set: true, value: "-1", want: http.StatusBadRequest},
		{name: "plus sign", set: true, value: "+1", want: http.StatusBadRequest},
		{name: "blank", set: true, value: "", want: http.StatusBadRequest},
		{name: "max plus one", set: true, value: "9223372036854775808", want: http.StatusBadRequest},
		{name: "huge overflow", set: true, value: "99999999999999999999999", want: http.StatusBadRequest},
		{name: "non decimal", set: true, value: "12a", want: http.StatusBadRequest},
		{name: "fraction", set: true, value: "1.5", want: http.StatusBadRequest},
		{name: "whitespace", set: true, value: " 1", want: http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := newChannelSubscription()
			sub.Close()
			proxy := &fakeSSEProxy{sub: sub}
			server := newSSEServer(t, proxy, newFakeHeartbeatTicker())

			req := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil)
			if tc.set {
				req.Header.Set("Last-Event-ID", tc.value)
			}
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == http.StatusBadRequest {
				assertSSEProblem(t, rec, http.StatusBadRequest)
				if proxy.subscribeHit != 0 {
					t.Errorf("Subscribe calls = %d, want 0", proxy.subscribeHit)
				}
				return
			}
			if proxy.after != tc.wantSeq {
				t.Errorf("after = %d, want %d", proxy.after, tc.wantSeq)
			}
		})
	}

	t.Run("repeated headers", func(t *testing.T) {
		sub := newChannelSubscription()
		sub.Close()
		proxy := &fakeSSEProxy{sub: sub}
		server := newSSEServer(t, proxy, newFakeHeartbeatTicker())

		req := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil)
		req.Header.Add("Last-Event-ID", "1")
		req.Header.Add("Last-Event-ID", "2")
		rec := httptest.NewRecorder()
		server.Handler().ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertSSEProblem(t, rec, http.StatusBadRequest)
		if proxy.subscribeHit != 0 {
			t.Errorf("Subscribe calls = %d, want 0", proxy.subscribeHit)
		}
	})
}

func TestACPSSEFrames(t *testing.T) {
	// Deliberately valid but non-compact JSON with preserved spacing and a
	// lexeme; the SSE writer must emit the stored bytes unchanged.
	const nonCompact = `{"jsonrpc": "2.0",  "id": 1e0, "result": {"x": "<&>"}}`
	const compact = `{"n":2}`

	sub := newChannelSubscription()
	proxy := &fakeSSEProxy{sub: sub}
	ticker := newFakeHeartbeatTicker()
	server := newSSEServer(t, proxy, ticker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := newFlushRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(rec, req)
		close(done)
	}()

	rec.waitFlush(t) // drain the initial header flush
	sub.push(t, acpstore.Event{Seq: 1, Kind: "notification", Payload: []byte(nonCompact)})
	rec.waitFlush(t)
	sub.push(t, acpstore.Event{Seq: 2, Kind: "notification", Payload: []byte(compact)})
	rec.waitFlush(t)
	sub.Close()
	waitHandlerDone(t, done)

	status, body, flushes, header := rec.snapshot()
	if status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if got := header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	if got := header.Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", got)
	}
	want := sseFrame(1, nonCompact) + sseFrame(2, compact)
	if body != want {
		t.Fatalf("body = %q, want exact %q", body, want)
	}
	if flushes < 2 {
		t.Errorf("flushes = %d, want at least 2 (one per event)", flushes)
	}
	if !ticker.stopped.Load() {
		t.Error("heartbeat ticker was not stopped when the stream ended")
	}
}

func TestACPSSEHeartbeat(t *testing.T) {
	sub := newChannelSubscription()
	proxy := &fakeSSEProxy{sub: sub}
	ticker := newFakeHeartbeatTicker()
	server := newSSEServer(t, proxy, ticker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := newFlushRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-sub.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never began streaming")
	}
	rec.waitFlush(t) // drain the initial header flush
	ticker.tick()
	rec.waitFlush(t)
	sub.Close()
	waitHandlerDone(t, done)

	_, body, _, header := rec.snapshot()
	if got := header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}
	if body != ": heartbeat\n\n" {
		t.Fatalf("body = %q, want %q", body, ": heartbeat\n\n")
	}
	if !ticker.stopped.Load() {
		t.Error("heartbeat ticker was not stopped when the stream ended")
	}
}

func TestACPSSEWriteFailure(t *testing.T) {
	sub := newChannelSubscription()
	proxy := &fakeSSEProxy{sub: sub}
	server := newSSEServer(t, proxy, newFakeHeartbeatTicker())

	rec := newFlushRecorder()
	rec.failWrite = true
	req := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil)
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(rec, req)
		close(done)
	}()

	sub.push(t, acpstore.Event{Seq: 1, Payload: []byte(`{"n":1}`)})
	waitHandlerDone(t, done)

	if sub.closeHits.Load() != 1 {
		t.Errorf("subscription close hits = %d, want 1", sub.closeHits.Load())
	}
}

func TestACPSSEUnknownServer(t *testing.T) {
	proxy := &fakeSSEProxy{subscribeErr: acpstore.ErrNotFound}
	server := newSSEServer(t, proxy, newFakeHeartbeatTicker())

	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/acp/ghost", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	assertSSEProblem(t, rec, http.StatusNotFound)
}

func TestACPSSEReplayFromStore(t *testing.T) {
	ctx := t.Context()
	store, err := acpstore.Open(ctx, filepath.Join(t.TempDir(), "sse.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	if _, err := store.CreateServer(ctx, "replay-1", "claude"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	payloads := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}
	for _, payload := range payloads {
		if _, err := store.AppendOutput(ctx, "replay-1", acpstore.Output{
			Kind:    "notification",
			Payload: []byte(payload),
		}); err != nil {
			t.Fatalf("append output: %v", err)
		}
	}

	proxy := acpproxy.New(store, acpruntime.Resolver{}, time.Minute, time.Minute, nil)
	server := newSSEServer(t, proxy, newFakeHeartbeatTicker())

	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/acp/replay-1", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
	}
	want := ""
	for i, payload := range payloads {
		want += sseFrame(int64(i+1), payload)
	}
	if got := rec.Body.String(); got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestACPSSEDisconnectIsolation(t *testing.T) {
	sub := newChannelSubscription()
	proxy := &fakeSSEProxy{sub: sub}
	server := newSSEServer(t, proxy, newFakeHeartbeatTicker())

	ctx, cancel := context.WithCancel(context.Background())
	rec := newFlushRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-sub.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never began streaming")
	}
	cancel()
	waitHandlerDone(t, done)

	if sub.closeHits.Load() != 1 {
		t.Errorf("subscription close hits = %d, want 1", sub.closeHits.Load())
	}
	if proxy.deleteHit != 0 {
		t.Errorf("Delete calls = %d, want 0 (disconnect must not kill the runtime)", proxy.deleteHit)
	}
}

func TestACPDeleteUnknown(t *testing.T) {
	proxy := &fakeSSEProxy{deleteErr: acpstore.ErrNotFound}
	server := newSSEServer(t, proxy, nil)

	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/acp/ghost", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	assertSSEProblem(t, rec, http.StatusNotFound)
	if proxy.deleteHit != 1 {
		t.Errorf("Delete calls = %d, want 1", proxy.deleteHit)
	}
}

func TestACPDeleteSuccess(t *testing.T) {
	proxy := &fakeSSEProxy{}
	server := newSSEServer(t, proxy, nil)

	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/acp/srv-1", nil))

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if proxy.deleteHit != 1 {
		t.Errorf("Delete calls = %d, want 1", proxy.deleteHit)
	}
}

func TestACPDeleteBlocksConcurrentPost(t *testing.T) {
	proxy := &fakeSSEProxy{
		deleteStarted: make(chan struct{}),
		releaseDelete: make(chan struct{}),
	}
	server := newSSEServer(t, proxy, nil)
	handler := server.Handler()

	deleteDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/acp/srv-1", nil))
		deleteDone <- rec
	}()

	select {
	case <-proxy.deleteStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Delete did not start")
	}

	body := `{"jsonrpc":"2.0","method":"session/cancel"}`
	postReq := httptest.NewRequest(http.MethodPost, "/v1/acp/srv-1", strings.NewReader(body))
	postReq.Header.Set("Content-Type", "application/json")
	postRec := httptest.NewRecorder()
	handler.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusConflict {
		t.Fatalf("concurrent POST status = %d, want %d (body %q)", postRec.Code, http.StatusConflict, postRec.Body.String())
	}
	assertSSEProblem(t, postRec, http.StatusConflict)

	close(proxy.releaseDelete)
	delRec := <-deleteDone
	if delRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d (body %q)", delRec.Code, http.StatusNoContent, delRec.Body.String())
	}
}

func TestACPDeletePruneFailureRetry(t *testing.T) {
	sub := newChannelSubscription()
	proxy := &fakeSSEProxy{sub: sub, deleteErr: errors.New("prune failed")}
	server := newSSEServer(t, proxy, newFakeHeartbeatTicker())
	handler := server.Handler()

	// Open a stream and ensure the handler reached it before deletion.
	sseCtx, sseCancel := context.WithCancel(context.Background())
	defer sseCancel()
	rec := newFlushRecorder()
	sseReq := httptest.NewRequest(http.MethodGet, "/v1/acp/srv-1", nil).WithContext(sseCtx)
	sseDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, sseReq)
		close(sseDone)
	}()
	select {
	case <-sub.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never began streaming")
	}

	// First DELETE: kill/wait succeeded but the durable prune failed.
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodDelete, "/v1/acp/srv-1", nil))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first DELETE status = %d, want %d (body %q)", first.Code, http.StatusInternalServerError, first.Body.String())
	}
	assertSSEProblem(t, first, http.StatusInternalServerError)

	// The attempt closed the stream; it must not be reopened.
	waitHandlerDone(t, sseDone)
	if sub.closeHits.Load() != 1 {
		t.Errorf("subscription close hits = %d, want 1", sub.closeHits.Load())
	}

	// A later DELETE retries prune and succeeds without restarting a process.
	proxy.setDeleteErr(nil)
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodDelete, "/v1/acp/srv-1", nil))
	if second.Code != http.StatusNoContent {
		t.Fatalf("second DELETE status = %d, want %d (body %q)", second.Code, http.StatusNoContent, second.Body.String())
	}
}

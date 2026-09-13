package httpapi

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

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

package app

import (
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/lifecycle"
)

// TestDrainChannelDrivenStageOrder gates each stage on a channel and proves the
// next stage cannot start until the previous one completes: the listener is
// closed, then the pre-drain hook runs to completion, then http.Server.Shutdown
// is entered, then post-drain runs. This is the exact staged contract, observed
// rather than inferred from completion order.
func TestDrainChannelDrivenStageOrder(t *testing.T) {
	restore := setShutdownGrace(t, 3*time.Second)
	defer restore()

	rec := &eventRecorder{}
	preEntered := make(chan struct{})
	releasePre := make(chan struct{})
	shutdownCalled := make(chan struct{})
	postCalled := make(chan struct{})

	listener := &fakeCloser{rec: rec}
	server := &fakeShutdowner{
		rec: rec,
		onShutdown: func(context.Context) {
			close(shutdownCalled)
		},
	}

	pre := &lifecycle.Registry{}
	if err := pre.Add("streams", func(context.Context) error {
		close(preEntered)
		<-releasePre
		rec.add("pre")
		return nil
	}); err != nil {
		t.Fatalf("add pre hook: %v", err)
	}
	post := &lifecycle.Registry{}
	if err := post.Add("database", func(context.Context) error {
		rec.add("post")
		close(postCalled)
		return nil
	}); err != nil {
		t.Fatalf("add post hook: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drained := make(chan struct{})
	go func() {
		drain(ctx, listener, server, pre, post, discardLogger(), "")
		close(drained)
	}()

	select {
	case <-preEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("pre-drain hook never entered")
	}
	// The listener must already be closed before pre-drain runs.
	if !listener.closed.Load() {
		t.Fatal("listener was not closed before pre-drain entered")
	}
	// While pre-drain is blocked, http Shutdown and post-drain must not run.
	select {
	case <-shutdownCalled:
		t.Fatal("http Shutdown ran before pre-drain completed")
	default:
	}
	select {
	case <-postCalled:
		t.Fatal("post-drain ran before http Shutdown")
	default:
	}

	close(releasePre)
	select {
	case <-shutdownCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("http Shutdown never ran after pre-drain completed")
	}
	select {
	case <-postCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("post-drain never ran after http Shutdown")
	}
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drain never returned")
	}

	want := []string{"listener", "pre", "shutdown", "post"}
	if got := rec.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("stage order = %v, want %v", got, want)
	}
}

// TestRunOpenSSEExitsBeforeHTTPDrain proves an open SSE stream cannot block
// shutdown: the pre-drain ACP hook closes the subscription before
// http.Server.Shutdown is called, so Run returns within the budget and the
// client stream observes closure rather than hanging.
func TestRunOpenSSEExitsBeforeHTTPDrain(t *testing.T) {
	restore := setShutdownGrace(t, 5*time.Second)
	defer restore()

	const token = "sse-shutdown-token"
	addr := freeAddress(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	dir := t.TempDir()
	getenv := testGetenv(map[string]string{
		"AGENT_BRIDGE_HOST":        host,
		"AGENT_BRIDGE_PORT":        port,
		"AGENT_BRIDGE_TOKEN":       token,
		"AGENT_BRIDGE_DB":          filepath.Join(dir, "bridge.db"),
		"AGENT_BRIDGE_PID_FILE":    filepath.Join(dir, "bridge.pid"),
		"AGENT_BRIDGE_IDLE_TTL_MS": "0",
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- Run(ctx, getenv, testIO()) }()
	waitForHealthAuth(t, "http://"+addr+"/v1/health", token)

	// Create a live runtime through the private mock agent so the SSE stream
	// has a server to subscribe to.
	initReq, err := http.NewRequest(http.MethodPost,
		"http://"+addr+"/v1/acp/sse?agent=mock",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`))
	if err != nil {
		t.Fatalf("build initialize: %v", err)
	}
	initReq.Header.Set("Content-Type", "application/json")
	initReq.Header.Set("Authorization", "Bearer "+token)
	initResp, err := (&http.Client{Timeout: 5 * time.Second}).Do(initReq)
	if err != nil {
		cancel()
		t.Fatalf("initialize: %v", err)
	}
	_, _ = io.Copy(io.Discard, initResp.Body)
	_ = initResp.Body.Close()
	if initResp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("initialize status = %d, want 200", initResp.StatusCode)
	}

	// Open the SSE stream and wait until response headers prove it is live.
	sseCtx, sseCancel := context.WithCancel(context.Background())
	defer sseCancel()
	sseReq, err := http.NewRequestWithContext(sseCtx, http.MethodGet, "http://"+addr+"/v1/acp/sse", nil)
	if err != nil {
		cancel()
		t.Fatalf("build SSE request: %v", err)
	}
	sseReq.Header.Set("Accept", "text/event-stream")
	sseReq.Header.Set("Authorization", "Bearer "+token)
	sseOpen := make(chan *http.Response, 1)
	go func() {
		resp, doErr := (&http.Client{}).Do(sseReq)
		if doErr != nil {
			sseOpen <- nil
			return
		}
		sseOpen <- resp
	}()

	var sseResp *http.Response
	select {
	case sseResp = <-sseOpen:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("SSE response headers never arrived")
	}
	if sseResp == nil {
		cancel()
		t.Fatal("SSE request failed")
	}
	if sseResp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("SSE status = %d, want 200", sseResp.StatusCode)
	}
	if ct := sseResp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		cancel()
		t.Fatalf("SSE Content-Type = %q, want text/event-stream", ct)
	}

	// Drain the body in the background; its closure is the observable signal.
	streamClosed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, sseResp.Body)
		_ = sseResp.Body.Close()
		close(streamClosed)
	}()

	start := time.Now()
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v, want nil after cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return; open SSE blocked shutdown")
	}
	if elapsed := time.Since(start); elapsed >= 4*time.Second {
		t.Fatalf("shutdown took %s; pre-drain did not close SSE before HTTP drain", elapsed)
	}

	select {
	case <-streamClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("open SSE client never observed closure")
	}
}

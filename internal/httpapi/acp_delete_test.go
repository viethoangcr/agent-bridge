package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

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

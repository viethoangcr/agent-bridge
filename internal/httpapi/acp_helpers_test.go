package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/httpapi"
)

const (
	validNotification = `{"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"s"}}`
	validInitialize   = `{"jsonrpc":"2.0","method":"initialize","id":"init-1","params":{}}`
	rawInitializeResp = `{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":1}}`
)

type fakeACP struct {
	reply       acpruntime.PostResult
	err         error
	stderr      string
	calls       int
	gotServerID string
	gotAgent    *string
	gotMethod   string
	gotPayload  json.RawMessage

	// live/pid drive the LivePID seam consulted for status PIDs.
	live bool
	pid  int
}

func (f *fakeACP) Post(_ context.Context, serverID string, agent *string, method string, payload json.RawMessage) (acpruntime.PostResult, error) {
	f.calls++
	f.gotServerID = serverID
	f.gotAgent = agent
	f.gotMethod = method
	f.gotPayload = append(json.RawMessage(nil), payload...)
	return f.reply, f.err
}

func (f *fakeACP) LivePID(string) (int, bool) { return f.pid, f.live }

func (f *fakeACP) Stderr(string) string { return f.stderr }

func (f *fakeACP) Subscribe(context.Context, string, int64) (acpproxy.Subscription, error) {
	return nil, errors.New("unexpected Subscribe")
}

func (f *fakeACP) Delete(context.Context, string) error {
	return errors.New("unexpected Delete")
}

func acpHandler(t *testing.T, proxy httpapi.ACPProxy) http.Handler {
	t.Helper()
	return httpapi.NewServer(httpapi.Dependencies{ACP: proxy}).Handler()
}

func postACP(t *testing.T, handler http.Handler, target, contentType, accept, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func newACPStore(t *testing.T) *acpstore.Store {
	t.Helper()
	store, err := acpstore.Open(t.Context(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store
}

func acpStoreHandler(t *testing.T, proxy httpapi.ACPProxy, store *acpstore.Store) http.Handler {
	t.Helper()
	return httpapi.NewServer(httpapi.Dependencies{ACP: proxy, ACPStore: store}).Handler()
}

func getACP(t *testing.T, handler http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func appendHTTPEvent(t *testing.T, store *acpstore.Store, serverID, payload string, sessionID *string) {
	t.Helper()
	if _, err := store.AppendOutput(t.Context(), serverID, acpstore.Output{
		Kind:      "notification",
		Payload:   []byte(payload),
		SessionID: sessionID,
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}
}

func acpStrPtr(s string) *string { return &s }

type acpEventBody struct {
	Seq         int64           `json:"seq"`
	Kind        string          `json:"kind"`
	Method      *string         `json:"method"`
	Payload     json.RawMessage `json:"payload"`
	SessionID   *string         `json:"sessionId"`
	CreatedAtMs int64           `json:"createdAtMs"`
}

type acpEventsBody struct {
	Events []acpEventBody `json:"events"`
}

func decodeACPEvents(t *testing.T, rec *httptest.ResponseRecorder) acpEventsBody {
	t.Helper()
	var body acpEventsBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode events body %q: %v", rec.Body.String(), err)
	}
	return body
}

func eventSeqs(events []acpEventBody) []int64 {
	seqs := make([]int64, 0, len(events))
	for _, event := range events {
		seqs = append(seqs, event.Seq)
	}
	return seqs
}

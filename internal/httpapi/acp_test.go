package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
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

// fakeACP is a deterministic ACPProxy that records the dispatch it received.
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

// LivePID reports the fake's configured live PID ownership.
func (f *fakeACP) LivePID(string) (int, bool) { return f.pid, f.live }

// Stderr makes fakeACP satisfy the optional stderr provider used for 502
// responses.
func (f *fakeACP) Stderr(string) string { return f.stderr }

// acpHandler builds the public handler with the fake proxy injected.
func acpHandler(t *testing.T, proxy httpapi.ACPProxy) http.Handler {
	t.Helper()
	return httpapi.NewServer(httpapi.Dependencies{ACP: proxy}).Handler()
}

// postACP drives one POST through the public assembly path.
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

func TestACPPostServerID(t *testing.T) {
	t.Run("valid identifiers", func(t *testing.T) {
		for _, id := range []string{"a", "A", "0", "._-", "Server-1.2_3", strings.Repeat("a", 128)} {
			t.Run(id, func(t *testing.T) {
				proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
				rec := postACP(t, acpHandler(t, proxy), "/v1/acp/"+id, "application/json", "", validNotification)

				if rec.Code != http.StatusAccepted {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusAccepted, rec.Body.String())
				}
				if proxy.gotServerID != id {
					t.Errorf("serverID = %q, want %q", proxy.gotServerID, id)
				}
			})
		}
	})

	t.Run("invalid identifiers", func(t *testing.T) {
		cases := []struct{ name, target string }{
			{name: "empty", target: "/v1/acp/"},
			{name: "decoded slash", target: "/v1/acp/a%2Fb"},
			{name: "non ascii", target: "/v1/acp/%C3%A9"},
			{name: "space", target: "/v1/acp/a%20b"},
			{name: "over 128 bytes", target: "/v1/acp/" + strings.Repeat("a", 129)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
				rec := postACP(t, acpHandler(t, proxy), tc.target, "application/json", "", validNotification)

				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				assertProblem(t, rec, http.StatusBadRequest)
				if proxy.calls != 0 {
					t.Errorf("proxy calls = %d, want 0", proxy.calls)
				}
			})
		}
	})
}

func TestACPPostContentType(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    int
		problem bool
	}{
		{name: "missing", value: "", want: http.StatusUnsupportedMediaType, problem: true},
		{name: "plain", value: "text/plain", want: http.StatusUnsupportedMediaType, problem: true},
		{name: "wrong subtype", value: "application/jsonx", want: http.StatusUnsupportedMediaType, problem: true},
		{name: "malformed", value: "application/", want: http.StatusUnsupportedMediaType, problem: true},
		{name: "exact", value: "application/json", want: http.StatusAccepted},
		{name: "uppercase", value: "APPLICATION/JSON", want: http.StatusAccepted},
		{name: "parameters", value: "application/json; charset=utf-8", want: http.StatusAccepted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
			rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", tc.value, "", validNotification)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if tc.problem {
				assertProblem(t, rec, tc.want)
			}
			if tc.want == http.StatusUnsupportedMediaType && proxy.calls != 0 {
				t.Errorf("proxy calls = %d, want 0", proxy.calls)
			}
		})
	}
}

func TestACPPostAccept(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  int
	}{
		{name: "missing", value: "", want: http.StatusAccepted},
		{name: "exact", value: "application/json", want: http.StatusAccepted},
		{name: "subtype wildcard", value: "application/*", want: http.StatusAccepted},
		{name: "full wildcard", value: "*/*", want: http.StatusAccepted},
		{name: "comma list", value: "text/plain, application/json", want: http.StatusAccepted},
		{name: "parameters", value: "application/json; q=0.9", want: http.StatusAccepted},
		{name: "wildcard with parameters", value: "application/*; q=0.1", want: http.StatusAccepted},
		{name: "incompatible plain", value: "text/plain", want: http.StatusNotAcceptable},
		{name: "incompatible subtype", value: "application/xml", want: http.StatusNotAcceptable},
		{name: "incompatible event stream", value: "text/event-stream", want: http.StatusNotAcceptable},
		{name: "q zero", value: "application/json; q=0", want: http.StatusNotAcceptable},
		{name: "q zero decimal", value: "application/json; q=0.0", want: http.StatusNotAcceptable},
		{name: "wildcard q zero", value: "*/*; q=0", want: http.StatusNotAcceptable},
		{name: "malformed q", value: "application/json; q=bogus", want: http.StatusNotAcceptable},
		{name: "q above one", value: "application/json; q=1.5", want: http.StatusNotAcceptable},
		{name: "q half", value: "application/json; q=0.5", want: http.StatusAccepted},
		{name: "q one", value: "application/json; q=1", want: http.StatusAccepted},
		{name: "q zero then acceptable", value: "application/json; q=0, application/json", want: http.StatusAccepted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
			rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", tc.value, validNotification)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == http.StatusNotAcceptable {
				assertProblem(t, rec, tc.want)
				if proxy.calls != 0 {
					t.Errorf("proxy calls = %d, want 0", proxy.calls)
				}
			}
		})
	}
}

func TestACPPostBodyLimit(t *testing.T) {
	const limit = 10 << 20
	base := validNotification
	exact := base + strings.Repeat(" ", limit-len(base))

	t.Run("exact limit accepted", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", exact)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusAccepted, rec.Body.String())
		}
		if string(proxy.gotPayload) != base {
			t.Errorf("payload = %q, want %q", proxy.gotPayload, base)
		}
	})

	t.Run("over limit is 413", func(t *testing.T) {
		over := exact + " "
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", over)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusRequestEntityTooLarge)
		if proxy.calls != 0 {
			t.Errorf("proxy calls = %d, want 0", proxy.calls)
		}
	})
}

func TestACPPostInvalidEnvelope(t *testing.T) {
	overID := `"` + strings.Repeat("a", 127) + `"` // 129 raw bytes with quotes
	cases := []struct {
		name string
		body string
	}{
		{name: "malformed", body: `{`},
		{name: "trailing value", body: `{"jsonrpc":"2.0","method":"x"} {"jsonrpc":"2.0"}`},
		{name: "empty body", body: ``},
		{name: "batch array", body: `[]`},
		{name: "batch object", body: `[{"jsonrpc":"2.0","method":"x"}]`},
		{name: "scalar string", body: `"x"`},
		{name: "scalar number", body: `1`},
		{name: "null", body: `null`},
		{name: "missing jsonrpc", body: `{"method":"x"}`},
		{name: "wrong jsonrpc", body: `{"jsonrpc":"1.0","method":"x"}`},
		{name: "null id", body: `{"jsonrpc":"2.0","method":"x","id":null}`},
		{name: "object id", body: `{"jsonrpc":"2.0","method":"x","id":{}}`},
		{name: "boolean id", body: `{"jsonrpc":"2.0","method":"x","id":true}`},
		{name: "empty method", body: `{"jsonrpc":"2.0","method":""}`},
		{name: "neither method nor id", body: `{"jsonrpc":"2.0"}`},
		{name: "response without result or error", body: `{"jsonrpc":"2.0","id":1}`},
		{name: "response with both result and error", body: `{"jsonrpc":"2.0","id":1,"result":{},"error":{}}`},
		{name: "raw id token over 128 bytes", body: `{"jsonrpc":"2.0","method":"x","id":` + overID + `}`},
		{name: "numeric exponent over limit", body: `{"jsonrpc":"2.0","method":"x","id":1e1000001}`},
		{name: "invalid utf8", body: "{\"jsonrpc\":\"2.0\",\"method\":\"x\xff\"}"},
		{name: "session id over 1024 bytes", body: `{"jsonrpc":"2.0","method":"session/prompt","id":1,"params":{"sessionId":"` + strings.Repeat("a", 1025) + `"}}`},
		{name: "lifecycle cwd over 4096 bytes", body: `{"jsonrpc":"2.0","method":"session/new","id":1,"params":{"cwd":"` + strings.Repeat("a", 4097) + `"}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
			rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", tc.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			assertProblem(t, rec, http.StatusBadRequest)
			if proxy.calls != 0 {
				t.Errorf("proxy calls = %d, want 0", proxy.calls)
			}
		})
	}
}

func TestACPPostValidEnvelopes(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Response: json.RawMessage(rawInitializeResp)}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", validInitialize)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		if proxy.gotMethod != "initialize" {
			t.Errorf("method = %q, want initialize", proxy.gotMethod)
		}
	})

	t.Run("notification", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", validNotification)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
		}
		if proxy.gotMethod != "session/cancel" {
			t.Errorf("method = %q, want session/cancel", proxy.gotMethod)
		}
	})

	t.Run("client response", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		body := `{"jsonrpc":"2.0","id":1,"result":{}}`
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", body)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
		}
		if proxy.gotMethod != "" {
			t.Errorf("method = %q, want empty for a client response", proxy.gotMethod)
		}
	})

	t.Run("original raw object is passed through unchanged", func(t *testing.T) {
		raw := `{"jsonrpc":"2.0", "method":"initialize", "id":1e0, "params":{ "a" : [ 1 , 2 ] }}`
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", raw)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
		}
		if string(proxy.gotPayload) != raw {
			t.Errorf("payload = %q, want %q", proxy.gotPayload, raw)
		}
	})
}

func TestACPPostAgent(t *testing.T) {
	t.Run("omitted first agent is 400", func(t *testing.T) {
		proxy := &fakeACP{err: acpproxy.ErrMissingAgent}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", validInitialize)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusBadRequest)
		if proxy.gotAgent != nil {
			t.Errorf("agent = %q, want nil", *proxy.gotAgent)
		}
	})

	t.Run("supplied agent is forwarded", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1?agent=claude", "application/json", "", validInitialize)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
		}
		if proxy.gotAgent == nil || *proxy.gotAgent != "claude" {
			t.Fatalf("agent = %v, want claude", proxy.gotAgent)
		}
	})

	t.Run("unknown agent is 400", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1?agent=bogus", "application/json", "", validInitialize)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusBadRequest)
		if proxy.calls != 0 {
			t.Errorf("proxy calls = %d, want 0", proxy.calls)
		}
	})

	t.Run("repeated agent is 400", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1?agent=claude&agent=codex", "application/json", "", validInitialize)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		assertProblem(t, rec, http.StatusBadRequest)
		if proxy.calls != 0 {
			t.Errorf("proxy calls = %d, want 0", proxy.calls)
		}
	})

	t.Run("empty agent is 400", func(t *testing.T) {
		proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1?agent=", "application/json", "", validInitialize)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
		if proxy.calls != 0 {
			t.Errorf("proxy calls = %d, want 0", proxy.calls)
		}
	})

	t.Run("conflicting agent is 409", func(t *testing.T) {
		proxy := &fakeACP{err: acpproxy.ErrAgentConflict}
		rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1?agent=codex", "application/json", "", validInitialize)

		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusConflict, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusConflict)
	})
}

func TestACPPostErrorMapping(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		stderr string
		want   int
	}{
		{name: "invalid envelope", err: acpruntime.ErrInvalidEnvelope, want: http.StatusBadRequest},
		{name: "missing agent", err: acpproxy.ErrMissingAgent, want: http.StatusBadRequest},
		{name: "agent conflict", err: acpproxy.ErrAgentConflict, want: http.StatusConflict},
		{name: "deleting", err: acpproxy.ErrDeleting, want: http.StatusConflict},
		{name: "reinitialize", err: acpproxy.ErrReinitialize, want: http.StatusConflict},
		{name: "deleted", err: acpstore.ErrDeleted, want: http.StatusConflict},
		{name: "store conflict", err: acpstore.ErrConflict, want: http.StatusConflict},
		{name: "duplicate id", err: acpruntime.ErrDuplicateID, want: http.StatusConflict},
		{name: "proxy closed", err: acpproxy.ErrClosed, want: http.StatusServiceUnavailable},
		{name: "runtime capacity", err: acpproxy.ErrRuntimeCapacity, want: http.StatusTooManyRequests},
		{name: "correlation capacity", err: acpruntime.ErrCapacity, want: http.StatusTooManyRequests},
		{name: "request timeout", err: acpruntime.ErrRequestTimeout, want: http.StatusGatewayTimeout},
		{name: "process exited", err: acpruntime.ErrExited, stderr: "agent exploded\n", want: http.StatusBadGateway},
		{name: "write failure", err: acpruntime.ErrWrite, stderr: "agent exploded\n", want: http.StatusBadGateway},
		{name: "generic process failure", err: errors.New("spawn failed"), want: http.StatusBadGateway},
		{name: "persistence failure", err: acpruntime.ErrPersistence, want: http.StatusInsufficientStorage},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxy := &fakeACP{err: tc.err, stderr: tc.stderr, reply: acpruntime.PostResult{}}
			rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", validNotification)

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
			assertProblem(t, rec, tc.want)

			var problem map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
				t.Fatalf("decode problem: %v", err)
			}
			gotStderr, hasStderr := problem["agentStderr"]
			if tc.want == http.StatusBadGateway && tc.stderr != "" {
				if !hasStderr || gotStderr != tc.stderr {
					t.Errorf("agentStderr = %v (present %v), want %q", gotStderr, hasStderr, tc.stderr)
				}
			} else if hasStderr {
				t.Errorf("unexpected agentStderr %v for status %d", gotStderr, tc.want)
			}
		})
	}
}

func TestACPPostResponseBytes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "normal", raw: `{"jsonrpc": "2.0", "id": 1e0, "result": {"x": "<&>"}}`},
		{name: "jsonrpc error", raw: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"auth required"}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxy := &fakeACP{reply: acpruntime.PostResult{Response: json.RawMessage(tc.raw)}}
			rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", validNotification)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}
			if got := rec.Result().Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			if got := rec.Body.String(); got != tc.raw {
				t.Errorf("body = %q, want exact %q", got, tc.raw)
			}
		})
	}
}

func TestACPPostAccepted(t *testing.T) {
	proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
	rec := postACP(t, acpHandler(t, proxy), "/v1/acp/s1", "application/json", "", validNotification)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if got := rec.Body.String(); got != "" {
		t.Errorf("body = %q, want empty", got)
	}
}

func TestACPPostAuth(t *testing.T) {
	proxy := &fakeACP{reply: acpruntime.PostResult{Accepted: true}}
	handler := httpapi.NewServer(httpapi.Dependencies{Token: "secret", ACP: proxy}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/acp/s1", strings.NewReader(validNotification))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if proxy.calls != 0 {
		t.Errorf("proxy calls = %d, want 0", proxy.calls)
	}
}

// newACPStore opens an isolated durable store for the state-endpoint tests.
func newACPStore(t *testing.T) *acpstore.Store {
	t.Helper()
	store, err := acpstore.Open(t.Context(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store
}

// acpStoreHandler builds the public handler with durable state injected.
func acpStoreHandler(t *testing.T, proxy httpapi.ACPProxy, store *acpstore.Store) http.Handler {
	t.Helper()
	return httpapi.NewServer(httpapi.Dependencies{ACP: proxy, ACPStore: store}).Handler()
}

// getACP drives one GET through the public assembly path.
func getACP(t *testing.T, handler http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

// appendHTTPEvent persists one notification event for the state tests.
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

// acpEventBody mirrors the documented events DTO.
type acpEventBody struct {
	Seq         int64           `json:"seq"`
	Kind        string          `json:"kind"`
	Method      *string         `json:"method"`
	Payload     json.RawMessage `json:"payload"`
	SessionID   *string         `json:"sessionId"`
	CreatedAtMs int64           `json:"createdAtMs"`
}

// acpEventsBody mirrors the documented events envelope.
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

func TestACPList(t *testing.T) {
	ctx := t.Context()
	store := newACPStore(t)
	handler := acpStoreHandler(t, &fakeACP{}, store)

	t.Run("empty store returns an empty array", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got := string(body["servers"]); got != "[]" {
			t.Fatalf("servers = %s, want []", got)
		}
	})

	t.Run("sorted by server ID with exactly five fields", func(t *testing.T) {
		for _, id := range []string{"zeta", "alpha", "mid"} {
			if _, err := store.CreateServer(ctx, id, "claude"); err != nil {
				t.Fatalf("create %s: %v", id, err)
			}
		}
		rec := getACP(t, handler, "/v1/acp")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Servers []map[string]any `json:"servers"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		want := []string{"alpha", "mid", "zeta"}
		if len(body.Servers) != len(want) {
			t.Fatalf("servers = %d, want %d", len(body.Servers), len(want))
		}
		for i, srv := range body.Servers {
			if srv["serverId"] != want[i] {
				t.Errorf("servers[%d] = %v, want %q", i, srv["serverId"], want[i])
			}
			if len(srv) != 5 {
				t.Errorf("server %v has %d fields, want exactly 5", srv, len(srv))
			}
			for _, field := range []string{"agent", "status", "createdAtMs", "updatedAtMs"} {
				if _, ok := srv[field]; !ok {
					t.Errorf("server %v missing %q", srv, field)
				}
			}
		}
	})
}

func TestACPStatus(t *testing.T) {
	ctx := t.Context()
	store := newACPStore(t)

	t.Run("unknown server is 404", func(t *testing.T) {
		handler := acpStoreHandler(t, &fakeACP{}, store)
		rec := getACP(t, handler, "/v1/acp/ghost/status")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusNotFound)
	})

	if _, err := store.CreateServer(ctx, "status-1", "claude"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := store.SetLive(ctx, "status-1", 1111); err != nil {
		t.Fatalf("set live: %v", err)
	}
	for _, sessionID := range []string{"s2", "s1"} {
		if _, err := store.AppendOutput(ctx, "status-1", acpstore.Output{
			Kind:     "response",
			Payload:  []byte(`{"ok":true}`),
			Mutation: &acpstore.SessionMutation{Lifecycle: "new", SessionID: sessionID, CWD: "/work"},
		}); err != nil {
			t.Fatalf("append session event: %v", err)
		}
	}

	type statusBody struct {
		ServerID     string   `json:"serverId"`
		Agent        string   `json:"agent"`
		Status       string   `json:"status"`
		CreatedAtMs  int64    `json:"createdAtMs"`
		LastEventSeq int64    `json:"lastEventSeq"`
		SessionIDs   []string `json:"sessionIds"`
		PID          *int     `json:"pid"`
		UpdatedAtMs  int64    `json:"updatedAtMs"`
	}

	t.Run("durable fields with sorted sessions and no PID when not live", func(t *testing.T) {
		handler := acpStoreHandler(t, &fakeACP{live: false}, store)
		rec := getACP(t, handler, "/v1/acp/status-1/status")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body statusBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.ServerID != "status-1" {
			t.Errorf("serverId = %q, want status-1", body.ServerID)
		}
		if body.Agent != "claude" {
			t.Errorf("agent = %q, want claude", body.Agent)
		}
		if body.Status != string(acpstore.StatusIdle) {
			t.Errorf("status = %q, want idle", body.Status)
		}
		if body.LastEventSeq != 2 {
			t.Errorf("lastEventSeq = %d, want 2", body.LastEventSeq)
		}
		if body.CreatedAtMs <= 0 || body.UpdatedAtMs <= 0 {
			t.Errorf("timestamps = %d/%d, want positive", body.CreatedAtMs, body.UpdatedAtMs)
		}
		if !slices.Equal(body.SessionIDs, []string{"s1", "s2"}) {
			t.Errorf("sessionIds = %v, want [s1 s2]", body.SessionIDs)
		}
		if body.PID != nil {
			t.Errorf("pid = %d, want omitted when not live", *body.PID)
		}
	})

	t.Run("PID present only when LivePID confirms", func(t *testing.T) {
		handler := acpStoreHandler(t, &fakeACP{live: true, pid: 4321}, store)
		rec := getACP(t, handler, "/v1/acp/status-1/status")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body statusBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.PID == nil || *body.PID != 4321 {
			t.Fatalf("pid = %v, want 4321", body.PID)
		}
	})
}

func TestACPEvents(t *testing.T) {
	ctx := t.Context()
	store := newACPStore(t)
	handler := acpStoreHandler(t, &fakeACP{}, store)

	if _, err := store.CreateServer(ctx, "events-1", "claude"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	for _, payload := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		appendHTTPEvent(t, store, "events-1", payload, acpStrPtr("s1"))
	}

	t.Run("defaults after 0 limit 100 ascending", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp/events-1/events")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusOK, rec.Body.String())
		}
		body := decodeACPEvents(t, rec)
		if got := eventSeqs(body.Events); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Fatalf("seqs = %v, want [1 2 3]", got)
		}
		if body.Events[0].Kind != "notification" {
			t.Errorf("kind = %q, want notification", body.Events[0].Kind)
		}
		if body.Events[0].SessionID == nil || *body.Events[0].SessionID != "s1" {
			t.Errorf("sessionId = %v, want s1", body.Events[0].SessionID)
		}
		if body.Events[0].CreatedAtMs <= 0 {
			t.Errorf("createdAtMs = %d, want positive", body.Events[0].CreatedAtMs)
		}
	})

	t.Run("after is exclusive", func(t *testing.T) {
		body := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-1/events?after=1"))
		if got := eventSeqs(body.Events); !slices.Equal(got, []int64{2, 3}) {
			t.Fatalf("seqs = %v, want [2 3]", got)
		}
	})

	t.Run("limit bounds", func(t *testing.T) {
		cases := []struct {
			name  string
			query string
			want  int
			seqs  []int64
		}{
			{name: "one", query: "?limit=1", want: http.StatusOK, seqs: []int64{1}},
			{name: "max", query: "?limit=1000", want: http.StatusOK, seqs: []int64{1, 2, 3}},
			{name: "zero", query: "?limit=0", want: http.StatusBadRequest},
			{name: "over max", query: "?limit=1001", want: http.StatusBadRequest},
			{name: "negative", query: "?limit=-1", want: http.StatusBadRequest},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := getACP(t, handler, "/v1/acp/events-1/events"+tc.query)
				if rec.Code != tc.want {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
				}
				if tc.want == http.StatusBadRequest {
					assertProblem(t, rec, tc.want)
					return
				}
				body := decodeACPEvents(t, rec)
				if got := eventSeqs(body.Events); !slices.Equal(got, tc.seqs) {
					t.Fatalf("seqs = %v, want %v", got, tc.seqs)
				}
			})
		}
	})

	t.Run("order ascending and descending", func(t *testing.T) {
		asc := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-1/events?order=asc"))
		desc := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-1/events?order=desc"))
		if got := eventSeqs(asc.Events); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Fatalf("asc seqs = %v, want [1 2 3]", got)
		}
		if got := eventSeqs(desc.Events); !slices.Equal(got, []int64{3, 2, 1}) {
			t.Fatalf("desc seqs = %v, want [3 2 1]", got)
		}
	})

	t.Run("after parsing through math.MaxInt64", func(t *testing.T) {
		cases := []struct {
			name  string
			value string
			want  int
		}{
			{name: "max int64", value: "9223372036854775807", want: http.StatusOK},
			{name: "max int64 plus one", value: "9223372036854775808", want: http.StatusBadRequest},
			{name: "negative", value: "-1", want: http.StatusBadRequest},
			{name: "plus sign", value: "+1", want: http.StatusBadRequest},
			{name: "non decimal", value: "1a", want: http.StatusBadRequest},
			{name: "fraction", value: "1.0", want: http.StatusBadRequest},
			{name: "empty", value: "", want: http.StatusBadRequest},
			{name: "whitespace", value: "%20", want: http.StatusBadRequest},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := getACP(t, handler, "/v1/acp/events-1/events?after="+tc.value)
				if rec.Code != tc.want {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
				}
				if tc.want == http.StatusBadRequest {
					assertProblem(t, rec, tc.want)
					return
				}
				if body := decodeACPEvents(t, rec); len(body.Events) != 0 {
					t.Fatalf("events = %v, want none past math.MaxInt64", body.Events)
				}
			})
		}
	})

	t.Run("repeated empty and unknown query keys are rejected", func(t *testing.T) {
		cases := []struct {
			name  string
			query string
		}{
			{name: "repeated after", query: "after=1&after=2"},
			{name: "repeated limit", query: "limit=1&limit=2"},
			{name: "repeated order", query: "order=asc&order=desc"},
			{name: "repeated session", query: "sessionId=s1&sessionId=s2"},
			{name: "unknown key", query: "bogus=1"},
			{name: "invalid order", query: "order=sideways"},
			{name: "empty order", query: "order="},
			{name: "empty session", query: "sessionId="},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := getACP(t, handler, "/v1/acp/events-1/events?"+tc.query)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				assertProblem(t, rec, http.StatusBadRequest)
			})
		}
	})

	t.Run("session filter", func(t *testing.T) {
		if _, err := store.CreateServer(ctx, "events-2", "claude"); err != nil {
			t.Fatalf("create server: %v", err)
		}
		if _, err := store.AppendOutput(ctx, "events-2", acpstore.Output{
			Kind:      "response",
			Payload:   []byte(`{"ok":true}`),
			SessionID: acpStrPtr("s2"),
			Mutation:  &acpstore.SessionMutation{Lifecycle: "new", SessionID: "s2", CWD: "/work"},
		}); err != nil {
			t.Fatalf("create session: %v", err)
		}
		appendHTTPEvent(t, store, "events-2", `{"n":4}`, acpStrPtr("s2"))
		appendHTTPEvent(t, store, "events-2", `{"n":5}`, acpStrPtr("s1"))

		body := decodeACPEvents(t, getACP(t, handler, "/v1/acp/events-2/events?sessionId=s2"))
		if got := eventSeqs(body.Events); !slices.Equal(got, []int64{1, 2}) {
			t.Fatalf("seqs = %v, want [1 2]", got)
		}
	})

	t.Run("session filter length boundary", func(t *testing.T) {
		atLimit := strings.Repeat("a", 1024)
		rec := getACP(t, handler, "/v1/acp/events-1/events?sessionId="+atLimit)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("1024-byte session status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		overLimit := strings.Repeat("a", 1025)
		rec = getACP(t, handler, "/v1/acp/events-1/events?sessionId="+overLimit)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("1025-byte session status = %d, want %d (body %q)", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("unknown session is 404 and unknown server takes precedence", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp/events-1/events?sessionId=missing")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unknown session status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusNotFound)

		rec = getACP(t, handler, "/v1/acp/ghost/events?sessionId=missing")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unknown server status = %d, want %d (body %q)", rec.Code, http.StatusNotFound, rec.Body.String())
		}
		assertProblem(t, rec, http.StatusNotFound)
	})

	t.Run("raw payload is embedded not JSON-string-quoted", func(t *testing.T) {
		rec := getACP(t, handler, "/v1/acp/events-1/events?limit=1")
		body := decodeACPEvents(t, rec)
		if len(body.Events) != 1 {
			t.Fatalf("events = %d, want 1", len(body.Events))
		}
		if got := string(body.Events[0].Payload); got != `{"n":1}` {
			t.Fatalf("payload = %q, want raw {\"n\":1}", got)
		}
		if strings.Contains(rec.Body.String(), `"payload":"`) {
			t.Fatalf("payload was JSON-string-quoted: %s", rec.Body.String())
		}
	})
}

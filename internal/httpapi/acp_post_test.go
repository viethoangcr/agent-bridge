package httpapi_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/httpapi"
)

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

// TestACPMalformedQueryRejected proves a percent-decoding error in ACP query
// strings is a 400 rather than a silently dropped parameter.
func TestACPMalformedQueryRejected(t *testing.T) {
	t.Run("events", func(t *testing.T) {
		handler := acpHandler(t, &fakeACP{})
		req := httptest.NewRequest(http.MethodGet, "/v1/acp/events-1/events", nil)
		req.URL.RawQuery = "limit=1&evil=%zz"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("post agent", func(t *testing.T) {
		handler := acpHandler(t, &fakeACP{})
		req := httptest.NewRequest(http.MethodPost, "/v1/acp/server-1", strings.NewReader(validNotification))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.URL.RawQuery = "agent=claude&evil=%zz"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("post agent unknown key", func(t *testing.T) {
		handler := acpHandler(t, &fakeACP{})
		req := httptest.NewRequest(http.MethodPost, "/v1/acp/server-1", strings.NewReader(validNotification))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.URL.RawQuery = "agent=claude&bogus=1"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assertProblem(t, rec, http.StatusBadRequest)
	})
}

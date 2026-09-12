package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// authNext is a sentinel handler that records whether authentication allowed a
// request through to the next handler.
func authNext(called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*called = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
}

func TestAuthenticate(t *testing.T) {
	const configured = "correct-horse-battery-staple"

	cases := []struct {
		name       string
		token      string
		method     string
		target     string
		headers    []string
		body       string
		wantStatus int
		wantPass   bool
	}{
		{
			name:   "unset token passes through without authorization",
			method: http.MethodGet, target: "/v1/health",
			wantStatus: http.StatusOK, wantPass: true,
		},
		{
			name:   "unset token passes through public root",
			method: http.MethodGet, target: "/",
			wantStatus: http.StatusOK, wantPass: true,
		},
		{
			name:   "configured token accepts exact bearer",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer " + configured},
			wantStatus: http.StatusOK, wantPass: true,
		},
		{
			name:   "configured token rejects missing authorization",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects wrong token",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer wrong-token"},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects shorter token",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer correct-horse"},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects longer token",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer " + configured + "-extra"},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects empty bearer token",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer "},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects empty authorization value",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{""},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects malformed bearer without separator",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer" + configured},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects lowercase scheme",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"bearer " + configured},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects wrong scheme",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Basic " + configured},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects duplicate authorization values",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer " + configured, "Bearer " + configured},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token rejects duplicate with an invalid second value",
			token:  configured,
			method: http.MethodGet, target: "/v1/health",
			headers:    []string{"Bearer " + configured, "Bearer wrong-token"},
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token leaves public root unauthenticated",
			token:  configured,
			method: http.MethodGet, target: "/",
			wantStatus: http.StatusOK, wantPass: true,
		},
		{
			name:   "configured token leaves public root unauthenticated despite credentials",
			token:  configured,
			method: http.MethodGet, target: "/",
			headers:    []string{"Bearer " + configured},
			wantStatus: http.StatusOK, wantPass: true,
		},
		{
			name:   "configured token leaves non-v1 paths to the router",
			token:  configured,
			method: http.MethodGet, target: "/unknown",
			wantStatus: http.StatusOK, wantPass: true,
		},
		{
			name:   "configured token protects acp paths with json-rpc body",
			token:  configured,
			method: http.MethodPost, target: "/v1/acp/demo",
			body:       `{"jsonrpc":"2.0","method":"initialize","id":1}`,
			wantStatus: http.StatusUnauthorized, wantPass: false,
		},
		{
			name:   "configured token accepts acp request with valid bearer",
			token:  configured,
			method: http.MethodPost, target: "/v1/acp/demo",
			headers:    []string{"Bearer " + configured},
			body:       `{"jsonrpc":"2.0","method":"initialize","id":1}`,
			wantStatus: http.StatusOK, wantPass: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			handler := authenticate(tc.token, authNext(&called))

			req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			for _, h := range tc.headers {
				req.Header.Add("Authorization", h)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			res := rec.Result()
			t.Cleanup(func() { _ = res.Body.Close() })

			if res.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}
			if called != tc.wantPass {
				t.Fatalf("next handler called = %v, want %v", called, tc.wantPass)
			}
			if tc.wantPass {
				return
			}

			if got := res.Header.Get("Content-Type"); got != "application/problem+json" {
				t.Errorf("Content-Type = %q, want application/problem+json", got)
			}
			body := rec.Body.String()
			var problem map[string]any
			if err := json.Unmarshal([]byte(body), &problem); err != nil {
				t.Fatalf("decode problem body %q: %v", body, err)
			}
			for _, field := range []string{"type", "title", "status", "detail"} {
				if _, ok := problem[field]; !ok {
					t.Errorf("missing problem field %q in %v", field, problem)
				}
			}
			if problem["status"] != float64(http.StatusUnauthorized) {
				t.Errorf("problem status = %v, want %d", problem["status"], http.StatusUnauthorized)
			}
			if strings.Contains(body, configured) {
				t.Errorf("problem body reflected configured token: %q", body)
			}
			for _, h := range tc.headers {
				token, ok := strings.CutPrefix(h, "Bearer ")
				if !ok || token == "" || token == configured {
					continue
				}
				if strings.Contains(body, token) {
					t.Errorf("problem body reflected supplied token %q: %q", token, body)
				}
			}
			for name, values := range res.Header {
				for _, value := range values {
					if strings.Contains(value, configured) {
						t.Errorf("response header %s reflected configured token", name)
					}
				}
			}
		})
	}
}

func TestAuthenticateBearerToken(t *testing.T) {
	cases := []struct {
		name   string
		values []string
		want   string
		wantOK bool
	}{
		{"missing header", nil, "", false},
		{"exact bearer", []string{"Bearer abc"}, "abc", true},
		{"case sensitive scheme", []string{"bearer abc"}, "", false},
		{"wrong scheme", []string{"Basic abc"}, "", false},
		{"missing separator", []string{"Bearerabc"}, "", false},
		{"empty token", []string{"Bearer "}, "", false},
		{"empty value", []string{""}, "", false},
		{"duplicate values", []string{"Bearer abc", "Bearer abc"}, "", false},
		{"leading whitespace", []string{" Bearer abc"}, "", false},
		{"extra whitespace is part of token", []string{"Bearer  abc"}, " abc", true},
		{"token with punctuation", []string{"Bearer a.-_~+/="}, "a.-_~+/=", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
			for _, v := range tc.values {
				req.Header.Add("Authorization", v)
			}
			got, ok := bearerToken(req)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("bearerToken() = (%q, %v), want (%q, %v)", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

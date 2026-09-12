package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/httpapi"
)

const (
	wantRootBody   = `{"name":"agent-bridge","docs":"https://github.com/viethoangcr/agent-bridge#readme"}`
	wantHealthBody = `{"status":"ok"}`
)

// requestServer drives one request through the public assembly path.
func requestServer(t *testing.T, token, method, target, auth string) *httptest.ResponseRecorder {
	t.Helper()
	handler := httpapi.NewServer(httpapi.Dependencies{Token: token}).Handler()
	req := httptest.NewRequest(method, target, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestServerRoutes(t *testing.T) {
	const token = "correct-horse-battery-staple"

	cases := []struct {
		name        string
		token       string
		method      string
		target      string
		auth        string
		wantStatus  int
		wantType    string
		wantBody    string
		wantProblem bool
		wantAllow   string
	}{
		{
			name: "root is public without a token", method: http.MethodGet, target: "/",
			wantStatus: http.StatusOK, wantType: "application/json", wantBody: wantRootBody,
		},
		{
			name: "health succeeds without a token", method: http.MethodGet, target: "/v1/health",
			wantStatus: http.StatusOK, wantType: "application/json", wantBody: wantHealthBody,
		},
		{
			name: "root stays public with a token configured", token: token, method: http.MethodGet, target: "/",
			wantStatus: http.StatusOK, wantType: "application/json", wantBody: wantRootBody,
		},
		{
			name: "root ignores an invalid bearer", token: token, method: http.MethodGet, target: "/",
			auth: "Bearer wrong", wantStatus: http.StatusOK, wantType: "application/json", wantBody: wantRootBody,
		},
		{
			name: "health requires auth when a token is configured", token: token, method: http.MethodGet, target: "/v1/health",
			wantStatus: http.StatusUnauthorized, wantProblem: true,
		},
		{
			name: "health rejects a wrong bearer", token: token, method: http.MethodGet, target: "/v1/health",
			auth: "Bearer wrong", wantStatus: http.StatusUnauthorized, wantProblem: true,
		},
		{
			name: "health accepts the configured bearer", token: token, method: http.MethodGet, target: "/v1/health",
			auth: "Bearer " + token, wantStatus: http.StatusOK, wantType: "application/json", wantBody: wantHealthBody,
		},
		{
			name: "unknown path is 404", method: http.MethodGet, target: "/unknown",
			wantStatus: http.StatusNotFound, wantProblem: true,
		},
		{
			name: "nested unknown subpath is 404", method: http.MethodGet, target: "/v1/health/extra",
			wantStatus: http.StatusNotFound, wantProblem: true,
		},
		{
			name: "unknown path is not authenticated", token: token, method: http.MethodGet, target: "/unknown",
			wantStatus: http.StatusNotFound, wantProblem: true,
		},
		{
			name: "wrong method on root is 405", method: http.MethodPost, target: "/",
			wantStatus: http.StatusMethodNotAllowed, wantProblem: true, wantAllow: "GET, HEAD",
		},
		{
			name: "wrong method on health is 405", method: http.MethodPost, target: "/v1/health",
			wantStatus: http.StatusMethodNotAllowed, wantProblem: true, wantAllow: "GET, HEAD",
		},
		{
			name: "head root succeeds", method: http.MethodHead, target: "/",
			wantStatus: http.StatusOK,
		},
		{
			name: "head health succeeds", method: http.MethodHead, target: "/v1/health",
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := requestServer(t, tc.token, tc.method, tc.target, tc.auth)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantProblem {
				assertProblem(t, rec, tc.wantStatus)
			}
			if tc.wantType != "" {
				if got := rec.Result().Header.Get("Content-Type"); got != tc.wantType {
					t.Errorf("Content-Type = %q, want %q", got, tc.wantType)
				}
			}
			if tc.wantBody != "" {
				if got := rec.Body.String(); got != tc.wantBody {
					t.Errorf("body = %q, want %q", got, tc.wantBody)
				}
			}
			if tc.wantAllow != "" {
				if got := rec.Result().Header.Get("Allow"); got != tc.wantAllow {
					t.Errorf("Allow = %q, want %q", got, tc.wantAllow)
				}
			}
		})
	}
}

func TestServerRejectsUncleanPaths(t *testing.T) {
	cases := []struct {
		name   string
		target string
	}{
		{name: "duplicate slash", target: "/v1//health"},
		{name: "dot dot segment", target: "/a/../unknown"},
		{name: "double slash root", target: "//"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := requestServer(t, "", http.MethodGet, tc.target, "")
			res := rec.Result()
			t.Cleanup(func() { res.Body.Close() })

			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d (Location %q, Content-Type %q, body %q)",
					rec.Code, http.StatusNotFound, res.Header.Get("Location"), res.Header.Get("Content-Type"), rec.Body.String())
			}
			if loc := res.Header.Get("Location"); loc != "" {
				t.Errorf("Location = %q, want no redirect", loc)
			}
			if body := rec.Body.String(); strings.Contains(strings.ToLower(body), "<html") {
				t.Errorf("body contains HTML: %q", body)
			}
			assertProblem(t, rec, http.StatusNotFound)
		})
	}
}

func TestServerMethodNotAllowedEveryRegisteredPath(t *testing.T) {
	paths := []string{"/", "/v1/health"}
	methods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodOptions,
	}

	for _, path := range paths {
		for _, method := range methods {
			t.Run(method+" "+path, func(t *testing.T) {
				rec := requestServer(t, "", method, path, "")

				if rec.Code != http.StatusMethodNotAllowed {
					t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
				}
				assertProblem(t, rec, http.StatusMethodNotAllowed)
				if got := rec.Result().Header.Get("Allow"); got != "GET, HEAD" {
					t.Errorf("Allow = %q, want %q", got, "GET, HEAD")
				}
			})
		}
	}
}

func TestServerHandlerIdentity(t *testing.T) {
	for _, token := range []string{"", "correct-horse-battery-staple"} {
		t.Run("token="+token, func(t *testing.T) {
			server := httpapi.NewServer(httpapi.Dependencies{Token: token})
			first := server.Handler()
			second := server.Handler()

			if first == nil || second == nil {
				t.Fatal("Handler() returned nil")
			}
			if reflect.TypeOf(first) != reflect.TypeOf(second) {
				t.Fatalf("Handler() types differ: %T vs %T", first, second)
			}
			if reflect.ValueOf(first).Pointer() != reflect.ValueOf(second).Pointer() {
				t.Errorf("Handler() returned different instances across calls")
			}
		})
	}
}

func TestServerProblemTitles(t *testing.T) {
	cases := []struct {
		name   string
		target string
		method string
		title  string
	}{
		{name: "not found", method: http.MethodGet, target: "/nope", title: "Not Found"},
		{name: "method not allowed", method: http.MethodPost, target: "/", title: "Method Not Allowed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := requestServer(t, "", tc.method, tc.target, "")
			var problem map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
				t.Fatalf("decode problem body %q: %v", rec.Body.String(), err)
			}
			if got := problem["title"]; got != tc.title {
				t.Errorf("title = %v, want %q", got, tc.title)
			}
			if got := problem["type"]; got != "about:blank" {
				t.Errorf("type = %v, want about:blank", got)
			}
			if detail, _ := problem["detail"].(string); strings.TrimSpace(detail) == "" {
				t.Errorf("detail is empty in %v", problem)
			}
		})
	}
}

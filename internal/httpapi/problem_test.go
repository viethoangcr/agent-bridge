package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/httpapi"
)

func TestWriteProblem(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{"bad request", http.StatusBadRequest},
		{"not found", http.StatusNotFound},
		{"method not allowed", http.StatusMethodNotAllowed},
		{"content too large", http.StatusRequestEntityTooLarge},
		{"internal server error", http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			title := http.StatusText(tc.status)
			detail := "failure for " + tc.name

			rec := httptest.NewRecorder()
			httpapi.WriteProblem(rec, httpapi.Problem{
				Type:   "about:blank",
				Title:  title,
				Status: tc.status,
				Detail: detail,
			})

			res := rec.Result()
			t.Cleanup(func() { res.Body.Close() })

			if got := res.Header.Get("Content-Type"); got != "application/problem+json" {
				t.Fatalf("Content-Type = %q, want %q", got, "application/problem+json")
			}
			if res.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.status)
			}

			var body map[string]any
			if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			for _, field := range []string{"type", "title", "status", "detail"} {
				if _, ok := body[field]; !ok {
					t.Errorf("missing required field %q in %v", field, body)
				}
			}
			if body["type"] != "about:blank" {
				t.Errorf("type = %v, want about:blank", body["type"])
			}
			if body["title"] != title {
				t.Errorf("title = %v, want %q", body["title"], title)
			}
			if body["status"] != float64(tc.status) {
				t.Errorf("status = %v, want %d", body["status"], tc.status)
			}
			if body["detail"] != detail {
				t.Errorf("detail = %v, want %q", body["detail"], detail)
			}
		})
	}
}

func TestWriteProblemExtensions(t *testing.T) {
	rec := httptest.NewRecorder()
	httpapi.WriteProblem(rec, httpapi.Problem{
		Type:   "about:blank",
		Title:  "Bad Request",
		Status: http.StatusBadRequest,
		Detail: "invalid input",
		Ext: map[string]any{
			"traceId":   "abc123",
			"retryable": true,
			"type":      "replaced-type",
			"title":     "replaced-title",
			"status":    http.StatusTeapot,
			"detail":    "replaced-detail",
		},
	})

	res := rec.Result()
	t.Cleanup(func() { res.Body.Close() })

	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if body["traceId"] != "abc123" {
		t.Errorf("traceId = %v, want abc123", body["traceId"])
	}
	if body["retryable"] != true {
		t.Errorf("retryable = %v, want true", body["retryable"])
	}
	if body["type"] != "about:blank" {
		t.Errorf("type = %v, extension must not replace type", body["type"])
	}
	if body["title"] != "Bad Request" {
		t.Errorf("title = %v, extension must not replace title", body["title"])
	}
	if body["status"] != float64(http.StatusBadRequest) {
		t.Errorf("status = %v, extension must not replace status", body["status"])
	}
	if body["detail"] != "invalid input" {
		t.Errorf("detail = %v, extension must not replace detail", body["detail"])
	}
}

package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewProblem(t *testing.T) {
	p := newProblem(http.StatusNotFound, detailNotFound)
	if p.Type != "about:blank" || p.Title != "Not Found" || p.Status != http.StatusNotFound || p.Detail != "not found" {
		t.Fatalf("newProblem() = %+v, want about:blank/Not Found/404/not found", p)
	}
}

func TestWriteProblemExt(t *testing.T) {
	rec := httptest.NewRecorder()
	writeProblemExt(rec, http.StatusBadGateway, "ACP agent process failure", map[string]any{"agentStderr": "boom"})
	if got := rec.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", got)
	}
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if body["agentStderr"] != "boom" || body["status"] != float64(http.StatusBadGateway) {
		t.Fatalf("body = %v", body)
	}
}

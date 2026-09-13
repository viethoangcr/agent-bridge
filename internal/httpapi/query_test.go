package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		allowed []string
		wantErr bool
	}{
		{"empty", "", nil, false},
		{"allowed", "a=1&b=2", []string{"a", "b"}, false},
		{"unknown key", "a=1&c=2", []string{"a", "b"}, true},
		{"malformed escape", "a=%zz", []string{"a"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.URL.RawQuery = tc.raw
			_, err := parseQuery(req, tc.allowed...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseQuery() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestSingleQuery(t *testing.T) {
	values := url.Values{"a": {"1"}, "empty": {""}, "multi": {"1", "2"}}
	tests := []struct {
		key    string
		want   string
		wantOK bool
	}{
		{"a", "1", true},
		{"empty", "", false},
		{"multi", "", false},
		{"missing", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			got, ok := singleQuery(values, tc.key)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("singleQuery(%q) = (%q,%v), want (%q,%v)", tc.key, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestRequireNoQuery(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/?x=1", nil)
	rec := httptest.NewRecorder()
	if requireNoQuery(rec, req, "invalid process query") {
		t.Fatal("requireNoQuery() = true, want false")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var problem map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if problem["detail"] != "invalid process query" {
		t.Fatalf("detail = %v, want invalid process query", problem["detail"])
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	if !requireNoQuery(rec, req, "invalid process query") {
		t.Fatal("requireNoQuery() = false, want true")
	}
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("status = %d body = %q, want 200 empty", rec.Code, rec.Body.String())
	}
}

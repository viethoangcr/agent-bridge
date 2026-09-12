package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/httpapi"
)

type payload struct {
	Name string `json:"name"`
}

func decodeBody(t *testing.T, body string, limit int64) (bool, payload, *httptest.ResponseRecorder) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	var dst payload
	ok := httpapi.DecodeJSON(rec, req, limit, &dst)
	return ok, dst, rec
}

func assertProblem(t *testing.T, rec *httptest.ResponseRecorder, status int) {
	t.Helper()
	res := rec.Result()
	t.Cleanup(func() { res.Body.Close() })

	if got := res.Header.Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", got)
	}
	if res.StatusCode != status {
		t.Fatalf("status = %d, want %d", res.StatusCode, status)
	}

	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode problem body: %v", err)
	}
	for _, field := range []string{"type", "title", "status", "detail"} {
		if _, ok := body[field]; !ok {
			t.Errorf("missing required field %q in %v", field, body)
		}
	}
	if body["status"] != float64(status) {
		t.Errorf("body status = %v, want %d", body["status"], status)
	}
}

func TestDecodeJSON(t *testing.T) {
	const body = `{"name":"bridge"}`

	t.Run("valid object", func(t *testing.T) {
		ok, dst, rec := decodeBody(t, body, int64(len(body)))
		if !ok {
			t.Fatalf("DecodeJSON = false, want true; response: %s", rec.Body.String())
		}
		if dst.Name != "bridge" {
			t.Errorf("decoded name = %q, want bridge", dst.Name)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("unexpected response body %q", rec.Body.String())
		}
	})

	t.Run("malformed json", func(t *testing.T) {
		ok, _, rec := decodeBody(t, `{"name":`, int64(len(body)))
		if ok {
			t.Fatal("DecodeJSON = true, want false")
		}
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("empty body", func(t *testing.T) {
		ok, _, rec := decodeBody(t, "", int64(len(body)))
		if ok {
			t.Fatal("DecodeJSON = true, want false")
		}
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("second json value", func(t *testing.T) {
		two := body + body
		ok, _, rec := decodeBody(t, two, int64(len(two)))
		if ok {
			t.Fatal("DecodeJSON = true, want false")
		}
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("trailing bytes", func(t *testing.T) {
		trailing := body + " trailing"
		ok, _, rec := decodeBody(t, trailing, int64(len(trailing)))
		if ok {
			t.Fatal("DecodeJSON = true, want false")
		}
		assertProblem(t, rec, http.StatusBadRequest)
	})

	t.Run("exact limit", func(t *testing.T) {
		ok, dst, rec := decodeBody(t, body, int64(len(body)))
		if !ok {
			t.Fatalf("DecodeJSON = false at exact limit, want true; response: %s", rec.Body.String())
		}
		if dst.Name != "bridge" {
			t.Errorf("decoded name = %q, want bridge", dst.Name)
		}
	})

	t.Run("over limit", func(t *testing.T) {
		ok, _, rec := decodeBody(t, body, int64(len(body))-1)
		if ok {
			t.Fatal("DecodeJSON = true over limit, want false")
		}
		assertProblem(t, rec, http.StatusRequestEntityTooLarge)
	})
}

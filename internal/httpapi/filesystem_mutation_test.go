package httpapi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

func TestFSMkdir(t *testing.T) {
	f := newFSServer(t)
	target := "/v1/fs/mkdir"
	body := mustMarshal(t, map[string]string{"directory": f.home, "name": "created"})

	rec := doFSRequest(t, f.server, http.MethodPost, target, "application/json", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var result filesystem.PathResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if want := filepath.Join(f.home, "created"); result.Path != want {
		t.Fatalf("path = %q, want %q", result.Path, want)
	}

	idempotent := doFSRequest(t, f.server, http.MethodPost, target, "Application/JSON; charset=utf-8", body)
	if idempotent.Code != http.StatusOK {
		t.Fatalf("idempotent status = %d, want %d: %s", idempotent.Code, http.StatusOK, idempotent.Body.String())
	}

	cases := []struct {
		name        string
		contentType string
		requestBody string
		target      string
		status      int
	}{
		{"missing content type", "", `{"directory":"` + f.home + `","name":"x"}`, target, http.StatusUnsupportedMediaType},
		{"wrong content type", "text/plain", `{"directory":"` + f.home + `","name":"x"}`, target, http.StatusUnsupportedMediaType},
		{"unknown field", "application/json", `{"directory":"` + f.home + `","name":"x","mode":5}`, target, http.StatusBadRequest},
		{"trailing value", "application/json", `{"directory":"` + f.home + `","name":"x"}{}`, target, http.StatusBadRequest},
		{"malformed", "application/json", `{"directory":`, target, http.StatusBadRequest},
		{"empty directory", "application/json", `{"directory":"","name":"x"}`, target, http.StatusBadRequest},
		{"slash name", "application/json", `{"directory":"` + f.home + `","name":"a/b"}`, target, http.StatusBadRequest},
		{"query present", "application/json", `{"directory":"` + f.home + `","name":"x"}`, target + "?x=1", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doFSRequest(t, f.server, http.MethodPost, tc.target, tc.contentType, []byte(tc.requestBody))
			assertProblem(t, rec, tc.status)
		})
	}
}

func TestFSMove(t *testing.T) {
	f := newFSServer(t)
	writeTree(t, filepath.Join(f.home, "source.txt"), "content")
	body := mustMarshal(t, map[string]string{"source": "source.txt", "destination": "nested/dest.txt"})

	rec := doFSRequest(t, f.server, http.MethodPost, "/v1/fs/move", "application/json", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var result filesystem.PathResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if want := filepath.Join(f.home, "nested", "dest.txt"); result.Path != want {
		t.Fatalf("path = %q, want %q", result.Path, want)
	}
	if _, err := os.Stat(filepath.Join(f.home, "source.txt")); !os.IsNotExist(err) {
		t.Fatalf("source still exists: %v", err)
	}

	// Incompatible replacement: a file onto an existing directory is a conflict.
	writeTree(t, filepath.Join(f.home, "file.txt"), "f")
	if err := os.Mkdir(filepath.Join(f.home, "adir"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	conflict := mustMarshal(t, map[string]string{"source": "file.txt", "destination": "adir"})
	assertProblem(t, doFSRequest(t, f.server, http.MethodPost, "/v1/fs/move", "application/json", conflict), http.StatusConflict)

	missingBody := mustMarshal(t, map[string]string{"source": "ghost.txt", "destination": "x.txt"})
	assertProblem(t, doFSRequest(t, f.server, http.MethodPost, "/v1/fs/move", "application/json", missingBody), http.StatusNotFound)

	for _, tc := range []struct {
		name        string
		contentType string
		requestBody string
		target      string
		status      int
	}{
		{"missing content type", "", `{"source":"a","destination":"b"}`, "/v1/fs/move", http.StatusUnsupportedMediaType},
		{"legacy field", "application/json", `{"from":"a","to":"b"}`, "/v1/fs/move", http.StatusBadRequest},
		{"unknown field", "application/json", `{"source":"a","destination":"b","extra":1}`, "/v1/fs/move", http.StatusBadRequest},
		{"empty source", "application/json", `{"source":"","destination":"b"}`, "/v1/fs/move", http.StatusBadRequest},
		{"empty destination", "application/json", `{"source":"a","destination":""}`, "/v1/fs/move", http.StatusBadRequest},
		{"query present", "application/json", `{"source":"a","destination":"b"}`, "/v1/fs/move?x=1", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := doFSRequest(t, f.server, http.MethodPost, tc.target, tc.contentType, []byte(tc.requestBody))
			assertProblem(t, rec, tc.status)
		})
	}
}

func TestFSStat(t *testing.T) {
	f := newFSServer(t)
	writeTree(t, filepath.Join(f.home, "stat.txt"), "abc")

	rec := doFSRequest(t, f.server, http.MethodGet, "/v1/fs/stat?"+fsQuery("path", "stat.txt"), "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var stat filesystem.Stat
	if err := json.Unmarshal(rec.Body.Bytes(), &stat); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stat.Path != filepath.Join(f.home, "stat.txt") || stat.Type != "file" || stat.Size != 3 {
		t.Fatalf("stat = %+v", stat)
	}
	if stat.Mode != 0o644 {
		t.Fatalf("mode = %o, want 644", stat.Mode)
	}

	assertProblem(t, doFSRequest(t, f.server, http.MethodGet, "/v1/fs/stat", "", nil), http.StatusBadRequest)
	assertProblem(t, doFSRequest(t, f.server, http.MethodGet, "/v1/fs/stat?"+fsQuery("path", "ghost"), "", nil), http.StatusNotFound)
	assertProblem(t, doFSRequest(t, f.server, http.MethodGet, "/v1/fs/stat?"+fsQuery("path", "stat.txt", "extra", "x"), "", nil), http.StatusBadRequest)
}

// tarGz builds a gzip-compressed tar stream from ordered directory and file

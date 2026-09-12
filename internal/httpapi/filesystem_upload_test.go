package httpapi

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

func tarGz(t *testing.T, dirs []string, files map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	tw := tar.NewWriter(gz)
	for _, dir := range dirs {
		if err := tw.WriteHeader(&tar.Header{Name: dir, Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
			t.Fatalf("tar dir: %v", err)
		}
	}
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatalf("tar file header: %v", err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("tar file body: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return buffer.Bytes()
}

func TestFSUploadBatch(t *testing.T) {
	f := newFSServer(t)
	archive := tarGz(t, nil, map[string]string{"a.txt": "hello"})

	rec := doFSRequest(t, f.server, http.MethodPost, "/v1/fs/upload-batch?"+fsQuery("directory", "uploads"), "", archive)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body struct {
		Files []filesystem.UploadedFile `json:"files"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := filepath.Join(f.home, "uploads", "a.txt")
	if len(body.Files) != 1 || body.Files[0].Path != want || body.Files[0].Size != 5 {
		t.Fatalf("files = %+v, want one %q size 5", body.Files, want)
	}
	stored, err := os.ReadFile(want)
	if err != nil || string(stored) != "hello" {
		t.Fatalf("stored = %q, err = %v", stored, err)
	}
}

func TestFSUploadBatchRejectsUnsafe(t *testing.T) {
	f := newFSServer(t)
	unsafe := tarGz(t, nil, map[string]string{"../evil.txt": "x"})

	rec := doFSRequest(t, f.server, http.MethodPost, "/v1/fs/upload-batch?"+fsQuery("directory", "uploads"), "", unsafe)
	assertProblem(t, rec, http.StatusBadRequest)
	if _, err := os.Stat(filepath.Join(f.home, "evil.txt")); !os.IsNotExist(err) {
		t.Fatalf("unsafe entry escaped: %v", err)
	}
}

func TestFSUploadBatchCompressedLimit(t *testing.T) {
	f := newFSServer(t)
	archive := tarGz(t, nil, map[string]string{"a.txt": "hello"})
	f.server.fsUploadLimit = int64(len(archive))

	atLimit := doFSRequest(t, f.server, http.MethodPost, "/v1/fs/upload-batch?"+fsQuery("directory", "at-limit"), "", archive)
	if atLimit.Code != http.StatusOK {
		t.Fatalf("at-limit status = %d, want %d: %s", atLimit.Code, http.StatusOK, atLimit.Body.String())
	}

	f.server.fsUploadLimit = int64(len(archive) - 1)
	over := doFSRequest(t, f.server, http.MethodPost, "/v1/fs/upload-batch?"+fsQuery("directory", "over-limit"), "", archive)
	assertProblem(t, over, http.StatusRequestEntityTooLarge)
	if _, err := os.Stat(filepath.Join(f.home, "over-limit")); !os.IsNotExist(err) {
		t.Fatalf("over-limit upload mutated destination: %v", err)
	}
}

func TestFSUploadBatchStrictQuery(t *testing.T) {
	f := newFSServer(t)
	for _, tc := range []struct {
		name   string
		target string
	}{
		{"missing directory", "/v1/fs/upload-batch"},
		{"empty directory", "/v1/fs/upload-batch?" + fsQuery("directory", "\x00")},
		{"unknown key", "/v1/fs/upload-batch?" + fsQuery("directory", "d", "extra", "x")},
		{"repeated directory", "/v1/fs/upload-batch?directory=a&directory=b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertProblem(t, doFSRequest(t, f.server, http.MethodPost, tc.target, "", nil), http.StatusBadRequest)
		})
	}
}

func TestFSWrongMethods(t *testing.T) {
	f := newFSServer(t)
	cases := []struct {
		name   string
		method string
		target string
		allow  string
	}{
		{"entries PUT", http.MethodPut, "/v1/fs/entries", "GET, HEAD"},
		{"file DELETE", http.MethodDelete, "/v1/fs/file", "GET, HEAD, PUT"},
		{"entry GET", http.MethodGet, "/v1/fs/entry", "DELETE"},
		{"mkdir GET", http.MethodGet, "/v1/fs/mkdir", "POST"},
		{"move GET", http.MethodGet, "/v1/fs/move", "POST"},
		{"stat POST", http.MethodPost, "/v1/fs/stat", "GET, HEAD"},
		{"upload GET", http.MethodGet, "/v1/fs/upload-batch", "POST"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doFSRequest(t, f.server, tc.method, tc.target, "", nil)
			assertProblem(t, rec, http.StatusMethodNotAllowed)
			if got := rec.Header().Get("Allow"); got != tc.allow {
				t.Fatalf("Allow = %q, want %q", got, tc.allow)
			}
		})
	}
}

func TestFSAuthApplies(t *testing.T) {
	home := t.TempDir()
	files, err := filesystem.New(home, &sync.Mutex{})
	if err != nil {
		t.Fatalf("filesystem.New: %v", err)
	}
	server := NewServer(Dependencies{Token: "secret-token", Files: files})

	rec := doFSRequest(t, server, http.MethodGet, "/v1/fs/entries?"+fsQuery("directory", home), "", nil)
	assertProblem(t, rec, http.StatusUnauthorized)
}

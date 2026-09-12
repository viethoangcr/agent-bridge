package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

func TestFSFilePutGetRoundTrip(t *testing.T) {
	f := newFSServer(t)
	payload := []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 'h', 'i'}

	put := doFSRequest(t, f.server, http.MethodPut, "/v1/fs/file?"+fsQuery("path", "raw/data.bin"), "text/plain", payload)
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want %d: %s", put.Code, http.StatusOK, put.Body.String())
	}
	if got := put.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("PUT Content-Type = %q, want application/json", got)
	}
	var result filesystem.FileResult
	if err := json.Unmarshal(put.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode PUT result: %v", err)
	}
	absolute := filepath.Join(f.home, "raw", "data.bin")
	if result.Path != absolute || result.Size != int64(len(payload)) {
		t.Fatalf("PUT result = %+v, want path %q size %d", result, absolute, len(payload))
	}

	get := doFSRequest(t, f.server, http.MethodGet, "/v1/fs/file?"+fsQuery("path", "raw/data.bin"), "", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", get.Code, http.StatusOK)
	}
	if got := get.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("GET Content-Type = %q, want application/octet-stream", got)
	}
	if got := get.Header().Get("Content-Length"); got != strconv.Itoa(len(payload)) {
		t.Fatalf("Content-Length = %q, want %d", got, len(payload))
	}
	if !bytes.Equal(get.Body.Bytes(), payload) {
		t.Fatalf("GET body = %v, want %v", get.Body.Bytes(), payload)
	}

	missing := doFSRequest(t, f.server, http.MethodGet, "/v1/fs/file?"+fsQuery("path", "raw/missing.bin"), "", nil)
	assertProblem(t, missing, http.StatusNotFound)
}

func TestFSFileStrictQuery(t *testing.T) {
	f := newFSServer(t)
	for _, tc := range []struct {
		name   string
		target string
		status int
	}{
		{"missing path", "/v1/fs/file", http.StatusBadRequest},
		{"empty path", "/v1/fs/file?" + fsQuery("path", "\x00"), http.StatusBadRequest},
		{"unknown key", "/v1/fs/file?" + fsQuery("path", "a", "extra", "x"), http.StatusBadRequest},
		{"repeated path", "/v1/fs/file?path=a&path=b", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := doFSRequest(t, f.server, http.MethodGet, tc.target, "", nil)
			assertProblem(t, rec, tc.status)
		})
	}
}

func TestFSFilePutLimit(t *testing.T) {
	f := newFSServer(t)
	f.server.fsFileLimit = 8

	atLimit := []byte("12345678")
	put := doFSRequest(t, f.server, http.MethodPut, "/v1/fs/file?"+fsQuery("path", "bounded.bin"), "", atLimit)
	if put.Code != http.StatusOK {
		t.Fatalf("at-limit PUT status = %d, want %d: %s", put.Code, http.StatusOK, put.Body.String())
	}

	over := []byte("123456789")
	put = doFSRequest(t, f.server, http.MethodPut, "/v1/fs/file?"+fsQuery("path", "bounded.bin"), "", over)
	assertProblem(t, put, http.StatusRequestEntityTooLarge)

	stored, err := os.ReadFile(filepath.Join(f.home, "bounded.bin"))
	if err != nil {
		t.Fatalf("read stored file: %v", err)
	}
	if !bytes.Equal(stored, atLimit) {
		t.Fatalf("stored = %q, want unchanged %q", stored, atLimit)
	}
}

func TestFSFilePutProductionLimit(t *testing.T) {
	f := newFSServer(t)
	if f.server.fsFileLimit != 512<<20 {
		t.Fatalf("file limit = %d, want %d", f.server.fsFileLimit, 512<<20)
	}
	if f.server.fsUploadLimit != 512<<20 {
		t.Fatalf("upload limit = %d, want %d", f.server.fsUploadLimit, 512<<20)
	}
}

func TestFSEntryDelete(t *testing.T) {
	f := newFSServer(t)
	writeTree(t, filepath.Join(f.home, "tree", "nested", "a.txt"), "a")

	del := doFSRequest(t, f.server, http.MethodDelete, "/v1/fs/entry?"+fsQuery("path", "tree"), "", nil)
	if del.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d: %s", del.Code, http.StatusNoContent, del.Body.String())
	}
	if del.Body.Len() != 0 {
		t.Fatalf("DELETE body = %q, want empty", del.Body.String())
	}
	if _, err := os.Stat(filepath.Join(f.home, "tree")); !os.IsNotExist(err) {
		t.Fatalf("tree still exists: err = %v", err)
	}

	missing := doFSRequest(t, f.server, http.MethodDelete, "/v1/fs/entry?"+fsQuery("path", "tree"), "", nil)
	assertProblem(t, missing, http.StatusNotFound)

	unknown := doFSRequest(t, f.server, http.MethodDelete, "/v1/fs/entry?"+fsQuery("path", "tree", "extra", "x"), "", nil)
	assertProblem(t, unknown, http.StatusBadRequest)
}

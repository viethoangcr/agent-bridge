package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

// fsHome is the captured HOME plus service used by most filesystem tests.
type fsHome struct {
	server *Server
	files  *filesystem.Service
	home   string
}

// newFSServer builds the public handler around a real filesystem service rooted
// at a temporary HOME.
func newFSServer(t *testing.T) fsHome {
	t.Helper()
	home := t.TempDir()
	files, err := filesystem.New(home, &sync.Mutex{})
	if err != nil {
		t.Fatalf("filesystem.New: %v", err)
	}
	return fsHome{server: NewServer(Dependencies{Files: files}), files: files, home: home}
}

// doFSRequest drives one request through the public assembly path.
func doFSRequest(t *testing.T, s *Server, method, target, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// fsQuery builds an escaped query string from an ordered key/value list. A
// value of "\x00" is encoded as a present-but-empty parameter.
func fsQuery(pairs ...string) string {
	values := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] == "\x00" {
			values[pairs[i]] = []string{""}
			continue
		}
		values[pairs[i]] = []string{pairs[i+1]}
	}
	return values.Encode()
}

// writeTree creates parent directories and writes content at an absolute path.
func writeTree(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir parents: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestFSEntriesDefaultAllAndSorted(t *testing.T) {
	f := newFSServer(t)
	writeTree(t, filepath.Join(f.home, "b.txt"), "bb")
	writeTree(t, filepath.Join(f.home, "a.txt"), "a")
	if err := os.Mkdir(filepath.Join(f.home, "c"), 0o755); err != nil {
		t.Fatalf("mkdir c: %v", err)
	}

	rec := doFSRequest(t, f.server, http.MethodGet, "/v1/fs/entries?"+fsQuery("directory", f.home), "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var wrapper map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &wrapper); err != nil {
		t.Fatalf("decode wrapper: %v", err)
	}
	if len(wrapper) != 1 {
		t.Fatalf("wrapper keys = %v, want only entries", wrapper)
	}
	var entries []filesystem.Entry
	if err := json.Unmarshal(wrapper["entries"], &entries); err != nil {
		t.Fatalf("decode entries: %v", err)
	}
	want := []filesystem.Entry{
		{Name: "a.txt", Path: filepath.Join(f.home, "a.txt"), Type: "file", Size: 1},
		{Name: "b.txt", Path: filepath.Join(f.home, "b.txt"), Type: "file", Size: 2},
		{Name: "c", Path: filepath.Join(f.home, "c"), Type: "dir"},
	}
	if len(entries) != len(want) {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}
	for i := range want {
		got := entries[i]
		if got.Name != want[i].Name || got.Path != want[i].Path || got.Type != want[i].Type {
			t.Fatalf("entry %d = %+v, want %+v", i, got, want[i])
		}
		if want[i].Type == "file" && got.Size != want[i].Size {
			t.Fatalf("entry %d size = %d, want %d", i, got.Size, want[i].Size)
		}
	}

	explicit := doFSRequest(t, f.server, http.MethodGet, "/v1/fs/entries?"+fsQuery("directory", f.home, "type", "all"), "", nil)
	if explicit.Code != http.StatusOK {
		t.Fatalf("type=all status = %d, want %d", explicit.Code, http.StatusOK)
	}
	if explicit.Body.String() != rec.Body.String() {
		t.Fatalf("type=all body = %s, want %s", explicit.Body.String(), rec.Body.String())
	}
}

func TestFSEntriesTypeFilter(t *testing.T) {
	f := newFSServer(t)
	writeTree(t, filepath.Join(f.home, "a.txt"), "a")
	if err := os.Mkdir(filepath.Join(f.home, "d"), 0o755); err != nil {
		t.Fatalf("mkdir d: %v", err)
	}

	for _, tc := range []struct {
		filter string
		want   []string
	}{
		{"file", []string{"a.txt"}},
		{"dir", []string{"d"}},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			rec := doFSRequest(t, f.server, http.MethodGet, "/v1/fs/entries?"+fsQuery("directory", f.home, "type", tc.filter), "", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}
			var body struct {
				Entries []filesystem.Entry `json:"entries"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			var names []string
			for _, entry := range body.Entries {
				names = append(names, entry.Name)
			}
			if !reflect.DeepEqual(names, tc.want) {
				t.Fatalf("names = %v, want %v", names, tc.want)
			}
		})
	}
}

func TestFSEntriesStrictQueryAndErrors(t *testing.T) {
	f := newFSServer(t)
	writeTree(t, filepath.Join(f.home, "a.txt"), "a")

	cases := []struct {
		name   string
		target string
		status int
	}{
		{"missing directory", "/v1/fs/entries", http.StatusBadRequest},
		{"empty directory", "/v1/fs/entries?" + fsQuery("directory", "\x00"), http.StatusBadRequest},
		{"unknown key", "/v1/fs/entries?" + fsQuery("directory", f.home, "extra", "x"), http.StatusBadRequest},
		{"repeated directory", "/v1/fs/entries?directory=a&directory=b", http.StatusBadRequest},
		{"repeated type", "/v1/fs/entries?directory=a&type=all&type=file", http.StatusBadRequest},
		{"empty type", "/v1/fs/entries?" + fsQuery("directory", f.home, "type", "\x00"), http.StatusBadRequest},
		{"unknown type", "/v1/fs/entries?" + fsQuery("directory", f.home, "type", "link"), http.StatusBadRequest},
		{"missing path", "/v1/fs/entries?" + fsQuery("directory", filepath.Join(f.home, "nope")), http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doFSRequest(t, f.server, http.MethodGet, tc.target, "", nil)
			assertProblem(t, rec, tc.status)
		})
	}
}

func TestFSMalformedQueryRejected(t *testing.T) {
	f := newFSServer(t)
	directory := "directory=" + url.QueryEscape(f.home) + "&evil=%zz"
	cases := []struct {
		name   string
		method string
		target string
		raw    string
	}{
		{"entries", http.MethodGet, "/v1/fs/entries", directory},
		{"file", http.MethodGet, "/v1/fs/file", "path=a&evil=%zz"},
		{"entry", http.MethodDelete, "/v1/fs/entry", "path=a&evil=%zz"},
		{"stat", http.MethodGet, "/v1/fs/stat", "path=a&evil=%zz"},
		{"upload", http.MethodPost, "/v1/fs/upload-batch", directory},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			req.URL.RawQuery = tc.raw
			rec := httptest.NewRecorder()
			f.server.Handler().ServeHTTP(rec, req)
			assertProblem(t, rec, http.StatusBadRequest)
		})
	}
}

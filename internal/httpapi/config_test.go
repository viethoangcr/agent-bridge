package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
	"github.com/viethoangcr/agent-bridge/internal/projectconfig"
)

// configServer is a public handler around real filesystem and project-config
// services sharing one mutation mutex, rooted at a temporary HOME.
type configServer struct {
	server *Server
	home   string
}

func newConfigServer(t *testing.T) configServer {
	t.Helper()
	home := t.TempDir()
	mutex := &sync.Mutex{}
	files, err := filesystem.New(home, mutex)
	if err != nil {
		t.Fatalf("filesystem.New: %v", err)
	}
	return configServer{
		server: NewServer(Dependencies{Files: files, Config: projectconfig.New(files, mutex)}),
		home:   home,
	}
}

// configPath is the fixed on-disk location for one config kind under home.
func configPath(home, kind string) string {
	return filepath.Join(home, ".agent-bridge", "config", kind+".json")
}

func assertConfigMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode(%s) = %04o, want %04o", path, got, want)
	}
}

func TestMCPConfigRoundTrip(t *testing.T) {
	c := newConfigServer(t)
	target := "/v1/config/mcp?" + fsQuery("directory", c.home)
	body := []byte(`{"alpha":{"command":"run","args":["--x"],"env":{"K":"v"}}}`)

	put := doFSRequest(t, c.server, http.MethodPut, target, "application/json", body)
	if put.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d: %s", put.Code, http.StatusNoContent, put.Body.String())
	}
	if put.Body.Len() != 0 {
		t.Fatalf("PUT body = %q, want empty", put.Body.String())
	}
	assertConfigMode(t, configPath(c.home, "mcp"), 0o600)

	stored, err := os.ReadFile(configPath(c.home, "mcp"))
	if err != nil {
		t.Fatalf("read stored config: %v", err)
	}

	get := doFSRequest(t, c.server, http.MethodGet, target, "", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	if ct := get.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("GET Content-Type = %q, want application/json", ct)
	}
	if !bytes.Equal(get.Body.Bytes(), stored) {
		t.Fatalf("GET body = %q, want exact stored bytes %q", get.Body.Bytes(), stored)
	}
	var got map[string]projectconfig.MCPServer
	if err := json.Unmarshal(get.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode GET object: %v", err)
	}
	if server := got["alpha"]; server.Command != "run" || len(server.Args) != 1 || server.Env["K"] != "v" {
		t.Fatalf("GET object = %+v, want the submitted server", server)
	}

	del := doFSRequest(t, c.server, http.MethodDelete, target, "", nil)
	if del.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d: %s", del.Code, http.StatusNoContent, del.Body.String())
	}
	if del.Body.Len() != 0 {
		t.Fatalf("DELETE body = %q, want empty", del.Body.String())
	}
	if _, err := os.Stat(configPath(c.home, "mcp")); !os.IsNotExist(err) {
		t.Fatalf("config still present after DELETE: stat error = %v", err)
	}
	assertProblem(t, doFSRequest(t, c.server, http.MethodGet, target, "", nil), http.StatusNotFound)
	assertProblem(t, doFSRequest(t, c.server, http.MethodDelete, target, "", nil), http.StatusNotFound)
}

func TestMCPConfigStrictQuery(t *testing.T) {
	c := newConfigServer(t)
	cases := []struct {
		name   string
		target string
	}{
		{"missing directory", "/v1/config/mcp"},
		{"empty directory", "/v1/config/mcp?directory="},
		{"repeated directory", "/v1/config/mcp?directory=a&directory=b"},
		{"unknown key", "/v1/config/mcp?directory=a&kind=mcp"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertProblem(t, doFSRequest(t, c.server, http.MethodGet, tc.target, "", nil), http.StatusBadRequest)
		})
	}
}

func TestMCPConfigValidation(t *testing.T) {
	c := newConfigServer(t)
	target := "/v1/config/mcp?" + fsQuery("directory", c.home)
	cases := []struct {
		name        string
		contentType string
		body        string
		status      int
	}{
		{"missing content type", "", `{}`, http.StatusUnsupportedMediaType},
		{"wrong content type", "text/plain", `{}`, http.StatusUnsupportedMediaType},
		{"array", "application/json", `[]`, http.StatusBadRequest},
		{"scalar", "application/json", `"x"`, http.StatusBadRequest},
		{"number", "application/json", `1`, http.StatusBadRequest},
		{"null", "application/json", `null`, http.StatusBadRequest},
		{"malformed", "application/json", `{"s":`, http.StatusBadRequest},
		{"unknown field", "application/json", `{"s":{"command":"run","bogus":1}}`, http.StatusBadRequest},
		{"empty command", "application/json", `{"s":{"command":""}}`, http.StatusBadRequest},
		{"non-string args", "application/json", `{"s":{"command":"run","args":"x"}}`, http.StatusBadRequest},
		{"trailing value", "application/json", `{"s":{"command":"run"}}{}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doFSRequest(t, c.server, http.MethodPut, target, tc.contentType, []byte(tc.body))
			assertProblem(t, rec, tc.status)
		})
	}
	if _, err := os.Stat(configPath(c.home, "mcp")); !os.IsNotExist(err) {
		t.Fatalf("rejected PUT created config: stat error = %v", err)
	}
}

func TestMCPConfigBodyLimit(t *testing.T) {
	c := newConfigServer(t)
	c.server.configJSONLimit = 16
	target := "/v1/config/mcp?" + fsQuery("directory", c.home)
	over := []byte(`{"s":{"command":"run"}}`)
	assertProblem(t, doFSRequest(t, c.server, http.MethodPut, target, "application/json", over), http.StatusRequestEntityTooLarge)
	if _, err := os.Stat(configPath(c.home, "mcp")); !os.IsNotExist(err) {
		t.Fatalf("over-limit PUT created config: stat error = %v", err)
	}
}

func TestMCPConfigDefaultBodyLimit(t *testing.T) {
	c := newConfigServer(t)
	if c.server.configJSONLimit != 10<<20 {
		t.Fatalf("config JSON limit = %d, want %d", c.server.configJSONLimit, 10<<20)
	}
}

func TestSkillsConfigRoundTrip(t *testing.T) {
	c := newConfigServer(t)
	target := "/v1/config/skills?" + fsQuery("directory", c.home)
	body := []byte(`{"any":[1,2,{"nested":null}],"text":"x"}`)

	put := doFSRequest(t, c.server, http.MethodPut, target, "application/json", body)
	if put.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d: %s", put.Code, http.StatusNoContent, put.Body.String())
	}
	if put.Body.Len() != 0 {
		t.Fatalf("PUT body = %q, want empty", put.Body.String())
	}
	assertConfigMode(t, configPath(c.home, "skills"), 0o600)

	stored, err := os.ReadFile(configPath(c.home, "skills"))
	if err != nil {
		t.Fatalf("read stored config: %v", err)
	}

	get := doFSRequest(t, c.server, http.MethodGet, target, "", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d: %s", get.Code, http.StatusOK, get.Body.String())
	}
	if !bytes.Equal(get.Body.Bytes(), stored) {
		t.Fatalf("GET body = %q, want exact stored bytes %q", get.Body.Bytes(), stored)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(get.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode GET object: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GET object keys = %v, want the submitted keys", got)
	}

	if del := doFSRequest(t, c.server, http.MethodDelete, target, "", nil); del.Code != http.StatusNoContent || del.Body.Len() != 0 {
		t.Fatalf("DELETE = %d %q, want 204 empty", del.Code, del.Body.String())
	}
	assertProblem(t, doFSRequest(t, c.server, http.MethodGet, target, "", nil), http.StatusNotFound)
	assertProblem(t, doFSRequest(t, c.server, http.MethodDelete, target, "", nil), http.StatusNotFound)
}

func TestSkillsConfigValidation(t *testing.T) {
	c := newConfigServer(t)
	target := "/v1/config/skills?" + fsQuery("directory", c.home)
	cases := []struct {
		name        string
		contentType string
		body        string
		status      int
	}{
		{"empty object", "application/json", `{}`, http.StatusNoContent},
		{"missing content type", "", `{}`, http.StatusUnsupportedMediaType},
		{"wrong content type", "text/plain", `{}`, http.StatusUnsupportedMediaType},
		{"array", "application/json", `[]`, http.StatusBadRequest},
		{"scalar", "application/json", `"x"`, http.StatusBadRequest},
		{"null", "application/json", `null`, http.StatusBadRequest},
		{"malformed", "application/json", `{"a":`, http.StatusBadRequest},
		{"trailing value", "application/json", `{}{}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doFSRequest(t, c.server, http.MethodPut, target, tc.contentType, []byte(tc.body))
			if tc.status == http.StatusNoContent {
				if rec.Code != http.StatusNoContent {
					t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusNoContent, rec.Body.String())
				}
				return
			}
			assertProblem(t, rec, tc.status)
		})
	}
}

func TestSkillsConfigStrictQuery(t *testing.T) {
	c := newConfigServer(t)
	cases := []struct {
		name   string
		target string
	}{
		{"missing directory", "/v1/config/skills"},
		{"empty directory", "/v1/config/skills?directory="},
		{"repeated directory", "/v1/config/skills?directory=a&directory=b"},
		{"unknown key", "/v1/config/skills?directory=a&kind=skills"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertProblem(t, doFSRequest(t, c.server, http.MethodDelete, tc.target, "", nil), http.StatusBadRequest)
		})
	}
}

func TestSkillsConfigBodyLimit(t *testing.T) {
	c := newConfigServer(t)
	c.server.configJSONLimit = 8
	target := "/v1/config/skills?" + fsQuery("directory", c.home)
	assertProblem(t, doFSRequest(t, c.server, http.MethodPut, target, "application/json", []byte(`{"any":[]}`)), http.StatusRequestEntityTooLarge)
}

func TestConfigUnavailable(t *testing.T) {
	s := NewServer(Dependencies{})
	rec := doFSRequest(t, s, http.MethodGet, "/v1/config/mcp?directory=/tmp", "", nil)
	assertProblem(t, rec, http.StatusServiceUnavailable)
}

// TestConfigConcurrentPutGetIntegrity issues concurrent whole-object PUTs and
// GETs through the public handler and requires every observed GET body to be
// one complete submitted object, with the final file mode 0600.
func TestConfigConcurrentPutGetIntegrity(t *testing.T) {
	c := newConfigServer(t)
	target := "/v1/config/mcp?" + fsQuery("directory", c.home)
	bodies := []string{
		`{"a":{"command":"one"}}`,
		`{"b":{"command":"two","args":["x"]}}`,
		`{"c":{"command":"three","env":{"K":"v"}}}`,
		`{"d":{"command":"four"}}`,
	}

	var writers sync.WaitGroup
	for _, body := range bodies {
		writers.Go(func() {
			for range 50 {
				rec := doFSRequest(t, c.server, http.MethodPut, target, "application/json", []byte(body))
				if rec.Code != http.StatusNoContent {
					t.Errorf("PUT status = %d, want %d: %s", rec.Code, http.StatusNoContent, rec.Body.String())
					return
				}
			}
		})
	}

	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				rec := doFSRequest(t, c.server, http.MethodGet, target, "", nil)
				if rec.Code == http.StatusNotFound {
					continue
				}
				if rec.Code != http.StatusOK {
					t.Errorf("GET status = %d, want 200: %s", rec.Code, rec.Body.String())
					return
				}
				var decoded map[string]projectconfig.MCPServer
				if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
					t.Errorf("GET observed malformed JSON %q: %v", rec.Body.String(), err)
					return
				}
				for _, server := range decoded {
					if server.Command == "" {
						t.Errorf("GET observed incomplete object %q", rec.Body.String())
						return
					}
				}
			}
		})
	}

	writers.Wait()
	close(done)
	readers.Wait()
	assertConfigMode(t, configPath(c.home, "mcp"), 0o600)
}

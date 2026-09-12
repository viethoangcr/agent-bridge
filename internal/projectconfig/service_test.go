package projectconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

func newTestService(t *testing.T, home string) (*Service, *sync.Mutex) {
	t.Helper()
	files, err := filesystem.New(home, &sync.Mutex{})
	if err != nil {
		t.Fatalf("filesystem.New(%q) error = %v", home, err)
	}
	mu := &sync.Mutex{}
	return New(files, mu), mu
}

func errorKind(t *testing.T, err error) filesystem.ErrorKind {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var e *filesystem.Error
	if !errors.As(err, &e) {
		t.Fatalf("error %v is not *filesystem.Error", err)
	}
	return e.Kind
}

func TestConfigPathResolvesFixedSuffix(t *testing.T) {
	home := t.TempDir()
	svc, _ := newTestService(t, home)

	absDir := t.TempDir()
	got, err := svc.configPath("mcp", absDir)
	if err != nil {
		t.Fatalf("configPath(mcp, absolute) error = %v", err)
	}
	if want := filepath.Join(absDir, ".agent-bridge", "config", "mcp.json"); got != want {
		t.Fatalf("configPath(mcp, absolute) = %q, want %q", got, want)
	}

	got, err = svc.configPath("skills", "project")
	if err != nil {
		t.Fatalf("configPath(skills, relative) error = %v", err)
	}
	if want := filepath.Join(home, "project", ".agent-bridge", "config", "skills.json"); got != want {
		t.Fatalf("configPath(skills, relative) = %q, want %q", got, want)
	}

	if _, err := svc.configPath("other", absDir); errorKind(t, err) != filesystem.ErrorKindInvalid {
		t.Fatalf("configPath(other) kind = %v, want invalid", errorKind(t, err))
	}
	if _, err := svc.configPath("mcp", "../escape"); errorKind(t, err) != filesystem.ErrorKindInvalid {
		t.Fatalf("configPath(../escape) kind = %v, want invalid", errorKind(t, err))
	}
}

func TestGetMissingDoesNotCreateRoots(t *testing.T) {
	svc, _ := newTestService(t, "")
	dir := t.TempDir()

	if _, err := svc.Get("mcp", dir); errorKind(t, err) != filesystem.ErrorKindNotFound {
		t.Fatalf("Get missing kind = %v, want not_found", errorKind(t, err))
	}
	if _, err := os.Stat(filepath.Join(dir, ".agent-bridge")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get created .agent-bridge root: stat err = %v", err)
	}
}

func TestPutMCPValidation(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		wantOK bool
	}{
		{name: "empty object", body: `{}`, wantOK: true},
		{name: "command only", body: `{"s":{"command":"run"}}`, wantOK: true},
		{name: "args and env", body: `{"s":{"command":"run","args":["a","b"],"env":{"K":"v"}}}`, wantOK: true},
		{name: "empty args and env", body: `{"s":{"command":"run","args":[],"env":{}}}`, wantOK: true},
		{name: "null args", body: `{"s":{"command":"run","args":null}}`},
		{name: "null env", body: `{"s":{"command":"run","env":null}}`},
		{name: "null args and env", body: `{"s":{"command":"run","args":null,"env":null}}`},
		{name: "non-object array", body: `[]`},
		{name: "non-object string", body: `"x"`},
		{name: "non-object number", body: `1`},
		{name: "non-object null", body: `null`},
		{name: "malformed", body: `{"s":`},
		{name: "unknown field", body: `{"s":{"command":"run","bogus":1}}`},
		{name: "empty command", body: `{"s":{"command":""}}`},
		{name: "non-string args", body: `{"s":{"command":"run","args":"x"}}`},
		{name: "non-string env value", body: `{"s":{"command":"run","env":{"K":1}}}`},
		{name: "trailing value", body: `{"s":{"command":"run"}} {}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, _ := newTestService(t, "")
			dir := t.TempDir()
			err := svc.Put("mcp", dir, json.RawMessage(tc.body))
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Put(%s) error = %v, want nil", tc.body, err)
				}
				got, err := svc.Get("mcp", dir)
				if err != nil {
					t.Fatalf("Get after Put error = %v", err)
				}
				if !json.Valid(got) {
					t.Fatalf("Get returned invalid JSON %q", got)
				}
				return
			}
			if errorKind(t, err) != filesystem.ErrorKindInvalid {
				t.Fatalf("Put(%s) kind = %v, want invalid", tc.body, errorKind(t, err))
			}
			if _, err := svc.Get("mcp", dir); errorKind(t, err) != filesystem.ErrorKindNotFound {
				t.Fatalf("rejected Put wrote a file: kind = %v", errorKind(t, err))
			}
		})
	}
}

func TestPutSkillsValidation(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		wantOK bool
	}{
		{name: "empty object", body: `{}`, wantOK: true},
		{name: "nested arbitrary values", body: `{"a":{"b":[1,2,{"c":null}]},"d":"text"}`, wantOK: true},
		{name: "null value inside", body: `{"a":null}`, wantOK: true},
		{name: "array top level", body: `[]`},
		{name: "scalar top level", body: `"x"`},
		{name: "null top level", body: `null`},
		{name: "malformed", body: `{"a":`},
		{name: "trailing value", body: `{} []`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, _ := newTestService(t, "")
			dir := t.TempDir()
			err := svc.Put("skills", dir, json.RawMessage(tc.body))
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Put(%s) error = %v, want nil", tc.body, err)
				}
				return
			}
			if errorKind(t, err) != filesystem.ErrorKindInvalid {
				t.Fatalf("Put(%s) kind = %v, want invalid", tc.body, errorKind(t, err))
			}
			if _, err := svc.Get("skills", dir); errorKind(t, err) != filesystem.ErrorKindNotFound {
				t.Fatalf("rejected Put wrote a file: kind = %v", errorKind(t, err))
			}
		})
	}
}

func TestPutWritesCanonicalMode0600(t *testing.T) {
	svc, _ := newTestService(t, "")
	dir := t.TempDir()

	if err := svc.Put("mcp", dir, json.RawMessage(`{"s":{"command":"run"}}`)); err != nil {
		t.Fatalf("Put error = %v", err)
	}
	path := filepath.Join(dir, ".agent-bridge", "config", "mcp.json")
	want := "{\n  \"s\": {\n    \"command\": \"run\"\n  }\n}\n"
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(got) != want {
		t.Fatalf("config bytes = %q, want %q", got, want)
	}
	assertMode(t, path, 0o600)

	if err := svc.Put("mcp", dir, json.RawMessage(`{"s":{"command":"other","args":["a"]}}`)); err != nil {
		t.Fatalf("overwrite Put error = %v", err)
	}
	assertMode(t, path, 0o600)

	configDirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat config dir: %v", err)
	}
	if perm := configDirInfo.Mode().Perm(); perm&^0o700 != 0 {
		t.Fatalf("config dir mode = %04o, want no group/other permission bits", perm)
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("read config dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "mcp.json" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("temp residue in config dir: %v", names)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode(%s) = %04o, want %04o", path, got, want)
	}
}

func TestDeleteRemovesFileAndMissingIsNotFound(t *testing.T) {
	svc, _ := newTestService(t, "")
	dir := t.TempDir()

	if err := svc.Delete("skills", dir); errorKind(t, err) != filesystem.ErrorKindNotFound {
		t.Fatalf("Delete missing kind = %v, want not_found", errorKind(t, err))
	}

	if err := svc.Put("skills", dir, json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatalf("Put error = %v", err)
	}
	if err := svc.Delete("skills", dir); err != nil {
		t.Fatalf("Delete error = %v", err)
	}
	if _, err := svc.Get("skills", dir); errorKind(t, err) != filesystem.ErrorKindNotFound {
		t.Fatalf("Get after Delete kind = %v, want not_found", errorKind(t, err))
	}
}

func TestFailedReplacementPreservesBytes(t *testing.T) {
	svc, _ := newTestService(t, "")
	dir := t.TempDir()
	path := filepath.Join(dir, ".agent-bridge", "config", "mcp.json")

	if err := svc.Put("mcp", dir, json.RawMessage(`{"s":{"command":"keep"}}`)); err != nil {
		t.Fatalf("Put error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	if err := svc.Put("mcp", dir, json.RawMessage(`{"s":{"command":""}}`)); errorKind(t, err) != filesystem.ErrorKindInvalid {
		t.Fatalf("failed Put kind = %v, want invalid", errorKind(t, err))
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config after failed Put: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("failed replacement changed bytes: before %q after %q", before, after)
	}
}

func TestConcurrentPutGetIntegrity(t *testing.T) {
	svc, _ := newTestService(t, "")
	dir := t.TempDir()

	bodies := []json.RawMessage{
		json.RawMessage(`{"a":{"command":"one"}}`),
		json.RawMessage(`{"b":{"command":"two","args":["x"]}}`),
		json.RawMessage(`{"c":{"command":"three","env":{"K":"v"}}}`),
		json.RawMessage(`{"d":{"command":"four"}}`),
	}

	var writers sync.WaitGroup
	for _, body := range bodies {
		writers.Go(func() {
			for range 50 {
				if err := svc.Put("mcp", dir, body); err != nil {
					t.Errorf("Put error = %v", err)
					return
				}
			}
		})
	}

	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				msg, err := svc.Get("mcp", dir)
				if err != nil {
					var e *filesystem.Error
					if errors.As(err, &e) && e.Kind == filesystem.ErrorKindNotFound {
						continue
					}
					t.Errorf("Get error = %v", err)
					return
				}
				var decoded map[string]MCPServer
				if err := json.Unmarshal(msg, &decoded); err != nil {
					t.Errorf("Get observed malformed JSON %q: %v", msg, err)
					return
				}
				for _, server := range decoded {
					if server.Command == "" {
						t.Errorf("Get observed incomplete object %q", msg)
						return
					}
				}
			}
		})
	}

	writers.Wait()
	close(done)
	readers.Wait()

	assertMode(t, filepath.Join(dir, ".agent-bridge", "config", "mcp.json"), 0o600)
}

func TestPutDeleteUseSharedMutationMutex(t *testing.T) {
	ops := []struct {
		name    string
		prepare func(t *testing.T, home string)
		call    func(svc *Service) error
	}{
		{
			name: "put",
			call: func(svc *Service) error {
				return svc.Put("mcp", "project", json.RawMessage(`{"a":{"command":"c"}}`))
			},
		},
		{
			name: "delete",
			prepare: func(t *testing.T, home string) {
				t.Helper()
				path := filepath.Join(home, "project", ".agent-bridge", "config", "mcp.json")
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatalf("mkdir config dir: %v", err)
				}
				if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
					t.Fatalf("write config: %v", err)
				}
			},
			call: func(svc *Service) error { return svc.Delete("mcp", "project") },
		},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			home := t.TempDir()
			if op.prepare != nil {
				op.prepare(t, home)
			}
			svc, mu := newTestService(t, home)

			mu.Lock()
			done := make(chan error, 1)
			go func() { done <- op.call(svc) }()
			for range 1000 {
				runtime.Gosched()
				select {
				case err := <-done:
					mu.Unlock()
					t.Fatalf("%s did not block on the injected mutation mutex (err=%v)", op.name, err)
				default:
				}
			}
			mu.Unlock()
			if err := <-done; err != nil {
				t.Fatalf("%s error = %v", op.name, err)
			}
		})
	}
}

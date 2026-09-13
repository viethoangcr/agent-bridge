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

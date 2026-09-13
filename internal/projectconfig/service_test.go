package projectconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

// TestConfigPathResolvesFixedSuffix proves the resolved path always appends the fixed suffix and rejects unknown kinds.
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

// TestGetMissingDoesNotCreateRoots proves a missing config reports not_found without creating directories.
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

// TestPutMCPValidation proves valid mcp configs round-trip and invalid ones are rejected without writing.
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

// TestPutSkillsValidation proves skills configs accept arbitrary object values and reject invalid shapes.
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

// TestDeleteRemovesFileAndMissingIsNotFound proves Delete removes the file and reports not_found when absent.
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

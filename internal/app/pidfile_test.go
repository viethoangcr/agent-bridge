package app

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestWritePIDFileContentAndMode asserts the decimal PID plus newline and 0600
// permissions required by the specification.
func TestWritePIDFileContentAndMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.pid")
	if err := writePIDFile(path); err != nil {
		t.Fatalf("writePIDFile() = %v, want nil", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	want := strconv.Itoa(os.Getpid()) + "\n"
	if string(got) != want {
		t.Fatalf("pid file = %q, want %q", got, want)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat pid file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("pid file mode = %o, want 0600", perm)
	}
}

// TestWritePIDFileRefusesExisting asserts O_EXCL semantics: an existing file is
// never replaced and keeps its original content.
func TestWritePIDFileRefusesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.pid")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatalf("seed pid file: %v", err)
	}

	if err := writePIDFile(path); err == nil {
		t.Fatal("writePIDFile() = nil, want refusal to replace existing file")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	if string(got) != "existing" {
		t.Fatalf("pid file = %q, want original content preserved", got)
	}
}

// TestRemovePIDFileIgnoresMissing asserts removing an already-absent file is
// success while other removal failures would still surface.
func TestRemovePIDFileIgnoresMissing(t *testing.T) {
	if err := removePIDFile(filepath.Join(t.TempDir(), "absent.pid")); err != nil {
		t.Fatalf("removePIDFile() = %v, want nil for missing file", err)
	}
}

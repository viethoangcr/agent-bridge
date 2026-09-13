package process

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManager_Run_SeparateStreamsAndInvalidUTF8(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)

	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperOutput)})
	if err != nil {
		t.Fatalf("Run(output): %v", err)
	}
	if res.Stdout != "stdout-payload" || res.Stderr != "stderr-payload" {
		t.Fatalf("stdout=%q stderr=%q, want separate payloads", res.Stdout, res.Stderr)
	}
	if res.StdoutTruncated || res.StderrTruncated {
		t.Fatalf("truncation flags = %v/%v, want false", res.StdoutTruncated, res.StderrTruncated)
	}

	res, err = m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperBadUTF8)})
	if err != nil {
		t.Fatalf("Run(bad-utf8): %v", err)
	}
	raw := []byte{0xff, 0xfe, 'h', 'i'}
	if want := strings.ToValidUTF8(string(raw), "\uFFFD"); res.Stdout != want {
		t.Fatalf("stdout = %q, want ToValidUTF8 %q", res.Stdout, want)
	}
}

func TestManager_Run_OutputCaps(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)
	setRunConfig(t, m, func(c *Config) { c.MaxOutputBytes = 14 })

	// The default cap equals the active max and exactly fits the helper output.
	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperOutput)})
	if err != nil {
		t.Fatalf("Run(default cap): %v", err)
	}
	if res.Stdout != "stdout-payload" || res.Stderr != "stderr-payload" {
		t.Fatalf("default cap truncated exact-boundary output: %q / %q", res.Stdout, res.Stderr)
	}
	if res.StdoutTruncated || res.StderrTruncated {
		t.Fatal("exact-boundary output was flagged truncated")
	}

	// A cap above the active max is rejected, never clamped.
	if _, err := m.Run(context.Background(), RunRequest{
		Command: os.Args[0], Env: helperEnv(helperOutput), MaxOutputBytes: ptrI64(15),
	}); !errors.Is(err, ErrValidation) {
		t.Fatalf("cap above max error = %v, want ErrValidation", err)
	}

	// One byte below the boundary truncates both independent streams.
	res, err = m.Run(context.Background(), RunRequest{
		Command: os.Args[0], Env: helperEnv(helperOutput), MaxOutputBytes: ptrI64(13),
	})
	if err != nil {
		t.Fatalf("Run(13): %v", err)
	}
	if len(res.Stdout) != 13 || !res.StdoutTruncated {
		t.Fatalf("stdout = %q (len %d) truncated=%v, want 13 bytes truncated", res.Stdout, len(res.Stdout), res.StdoutTruncated)
	}
	if len(res.Stderr) != 13 || !res.StderrTruncated {
		t.Fatalf("stderr = %q (len %d) truncated=%v, want 13 bytes truncated", res.Stderr, len(res.Stderr), res.StderrTruncated)
	}
}

func TestManager_Run_TruncationFlagsIndependent(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)

	res, err := m.Run(context.Background(), RunRequest{
		Command:        "/bin/sh",
		Args:           []string{"-c", "head -c 20 /dev/zero; head -c 5 /dev/zero >&2"},
		MaxOutputBytes: ptrI64(10),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Stdout) != 10 || !res.StdoutTruncated {
		t.Fatalf("stdout len=%d truncated=%v, want 10 truncated", len(res.Stdout), res.StdoutTruncated)
	}
	if res.Stderr != "\x00\x00\x00\x00\x00" || res.StderrTruncated {
		t.Fatalf("stderr = %q len=%d truncated=%v, want 5 bytes not truncated", res.Stderr, len(res.Stderr), res.StderrTruncated)
	}
}

func TestManager_Run_TruncationContinuesDraining(t *testing.T) {
	requireLinuxProcess(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "big.bin")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 200000), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	m := newRunManager(t, 20*MaxOutputBytes, 4)
	res, err := m.Run(context.Background(), RunRequest{
		Command:        "/bin/sh",
		Args:           []string{"-c", "/bin/cat " + path},
		MaxOutputBytes: ptrI64(1024),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Stdout) != 1024 || !res.StdoutTruncated {
		t.Fatalf("stdout len=%d truncated=%v, want 1024 truncated while draining the rest", len(res.Stdout), res.StdoutTruncated)
	}
}

func TestManager_Run_CaptureDrainsAndTruncates(t *testing.T) {
	got, truncated := capture(strings.NewReader("0123456789"), 4)
	if got != "0123" || !truncated {
		t.Fatalf("capture = %q, %v; want 0123, true", got, truncated)
	}
	got, truncated = capture(strings.NewReader("0123"), 4)
	if got != "0123" || truncated {
		t.Fatalf("capture exact = %q, %v; want 0123, false", got, truncated)
	}
}

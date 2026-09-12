package process

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"testing"
	"time"
)

// --- Task 4.5: input, stop, kill, and group semantics ---

// setInputLimit installs a small decoded input limit so boundary behavior can
// be exercised without allocating multi-megabyte payloads.
func setInputLimit(t *testing.T, m *Manager, limit int) {
	t.Helper()
	cfg := m.config.load()
	cfg.MaxInputBytesPerRequest = limit
	if err := m.config.update(cfg); err != nil {
		t.Fatalf("update maxInputBytesPerRequest: %v", err)
	}
}

func lookupProcess(t *testing.T, m *Manager, id string) *managedProcess {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.processes[id]
	if p == nil {
		t.Fatalf("process %s not found", id)
	}
	return p
}

// waitForStdout polls until the process's combined stdout decodes exactly to
// want. Output arrives asynchronously from the pump, so this is a bounded
// observation loop rather than a timed wait.
func waitForStdout(t *testing.T, m *Manager, id string, want []byte) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if bytes.Equal(decodeLogData(t, mustLogs(t, m, id, LogQuery{Stream: "stdout"})), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("stdout never matched %d bytes of input", len(want))
}

func TestManager_Input_RoundTripEncoding(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)

	binary := []byte{0x00, 0x01, 0xfe, 0xff}
	cases := []struct {
		name     string
		encoding string
		wire     []byte
		want     []byte
	}{
		{"utf8", "utf8", []byte("héllo ✓ wörld"), []byte("héllo ✓ wörld")},
		{"base64", "base64", []byte(base64.StdEncoding.EncodeToString(binary)), binary},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := DecodeInput(tc.encoding, tc.wire)
			if err != nil {
				t.Fatalf("DecodeInput(%q): %v", tc.encoding, err)
			}
			if !bytes.Equal(decoded, tc.want) {
				t.Fatalf("DecodeInput(%q) = %v, want %v", tc.encoding, decoded, tc.want)
			}

			snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperStdinEcho)})
			n, err := m.WriteInput(context.Background(), snap.ID, decoded)
			if err != nil {
				t.Fatalf("WriteInput: %v", err)
			}
			if n != len(decoded) {
				t.Fatalf("bytesWritten = %d, want %d", n, len(decoded))
			}
			waitForStdout(t, m, snap.ID, tc.want)
			if _, err := m.Kill(snap.ID); err != nil {
				t.Fatalf("Kill: %v", err)
			}
		})
	}
}

func TestManager_Input_InvalidEncoding(t *testing.T) {
	if _, err := DecodeInput("hex", []byte("00")); !errors.Is(err, ErrValidation) {
		t.Fatalf("DecodeInput(hex) error = %v, want ErrValidation", err)
	}
	if _, err := DecodeInput("", []byte("data")); !errors.Is(err, ErrValidation) {
		t.Fatalf("DecodeInput(empty) error = %v, want ErrValidation", err)
	}
	if _, err := DecodeInput("base64", []byte("not base64!")); !errors.Is(err, ErrValidation) {
		t.Fatalf("DecodeInput(malformed base64) error = %v, want ErrValidation", err)
	}
	got, err := DecodeInput("utf8", []byte("plain"))
	if err != nil || string(got) != "plain" {
		t.Fatalf("DecodeInput(utf8) = %q, %v; want plain, nil", got, err)
	}
}

func TestManager_Input_DecodedLimitBoundary(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)
	setInputLimit(t, m, 8)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperStdinEcho)})

	atLimit := []byte("12345678")
	n, err := m.WriteInput(context.Background(), snap.ID, atLimit)
	if err != nil || n != len(atLimit) {
		t.Fatalf("WriteInput(at limit) = %d, %v; want %d, nil", n, err, len(atLimit))
	}
	waitForStdout(t, m, snap.ID, atLimit)

	over := []byte("123456789")
	n, err = m.WriteInput(context.Background(), snap.ID, over)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("WriteInput(over limit) error = %v, want ErrPayloadTooLarge", err)
	}
	if n != 0 {
		t.Fatalf("WriteInput(over limit) wrote %d bytes, want 0", n)
	}
}

func TestManager_Input_StateMapping(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)
	env := helperEnv(helperExitCode)
	env[processHelperExitEnv] = "0"
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	waitProcessDone(t, m, snap.ID)

	if _, err := m.WriteInput(context.Background(), snap.ID, []byte("x")); !errors.Is(err, ErrConflict) {
		t.Fatalf("WriteInput(exited) error = %v, want ErrConflict", err)
	}
	if _, err := m.WriteInput(context.Background(), "proc_missing", []byte("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("WriteInput(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestManager_Input_SingleWriterAdmissionCancellation(t *testing.T) {
	requireLinuxProcess(t)
	const limit = 1 << 20
	m := newTestManager(t, 4)
	setInputLimit(t, m, limit)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})

	// The first writer fills the pipe and blocks because the child never reads.
	first := make(chan error, 1)
	go func() {
		_, err := m.WriteInput(context.Background(), snap.ID, make([]byte, limit))
		first <- err
	}()

	p := lookupProcess(t, m, snap.ID)
	deadline := time.Now().Add(5 * time.Second)
	for len(p.inputAdmission) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if len(p.inputAdmission) != 1 {
		t.Fatal("first writer never acquired the sole input admission slot")
	}

	// A queued writer waits for the slot; cancellation releases it promptly
	// without disturbing the process or the first writer.
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := m.WriteInput(ctx, snap.ID, []byte("queued"))
		second <- struct {
			n   int
			err error
		}{n, err}
	}()
	cancel()
	select {
	case res := <-second:
		if !errors.Is(res.err, ErrGateway) {
			t.Fatalf("canceled queued write error = %v, want ErrGateway", res.err)
		}
		if res.n != 0 {
			t.Fatalf("canceled queued write wrote %d bytes, want 0", res.n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued writer did not observe cancellation")
	}
	if got, err := m.Get(snap.ID); err != nil || got.Status != StatusRunning {
		t.Fatalf("process after canceled queued write = %+v, %v; want running", got, err)
	}

	// Killing the group unblocks the first writer; the partial failure is a
	// gateway error.
	if _, err := m.Kill(snap.ID); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	select {
	case err := <-first:
		if !errors.Is(err, ErrGateway) {
			t.Fatalf("blocked partial write error = %v, want ErrGateway", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first writer did not return after group kill")
	}
}

func TestManager_Input_PreWriteCancellationKeepsProcessUsable(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperStdinEcho)})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	n, err := m.WriteInput(ctx, snap.ID, []byte("dropped"))
	if !errors.Is(err, ErrGateway) {
		t.Fatalf("pre-write deadline error = %v, want ErrGateway", err)
	}
	if n != 0 {
		t.Fatalf("pre-write deadline wrote %d bytes, want 0", n)
	}
	if got, gerr := m.Get(snap.ID); gerr != nil || got.Status != StatusRunning {
		t.Fatalf("process after pre-write rejection = %+v, %v; want running", got, gerr)
	}

	want := []byte("usable")
	n, err = m.WriteInput(context.Background(), snap.ID, want)
	if err != nil || n != len(want) {
		t.Fatalf("subsequent WriteInput = %d, %v; want %d, nil", n, err, len(want))
	}
	waitForStdout(t, m, snap.ID, want)
}

func TestManager_Input_PartialWriteKillsGroup(t *testing.T) {
	requireLinuxProcess(t)
	const limit = 1 << 20
	m := newTestManager(t, 4)
	setInputLimit(t, m, limit)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	p := lookupProcess(t, m, snap.ID)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	payload := make([]byte, limit)
	n, err := m.WriteInput(ctx, snap.ID, payload)
	if !errors.Is(err, ErrGateway) {
		t.Fatalf("partial write error = %v, want ErrGateway", err)
	}
	if n <= 0 || n >= len(payload) {
		t.Fatalf("partial write bytes = %d, want 0 < n < %d", n, len(payload))
	}
	waitProcessDone(t, m, snap.ID)
	waitGoneOrZombie(t, p.pid)
}

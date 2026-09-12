package acpruntime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestRuntimeStartRejectsNonPositiveTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			store := newRuntimeStore(t)
			pidPath := filepath.Join(t.TempDir(), "helper-pids.json")
			env := withEnv(os.Environ(), helperEnv, helperProcessGroup)
			env = withEnv(env, helperPIDFileEnv, pidPath)
			spec := LaunchSpec{Program: os.Args[0], Env: env}

			r, err := Start(t.Context(), store, "srv", spec, timeout, testLogger())
			if err == nil {
				t.Fatalf("Start(%s) error = nil, want rejection", timeout)
			}
			if r != nil {
				t.Errorf("Start(%s) returned runtime %+v, want nil", timeout, r)
			}

			// No spawn happened: the row stays creating with no PID.
			server, err := store.Server(t.Context(), "srv")
			if err != nil {
				t.Fatalf("Server(srv): %v", err)
			}
			if server.Status != acpstore.StatusCreating {
				t.Errorf("status = %q, want %q (spawned despite bad timeout)", server.Status, acpstore.StatusCreating)
			}
			if server.PID != nil {
				t.Errorf("PID = %d, want nil (spawned despite bad timeout)", *server.PID)
			}
			if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("pid file %s exists, want no helper spawn", pidPath)
			}
		})
	}
}

func TestRuntimeStartLaunchesLiveProcessGroup(t *testing.T) {
	requireLinux(t)
	r, store, pidPath := startHelperRuntime(t, helperProcessGroup, time.Minute)
	t.Cleanup(func() {
		if pids, ok := tryReadHelperPIDs(pidPath); ok {
			killReportedPIDs(pids)
		}
	})

	pid := r.PID()
	if pid <= 1 {
		t.Fatalf("PID() = %d, want > 1", pid)
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatalf("Getpgid(%d): %v", pid, err)
	}
	if pgid != pid {
		t.Errorf("child PGID = %d, want its own group %d (Setpgid)", pgid, pid)
	}
	if testPGID := syscall.Getpgrp(); pgid == testPGID {
		t.Errorf("child PGID = %d equals the test process group, want a distinct group", pgid)
	}

	server, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server(srv): %v", err)
	}
	if server.Status != acpstore.StatusIdle {
		t.Errorf("status = %q, want %q", server.Status, acpstore.StatusIdle)
	}
	if server.PID == nil || *server.PID != pid {
		t.Errorf("stored PID = %v, want %d", server.PID, pid)
	}
}

func TestRuntimeStartWriteLineFraming(t *testing.T) {
	buf := &nopWriteCloser{Buffer: new(bytes.Buffer)}
	r := &Runtime{stdin: buf}
	payload := []byte(`{"jsonrpc":"2.0","id":1}`)

	if err := r.writeLine(payload); err != nil {
		t.Fatalf("writeLine: %v", err)
	}
	if got, want := buf.String(), string(payload)+"\n"; got != want {
		t.Errorf("writeLine framing = %q, want %q", got, want)
	}
}

func TestRuntimeProcessGroupKill(t *testing.T) {
	requireLinux(t)
	r, store, pidPath := startHelperRuntime(t, helperProcessGroup, time.Minute)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i, err := range concurrentKillers(ctx, r, 5) {
		if err != nil {
			t.Errorf("concurrent Kill[%d] error = %v, want nil", i, err)
		}
	}
	if err := r.Kill(ctx); err != nil {
		t.Errorf("repeated Kill error = %v, want nil", err)
	}
	if got := r.PID(); got != 0 {
		t.Errorf("PID() after Kill = %d, want 0", got)
	}

	waitReaped(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)

	server, err := store.Server(context.Background(), "srv")
	if err != nil {
		t.Fatalf("Server(srv): %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Errorf("status after Kill = %q, want %q", server.Status, acpstore.StatusExited)
	}
	if server.PID != nil {
		t.Errorf("stored PID after Kill = %d, want nil", *server.PID)
	}
}

func TestRuntimeProcessGroupChildExit(t *testing.T) {
	requireLinux(t)
	r, store, pidPath := startHelperRuntime(t, helperChildExits, time.Minute)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	done := make(chan error, 1)
	go func() { done <- r.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after the direct child exited")
	}

	if got := r.PID(); got != 0 {
		t.Errorf("PID() after direct-child exit = %d, want 0", got)
	}
	waitReaped(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)

	server, err := store.Server(context.Background(), "srv")
	if err != nil {
		t.Fatalf("Server(srv): %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Errorf("status after direct-child exit = %q, want %q", server.Status, acpstore.StatusExited)
	}
	if server.PID != nil {
		t.Errorf("stored PID after direct-child exit = %d, want nil", *server.PID)
	}
}

func TestRuntimeSpawnFailureMarksExited(t *testing.T) {
	store := newRuntimeStore(t)
	spec := LaunchSpec{Program: filepath.Join(t.TempDir(), "missing-agent-binary-xyz")}

	r, err := Start(t.Context(), store, "srv", spec, time.Minute, testLogger())
	if err == nil {
		t.Fatal("Start(missing binary) error = nil, want spawn failure")
	}
	if r != nil {
		t.Errorf("Start(missing binary) returned runtime %+v, want nil", r)
	}
	t.Logf("spawn error: %v", err)

	server, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server(srv): %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Errorf("status after failed spawn = %q, want %q", server.Status, acpstore.StatusExited)
	}
	if server.PID != nil {
		t.Errorf("stored PID after failed spawn = %d, want nil", *server.PID)
	}
}

// cleanupReportedPIDs registers bounded test-owned cleanup for reported PIDs.
func cleanupReportedPIDs(t *testing.T, pids helperPIDs) {
	t.Helper()
	t.Cleanup(func() { killReportedPIDs(pids) })
}

package acpruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/mockagent"
)

// Test-binary helper protocol. The test binary re-execs itself with helperEnv
// set to one of the modes below; testhelperPIDsEnv names a file where the
// helper records the direct and grandchild PIDs as one JSON line.
const (
	helperEnv        = "AGENT_BRIDGE_TEST_RUNTIME_HELPER"
	helperPIDFileEnv = "AGENT_BRIDGE_TEST_RUNTIME_PIDFILE"

	helperProcessGroup = "process-group"
	helperChildExits   = "child-exits"
	helperGrandchild   = "grandchild"
)

// helperPIDs is the single JSON line a helper prints and records.
type helperPIDs struct {
	Direct     int `json:"direct"`
	Grandchild int `json:"grandchild"`
}

func TestMain(m *testing.M) {
	// The private mock mode re-execs this test binary in place of the bridge
	// binary for the stdout-ingestion integration tests.
	if os.Getenv(mockEnvVar) == "1" {
		if err := mockagent.Run(context.Background(), os.Stdin, os.Stdout, os.Stderr); err != nil {
			fmt.Fprintln(os.Stderr, "mock agent:", err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	switch os.Getenv(helperEnv) {
	case helperProcessGroup:
		runProcessGroupHelper()
	case helperChildExits:
		runChildExitsHelper()
	case helperGrandchild:
		runGrandchildHelper()
	}
	os.Exit(m.Run())
}

// runProcessGroupHelper starts a long-lived grandchild in the same process
// group, records both PIDs, and blocks.
func runProcessGroupHelper() {
	grandchild := startGrandchild()
	recordHelperPIDs(grandchild)
	blockForever()
}

// runChildExitsHelper starts a grandchild, records both PIDs, then exits the
// direct child immediately while the grandchild keeps the inherited pipes.
func runChildExitsHelper() {
	grandchild := startGrandchild()
	recordHelperPIDs(grandchild)
	os.Exit(0)
}

// runGrandchildHelper is the grandchild mode: it holds inherited pipes open and
// blocks until the process group is killed.
func runGrandchildHelper() {
	blockForever()
}

func startGrandchild() int {
	cmd := exec.Command(os.Args[0])
	cmd.Env = withEnv(os.Environ(), helperEnv, helperGrandchild)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start grandchild:", err)
		os.Exit(2)
	}
	return cmd.Process.Pid
}

// recordHelperPIDs writes the JSON PID line to the pid file and stdout.
func recordHelperPIDs(grandchild int) {
	line, err := json.Marshal(helperPIDs{Direct: os.Getpid(), Grandchild: grandchild})
	if err != nil {
		os.Exit(2)
	}
	line = append(line, '\n')
	if path := os.Getenv(helperPIDFileEnv); path != "" {
		if err := os.WriteFile(path, line, 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "write pid file:", err)
			os.Exit(2)
		}
	}
	_, _ = os.Stdout.Write(line)
}

func blockForever() {
	for {
		time.Sleep(time.Hour)
	}
}

// withEnv returns environ with key replaced by value (last occurrence wins for
// os/exec callers, but this is explicit and test-friendly).
func withEnv(environ []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(environ)+1)
	for _, entry := range environ {
		if len(entry) >= len(prefix) && entry[:len(prefix)] == prefix {
			continue
		}
		out = append(out, entry)
	}
	return append(out, prefix+value)
}

// requireLinux skips on non-Linux platforms; production is Linux-only but the
// process-group tests use Linux /proc semantics.
func requireLinux(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("process-group tests require Linux")
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newRuntimeStore opens a store with one creating server row for "srv".
func newRuntimeStore(t *testing.T) *acpstore.Store {
	t.Helper()
	store, err := acpstore.Open(t.Context(), filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := store.CreateServer(t.Context(), "srv", "mock"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	return store
}

// startHelperRuntime starts a runtime running the test binary in mode with a
// pid file. A bounded cleanup always terminates any reported PIDs.
func startHelperRuntime(t *testing.T, mode string, timeout time.Duration) (*Runtime, *acpstore.Store, string) {
	t.Helper()
	store := newRuntimeStore(t)
	pidPath := filepath.Join(t.TempDir(), "helper-pids.json")
	env := withEnv(os.Environ(), helperEnv, mode)
	env = withEnv(env, helperPIDFileEnv, pidPath)
	spec := LaunchSpec{Program: os.Args[0], Env: env}

	r, err := Start(t.Context(), store, "srv", spec, timeout, testLogger())
	if err != nil {
		t.Fatalf("Start(%s): %v", mode, err)
	}
	t.Cleanup(func() {
		if pids, ok := tryReadHelperPIDs(pidPath); ok {
			killReportedPIDs(pids)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Kill(ctx)
	})
	return r, store, pidPath
}

// readHelperPIDs polls the pid file until the helper reports both PIDs.
func readHelperPIDs(t *testing.T, path string) helperPIDs {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pids, ok := tryReadHelperPIDs(path); ok {
			return pids
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("helper did not report PIDs to %s", path)
	return helperPIDs{}
}

func tryReadHelperPIDs(path string) (helperPIDs, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return helperPIDs{}, false
	}
	var pids helperPIDs
	if err := json.Unmarshal(bytes.TrimSpace(data), &pids); err != nil {
		return helperPIDs{}, false
	}
	if pids.Direct <= 0 || pids.Grandchild <= 0 {
		return helperPIDs{}, false
	}
	return pids, true
}

// killReportedPIDs is best-effort bounded cleanup for helper PIDs. -direct is
// safe: it only names a group when the runtime used Setpgid, otherwise it is
// an unused PGID and ESRCH.
func killReportedPIDs(pids helperPIDs) {
	_ = syscall.Kill(-pids.Direct, syscall.SIGKILL)
	_ = syscall.Kill(pids.Direct, syscall.SIGKILL)
	_ = syscall.Kill(pids.Grandchild, syscall.SIGKILL)
}

// processState reads the state character from /proc/<pid>/stat after the
// parenthesized command, returning false when the process is gone.
func processState(pid int) (byte, bool) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	// The command may contain spaces and parentheses; the state is the first
	// field after the final ')' and its trailing space.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return 0, false
	}
	return data[end+2], true
}

// waitReaped requires the direct child to disappear (reaped), using a bounded
// deadline. It never uses kill(pid, 0) as evidence.
func waitReaped(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := processState(pid); !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("direct child %d was not reaped within deadline", pid)
}

// waitGoneOrZombie requires a descendant to disappear or become a zombie.
func waitGoneOrZombie(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, ok := processState(pid)
		if !ok || state == 'Z' {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("descendant %d remains in non-zombie state", pid)
}

// nopWriteCloser captures writes in memory for the writeLine framing test.
type nopWriteCloser struct{ *bytes.Buffer }

func (nopWriteCloser) Close() error { return nil }

// concurrentKillers calls Kill from n goroutines and returns their errors.
func concurrentKillers(ctx context.Context, r *Runtime, n int) []error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = r.Kill(ctx)
		}(i)
	}
	wg.Wait()
	return errs
}

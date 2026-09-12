package process

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// newTestManagerWithBase returns a manager with small injected budgets and a
// small concurrency limit, registering bounded cleanup for every process.
func newTestManagerWithBase(t *testing.T, base []string, cwd string, maxConcurrent int) *Manager {
	t.Helper()
	m, err := newManagerWithBudgets(base, cwd, managerBudgets{
		retainedLogMemoryBytes: 1 << 20,
		activeRunPeakBytes:     1 << 20,
	})
	if err != nil {
		t.Fatalf("newManagerWithBudgets: %v", err)
	}
	cfg := DefaultConfig()
	cfg.MaxConcurrentProcesses = maxConcurrent
	if err := m.config.update(cfg); err != nil {
		t.Fatalf("install test config: %v", err)
	}
	t.Cleanup(func() { cleanupManager(t, m) })
	return m
}

func newTestManager(t *testing.T, maxConcurrent int) *Manager {
	t.Helper()
	return newTestManagerWithBase(t, nil, t.TempDir(), maxConcurrent)
}

// helperEnv returns a request overlay selecting one helper mode.
func helperEnv(mode string) map[string]string {
	return map[string]string{processHelperEnv: mode}
}

func startHelper(t *testing.T, m *Manager, req StartRequest) Snapshot {
	t.Helper()
	snap, err := m.Start(req)
	if err != nil {
		t.Fatalf("Start(%+v): %v", req, err)
	}
	return snap
}

// waitProcessDone blocks until the record's publisher has marked it exited and
// released its capacity, or fails the test.
func waitProcessDone(t *testing.T, m *Manager, id string) {
	t.Helper()
	m.mu.Lock()
	p := m.processes[id]
	m.mu.Unlock()
	if p == nil {
		t.Fatalf("process %s not found while waiting for exit", id)
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("process %s did not exit", id)
	}
}

func activeProcesses(m *Manager) int {
	m.reserveMu.Lock()
	defer m.reserveMu.Unlock()
	return m.activeProcesses
}

// cleanupManager kills any still-running managed groups and waits for every
// record to finish. It never leaves helper processes behind on failure.
func cleanupManager(t *testing.T, m *Manager) {
	t.Helper()
	m.mu.Lock()
	procs := make([]*managedProcess, 0, len(m.processes))
	for _, p := range m.processes {
		procs = append(procs, p)
	}
	m.mu.Unlock()

	for _, p := range procs {
		m.mu.Lock()
		running := p.status == StatusRunning
		m.mu.Unlock()
		if running {
			_ = p.signalGroup(syscall.SIGKILL)
		}
	}
	for _, p := range procs {
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			t.Errorf("managed process %s did not exit during cleanup", p.id)
		}
	}
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
	if pids.Direct <= 1 || pids.Grandchild <= 1 {
		return helperPIDs{}, false
	}
	return pids, true
}

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

func killHelperPIDs(pids helperPIDs) {
	_ = syscall.Kill(-pids.Direct, syscall.SIGKILL)
	_ = syscall.Kill(pids.Direct, syscall.SIGKILL)
	_ = syscall.Kill(pids.Grandchild, syscall.SIGKILL)
}

// processState reads the state character from /proc/<pid>/stat after the
// parenthesized command, reporting false when the process is gone.
func processState(pid int) (byte, bool) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return 0, false
	}
	return data[end+2], true
}

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

func requireLinuxProcess(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("managed process-group tests require Linux")
	}
}

func readHelperReport(t *testing.T, path string) helperReport {
	t.Helper()
	for range 200 {
		data, err := os.ReadFile(path)
		if err == nil {
			var report helperReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatalf("decode helper report: %v", err)
			}
			return report
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("helper report %s was not written", path)
	return helperReport{}
}

func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, entry := range env {
		if key, value, found := strings.Cut(entry, "="); found {
			out[key] = value
		}
	}
	return out
}

func snapshotIDs(snapshots []Snapshot) []string {
	ids := make([]string, len(snapshots))
	for i, s := range snapshots {
		ids[i] = s.ID
	}
	return ids
}

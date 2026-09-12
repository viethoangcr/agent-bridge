package process

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

// --- Task 4.4: pump and query managed logs ---

// newTestManagerWithLogBudget returns a manager with an injected aggregate
// retained-log budget so charged-memory eviction can be exercised with small
// allocations.
func newTestManagerWithLogBudget(t *testing.T, logBudget, maxConcurrent int) *Manager {
	t.Helper()
	m, err := newManagerWithBudgets(nil, t.TempDir(), managerBudgets{
		retainedLogMemoryBytes: logBudget,
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

// setLogCap installs a new per-process log byte cap; it affects only rings
// created after the update.
func setLogCap(t *testing.T, m *Manager, capBytes int) {
	t.Helper()
	cfg := m.config.load()
	cfg.MaxLogBytesPerProcess = capBytes
	if err := m.config.update(cfg); err != nil {
		t.Fatalf("update maxLogBytesPerProcess: %v", err)
	}
}

// testManagedProcess returns an unregistered, already-exited record with the
// given per-process ring cap. It lets log accounting be exercised without
// spawning a child.
func testManagedProcess(id string, ringCap int) *managedProcess {
	done := make(chan struct{})
	close(done)
	return &managedProcess{id: id, status: StatusExited, ring: newLogRing(ringCap), done: done, inputAdmission: make(chan struct{}, 1)}
}

func registerTestProcess(m *Manager, p *managedProcess) {
	m.mu.Lock()
	m.processes[p.id] = p
	m.mu.Unlock()
}

func mustLogs(t *testing.T, m *Manager, id string, q LogQuery) []LogEntry {
	t.Helper()
	entries, err := m.Logs(id, q)
	if err != nil {
		t.Fatalf("Logs(%s, %+v): %v", id, q, err)
	}
	return entries
}

func decodeLogData(t *testing.T, entries []LogEntry) []byte {
	t.Helper()
	var out []byte
	for _, e := range entries {
		if e.Encoding != "base64" {
			t.Fatalf("entry %d encoding = %q, want base64", e.Sequence, e.Encoding)
		}
		raw, err := base64.StdEncoding.DecodeString(e.Data)
		if err != nil {
			t.Fatalf("entry %d data is not base64: %v", e.Sequence, err)
		}
		out = append(out, raw...)
	}
	return out
}

func retainedLogCharge(m *Manager) int {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	return m.logCharge
}

func ringContains(e *logEntry) bool {
	for _, cur := range e.ring.entries {
		if cur == e {
			return true
		}
	}
	return false
}

func startCat(t *testing.T, m *Manager, path string) Snapshot {
	t.Helper()
	snap, err := m.Start(StartRequest{Command: "/bin/sh", Args: []string{"-c", "/bin/cat " + path}})
	if err != nil {
		t.Fatalf("Start(cat %s): %v", path, err)
	}
	return snap
}

func writePattern(t *testing.T, path string, n int, seed byte) []byte {
	t.Helper()
	data := make([]byte, n)
	for i := range data {
		data[i] = seed + byte(i%26)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return data
}

func TestManager_Logs_SplitsOutputAndDecodesPerStream(t *testing.T) {
	requireLinuxProcess(t)

	dir := t.TempDir()
	stdoutPath := filepath.Join(dir, "stdout.bin")
	stderrPath := filepath.Join(dir, "stderr.bin")
	stdoutWant := writePattern(t, stdoutPath, 20000, 'A')
	stderrWant := writePattern(t, stderrPath, 5000, 'a')

	m := newTestManager(t, 4)
	snap, err := m.Start(StartRequest{
		Command: "/bin/sh",
		Args:    []string{"-c", "/bin/cat " + stdoutPath + "; /bin/cat " + stderrPath + " >&2"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitProcessDone(t, m, snap.ID)

	stdoutEntries := mustLogs(t, m, snap.ID, LogQuery{Stream: "stdout"})
	if got := decodeLogData(t, stdoutEntries); !bytes.Equal(got, stdoutWant) {
		t.Fatalf("stdout decode = %d bytes, want the original %d bytes", len(got), len(stdoutWant))
	}
	if len(stdoutEntries) < 3 {
		t.Fatalf("stdout entries = %d, want >8KiB split into at least 3 chunks", len(stdoutEntries))
	}
	for _, e := range stdoutEntries {
		raw, err := base64.StdEncoding.DecodeString(e.Data)
		if err != nil {
			t.Fatalf("entry %d data not base64: %v", e.Sequence, err)
		}
		if len(raw) > pumpBufferSize {
			t.Fatalf("entry %d decoded to %d bytes, want <= %d", e.Sequence, len(raw), pumpBufferSize)
		}
	}

	stderrEntries := mustLogs(t, m, snap.ID, LogQuery{Stream: "stderr"})
	if got := decodeLogData(t, stderrEntries); !bytes.Equal(got, stderrWant) {
		t.Fatalf("stderr decode = %d bytes, want the original %d bytes", len(got), len(stderrWant))
	}

	all := mustLogs(t, m, snap.ID, LogQuery{})
	var last int64
	for i, e := range all {
		if i == 0 && e.Sequence != 1 {
			t.Fatalf("first sequence = %d, want 1", e.Sequence)
		}
		if e.Sequence <= last {
			t.Fatalf("sequences not increasing: %d after %d", e.Sequence, last)
		}
		last = e.Sequence
		if e.Stream != "stdout" && e.Stream != "stderr" {
			t.Fatalf("entry %d stream = %q, want stdout or stderr", e.Sequence, e.Stream)
		}
	}
}

// TestManager_Start_LargeBurstFullyCaptured is the pipe-ownership regression:
// a burst larger than the kernel pipe buffer must be fully drained by the
// pumps even though cmd.Wait reaps the direct child concurrently. With
// cmd.StdoutPipe/cmd.StderrPipe, Wait closes the read ends out from under the
// pumps and the tail of the burst is lost.
func TestManager_Start_LargeBurstFullyCaptured(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManagerWithLogBudget(t, 4<<20, 4)

	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBurst)})
	waitProcessDone(t, m, snap.ID)

	got := decodeLogData(t, mustLogs(t, m, snap.ID, LogQuery{}))
	if !bytes.Equal(got, burstPayload()) {
		t.Fatalf("captured %d of %d burst bytes", len(got), helperBurstBytes)
	}
}

func TestManager_Logs_CreationTimeRingCap(t *testing.T) {
	requireLinuxProcess(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "big.bin")
	want := writePattern(t, path, 20000, 'x')

	m := newTestManagerWithLogBudget(t, 1<<20, 4)
	setLogCap(t, m, 4096)

	first := startCat(t, m, path)
	waitProcessDone(t, m, first.ID)
	got := decodeLogData(t, mustLogs(t, m, first.ID, LogQuery{}))
	if len(got) > 4096 || len(got) == len(want) {
		t.Fatalf("small capped ring retained %d bytes, want <=4096 and less than %d", len(got), len(want))
	}

	setLogCap(t, m, 1<<20)
	second := startCat(t, m, path)
	waitProcessDone(t, m, second.ID)
	got2 := decodeLogData(t, mustLogs(t, m, second.ID, LogQuery{}))
	if !bytes.Equal(got2, want) {
		t.Fatalf("large capped ring retained %d bytes, want full %d", len(got2), len(want))
	}

	// Lowering the cap again must not affect the already-created second ring.
	setLogCap(t, m, 1)
	third := startCat(t, m, path)
	waitProcessDone(t, m, third.ID)
	if got3 := decodeLogData(t, mustLogs(t, m, third.ID, LogQuery{})); len(got3) != 0 {
		t.Fatalf("cap-1 ring retained %d bytes, want 0", len(got3))
	}
	if again := decodeLogData(t, mustLogs(t, m, second.ID, LogQuery{})); !bytes.Equal(again, want) {
		t.Fatalf("existing ring changed after config update: got %d bytes, want %d", len(again), len(want))
	}
}

func TestManager_Logs_RepeatedQueriesReturnFreshBase64(t *testing.T) {
	requireLinuxProcess(t)

	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperOutput)})
	waitProcessDone(t, m, snap.ID)

	first := mustLogs(t, m, snap.ID, LogQuery{})
	if len(first) == 0 {
		t.Fatal("Logs returned no entries")
	}
	chargeBefore := retainedLogCharge(m)
	originalSeq := first[0].Sequence
	originalData := first[0].Data

	first[0].Data = "dGFtcGVyZWQ="
	first[0].Sequence = 999
	first[0].Stream = "stderr"

	second := mustLogs(t, m, snap.ID, LogQuery{})
	if second[0].Sequence != originalSeq || second[0].Data != originalData {
		t.Fatalf("query DTOs share state: %+v, want sequence %d data %q", second[0], originalSeq, originalData)
	}
	if retainedLogCharge(m) != chargeBefore {
		t.Fatal("querying logs changed aggregate retained charge")
	}

	m.mu.Lock()
	p := m.processes[snap.ID]
	m.mu.Unlock()
	m.logMu.Lock()
	rawTotal := 0
	for _, e := range p.ring.entries {
		rawTotal += len(e.raw)
	}
	m.logMu.Unlock()
	if decoded := decodeLogData(t, second); len(decoded) != rawTotal {
		t.Fatalf("ring retains %d raw bytes but DTO decodes to %d; base64 state may be retained", rawTotal, len(decoded))
	}
}

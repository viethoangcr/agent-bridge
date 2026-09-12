package process

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/childenv"
)

func TestManager_Start_ValidatesCommand(t *testing.T) {
	m := newTestManager(t, 4)

	for _, command := range []string{"", "   ", "\t\n"} {
		if _, err := m.Start(StartRequest{Command: command}); !errors.Is(err, ErrValidation) {
			t.Fatalf("Start(command=%q) error = %v, want ErrValidation", command, err)
		}
	}

	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after invalid starts, want 0", got)
	}
	if got := m.List(); len(got) != 0 {
		t.Fatalf("List() = %v after invalid starts, want empty", got)
	}
}

func TestManager_Start_DefensivelyCopiesBaseEnv(t *testing.T) {
	base := []string{"COPY=original"}
	m := newTestManagerWithBase(t, base, t.TempDir(), 4)

	base[0] = "COPY=mutated"
	if got := m.baseEnv[0]; got != "COPY=original" {
		t.Fatalf("manager base env = %q, want defensively copied COPY=original", got)
	}
}

func TestManager_Start_MergesEnvAndCwd(t *testing.T) {
	requireLinuxProcess(t)

	reportPath := filepath.Join(t.TempDir(), "report.json")
	overrideCwd := t.TempDir()
	base := childenv.Sanitized([]string{
		"KEEP=base",
		"CRED=secret",
		"AGENT_BRIDGE_TOKEN=leak",
		"AGENT_BRIDGE_PID_FILE=/tmp/bridge.pid",
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1",
	})
	defaultCwd := t.TempDir()
	m := newTestManagerWithBase(t, base, defaultCwd, 4)

	env := helperEnv(helperEnvCwd)
	env[processHelperReportEnv] = reportPath
	env["KEEP"] = "override"
	env["EXTRA"] = "added"

	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env, Cwd: overrideCwd})
	waitProcessDone(t, m, snap.ID)

	report := readHelperReport(t, reportPath)
	if report.Cwd != overrideCwd {
		t.Fatalf("helper cwd = %q, want override %q", report.Cwd, overrideCwd)
	}
	got := envMap(report.Env)
	if got["KEEP"] != "override" {
		t.Fatalf("KEEP = %q, want request overlay override", got["KEEP"])
	}
	if got["EXTRA"] != "added" {
		t.Fatalf("EXTRA = %q, want added", got["EXTRA"])
	}
	if got["CRED"] != "secret" {
		t.Fatalf("CRED = %q, want retained credential secret", got["CRED"])
	}
	for _, key := range []string{
		"AGENT_BRIDGE_TOKEN",
		"AGENT_BRIDGE_PID_FILE",
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
	} {
		if _, present := got[key]; present {
			t.Fatalf("sanitized bridge control %s leaked into child environment", key)
		}
	}

	// An omitted cwd resolves to the manager's startup cwd.
	env = helperEnv(helperEnvCwd)
	env[processHelperReportEnv] = reportPath
	snap = startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	waitProcessDone(t, m, snap.ID)
	if report = readHelperReport(t, reportPath); report.Cwd != defaultCwd {
		t.Fatalf("default helper cwd = %q, want manager cwd %q", report.Cwd, defaultCwd)
	}
}

func TestManager_Start_SpawnFailureKeepsCapacity(t *testing.T) {
	m := newTestManager(t, 4)

	_, err := m.Start(StartRequest{Command: filepath.Join(t.TempDir(), "does-not-exist")})
	if err == nil {
		t.Fatal("Start(nonexistent) = nil error, want spawn failure")
	}
	if errors.Is(err, ErrValidation) || errors.Is(err, ErrCapacity) {
		t.Fatalf("Start(nonexistent) error = %v, want spawn failure", err)
	}
	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after spawn failure, want 0", got)
	}
	if got := m.List(); len(got) != 0 {
		t.Fatalf("List() = %v after spawn failure, want empty", got)
	}
}

func TestManager_Snapshot_SortedUniqueAndImmutable(t *testing.T) {
	requireLinuxProcess(t)

	m := newTestManager(t, 8)
	args := []string{"original"}
	first := startHelper(t, m, StartRequest{Command: os.Args[0], Args: args, Env: helperEnv(helperBlocking)})
	second := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	third := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})

	seen := map[string]bool{first.ID: true, second.ID: true, third.ID: true}
	if len(seen) != 3 {
		t.Fatalf("IDs are not unique: %q, %q, %q", first.ID, second.ID, third.ID)
	}

	list := m.List()
	if !sort.SliceIsSorted(list, func(i, j int) bool { return list[i].ID < list[j].ID }) {
		t.Fatalf("List() not sorted by ID: %v", snapshotIDs(list))
	}
	if len(list) != 3 {
		t.Fatalf("List() length = %d, want 3", len(list))
	}

	// Mutating the returned snapshot or the request slice must not affect
	// stored state.
	first.Args[0] = "mutated-snapshot"
	args[0] = "mutated-request"
	got, err := m.Get(first.ID)
	if err != nil {
		t.Fatalf("Get(%s): %v", first.ID, err)
	}
	if len(got.Args) != 1 || got.Args[0] != "original" {
		t.Fatalf("stored args = %v, want [original]", got.Args)
	}
}

// TestManager_Snapshot_OmittedArgsSerializeAsEmptyArray keeps the public
// snapshot contract JSON-stable: omitted request args must render as [], never
// null, while explicit args pass through unchanged.
func TestManager_Snapshot_OmittedArgsSerializeAsEmptyArray(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)

	omitted := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	if omitted.Args == nil || len(omitted.Args) != 0 {
		t.Fatalf("omitted args snapshot = %#v, want a non-nil empty slice", omitted.Args)
	}
	encoded, err := json.Marshal(omitted)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if !strings.Contains(string(encoded), `"args":[]`) {
		t.Fatalf("snapshot JSON = %s, want args serialized as []", encoded)
	}

	explicit := startHelper(t, m, StartRequest{Command: os.Args[0], Args: []string{"a", "b"}, Env: helperEnv(helperBlocking)})
	if len(explicit.Args) != 2 || explicit.Args[0] != "a" || explicit.Args[1] != "b" {
		t.Fatalf("explicit args snapshot = %v, want [a b]", explicit.Args)
	}
}

func TestManager_Snapshot_RunningIncludesPID(t *testing.T) {
	requireLinuxProcess(t)

	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})

	if snap.Status != StatusRunning {
		t.Fatalf("status = %q, want running", snap.Status)
	}
	if snap.PID == nil || *snap.PID <= 1 {
		t.Fatalf("running snapshot PID = %v, want a valid group leader PID", snap.PID)
	}
	m.mu.Lock()
	recordPID := m.processes[snap.ID].pid
	m.mu.Unlock()
	if *snap.PID != recordPID {
		t.Fatalf("snapshot PID = %d, want record PID %d", *snap.PID, recordPID)
	}
}

func TestManager_Snapshot_ExitRecordsCodeAndTime(t *testing.T) {
	requireLinuxProcess(t)

	m := newTestManager(t, 4)
	env := helperEnv(helperExitCode)
	env[processHelperExitEnv] = "7"
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	waitProcessDone(t, m, snap.ID)

	got, err := m.Get(snap.ID)
	if err != nil {
		t.Fatalf("Get(%s): %v", snap.ID, err)
	}
	if got.Status != StatusExited {
		t.Fatalf("status = %q, want exited", got.Status)
	}
	if got.PID != nil {
		t.Fatalf("exited snapshot PID = %d, want omitted", *got.PID)
	}
	if got.ExitCode == nil || *got.ExitCode != 7 {
		t.Fatalf("exitCode = %v, want 7", got.ExitCode)
	}
	if got.ExitedAtMs == nil || *got.ExitedAtMs <= 0 {
		t.Fatalf("exitedAtMs = %v, want a positive timestamp", got.ExitedAtMs)
	}
}

func TestManager_Start_KillsDescendantGroupOnDirectExit(t *testing.T) {
	requireLinuxProcess(t)

	pidPath := filepath.Join(t.TempDir(), "pids.json")
	t.Cleanup(func() {
		if pids, ok := tryReadHelperPIDs(pidPath); ok {
			killHelperPIDs(pids)
		}
	})
	env := helperEnv(helperChildExits)
	env[processHelperPIDEnv] = pidPath

	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	pids := readHelperPIDs(t, pidPath)

	// The direct child exits immediately; the waiter must SIGKILL the group so
	// the descendant cannot hold the inherited pipes open, then reap and
	// publish the real exit status.
	waitProcessDone(t, m, snap.ID)
	waitGoneOrZombie(t, pids.Grandchild)

	got, err := m.Get(snap.ID)
	if err != nil {
		t.Fatalf("Get(%s): %v", snap.ID, err)
	}
	if got.ExitCode == nil || *got.ExitCode != 0 {
		t.Fatalf("exitCode = %v, want 0 after the direct child's own exit", got.ExitCode)
	}
	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after descendant termination, want 0", got)
	}
}

func TestManager_Delete_UnknownIsNotFound(t *testing.T) {
	m := newTestManager(t, 4)

	if _, err := m.Get("proc_missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(unknown) error = %v, want ErrNotFound", err)
	}
	if err := m.Delete("proc_missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestManager_Delete_RunningIsConflict(t *testing.T) {
	requireLinuxProcess(t)

	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})

	if err := m.Delete(snap.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("Delete(running) error = %v, want ErrConflict", err)
	}
	if _, err := m.Get(snap.ID); err != nil {
		t.Fatalf("running record should survive a rejected delete: %v", err)
	}
}

func TestManager_Delete_ExitedRemovesAndIDNeverReused(t *testing.T) {
	requireLinuxProcess(t)

	m := newTestManager(t, 4)
	env := helperEnv(helperExitCode)
	env[processHelperExitEnv] = "0"
	first := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	waitProcessDone(t, m, first.ID)

	if err := m.Delete(first.ID); err != nil {
		t.Fatalf("Delete(exited) error = %v, want nil", err)
	}
	if _, err := m.Get(first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(deleted) error = %v, want ErrNotFound", err)
	}

	second := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	if second.ID == first.ID {
		t.Fatalf("ID %q was reused after delete", first.ID)
	}
}

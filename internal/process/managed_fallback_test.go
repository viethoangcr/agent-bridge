package process

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

// forceFallback makes the manager's sole waiter take the reap-first fallback
// path so the direct-only gate contract can be exercised deterministically on
// Linux, where the unreaped observation would otherwise be taken.
func forceFallback(m *Manager) {
	m.observeExit = func(int) bool { return false }
}

func awaitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestManager_Fallback_ReapInterleavedSignalsLeaveUnrelatedGroupAlive forces the
// fallback path and pauses the sole waiter after it has reaped the direct child.
// While paused, the captured PGID is repointed at a live unrelated group to
// simulate recycling, then the external stop and kill paths are exercised. The
// gate must already be in direct-only mode so no negative-PGID signal can ever
// reach the unrelated group.
func TestManager_Fallback_ReapInterleavedSignalsLeaveUnrelatedGroupAlive(t *testing.T) {
	requireLinuxProcess(t)
	unrelated := startSentinelGroup(t)

	m := newTestManager(t, 4)
	forceFallback(m)
	entered := make(chan struct{})
	release := make(chan struct{})
	m.afterReap = func(p *managedProcess) {
		// The direct child has been reaped, so its captured PGID may already be
		// recycled. Repoint it at the unrelated live group to model that.
		p.signals.mu.Lock()
		p.signals.pid = unrelated.Pid
		p.signals.mu.Unlock()
		close(entered)
		<-release
	}

	env := helperEnv(helperExitCode)
	env[processHelperExitEnv] = "0"
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	awaitClosed(t, entered, "fallback waiter to reap and pause")

	p := managerProcess(t, m, snap.ID)
	if err := p.signalGroup(syscall.SIGTERM); err != nil {
		t.Fatalf("fallback external SIGTERM error = %v, want nil", err)
	}
	if err := p.signalGroup(syscall.SIGKILL); err != nil {
		t.Fatalf("fallback external SIGKILL error = %v, want nil", err)
	}

	close(release)
	waitProcessDone(t, m, snap.ID)

	if state, ok := processState(unrelated.Pid); !ok || state == 'Z' {
		t.Fatalf("unrelated group process %d state = %q (present %v), want alive after fallback stop/kill", unrelated.Pid, state, ok)
	}
}

func managerProcess(t *testing.T, m *Manager, id string) *managedProcess {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.processes[id]
	if p == nil {
		t.Fatalf("process %s not found", id)
	}
	return p
}

// TestSignalGate_DirectOnlySignalsDirectChildNotGroup proves the fallback gate
// contract: in direct-only mode a signal reaches the direct child's process
// handle and never the captured negative PGID, even when the pid field points
// at a live unrelated group.
func TestSignalGate_DirectOnlySignalsDirectChildNotGroup(t *testing.T) {
	requireLinuxProcess(t)
	unrelated := startSentinelGroup(t)
	child := startSentinelGroup(t)

	gate := &signalGate{pid: unrelated.Pid, proc: child}
	gate.fallback()
	if err := gate.signal(syscall.SIGKILL); err != nil {
		t.Fatalf("direct-only signal error = %v, want nil", err)
	}

	waitGoneOrZombie(t, child.Pid)
	if state, ok := processState(unrelated.Pid); !ok || state == 'Z' {
		t.Fatalf("unrelated group %d state = %q (present %v), want alive after a direct-only signal", unrelated.Pid, state, ok)
	}
}

// TestManager_Fallback_ExternalSignalsTerminateDirectChild proves an external
// stop or kill still terminates the direct child when the manager is forced
// onto the fallback path.
func TestManager_Fallback_ExternalSignalsTerminateDirectChild(t *testing.T) {
	requireLinuxProcess(t)
	for name, signal := range map[string]func(*Manager, string) (Snapshot, error){
		"stop": (*Manager).Stop,
		"kill": (*Manager).Kill,
	} {
		t.Run(name, func(t *testing.T) {
			m := newTestManager(t, 4)
			forceFallback(m)
			snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
			if snap.PID == nil {
				t.Fatal("running snapshot has no PID")
			}
			pid := *snap.PID

			got, err := signal(m, snap.ID)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got.Status != StatusExited {
				t.Fatalf("%s status = %q, want exited", name, got.Status)
			}
			waitProcessDone(t, m, snap.ID)
			waitGoneOrZombie(t, pid)
		})
	}
}

// TestManager_Run_FallbackUsesSameOrdering proves the one-shot path takes the
// same direct-only ordering: a forced fallback records the real exit status
// without a post-reap group signal.
func TestManager_Run_FallbackUsesSameOrdering(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)
	forceFallback(m)

	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperOutput)})
	if err != nil {
		t.Fatalf("Run(fallback): %v", err)
	}
	if res.ExitCode == nil || *res.ExitCode != 0 {
		t.Fatalf("exitCode = %v, want 0", res.ExitCode)
	}
	if res.Stdout != "stdout-payload" {
		t.Fatalf("stdout = %q, want captured payload", res.Stdout)
	}
	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after fallback run, want 0", got)
	}
}

// TestManager_Run_FallbackTimeoutSignalsDirectChild proves a timeout on the
// one-shot fallback path kills the direct child through its handle and still
// reaps it.
func TestManager_Run_FallbackTimeoutSignalsDirectChild(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 4)
	forceFallback(m)

	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperBlocking), TimeoutMs: ptrI64(150)})
	if err != nil {
		t.Fatalf("Run(fallback timeout): %v", err)
	}
	if !res.TimedOut {
		t.Fatal("TimedOut = false, want true")
	}
	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after fallback timeout, want 0", got)
	}
}

package process

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

func TestManager_Stop_ReturnsSnapshotAndIdempotent(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})

	stopped, err := m.Stop(snap.ID)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if stopped.Status != StatusExited || stopped.PID != nil {
		t.Fatalf("Stop snapshot = %+v, want exited without PID", stopped)
	}
	again, err := m.Stop(snap.ID)
	if err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if again.Status != StatusExited {
		t.Fatalf("idempotent Stop status = %q, want exited", again.Status)
	}
	if _, err := m.Stop("proc_missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Stop(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestManager_Kill_ReturnsSnapshotAndIdempotent(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})

	killed, err := m.Kill(snap.ID)
	if err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if killed.Status != StatusExited || killed.PID != nil {
		t.Fatalf("Kill snapshot = %+v, want exited without PID", killed)
	}
	again, err := m.Kill(snap.ID)
	if err != nil {
		t.Fatalf("second Kill: %v", err)
	}
	if again.Status != StatusExited {
		t.Fatalf("idempotent Kill status = %q, want exited", again.Status)
	}
	if _, err := m.Kill("proc_missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Kill(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestManager_ProcessGroup_StopTerminatesDescendants(t *testing.T) {
	requireLinuxProcess(t)
	pidPath := filepath.Join(t.TempDir(), "pids.json")
	t.Cleanup(func() {
		if pids, ok := tryReadHelperPIDs(pidPath); ok {
			killHelperPIDs(pids)
		}
	})
	env := helperEnv(helperChild)
	env[processHelperPIDEnv] = pidPath

	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	pids := readHelperPIDs(t, pidPath)

	if _, err := m.Stop(snap.ID); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	waitGoneOrZombie(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)
}

func TestManager_ProcessGroup_KillTerminatesDescendants(t *testing.T) {
	requireLinuxProcess(t)
	pidPath := filepath.Join(t.TempDir(), "pids.json")
	t.Cleanup(func() {
		if pids, ok := tryReadHelperPIDs(pidPath); ok {
			killHelperPIDs(pids)
		}
	})
	env := helperEnv(helperChild)
	env[processHelperPIDEnv] = pidPath

	m := newTestManager(t, 4)
	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: env})
	pids := readHelperPIDs(t, pidPath)

	if _, err := m.Kill(snap.ID); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitGoneOrZombie(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)
}

// startSentinelGroup starts an independent process group that outlives the
// manager and is cleaned up at test end.
func startSentinelGroup(t *testing.T) *os.Process {
	t.Helper()
	cmd := exec.Command("/bin/sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sentinel group: %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})
	return cmd.Process
}

// TestSignalGate_ExternalSignalsStopAfterExit proves the gate contract every
// external signal path relies on: signals reach a live captured group, and
// once the waiter's exit transition has run, no external path can signal that
// PID again even if the number is recycled by an unrelated group.
func TestSignalGate_ExternalSignalsStopAfterExit(t *testing.T) {
	requireLinuxProcess(t)

	t.Run("signal reaches a live group", func(t *testing.T) {
		proc := startSentinelGroup(t)
		gate := &signalGate{pid: proc.Pid}
		if err := gate.signal(syscall.SIGKILL); err != nil {
			t.Fatalf("signal(live) error = %v, want nil", err)
		}
		waitGoneOrZombie(t, proc.Pid)
	})

	t.Run("signal after exit is a no-op", func(t *testing.T) {
		// The waiter's exit transition kills and then marks the group exited.
		exited := startSentinelGroup(t)
		gate := &signalGate{pid: exited.Pid}
		gate.exit()
		waitGoneOrZombie(t, exited.Pid)
		gate.mu.Lock()
		marked := gate.exited
		gate.mu.Unlock()
		if !marked {
			t.Fatal("exit did not mark the gate exited")
		}

		// Simulate PID reuse: repointing the gate at an unrelated live group
		// must not let an external signal reach it.
		recycled := startSentinelGroup(t)
		gate.pid = recycled.Pid
		if err := gate.signal(syscall.SIGKILL); err != nil {
			t.Fatalf("signal after exit error = %v, want nil", err)
		}
		if err := syscall.Kill(recycled.Pid, 0); err != nil {
			t.Fatalf("gate signaled a recycled PGID after exit: %v", err)
		}
	})
}

// TestSignalGate_ConcurrentSignalAndExit exercises the gate under -race: many
// external signals may race the sole waiter's exit transition, which must be
// the final signal and leave the group marked exited.
func TestSignalGate_ConcurrentSignalAndExit(t *testing.T) {
	requireLinuxProcess(t)
	proc := startSentinelGroup(t)
	gate := &signalGate{pid: proc.Pid}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = gate.signal(syscall.Signal(0))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		gate.exit()
	}()
	wg.Wait()

	gate.mu.Lock()
	exited := gate.exited
	gate.mu.Unlock()
	if !exited {
		t.Fatal("gate was not marked exited by the waiter transition")
	}
	waitGoneOrZombie(t, proc.Pid)
}

func TestManager_ProcessGroup_SignalGuards(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)

	// Signal zero performs only an existence check, so a regression in the
	// PID guard cannot harm the test process or the host.
	for _, pid := range []int{0, 1, -5} {
		p := &managedProcess{id: "guard", pid: pid, signals: signalGate{pid: pid}, status: StatusRunning}
		if err := p.signalGroup(syscall.Signal(0)); err != nil {
			t.Fatalf("signalGroup(pid=%d, 0) error = %v, want nil", pid, err)
		}
	}

	// Stop and Kill on an exited record are idempotent and never signal.
	exited := testManagedProcess("exited", 1<<20)
	exited.pid = 999999
	registerTestProcess(m, exited)
	got, err := m.Stop("exited")
	if err != nil || got.Status != StatusExited {
		t.Fatalf("Stop(exited) = %+v, %v; want exited, nil", got, err)
	}
	got, err = m.Kill("exited")
	if err != nil || got.Status != StatusExited {
		t.Fatalf("Kill(exited) = %+v, %v; want exited, nil", got, err)
	}
}

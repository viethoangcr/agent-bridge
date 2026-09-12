package process

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// TestObserveExit_LeavesZombieBeforeGroupKill proves the no-reap contract both
// waiters depend on: observeExit reports a terminated direct child while it is
// still waitable, so its PID is still owned by its zombie and cannot be reused
// before the signal gate kills the captured group. cmd.Wait afterwards reaps
// the child and returns its real exit status.
func TestObserveExit_LeavesZombieBeforeGroupKill(t *testing.T) {
	requireLinuxProcess(t)

	cmd := exec.Command("/bin/sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	pid := cmd.Process.Pid
	reaped := false
	t.Cleanup(func() {
		if reaped {
			return
		}
		// The child is still unreaped here, so this group kill cannot target
		// a recycled PID; then reap to avoid leaving a zombie.
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = cmd.Wait()
	})

	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill child group: %v", err)
	}
	if !observeExit(pid) {
		t.Fatal("observeExit = false, want the terminated child observed")
	}

	state, ok := processState(pid)
	if !ok || state != 'Z' {
		t.Fatalf("child state after observeExit = %q (present %v), want zombie 'Z'", state, ok)
	}
	// The zombie still owns its PGID, so the group kill cannot hit an
	// unrelated recycled group, and cmd.Wait can still reap.
	if err := syscall.Kill(-pid, 0); err != nil {
		t.Fatalf("group check after observeExit: %v", err)
	}

	waitErr := cmd.Wait()
	reaped = true
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("cmd.Wait after observeExit = %v, want the real SIGKILL exit status", waitErr)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("reaped status = %v, want signaled SIGKILL", exitErr.Sys())
	}
	if _, ok := processState(pid); ok {
		t.Fatal("child still present after cmd.Wait reaped it")
	}
}

// TestObserveExit_NonChildReportsFalse covers the fallback trigger: a PID that
// is not an unreaped child (ECHILD) cannot be observed, so the waiters must
// keep the reap-then-kill order.
func TestObserveExit_NonChildReportsFalse(t *testing.T) {
	requireLinuxProcess(t)

	if observeExit(os.Getpid()) {
		t.Fatal("observeExit(self) = true, want false")
	}
	if observeExit(1) {
		t.Fatal("observeExit(1) = true, want false")
	}
}

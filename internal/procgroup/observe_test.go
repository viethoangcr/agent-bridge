package procgroup

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
)

// TestObserveExit_LeavesZombieBeforeGroupKill proves the no-reap contract both
// waiters depend on: ObserveExit reports a terminated direct child while it is
// still waitable, so its PID is still owned by its zombie and cannot be reused
// before the signal gate kills the captured group. cmd.Wait afterwards reaps
// the child and returns its real exit status.
func TestObserveExit_LeavesZombieBeforeGroupKill(t *testing.T) {
	requireLinux(t)

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
	if !ObserveExit(pid) {
		t.Fatal("ObserveExit = false, want the terminated child observed")
	}

	state, ok := processState(pid)
	if !ok || state != 'Z' {
		t.Fatalf("child state after ObserveExit = %q (present %v), want zombie 'Z'", state, ok)
	}
	// The zombie still owns its PGID, so the group kill cannot hit an
	// unrelated recycled group, and cmd.Wait can still reap.
	if err := syscall.Kill(-pid, 0); err != nil {
		t.Fatalf("group check after ObserveExit: %v", err)
	}

	waitErr := cmd.Wait()
	reaped = true
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("cmd.Wait after ObserveExit = %v, want the real SIGKILL exit status", waitErr)
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
	requireLinux(t)

	if ObserveExit(os.Getpid()) {
		t.Fatal("ObserveExit(self) = true, want false")
	}
	if ObserveExit(1) {
		t.Fatal("ObserveExit(1) = true, want false")
	}
}

func requireLinux(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("process-group observation tests require Linux")
	}
}

// processState reads the state character from /proc/<pid>/stat after the
// parenthesized command, reporting false when the process is gone.
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

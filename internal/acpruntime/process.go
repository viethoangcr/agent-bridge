package acpruntime

import (
	"fmt"
	"io"
	"os/exec"
	"syscall"

	"github.com/viethoangcr/agent-bridge/internal/procgroup"
)

// openProcessPipes captures the child's stdin/stdout/stderr before spawn. On a
// later pipe failure it closes the pipes already captured so no descriptor is
// leaked.
func openProcessPipes(cmd *exec.Cmd, program string) (io.WriteCloser, io.ReadCloser, io.ReadCloser, error) {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("acpruntime: stdin pipe for %q: %w", program, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		closePipe(stdin)
		return nil, nil, nil, fmt.Errorf("acpruntime: stdout pipe for %q: %w", program, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		closePipe(stdin)
		closePipe(stdout)
		return nil, nil, nil, fmt.Errorf("acpruntime: stderr pipe for %q: %w", program, err)
	}
	return stdin, stdout, stderr, nil
}

// observedExit reports whether pid's exit can be peeked without reaping. It
// uses the test seam when set and procgroup.ObserveExit otherwise.
func (r *Runtime) observedExit(pid int) bool {
	if r.observeExit != nil {
		return r.observeExit(r, pid)
	}
	return procgroup.ObserveExit(pid)
}

// signalUnreapedGroup SIGKILLs the process group led by pid. The caller must
// hold an unreaped child (procgroup.ObserveExit has seen it exit but not reaped
// it), so the kernel still owns the PID and cannot have recycled the PGID for
// an unrelated group. It is the only place that signals a negative PGID.
func signalUnreapedGroup(pid int) {
	if pid <= 1 {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

// waitProcess is the sole cmd.Wait owner and the sole group-kill owner. On
// Linux it first blocks in procgroup.ObserveExit until the direct child exits
// without reaping it, SIGKILLs the captured negative PGID while the zombie
// still owns the PID so a descendant holding the inherited pipes cannot delay
// group teardown, and only then reaps with cmd.Wait so the real exit status is
// available. Platforms without an unreaped observation reap first and send no
// post-reap group signal: once the PID may be recycled the group cleanup is
// best-effort. Either way it stops and joins the writer, then joins the pumps
// and publishes exit.
func (r *Runtime) waitProcess() {
	var pid int
	if r.cmd.Process != nil {
		pid = r.cmd.Process.Pid
	}
	var err error
	if r.observedExit(pid) {
		r.closeTerminal()
		signalUnreapedGroup(pid)
		err = r.cmd.Wait()
	} else {
		err = r.cmd.Wait()
		r.closeTerminal()
	}
	if hook := r.afterReap; hook != nil {
		hook()
	}
	r.stopWriter()
	r.closeStdin()
	r.awaitWriterStopped()
	r.pumps.Wait()
	r.finish()
	r.waitErr = err
	close(r.done)
}

// writeRaw writes complete record bytes to the child's stdin and reports how
// many bytes the pipe accepted. The writer goroutine is its only production
// caller; writeLine remains for direct framing tests.
func (r *Runtime) writeRaw(record []byte) (int, error) {
	r.writeMu.Lock()
	stdin := r.stdin
	r.writeMu.Unlock()
	if stdin == nil {
		return 0, errNotRunning
	}
	return stdin.Write(record)
}

// writeLine writes one newline-delimited payload to the child's stdin.
func (r *Runtime) writeLine(line []byte) error {
	buf := make([]byte, 0, len(line)+1)
	buf = append(buf, line...)
	buf = append(buf, '\n')
	_, err := r.writeRaw(buf)
	return err
}

// abortAfterSpawn terminates a just-spawned child and its group and records the
// server exited for any failure between spawn and live publication. It kills
// the direct child first through the same handle external Kill uses, so the
// later unreaped observation cannot block on a live child; on Linux it then
// SIGKILLs the group while the child is still unreaped, otherwise it reaps
// without a post-reap group signal (best-effort cleanup).
func (r *Runtime) abortAfterSpawn(reason error, pipes ...io.Closer) error {
	var pid int
	if r.cmd.Process != nil {
		pid = r.cmd.Process.Pid
	}
	r.cancel()
	_ = r.killChild()
	if r.observedExit(pid) {
		signalUnreapedGroup(pid)
		_ = r.cmd.Wait()
	} else {
		_ = r.cmd.Wait()
	}
	r.closeStdin()
	for _, pipe := range pipes {
		closePipe(pipe)
	}
	r.markExited()
	return reason
}

// finish publishes exit after the pumps have joined: it persists the synthetic
// agent-exited notification, marks the row exited (failing every pending
// correlation with ErrExited), and closes the wakeup channel last so consumers
// can perform a final replay query.
func (r *Runtime) finish() {
	r.cancel()
	r.closeStdin()
	r.persistSynthetic(agentExitedMethod, nil)
	r.markExited()
	r.closeWake()
}

func (r *Runtime) closeStdin() {
	r.writeMu.Lock()
	stdin := r.stdin
	r.stdin = nil
	r.writeMu.Unlock()
	// Close outside the lock so a writer blocked in Write is unblocked instead
	// of deadlocking on the mutex.
	closePipe(stdin)
}

func closePipe(c io.Closer) {
	if c != nil {
		_ = c.Close()
	}
}

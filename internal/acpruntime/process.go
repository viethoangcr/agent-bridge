package acpruntime

import (
	"errors"
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

// captureProcessGroup records the direct child's PID and its owned process
// group. Setpgid guarantees the direct child leads its own group, so the PGID
// equals the child PID even when the lookup itself fails.
func (r *Runtime) captureProcessGroup() {
	pgid := r.cmd.Process.Pid
	if captured, err := syscall.Getpgid(r.cmd.Process.Pid); err == nil && captured > 1 {
		pgid = captured
	}
	r.pgid.Store(int64(pgid))
	r.pid.Store(int64(r.cmd.Process.Pid))
}

// waitProcess is the sole cmd.Wait owner. On Linux it first observes the direct
// child's exit without reaping it, SIGKILLs the captured negative PGID while
// the zombie still owns the PID so a descendant holding the inherited pipes
// cannot delay group teardown, and only then reaps with cmd.Wait so the real
// exit status is available. Platforms without an unreaped observation keep the
// reap-then-kill order. Either way it closes the terminal gate before killing
// the group, stops and joins the writer, then joins the pumps and publishes
// exit.
func (r *Runtime) waitProcess() {
	var pid int
	if r.cmd.Process != nil {
		pid = r.cmd.Process.Pid
	}
	var err error
	if procgroup.ObserveExit(pid) {
		r.closeTerminal()
		_ = r.killProcessGroup()
		err = r.cmd.Wait()
	} else {
		err = r.cmd.Wait()
		r.closeTerminal()
		_ = r.killProcessGroup()
	}
	// The direct child is reaped: its PID/PGID may be recycled, so clear the
	// captured group id. No later Kill can then signal a reused group.
	r.pgid.Store(0)
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

// abortAfterSpawn terminates a just-spawned group and records the server
// exited for any failure between spawn and live publication.
func (r *Runtime) abortAfterSpawn(reason error, pipes ...io.Closer) error {
	_ = r.killProcessGroup()
	_ = r.cmd.Wait()
	r.cancel()
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

// killProcessGroup sends SIGKILL to the captured negative PGID. It is a no-op
// before the PGID is captured and tolerant of an already-dead group.
func (r *Runtime) killProcessGroup() error {
	pgid := r.pgid.Load()
	if pgid <= 1 {
		return nil
	}
	err := syscall.Kill(int(-pgid), syscall.SIGKILL)
	if err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
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

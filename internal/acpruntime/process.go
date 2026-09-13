package acpruntime

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"

	"github.com/viethoangcr/agent-bridge/internal/procgroup"
)

// signalGate is the per-runtime synchronized signal gate. Every external signal
// path holds it while validating the captured negative PGID. The sole waiter
// SIGKILLs the group and marks it exited under the same gate before reaping the
// direct child, so once exit state is published no external path can signal a
// recycled PGID. On Linux the waiter marks the group exited after
// procgroup.ObserveExit peeked the direct child's exit unreaped, while the
// zombie still owns the PID.
type signalGate struct {
	mu     sync.Mutex
	pgid   int
	exited bool
}

// signal is the external signaling path. It is a no-op for an invalid PGID or a
// group the waiter already marked exited, and treats ESRCH as exited.
func (g *signalGate) signal(sig syscall.Signal) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.exited || g.pgid <= 1 {
		return nil
	}
	if err := syscall.Kill(-g.pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// exit is the waiter's transition: under the gate it SIGKILLs the captured
// negative PGID and then marks the group exited and clears the id, so no later
// external signal can target the recycled PGID. It is idempotent.
func (g *signalGate) exit() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.exited {
		return
	}
	if g.pgid > 1 {
		_ = syscall.Kill(-g.pgid, syscall.SIGKILL)
	}
	g.pgid = 0
	g.exited = true
}

// Load reports the captured PGID, or 0 once the waiter has marked the group
// exited.
func (g *signalGate) Load() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return int64(g.pgid)
}

// Store records the captured PGID. It leaves the exited mark untouched so a
// test can simulate PGID reuse after exit without reopening the gate.
func (g *signalGate) Store(pgid int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pgid = int(pgid)
}

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

// killProcessGroup sends SIGKILL through the signal gate. It is a no-op before
// the PGID is captured, after the waiter marked the group exited, and for an
// already-dead group.
func (r *Runtime) killProcessGroup() error {
	return r.pgid.signal(syscall.SIGKILL)
}

// waitProcess is the sole cmd.Wait owner. On Linux it first observes the direct
// child's exit without reaping it, closes the signal gate (SIGKILLing the
// captured negative PGID) while the zombie still owns the PID so a descendant
// holding the inherited pipes cannot delay group teardown, and only then reaps
// with cmd.Wait so the real exit status is available. Platforms without an
// unreaped observation keep the reap-then-exit order. Either way it closes the
// terminal gate before killing the group, stops and joins the writer, then
// joins the pumps and publishes exit.
func (r *Runtime) waitProcess() {
	var pid int
	if r.cmd.Process != nil {
		pid = r.cmd.Process.Pid
	}
	var err error
	if procgroup.ObserveExit(pid) {
		r.closeTerminal()
		r.pgid.exit()
		err = r.cmd.Wait()
	} else {
		err = r.cmd.Wait()
		r.closeTerminal()
		r.pgid.exit()
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

// abortAfterSpawn terminates a just-spawned group and records the server
// exited for any failure between spawn and live publication. It closes the
// signal gate before reaping on Linux so the group kill cannot race PID reuse.
func (r *Runtime) abortAfterSpawn(reason error, pipes ...io.Closer) error {
	var pid int
	if r.cmd.Process != nil {
		pid = r.cmd.Process.Pid
	}
	if procgroup.ObserveExit(pid) {
		r.pgid.exit()
		_ = r.cmd.Wait()
	} else {
		_ = r.cmd.Wait()
		r.pgid.exit()
	}
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

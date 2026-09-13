package process

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/procgroup"
)

// pumpBufferSize is the bridge-observed chunk size for managed output. pump
// forwards each chunk to the process's log ring, and capture uses it to drain
// one-shot output.
const pumpBufferSize = 8 << 10

// signalGate is the per-group synchronized signal gate. Every external signal
// path holds it while validating the captured leader PID. The group's sole
// waiter uses exit to SIGKILL the captured PGID and mark the group exited under
// the same gate, so once exit state is published no external path can signal a
// recycled PGID. On Linux the waiter calls exit after observing the direct
// child's exit unreaped, so the kill itself cannot race PID recycling either.
//
// Platforms without an unreaped observation use fallback instead: before
// reaping, the gate becomes direct-only and every external signal targets only
// the stdlib process handle, which is race-safe with cmd.Wait. The waiter then
// marks the group exited with no post-reap group signal, since the captured
// PGID may already be recycled.
type signalGate struct {
	mu         sync.Mutex
	pid        int
	proc       *os.Process
	directOnly bool
	exited     bool
}

// signal is the external signaling path. It is a no-op for an invalid leader
// PID or a group the waiter has already marked exited, and treats ESRCH as
// already exited. In direct-only mode it signals the direct child through the
// stdlib process handle and never a negative PGID.
func (g *signalGate) signal(sig syscall.Signal) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.exited {
		return nil
	}
	if g.directOnly {
		return g.signalDirect(sig)
	}
	if g.pid <= 1 {
		return nil
	}
	if err := syscall.Kill(-g.pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// signalDirect signals only the direct child through its os.Process handle,
// which the standard library makes race-safe with cmd.Wait. os.ErrProcessDone
// means the child was already reaped, the documented exited behavior. It never
// signals a negative PGID.
func (g *signalGate) signalDirect(sig syscall.Signal) error {
	if g.proc == nil {
		return nil
	}
	if err := g.proc.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

// exit is the Linux unreaped waiter's transition. Under the gate it SIGKILLs
// the captured negative PGID (the waiter is the group's sole owner) and then
// marks the group exited, so no later external signal path can ever target that
// PID. The waiter calls it after procgroup.ObserveExit peeked the direct child's
// exit without reaping it, while the zombie still owns the PID. It must never
// be called after cmd.Wait reaps.
func (g *signalGate) exit() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.exited {
		return
	}
	if g.pid > 1 {
		_ = syscall.Kill(-g.pid, syscall.SIGKILL)
	}
	g.exited = true
}

// fallback transitions the gate to direct-only before the fallback waiter reaps
// the direct child. From this point external signals target only the stdlib
// process handle, which is race-safe with cmd.Wait, and never a negative PGID.
func (g *signalGate) fallback() {
	g.mu.Lock()
	g.directOnly = true
	g.mu.Unlock()
}

// markExited is the fallback waiter's transition after cmd.Wait. It marks the
// group exited without signaling: the captured PGID may already be recycled, so
// descendant cleanup is best-effort and no negative PGID is ever signaled.
func (g *signalGate) markExited() {
	g.mu.Lock()
	g.exited = true
	g.mu.Unlock()
}

// managedProcess is one live or exited managed process record. Immutable
// identity fields are set before the record is inserted into the manager map;
// status and exit fields are guarded by Manager.mu.
type managedProcess struct {
	id          string
	command     string
	args        []string
	cwd         string
	createdAtMs int64

	manager        *Manager
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	inputAdmission chan struct{}
	ring           *logRing

	status  Status
	pid     int
	signals signalGate

	exitCode   *int
	exitedAtMs *int64

	pumps sync.WaitGroup
	done  chan struct{}
}

// snapshot returns a copied public view. The caller must hold Manager.mu while
// status or exit fields may be changing.
func (p *managedProcess) snapshot() Snapshot {
	out := Snapshot{
		ID:      p.id,
		Command: p.command,
		// A non-nil empty slice keeps omitted args serialized as [] rather
		// than null; explicit args are copied unchanged.
		Args:        append([]string{}, p.args...),
		Cwd:         p.cwd,
		Status:      p.status,
		CreatedAtMs: p.createdAtMs,
	}
	if p.status == StatusRunning && p.pid > 1 {
		pid := p.pid
		out.PID = &pid
	}
	if p.status == StatusExited {
		if p.exitCode != nil {
			code := *p.exitCode
			out.ExitCode = &code
		}
		if p.exitedAtMs != nil {
			at := *p.exitedAtMs
			out.ExitedAtMs = &at
		}
	}
	return out
}

// pump reads r in 8KiB chunks until EOF or a read error and forwards every
// non-empty chunk to the manager log lock with the stream name. It never holds
// the manager lock across a read, so a blocked child pipe cannot stall appends.
func (p *managedProcess) pump(stream string, r io.Reader) {
	defer p.pumps.Done()
	// The pump owns its read end; closing it here (and only here) guarantees
	// cmd.Wait can never close it out from under the pump.
	if c, ok := r.(io.Closer); ok {
		defer closePipe(c)
	}
	buf := make([]byte, pumpBufferSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			p.manager.appendLog(p, stream, time.Now().UnixMilli(), buf[:n])
		}
		if err != nil {
			return
		}
	}
}

func (m *Manager) observedExit(pid int) bool {
	if m.observeExit != nil {
		return m.observeExit(pid)
	}
	return procgroup.ObserveExit(pid)
}

// wait is the sole owner of cmd.Wait for the record. On Linux it first observes
// the direct child's exit without reaping it, SIGKILLs the captured negative
// PGID through the signal gate while the zombie still owns the PID so a
// descendant cannot hold the inherited pipes open, and only then reaps the
// child with cmd.Wait so the real exit status is available. Platforms without
// an unreaped observation put the gate in direct-only mode before reaping and
// then mark the group exited with no post-reap group signal, because the
// captured PGID may already be recycled; descendant cleanup is best-effort.
// Finally it joins both pumps; the caller publishes exit state and releases
// capacity only after wait returns. No gate lock is held across cmd.Wait.
func (p *managedProcess) wait() {
	if p.manager.observedExit(p.pid) {
		p.signals.exit()
		_ = p.cmd.Wait()
	} else {
		p.signals.fallback()
		_ = p.cmd.Wait()
		if p.manager.afterReap != nil {
			p.manager.afterReap(p)
		}
		p.signals.markExited()
	}
	p.pumps.Wait()
}

// signalGroup sends sig through the group's signal gate. It is the external
// signaling path: once the waiter has marked the group exited, it never sends.
func (p *managedProcess) signalGroup(sig syscall.Signal) error {
	return p.signals.signal(sig)
}

// waitUntil blocks until the process's publisher has marked it exited or d
// elapses, whichever comes first, and reports whether it exited. The duration
// is a fixed server-owned bound, never a request-controlled one.
func (p *managedProcess) waitUntil(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-p.done:
		return true
	case <-timer.C:
		return false
	}
}

// DecodeInput decodes a process input payload according to encoding. base64
// accepts standard padded base64; utf8 passes the bytes through unchanged. Any
// other encoding, or malformed base64, is ErrValidation.
func DecodeInput(encoding string, data []byte) ([]byte, error) {
	switch encoding {
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(string(data))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid base64 input", ErrValidation)
		}
		return decoded, nil
	case "utf8":
		return append([]byte(nil), data...), nil
	default:
		return nil, fmt.Errorf("%w: encoding must be base64 or utf8", ErrValidation)
	}
}

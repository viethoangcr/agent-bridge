package process

import (
	"context"
	"fmt"
	"io"
	"syscall"
	"time"
)

// Fixed server-owned signal waits and input deadline. These bound every stop,
// kill, and stdin write regardless of caller-supplied durations.
const (
	stopWait          = 2 * time.Second
	killWait          = 1 * time.Second
	inputWriteTimeout = 5 * time.Second
)

// writeDeadliner is implemented by *os.File write pipes, which support
// poller-backed write deadlines.
type writeDeadliner interface {
	SetWriteDeadline(time.Time) error
}

// WriteInput writes decoded bytes to a running process's stdin. An exited
// record is ErrConflict, an unknown ID is ErrNotFound, and decoded data above
// the active maxInputBytesPerRequest is ErrPayloadTooLarge. At most one writer
// is admitted per process through a capacity-one channel; a writer waiting for
// the slot aborts on context cancellation. Each admitted write gets a fixed
// server-owned five-second deadline, or the request deadline when earlier.
//
// Cancellation or timeout before any byte is written releases admission and
// leaves the process usable. A write failure or timeout after a partial write
// immediately SIGKILLs the captured negative PGID so a half-written stream is
// never reused, and maps to ErrGateway.
func (m *Manager) WriteInput(ctx context.Context, id string, data []byte) (int, error) {
	m.mu.Lock()
	p, ok := m.processes[id]
	if !ok {
		m.mu.Unlock()
		return 0, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if p.status != StatusRunning {
		m.mu.Unlock()
		return 0, fmt.Errorf("%w: process %s has exited", ErrConflict, id)
	}
	admission := p.inputAdmission
	stdin := p.stdin
	m.mu.Unlock()

	if limit := m.config.load().MaxInputBytesPerRequest; len(data) > limit {
		return 0, fmt.Errorf("%w: %d decoded bytes exceed limit %d", ErrPayloadTooLarge, len(data), limit)
	}

	select {
	case admission <- struct{}{}:
	case <-ctx.Done():
		return 0, fmt.Errorf("%w: input admission: %v", ErrGateway, ctx.Err())
	}
	defer func() { <-admission }()

	m.mu.Lock()
	running := p.status == StatusRunning
	m.mu.Unlock()
	if !running {
		return 0, fmt.Errorf("%w: process %s has exited", ErrConflict, id)
	}
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("%w: input write: %v", ErrGateway, err)
	}

	deadline := time.Now().Add(inputWriteTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if wd, ok := stdin.(writeDeadliner); ok {
		if err := wd.SetWriteDeadline(deadline); err != nil {
			return 0, fmt.Errorf("%w: set input deadline: %v", ErrGateway, err)
		}
		defer func() { _ = wd.SetWriteDeadline(time.Time{}) }()
	}

	n, err := stdin.Write(data)
	if n == len(data) {
		return n, nil
	}
	if n > 0 {
		_ = p.signalGroup(syscall.SIGKILL)
		return n, fmt.Errorf("%w: input write failed after %d of %d bytes: %v", ErrGateway, n, len(data), err)
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	return n, fmt.Errorf("%w: input write: %v", ErrGateway, err)
}

// Stop sends SIGTERM to the process group, waits at most two seconds for the
// direct child to exit, and returns the current snapshot. An exited process is
// idempotent and is never signaled; an unknown ID is ErrNotFound.
func (m *Manager) Stop(id string) (Snapshot, error) {
	return m.signalAndWait(id, syscall.SIGTERM, stopWait)
}

// Kill sends SIGKILL to the process group, waits at most one second for the
// direct child to exit, and returns the current snapshot. An exited process is
// idempotent and is never signaled; an unknown ID is ErrNotFound.
func (m *Manager) Kill(id string) (Snapshot, error) {
	return m.signalAndWait(id, syscall.SIGKILL, killWait)
}

// signalAndWait signals a running record's captured negative PGID and waits a
// fixed server-owned bound for exit. It captures the live leader under the
// registry lock and never signals a record that has already exited.
func (m *Manager) signalAndWait(id string, sig syscall.Signal, wait time.Duration) (Snapshot, error) {
	m.mu.Lock()
	p, ok := m.processes[id]
	if !ok {
		m.mu.Unlock()
		return Snapshot{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if p.status != StatusRunning {
		snap := p.snapshot()
		m.mu.Unlock()
		return snap, nil
	}
	m.mu.Unlock()

	if err := p.signalGroup(sig); err != nil {
		return Snapshot{}, fmt.Errorf("%w: signal process %s: %v", ErrGateway, id, err)
	}
	p.waitUntil(wait)

	m.mu.Lock()
	snap := p.snapshot()
	m.mu.Unlock()
	return snap, nil
}

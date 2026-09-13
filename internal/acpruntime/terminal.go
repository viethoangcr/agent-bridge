package acpruntime

import (
	"context"
	"sync"
)

// markExited records terminal exit, clears the live PID, fails every pending
// correlation with ErrExited, and marks the server row exited. The first call
// wins; later calls are idempotent confirmations.
func (r *Runtime) markExited() {
	r.pid.Store(0)
	r.markTerminal(ErrExited)
}

// markTerminal records terminal exit while serialized by the status mutex so a
// queued reconciliation observes terminal state and performs no later busy/idle
// write. It closes the terminal gate first, then fails every pending waiter
// with the supplied error on the first call, then marks the row exited where
// storage permits.
func (r *Runtime) markTerminal(err error) {
	r.statusMu.Lock()
	first := !r.statusExited
	r.statusExited = true
	r.statusMu.Unlock()

	// Gate before clear: reserve holds corrMu while checking the gate and
	// inserting, so an insert either precedes failPending's snapshot and is
	// failed, or observes the closed gate and is rejected.
	r.closeTerminal()
	if hook := r.beforeTerminalClear; hook != nil {
		hook()
	}
	r.exited.Store(true)

	// Gate -> stop/drain writer -> clear correlations -> kill: once the gate
	// and signal are closed and the serializer has drained, no queued record
	// can be left uncompleted before waiters are failed.
	r.stopWriter()
	r.awaitWriterStopped()

	if first {
		r.failPending(err)
	}
	if r.store == nil {
		return
	}
	if markErr := r.store.MarkExited(context.Background(), r.serverID); markErr != nil {
		r.log.Error("mark server exited", "server_id", r.serverID, "error", markErr)
	}
}

// closeTerminal closes the single terminal signal exactly once. The admission
// gate is marked closed and drained before this returns, so every writer
// admission either completed its enqueue before closure or observed the closed
// gate/signal and rejected; nothing can enqueue afterwards.
func (r *Runtime) closeTerminal() {
	r.terminalOnce.Do(func() {
		r.gate.close()
		close(r.terminal)
		r.gate.wait()
	})
}

// admissionGate serializes writer admission against terminal closure. close
// marks the gate closed under the mutex; wait blocks until every admit that
// entered before closure has left.
type admissionGate struct {
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// enter reports whether a writer admission may proceed. It returns false once
// the gate is closed.
func (g *admissionGate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.wg.Add(1)
	return true
}

// leave releases one admitted writer admission.
func (g *admissionGate) leave() { g.wg.Done() }

// close marks the gate closed; no later enter succeeds.
func (g *admissionGate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

// wait blocks until every in-flight admission has left.
func (g *admissionGate) wait() { g.wg.Wait() }

// terminalClosed reports whether the terminal gate has closed. A runtime built
// without a terminal channel never reports closed.
func (r *Runtime) terminalClosed() bool {
	select {
	case <-r.terminal:
		return true
	default:
		return false
	}
}

// failPersistence closes the admission gate with ErrPersistence before it
// kills the process group. Closing the gate first means no post can reserve or
// write while the failing path tears down, and failPending (the single terminal
// path) delivers ErrPersistence to every retained waiter. It never holds the
// correlation mutex during SQL or signaling.
func (r *Runtime) failPersistence(cause error) {
	r.log.Error("persist runtime state", "server_id", r.serverID, "error", cause)
	r.markTerminal(ErrPersistence)
	_ = r.killProcessGroup()
}

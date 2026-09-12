package acpruntime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// maxCorrelations is the fixed total of waiting, grace-retained, and committing
// correlations one runtime permits.
const maxCorrelations = 256

// defaultGraceDuration is the fixed late-response grace retained for a
// lifecycle request after its configured request timeout.
const defaultGraceDuration = 30 * time.Second

// graceKillTimeout bounds how long a grace-expiry kill waits for the runtime to
// join its process-group work before giving up.
const graceKillTimeout = 10 * time.Second

// correlationState is the lifecycle of one reserved request ID.
type correlationState uint8

const (
	corrWaiting correlationState = iota
	corrGrace
	corrCommitting
)

// runtimeTimer is the injectable one-shot timer used for the request deadline
// and the lifecycle grace. Stop is safe to call repeatedly.
type runtimeTimer struct {
	ch   <-chan time.Time
	stop func()
}

// C returns the timer's expiry channel.
func (t *runtimeTimer) C() <-chan time.Time { return t.ch }

// Stop cancels the timer.
func (t *runtimeTimer) Stop() {
	if t.stop != nil {
		t.stop()
	}
}

// realRuntimeTimer is the production timer factory.
func realRuntimeTimer(d time.Duration) *runtimeTimer {
	t := time.NewTimer(d)
	return &runtimeTimer{ch: t.C, stop: func() { t.Stop() }}
}

// responseOutcome is the terminal result delivered to a request waiter.
type responseOutcome struct {
	response json.RawMessage
	err      error
}

func (o responseOutcome) result() (PostResult, error) {
	if o.err != nil {
		return PostResult{}, o.err
	}
	return PostResult{Response: o.response}, nil
}

// pendingRequest is one reserved correlation. It holds only the bounded
// canonical ID, lifecycle enum, optional bounded session ID/cwd, its state, and
// the single waiter. The waiter delivery is once-only so a timeout, a commit,
// and a runtime failure cannot both answer the same request.
type pendingRequest struct {
	key       string
	id        json.RawMessage
	lifecycle Lifecycle
	sessionID *string
	cwd       *string

	state   correlationState
	written bool

	// waiter is created once in reserve and never replaced. deliver sends at
	// most one buffered outcome, so a cancelled caller leaving it unread cannot
	// block the manager and a second outcome can never be delivered.
	waiter      chan responseOutcome
	deliverOnce sync.Once

	timeout *runtimeTimer
	grace   *runtimeTimer

	// expired broadcasts the request deadline to the admission and write
	// waiters. The manager closes it when the deadline timer fires; a closed
	// channel is always ready, so the manager winning the one-shot timer race
	// cannot strand admit/awaitWrite past the deadline.
	expired    chan time.Time
	expireOnce sync.Once

	doneOnce sync.Once
	done     chan struct{}
}

// deliver sends the terminal outcome to the waiter exactly once.
func (e *pendingRequest) deliver(o responseOutcome) {
	e.deliverOnce.Do(func() {
		e.waiter <- o
	})
}

// closeDone signals the manager goroutine that the entry is no longer live.
func (e *pendingRequest) closeDone() {
	e.doneOnce.Do(func() { close(e.done) })
}

// broadcastExpired closes the deadline broadcast channel exactly once. A
// closed channel is always ready, so every waiter observing it wakes together.
func (e *pendingRequest) broadcastExpired() {
	e.expireOnce.Do(func() { close(e.expired) })
}

// stopTimers cancels the request deadline and any lifecycle grace timer.
func (e *pendingRequest) stopTimers() {
	if e.timeout != nil {
		e.timeout.Stop()
	}
	if e.grace != nil {
		e.grace.Stop()
	}
}

// reserve inserts one waiting correlation before writer admission. It rejects a
// duplicate canonical ID, an over-capacity map, and a terminal runtime before
// returning. Crossing from zero to nonzero reconciles durable status to busy.
func (r *Runtime) reserve(pending Pending) (*pendingRequest, error) {
	key, err := idKey(pending.ID)
	if err != nil {
		return nil, err
	}

	r.corrMu.Lock()
	if r.terminalClosed() || r.poisoned.Load() {
		r.corrMu.Unlock()
		return nil, ErrExited
	}
	if _, duplicate := r.corr[key]; duplicate {
		r.corrMu.Unlock()
		return nil, ErrDuplicateID
	}
	if len(r.corr) >= maxCorrelations {
		r.corrMu.Unlock()
		return nil, ErrCapacity
	}
	entry := &pendingRequest{
		key:       key,
		id:        cloneRaw(pending.ID),
		lifecycle: pending.Lifecycle,
		sessionID: cloneStringPtr(pending.SessionID),
		cwd:       cloneStringPtr(pending.CWD),
		state:     corrWaiting,
		waiter:    make(chan responseOutcome, 1),
		done:      make(chan struct{}),
		timeout:   r.deadlineTimer(),
		expired:   make(chan time.Time),
	}
	r.corr[key] = entry
	empty := len(r.corr) == 1
	r.corrMu.Unlock()

	if empty {
		if err := r.reconcileStatus(); err != nil {
			r.failPersistence(err)
			return nil, ErrPersistence
		}
	}
	go r.manageRequest(entry)
	return entry, nil
}

// releaseEntry removes a correlation that failed writer admission or a pre-write
// timeout. It leaves a grace/committing entry to its manager.
func (r *Runtime) releaseEntry(entry *pendingRequest) {
	r.corrMu.Lock()
	if r.corr[entry.key] != entry || entry.state != corrWaiting {
		r.corrMu.Unlock()
		return
	}
	delete(r.corr, entry.key)
	empty := len(r.corr) == 0
	entry.stopTimers()
	entry.closeDone()
	r.corrMu.Unlock()
	if empty {
		r.reconcileStatusAndFail()
	}
}

// markWritten records that the complete request record reached the agent, so a
// later timeout retains lifecycle grace instead of releasing immediately.
func (r *Runtime) markWritten(entry *pendingRequest) {
	r.corrMu.Lock()
	if r.corr[entry.key] == entry {
		entry.written = true
	}
	r.corrMu.Unlock()
}

// manageRequest owns one request's deadline. It exits when the entry's done
// channel closes (committed, released, or failed) or the deadline expires.
func (r *Runtime) manageRequest(entry *pendingRequest) {
	select {
	case <-entry.timeout.C():
		entry.broadcastExpired()
		r.onRequestTimeout(entry)
	case <-entry.done:
	}
}

// onRequestTimeout applies the timeout rules under the correlation mutex: a
// pre-write request releases its slot immediately; a written non-lifecycle
// request releases its slot; a written lifecycle request moves to the fixed
// grace while retaining its ID, metadata, slot, duplicate reservation,
// capacity and busy accounting.
func (r *Runtime) onRequestTimeout(entry *pendingRequest) {
	r.corrMu.Lock()
	if r.corr[entry.key] != entry || entry.state != corrWaiting {
		r.corrMu.Unlock()
		return
	}

	if entry.written && entry.lifecycle != LifecycleNone {
		entry.state = corrGrace
		entry.grace = r.graceTimer()
		grace := entry.grace
		r.corrMu.Unlock()
		entry.deliver(responseOutcome{err: ErrRequestTimeout})
		go r.manageGrace(entry, grace)
		return
	}

	delete(r.corr, entry.key)
	empty := len(r.corr) == 0
	entry.stopTimers()
	entry.closeDone()
	r.corrMu.Unlock()
	entry.deliver(responseOutcome{err: ErrRequestTimeout})
	if empty {
		r.reconcileStatusAndFail()
	}
}

// manageGrace waits for the lifecycle grace to expire or the entry to commit.
// It exits on either signal.
func (r *Runtime) manageGrace(entry *pendingRequest, grace *runtimeTimer) {
	select {
	case <-grace.C():
		r.expireLifecycle(entry)
	case <-entry.done:
	}
}

// expireLifecycle kills and exits the runtime for an unclaimed grace entry,
// releasing every slot, timer, waiter, and runtime-owned completion channel.
// It is a no-op once the entry committed or was removed.
func (r *Runtime) expireLifecycle(entry *pendingRequest) {
	r.corrMu.Lock()
	if r.corr[entry.key] != entry || entry.state != corrGrace {
		r.corrMu.Unlock()
		return
	}
	r.corrMu.Unlock()

	// markTerminal closes the terminal gate before clearing the retained
	// grace/committing entries and before Kill below, so no post can reserve or
	// write through the teardown window.
	r.markTerminal(ErrExited)

	ctx, cancel := context.WithTimeout(context.Background(), graceKillTimeout)
	defer cancel()
	_ = r.Kill(ctx)
}

// awaitResponse blocks until the manager delivers a committed response, a
// timeout, or a terminal error. Caller cancellation returns immediately; the
// entry's timeout, grace, and output persistence continue under the manager.
func (r *Runtime) awaitResponse(ctx context.Context, entry *pendingRequest) (PostResult, error) {
	select {
	case out := <-entry.waiter:
		return out.result()
	case <-ctx.Done():
		return PostResult{}, ctx.Err()
	}
}

// completeResponse releases one committing entry after its output committed.
// Strict order: remove the exact committing generation, run serialized status
// reconciliation (which re-reads the current count), then complete the attached
// waiter and signal the capacity-one wakeup.
func (r *Runtime) completeResponse(entry *pendingRequest, response json.RawMessage) {
	r.corrMu.Lock()
	present := r.corr[entry.key] == entry
	if present {
		delete(r.corr, entry.key)
		entry.stopTimers()
		entry.closeDone()
	}
	r.corrMu.Unlock()
	if !present {
		return
	}

	if hook := r.beforeReconcileStatus; hook != nil {
		hook()
	}

	if err := r.reconcileStatus(); err != nil {
		entry.deliver(responseOutcome{err: ErrPersistence})
		r.failPersistence(err)
		return
	}
	entry.deliver(responseOutcome{response: response})
	r.signalCommitted()
}

// publishLive performs the initial creating-to-idle publication under the same
// status mutex as every later transition, so a terminal mark can never be
// overwritten by the initial row write. It also records the child PID.
func (r *Runtime) publishLive(ctx context.Context, pid int) error {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	if r.statusExited {
		return ErrExited
	}
	if r.store == nil {
		return nil
	}
	return r.store.SetLive(ctx, r.serverID, pid)
}

// reconcileStatus serializes one runtime status write. It holds statusMu,
// briefly reads the current correlation total under corrMu, releases corrMu
// before any SQL, writes busy when the total is positive or idle when it is
// zero, then releases statusMu. A terminal runtime performs no write, so a
// stale completion can never overwrite exited.
func (r *Runtime) reconcileStatus() error {
	r.statusMu.Lock()
	if r.statusExited {
		r.statusMu.Unlock()
		return nil
	}
	r.corrMu.Lock()
	count := len(r.corr)
	r.corrMu.Unlock()

	status := acpstore.StatusIdle
	if count > 0 {
		status = acpstore.StatusBusy
	}
	var err error
	if r.store != nil {
		err = r.store.SetStatus(context.Background(), r.serverID, status)
	}
	r.statusMu.Unlock()
	if err != nil {
		return errors.Join(ErrPersistence, err)
	}
	return nil
}

// reconcileStatusAndFail reconciles and, on failure, fails the runtime.
func (r *Runtime) reconcileStatusAndFail() {
	if err := r.reconcileStatus(); err != nil {
		r.failPersistence(err)
	}
}

// failPending clears every correlation, stops its timers, closes its manager,
// and delivers err to any attached waiter.
func (r *Runtime) failPending(err error) {
	r.corrMu.Lock()
	entries := make([]*pendingRequest, 0, len(r.corr))
	for _, entry := range r.corr {
		entries = append(entries, entry)
	}
	r.corr = make(map[string]*pendingRequest)
	r.corrMu.Unlock()

	for _, entry := range entries {
		entry.stopTimers()
		entry.closeDone()
		entry.deliver(responseOutcome{err: err})
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

// deadlineTimer returns the entry deadline timer from the injected seam or the
// production real-time timer.
func (r *Runtime) deadlineTimer() *runtimeTimer {
	if r.newDeadlineTimer != nil {
		return r.newDeadlineTimer(r.requestTimeout)
	}
	return realRuntimeTimer(r.requestTimeout)
}

// graceTimer returns the lifecycle grace timer from the injected seam or the
// production real-time timer.
func (r *Runtime) graceTimer() *runtimeTimer {
	if r.newGraceTimer != nil {
		return r.newGraceTimer(r.graceDuration)
	}
	return realRuntimeTimer(r.graceDuration)
}

// cloneStringPtr copies an optional string so retained metadata does not alias
// the request's decoded envelope.
func cloneStringPtr(p *string) *string {
	if p == nil {
		return nil
	}
	value := *p
	return &value
}

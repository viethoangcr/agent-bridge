// Package acpproxy owns the live lifecycle of ACP server runtimes: per-server
// creation and recreation, activity leasing, capacity admission, and (in later
// tasks) subscriptions, reaping, deletion, and shutdown. It delegates all
// protocol, persistence, and subprocess mechanics to Phase 02.
package acpproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// Sentinel errors the HTTP layer maps to problem responses.
var (
	// ErrMissingAgent reports a first POST that did not name an agent.
	ErrMissingAgent = errors.New("agent is required for a new server")
	// ErrAgentConflict reports a POST against a server owned by another agent.
	ErrAgentConflict = errors.New("server uses a different agent")
	// ErrReinitialize reports a request to an exited server that is not
	// initialize.
	ErrReinitialize = errors.New("exited server requires initialize")
	// ErrDeleting reports a server whose lifecycle has been gated for deletion
	// or shutdown.
	ErrDeleting = errors.New("server is deleting")
	// ErrClosed reports a POST during proxy shutdown.
	ErrClosed = errors.New("proxy is shutting down")
	// ErrRuntimeCapacity reports the fixed 64-runtime live limit.
	ErrRuntimeCapacity = errors.New("ACP runtime capacity reached")
)

// maxLiveRuntimes is the fixed production cap on concurrent live ACP runtimes.
const maxLiveRuntimes = 64

// initializeMethod is the only method that may recreate an exited server.
const initializeMethod = "initialize"

// runtime is the narrow Phase 02 subprocess surface the proxy consumes. Kill
// signals the captured process group and waits for the process and its pumps;
// Wait is the idempotent completion confirmation.
type runtime interface {
	Post(ctx context.Context, payload json.RawMessage) (acpruntime.PostResult, error)
	Events() <-chan struct{}
	PID() int
	Wait() error
	Kill(ctx context.Context) error
}

// runtimeFactory creates one live runtime for a resolved launch spec. It is
// injected so tests can supply fake runtimes without spawning processes.
type runtimeFactory func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (runtime, error)

// Proxy is the per-server lifecycle owner. It keeps at most maxLiveRuntimes
// live instances and serializes creation, recreation, and termination per
// server ID with a keyed lifecycle lock.
type Proxy struct {
	store          *acpstore.Store
	resolver       acpruntime.Resolver
	requestTimeout time.Duration
	idleTTL        time.Duration
	log            *slog.Logger
	factory        runtimeFactory

	// newSubscriptionTicker is the injected fallback-poll seam. Production
	// uses subscriptionFallbackInterval; tests advance a fake ticker.
	newSubscriptionTicker func(time.Duration) subscriptionTicker

	// beforeCreateWait, when non-nil, runs immediately before a caller blocks
	// on a placeholder's ready channel. It is nil in production and exists only
	// so tests can synchronize on waiter arrival without polling.
	beforeCreateWait func()

	// Test-only ordering seams: beforeSpawnRegister runs after the closed gate
	// check and before the atomic spawn registration; beforeAllowServerSubs
	// runs after create's finalize gate and before the atomic subscription
	// publication; afterDeleteMark runs after DELETE marks the server. They are
	// nil in production.
	beforeSpawnRegister   func()
	beforeAllowServerSubs func()
	afterDeleteMark       func()

	// now is the injected clock used for idle-deadline decisions; the reaper's
	// cadence comes from newReaperTicker.
	now             func() time.Time
	newReaperTicker func(time.Duration) reaperTicker

	// reaperMu guards the single reaper goroutine's lifecycle so Shutdown can
	// stop and join it exactly once.
	reaperMu      sync.Mutex
	reaperStarted bool
	reaperCancel  context.CancelFunc
	reaperDone    chan struct{}

	// shutdownMu guards the idempotent Shutdown result and the retired runtimes
	// Confirm re-waits on.
	shutdownMu      sync.Mutex
	shutdownStarted bool
	shutdownDone    chan struct{}
	shutdownErr     error

	// retired records every runtime detached by Shutdown so Confirm can perform
	// an idempotent Wait confirmation after HTTP drain. Guarded by mu.
	retired []runtime

	// deleting marks server IDs whose durable rows are mid-prune; a concurrent
	// Post must not recreate them. Guarded by mu.
	deleting map[string]struct{}

	// retainedStderr holds one exited generation's redacted stderr tail per
	// server until the failing HTTP request consumes it or a replacement
	// generation starts. Guarded by mu.
	retainedStderr map[string]string

	// subReg tracks every registered subscription per server, including
	// subscriptions for servers whose runtime already exited, so DELETE and
	// shutdown can always hard-close SSE. It shares the proxy mutex with the
	// deleting gate and the SSE-closed markers so registration, marking, and
	// publication are atomic with respect to each other.
	subReg map[string]map[*subscription]struct{}

	// subRegClosed marks server IDs whose SSE was hard-closed by DELETE, and
	// subRegShutdown marks proxy shutdown; a subscription racing either is
	// refused so no late registration can escape the close.
	subRegClosed   map[string]struct{}
	subRegShutdown bool

	// spawnCancels holds the cancel function of every in-flight creation's
	// spawn context. Shutdown cancels them so an unpublished spawn is aborted
	// (and its child killed by CommandContext) instead of outliving the bridge.
	// Guarded by mu.
	spawnCancels map[*instance]context.CancelFunc

	// retireLeaseTimeout bounds how long a dead gated generation waits for its
	// activity leases to drain before its slot is released anyway. It exists so
	// a failed DELETE can never permanently consume runtime capacity, and it is
	// a field so tests can shrink it.
	retireLeaseTimeout time.Duration

	// nextGen assigns each live instance a monotonic generation so a stale
	// reaper/delete decision can be rejected without touching a replacement.
	nextGen atomic.Uint64

	// storeServer is the durable server-read seam, defaulting to store.Server.
	// It exists so tests can pause store I/O and prove no lifecycle/global lock
	// is held during SQL.
	storeServer func(context.Context, string) (acpstore.Server, error)

	// deleteServer is the injected prune seam, defaulting to store.DeleteServer.
	deleteServer func(context.Context, string) error

	// createServer and setStatus are the durable creation seams, defaulting to
	// the store methods, so tests can inject persistence failures.
	createServer func(context.Context, string, string) (acpstore.Server, error)
	setStatus    func(context.Context, string, acpstore.Status) error

	// mu guards the live map, the keyed-lock table, lock reference counts, the
	// retired list, and the deleting gate. It is never held during resolution,
	// spawn, store I/O, Runtime.Post, waits, or kills.
	mu     sync.Mutex
	live   map[string]*instance
	locks  map[string]*lifecycleLock
	closed atomic.Bool
}

// lifecycleLock serializes creation, recreation, and termination for one
// server ID. refs counts holders and waiters and is guarded by Proxy.mu so the
// table can be pruned when a server has no live instance and no waiters.
type lifecycleLock struct {
	mu   sync.Mutex
	refs int
}

// instance is one live runtime and its activity/gate state. activity,
// generation, terminating, deleting, detached, closed, and zero are guarded by
// the server's lifecycle lock. terminating/deleting/detached/closed plus
// waitActivityZero are the gate/lease primitives the reaper, DELETE, and
// shutdown drive.
type instance struct {
	serverID    string
	agent       string
	runtime     runtime
	lock        *lifecycleLock
	generation  uint64
	activity    int
	terminating bool
	deleting    bool
	detached    bool
	closed      bool

	// creating marks a placeholder reserved while its durable-row read,
	// resolution, and spawn I/O are still in flight. ready closes when the
	// creation publishes its runtime or is abandoned, and createErr records an
	// abandonment cause so waiters inherit it. All three are guarded by the
	// server's lifecycle lock.
	creating  bool
	ready     chan struct{}
	createErr error

	// zero is closed whenever activity is zero and replaced by acquireActivity
	// on the next lease, giving termination waits a cancellation-aware signal.
	zero chan struct{}

	// watched is closed by dropLive once the live slot has been released,
	// giving tests an explicit cleanup signal instead of polling.
	watched     chan struct{}
	watchedOnce sync.Once

	// subMu guards the attached subscribers and the two terminal flags.
	// runtimeDown is set when the runtime closes its wakeup channel; subsClosed
	// is set by DELETE/shutdown. Both stop new registrations.
	subMu       sync.Mutex
	subs        map[*subscription]struct{}
	runtimeDown bool
	subsClosed  bool
}

// New constructs a production Proxy bound to acpruntime.Start.
func New(store *acpstore.Store, resolver acpruntime.Resolver, requestTimeout, idleTTL time.Duration, log *slog.Logger) *Proxy {
	return newWithFactory(store, resolver, requestTimeout, idleTTL, log,
		func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (runtime, error) {
			return acpruntime.Start(ctx, store, serverID, spec, requestTimeout, log)
		})
}

// newWithFactory constructs a Proxy with an injected runtime factory.
func newWithFactory(store *acpstore.Store, resolver acpruntime.Resolver, requestTimeout, idleTTL time.Duration, log *slog.Logger, factory runtimeFactory) *Proxy {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	p := &Proxy{
		store:              store,
		resolver:           resolver,
		requestTimeout:     requestTimeout,
		idleTTL:            idleTTL,
		log:                log,
		factory:            factory,
		live:               make(map[string]*instance),
		locks:              make(map[string]*lifecycleLock),
		deleting:           make(map[string]struct{}),
		retainedStderr:     make(map[string]string),
		subReg:             make(map[string]map[*subscription]struct{}),
		subRegClosed:       make(map[string]struct{}),
		spawnCancels:       make(map[*instance]context.CancelFunc),
		retireLeaseTimeout: 30 * time.Second,
		now:                time.Now,
		newSubscriptionTicker: func(d time.Duration) subscriptionTicker {
			return realSubscriptionTicker{ticker: time.NewTicker(d)}
		},
		newReaperTicker: func(d time.Duration) reaperTicker {
			return realReaperTicker{ticker: time.NewTicker(d)}
		},
	}
	if store != nil {
		p.storeServer = store.Server
		p.deleteServer = store.DeleteServer
		p.createServer = store.CreateServer
		p.setStatus = store.SetStatus
	}
	return p
}

// Post routes one validated client envelope to the current runtime, creating
// or recreating it when necessary. Creation reserves a per-ID placeholder
// under the lifecycle lock, then performs the durable read, resolution, and
// spawn I/O without any lifecycle or global lock before publishing the
// runtime. Concurrent callers finding a placeholder wait on its ready channel
// without any lock and recheck the current generation and gates afterwards.
// The activity lease is taken under the lifecycle lock after those rechecks,
// and Runtime.Post runs without any lock. The original payload is passed
// through unchanged; only Runtime.Post compacts it.
func (p *Proxy) Post(ctx context.Context, serverID string, agent *string, method string, payload json.RawMessage) (acpruntime.PostResult, error) {
	if p.closed.Load() {
		return acpruntime.PostResult{}, ErrClosed
	}

	lk := p.keyedLock(serverID)
	defer p.releaseKeyedLock(serverID, lk)

	inst, err := p.acquireForPost(ctx, lk, serverID, agent, method)
	if err != nil {
		return acpruntime.PostResult{}, err
	}
	defer inst.releaseActivity()
	return inst.runtime.Post(ctx, payload)
}

// acquireForPost returns a live instance with one activity lease held. When no
// instance is live it claims a creating placeholder under the lifecycle lock,
// runs the creation I/O unlocked, and rechecks. A caller that finds another
// goroutine's placeholder waits on ready without any lock; it uses the live
// replacement when one was published and inherits the placeholder's error when
// the creation was abandoned.
func (p *Proxy) acquireForPost(ctx context.Context, lk *lifecycleLock, serverID string, agent *string, method string) (*instance, error) {
	for {
		lk.mu.Lock()
		if p.closed.Load() {
			lk.mu.Unlock()
			return nil, ErrClosed
		}
		if p.isDeleting(serverID) {
			lk.mu.Unlock()
			return nil, ErrDeleting
		}
		live := p.lookupLive(serverID)
		if live == nil {
			placeholder := p.newInstance(serverID, "", lk)
			placeholder.creating = true
			placeholder.ready = make(chan struct{})
			if err := p.reserveLive(placeholder); err != nil {
				lk.mu.Unlock()
				return nil, err
			}
			lk.mu.Unlock()

			if err := p.create(ctx, lk, placeholder, serverID, agent, method); err != nil {
				return nil, err
			}
			// create published the placeholder. Re-check and take the lease
			// under the same lock so a concurrent DELETE cannot slip between
			// the health check and the re-acquisition and make this caller
			// recreate the server the DELETE just pruned. A natural exit drops
			// the instance without setting the terminal flags, so the existing
			// retry/reinitialize path is preserved.
			lk.mu.Lock()
			if p.lookupLive(serverID) == placeholder && !placeholder.creating &&
				!placeholder.terminating && !placeholder.deleting && !placeholder.detached && !placeholder.closed {
				if err := placeholder.acquireActivity(); err != nil {
					lk.mu.Unlock()
					return nil, err
				}
				lk.mu.Unlock()
				return placeholder, nil
			}
			gated := placeholder.terminating || placeholder.deleting || placeholder.detached || placeholder.closed
			lk.mu.Unlock()
			if gated {
				return nil, ErrDeleting
			}
			continue
		}
		if live.creating {
			waited, ready := live, live.ready
			lk.mu.Unlock()
			if hook := p.beforeCreateWait; hook != nil {
				hook()
			}
			select {
			case <-ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			lk.mu.Lock()
			current := p.lookupLive(serverID)
			if current != nil && !current.creating &&
				!current.terminating && !current.deleting && !current.detached && !current.closed {
				if err := current.acquireActivity(); err != nil {
					lk.mu.Unlock()
					return nil, err
				}
				lk.mu.Unlock()
				return current, nil
			}
			gated := waited.terminating || waited.deleting || waited.detached || waited.closed
			createErr := waited.createErr
			lk.mu.Unlock()
			// A DELETE that gated the placeholder while this waiter slept wins:
			// never recreate a server the DELETE just pruned, even after the
			// global deleting gate cleared.
			if gated {
				return nil, ErrDeleting
			}
			// A published replacement wins; otherwise the waiter inherits the
			// failed creation's error instead of retrying it.
			if current == nil && createErr != nil {
				return nil, createErr
			}
			continue
		}
		if live.terminating || live.deleting || live.detached || live.closed {
			lk.mu.Unlock()
			return nil, ErrDeleting
		}
		if agent != nil && *agent != "" && *agent != live.agent {
			lk.mu.Unlock()
			return nil, ErrAgentConflict
		}
		if err := live.acquireActivity(); err != nil {
			lk.mu.Unlock()
			return nil, err
		}
		lk.mu.Unlock()
		return live, nil
	}
}

// create performs the durable read, resolution, row transition, and spawn for a
// reserved placeholder without holding any lifecycle or global lock. On
// success it attaches the runtime under the lifecycle lock and wakes waiters;
// on failure it abandons the placeholder and returns the error. A DELETE or
// shutdown that gated the placeholder while the spawn was in flight wins: the
// fresh runtime is killed outside every lock and the placeholder abandoned.
func (p *Proxy) create(ctx context.Context, lk *lifecycleLock, inst *instance, serverID string, agent *string, method string) error {
	// A new generation supersedes any tail retained for an older failure, so a
	// resolution or spawn failure never attaches stale agent stderr.
	p.clearRetainedStderr(serverID)
	if p.closed.Load() {
		return p.abandonCreate(ctx, inst, nil, ErrClosed)
	}
	server, err := p.storeServer(ctx, serverID)
	newServer := false
	switch {
	case errors.Is(err, acpstore.ErrNotFound):
		if agent == nil || *agent == "" {
			return p.abandonCreate(ctx, inst, nil, ErrMissingAgent)
		}
		newServer = true
		server = acpstore.Server{ServerID: serverID, Agent: *agent}
	case err != nil:
		return p.abandonCreate(ctx, inst, nil, persistenceFailure(err))
	default:
		if agent != nil && *agent != "" && *agent != server.Agent {
			return p.abandonCreate(ctx, inst, nil, ErrAgentConflict)
		}
		if method != initializeMethod {
			return p.abandonCreate(ctx, inst, nil, ErrReinitialize)
		}
	}

	spec, err := p.resolver.Resolve(server.Agent)
	if err != nil {
		return p.abandonCreate(ctx, inst, nil, err)
	}

	if newServer {
		if _, err := p.createServer(ctx, serverID, server.Agent); err != nil {
			return p.abandonCreate(ctx, inst, nil, persistenceFailure(err))
		}
	} else if err := p.setStatus(ctx, serverID, acpstore.StatusCreating); err != nil {
		return p.abandonCreate(ctx, inst, nil, persistenceFailure(err))
	}

	// A runtime outlives the HTTP request that created it: detach the spawn
	// context so request completion/cancellation cannot kill the subprocess.
	// The proxy owns termination through Kill, DELETE, the reaper, and shutdown.
	// Shutdown cancels this spawn context, so an in-flight spawn is aborted
	// before publication; the cancel is deregistered before the runtime is
	// published so a live runtime is never killed by shutdown's sweep.
	if p.closed.Load() {
		return p.abandonCreate(ctx, inst, nil, ErrClosed)
	}
	if hook := p.beforeSpawnRegister; hook != nil {
		hook()
	}
	spawnCtx, cancelSpawn := context.WithCancel(context.WithoutCancel(ctx))
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		cancelSpawn()
		return p.abandonCreate(ctx, inst, nil, ErrClosed)
	}
	p.spawnCancels[inst] = cancelSpawn
	p.mu.Unlock()
	rt, err := p.factory(spawnCtx, p.store, serverID, spec, p.requestTimeout, p.log)
	p.mu.Lock()
	delete(p.spawnCancels, inst)
	p.mu.Unlock()
	if err != nil {
		return p.abandonCreate(ctx, inst, nil, startupFailure(err))
	}

	lk.mu.Lock()
	if p.live[serverID] != inst || inst.terminating || inst.deleting || inst.detached || inst.closed || p.closed.Load() {
		closed := p.closed.Load()
		lk.mu.Unlock()
		gateErr := ErrDeleting
		if closed {
			gateErr = ErrClosed
		}
		return p.abandonCreate(ctx, inst, rt, gateErr)
	}
	if hook := p.beforeAllowServerSubs; hook != nil {
		hook()
	}
	if !p.allowServerSubs(serverID) {
		lk.mu.Unlock()
		return p.abandonCreate(ctx, inst, rt, ErrDeleting)
	}
	inst.agent = server.Agent
	inst.runtime = rt
	inst.creating = false
	close(inst.ready)
	lk.mu.Unlock()

	go p.watch(inst)
	go p.watchEvents(inst)
	return nil
}

// abandonCreate removes a placeholder, releases exactly one capacity slot, and
// wakes its waiters, which inherit err. A spawn that could not be published is
// killed first, outside every lock, so no process survives a delete or shutdown
// that gated the placeholder mid-spawn.
func (p *Proxy) abandonCreate(ctx context.Context, inst *instance, rt runtime, err error) error {
	if rt != nil {
		_ = rt.Kill(context.WithoutCancel(ctx))
	}
	lk := inst.lock
	lk.mu.Lock()
	p.dropLive(inst)
	inst.createErr = err
	inst.creating = false
	close(inst.ready)
	lk.mu.Unlock()
	return err
}

// LivePID reports the direct-child PID of serverID's current live generation.
// It returns false for an unknown server or one that is gated for termination,
// so durable PIDs are never exposed as live.
func (p *Proxy) LivePID(serverID string) (int, bool) {
	inst := p.lookupLive(serverID)
	if inst == nil {
		return 0, false
	}
	lk := inst.lock
	lk.mu.Lock()
	defer lk.mu.Unlock()
	if p.lookupLive(serverID) != inst || inst.runtime == nil {
		return 0, false
	}
	if inst.terminating || inst.deleting || inst.detached || inst.closed {
		return 0, false
	}
	return inst.runtime.PID(), true
}

// Stderr returns the current live generation's retained, redacted agent stderr
// tail for the HTTP 502 problem extension. When the watcher already released an
// exited generation, it falls back to that generation's tail so error mapping
// after a failing Post still sees it; the retained tail is consumed by the
// read. It returns "" when no generation exists or none exposed a stderr tail.
func (p *Proxy) Stderr(serverID string) string {
	inst := p.lookupLive(serverID)
	if inst != nil {
		lk := inst.lock
		lk.mu.Lock()
		live := p.lookupLive(serverID) == inst && inst.runtime != nil
		stderr := ""
		if live {
			stderr = runtimeStderr(inst.runtime)
		}
		lk.mu.Unlock()
		if live {
			return stderr
		}
	}
	return p.takeRetainedStderr(serverID)
}

// runtimeStderr returns a runtime's redacted stderr tail when it exposes one.
func runtimeStderr(rt runtime) string {
	if provider, ok := rt.(interface{ Stderr() string }); ok {
		return provider.Stderr()
	}
	return ""
}

// retainStderr stores the exiting generation's redacted tail for the error
// mapping that follows its removal, unless a newer generation already owns the
// server's live slot.
func (p *Proxy) retainStderr(inst *instance) {
	stderr := runtimeStderr(inst.runtime)
	if stderr == "" {
		return
	}
	p.mu.Lock()
	if current, ok := p.live[inst.serverID]; !ok || current == inst {
		p.retainedStderr[inst.serverID] = stderr
	}
	p.mu.Unlock()
}

// takeRetainedStderr releases and returns the retained tail so a mapped
// process failure consumes it exactly once.
func (p *Proxy) takeRetainedStderr(serverID string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	stderr := p.retainedStderr[serverID]
	delete(p.retainedStderr, serverID)
	return stderr
}

// clearRetainedStderr releases a retained tail when a new generation supersedes
// it or its server is deleted.
func (p *Proxy) clearRetainedStderr(serverID string) {
	p.mu.Lock()
	delete(p.retainedStderr, serverID)
	p.mu.Unlock()
}

// acquireActivity registers one active lease. The caller holds the lifecycle
// lock. It refuses leases once the instance is terminating, detached, or still
// a creation placeholder without a runtime.
func (i *instance) acquireActivity() error {
	if i.terminating || i.detached || i.creating {
		return ErrDeleting
	}
	if i.activity == 0 {
		i.zero = make(chan struct{})
	}
	i.activity++
	return nil
}

// releaseActivity drops one active lease, waking termination waiters when the
// count reaches zero. It takes the lifecycle lock itself and is safe against an
// unmatched release.
func (i *instance) releaseActivity() {
	i.lock.mu.Lock()
	defer i.lock.mu.Unlock()
	if i.activity == 0 {
		return
	}
	i.activity--
	if i.activity == 0 {
		close(i.zero)
	}
}

// waitActivityZero blocks until no active leases remain or ctx ends. The caller
// must hold the lifecycle lock; it is released while waiting and reacquired
// before returning. This is the 3.3 reaper/delete drain primitive.
func (i *instance) waitActivityZero(ctx context.Context) error {
	for i.activity > 0 {
		drained := i.zero
		i.lock.mu.Unlock()
		select {
		case <-ctx.Done():
			i.lock.mu.Lock()
			return ctx.Err()
		case <-drained:
			i.lock.mu.Lock()
		}
	}
	return nil
}

// newInstance builds a drained, ungated instance bound to lk with the next
// generation. A newer generation's presence in the live map is what makes an
// in-flight reaper or delete decision for an older instance stale.
func (p *Proxy) newInstance(serverID, agent string, lk *lifecycleLock) *instance {
	inst := &instance{
		serverID:   serverID,
		agent:      agent,
		lock:       lk,
		generation: p.nextGen.Add(1),
		zero:       make(chan struct{}),
		watched:    make(chan struct{}),
	}
	close(inst.zero)
	return inst
}

// isDeleting reports whether serverID's durable row is mid-prune.
func (p *Proxy) isDeleting(serverID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.deleting[serverID]
	return ok
}

// markDeleting gates new leases and recreation for serverID during prune.
func (p *Proxy) markDeleting(serverID string) {
	p.mu.Lock()
	p.deleting[serverID] = struct{}{}
	// Mark SSE closed in the same critical section as the deleting gate so a
	// create cannot publish and allow subscriptions between the two.
	p.subRegClosed[serverID] = struct{}{}
	p.mu.Unlock()
}

// clearDeleting releases the prune gate once pruning has succeeded or failed;
// a failed prune leaves the durable row exited and retryable by a later DELETE.
func (p *Proxy) clearDeleting(serverID string) {
	p.mu.Lock()
	delete(p.deleting, serverID)
	p.mu.Unlock()
}

// lookupLive returns the current instance for serverID, if any.
func (p *Proxy) lookupLive(serverID string) *instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.live[serverID]
}

// reserveLive inserts inst into the live map if the fixed capacity allows.
func (p *Proxy) reserveLive(inst *instance) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return ErrClosed
	}
	if len(p.live) >= maxLiveRuntimes {
		return ErrRuntimeCapacity
	}
	p.live[inst.serverID] = inst
	return nil
}

// dropLive removes inst from the live map and prunes the keyed lock once it has
// no live owner and no waiters. An instance replaced by a newer generation is
// left untouched. Attached subscribers are terminated so a spawn failure or
// runtime exit cannot strand them.
func (p *Proxy) dropLive(inst *instance) {
	p.mu.Lock()
	if p.live[inst.serverID] == inst {
		delete(p.live, inst.serverID)
	}
	if lk, ok := p.locks[inst.serverID]; ok && lk == inst.lock && lk.refs == 0 {
		delete(p.locks, inst.serverID)
	}
	p.mu.Unlock()
	p.terminateSubscribers(inst)
	inst.watchedOnce.Do(func() { close(inst.watched) })
}

// watch releases the instance's live slot exactly once when its runtime exits
// naturally. A generation owned by DELETE or shutdown keeps its slot and
// lifecycle state until that owner finishes draining and pruning, so a failed
// termination can always be retried on the same instance. Natural exit
// terminates subscribers through dropLive's soft runtimeDown path, preserving
// their one final replay; the redacted stderr tail is retained for error
// mapping. DELETE/shutdown hard-closes subscriptions before it releases the
// slot.
func (p *Proxy) watch(inst *instance) {
	_ = inst.runtime.Wait()
	inst.lock.mu.Lock()
	gated := inst.deleting || inst.closed
	inst.lock.mu.Unlock()
	if gated {
		return
	}
	p.retainStderr(inst)
	p.dropLive(inst)
}

// keyedLock returns the lifecycle lock for serverID, creating it on demand and
// counting the caller as a holder. releaseKeyedLock must pair with it.
func (p *Proxy) keyedLock(serverID string) *lifecycleLock {
	p.mu.Lock()
	defer p.mu.Unlock()
	lk := p.locks[serverID]
	if lk == nil {
		lk = &lifecycleLock{}
		p.locks[serverID] = lk
	}
	lk.refs++
	return lk
}

// releaseKeyedLock drops one holder and prunes the lock table when the server
// has no live instance and no remaining holders.
func (p *Proxy) releaseKeyedLock(serverID string, lk *lifecycleLock) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if lk.refs > 0 {
		lk.refs--
	}
	if lk.refs != 0 || p.locks[serverID] != lk {
		return
	}
	if _, live := p.live[serverID]; !live {
		delete(p.locks, serverID)
	}
}

// Delete marks the server deleting, blocks new leases, immediately
// signal-and-waits the current runtime outside every lock, drains activity
// leases, confirms completion, and prunes the durable row. A creation in
// flight is awaited without any lock, then terminated like any live runtime. A
// prune failure leaves the row exited, removes the live instance, clears the
// deleting gate, and is retryable by a later Delete without restarting a
// process.
func (p *Proxy) Delete(ctx context.Context, serverID string) error {
	lk := p.keyedLock(serverID)
	defer p.releaseKeyedLock(serverID, lk)
	defer p.clearRetainedStderr(serverID)

	p.markDeleting(serverID)
	if hook := p.afterDeleteMark; hook != nil {
		hook()
	}
	p.closeSubscriptionsByServer(serverID)
	for {
		lk.mu.Lock()
		inst := p.lookupLive(serverID)
		if inst == nil {
			lk.mu.Unlock()
			if _, err := p.store.Server(ctx, serverID); err != nil {
				p.clearDeleting(serverID)
				return err
			}
			err := p.deleteServer(ctx, serverID)
			p.clearDeleting(serverID)
			return err
		}
		if inst.creating {
			// A first POST is mid-creation; wait for it without holding any
			// lock, then re-evaluate and terminate whatever it published.
			ready := inst.ready
			lk.mu.Unlock()
			select {
			case <-ready:
			case <-ctx.Done():
				p.clearDeleting(serverID)
				return ctx.Err()
			}
			continue
		}
		inst.deleting = true
		inst.terminating = true
		inst.detached = true
		rt := inst.runtime
		lk.mu.Unlock()

		// Close streams and signal immediately, outside every lock, so active
		// Runtime.Post calls return without waiting out the request timeout.
		p.closeSubscriptions(inst)
		killErr := rt.Kill(ctx)
		lk.mu.Lock()
		waitErr := inst.waitActivityZero(ctx)
		lk.mu.Unlock()
		var confirmErr error
		if killErr == nil {
			confirmErr = waitRuntime(ctx, rt)
		}
		if err := errors.Join(killErr, waitErr, confirmErr); err != nil {
			// Termination did not complete: keep the deleting gate and the live
			// instance so a later DELETE retries without pruning durable rows
			// while a process or lease may still be active. If the process is
			// already dead, a bounded background retire releases the slot once
			// the leases drain so capacity cannot leak without a retry.
			if killErr == nil {
				go p.retireDeadGeneration(inst)
			}
			return err
		}
		lk.mu.Lock()
		inst.closed = true
		lk.mu.Unlock()
		p.dropLive(inst)

		if delErr := p.deleteServer(ctx, serverID); delErr != nil {
			// Prune failure after completed termination: the live instance is
			// gone and the durable row stays exited for a retryable DELETE.
			p.clearDeleting(serverID)
			return delErr
		}
		p.clearDeleting(serverID)
		return nil
	}
}

// Shutdown idempotently blocks new work, stops the reaper, closes
// subscriptions, signal-and-waits every current runtime, drains activity
// leases, and confirms completion while preserving durable rows. It never
// signals a PID read only from durable state.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.shutdownMu.Lock()
	if p.shutdownStarted {
		done := p.shutdownDone
		p.shutdownMu.Unlock()
		<-done
		return p.shutdownErr
	}
	p.shutdownStarted = true
	p.shutdownDone = make(chan struct{})
	p.shutdownMu.Unlock()

	err := p.shutdown(ctx)

	p.shutdownMu.Lock()
	p.shutdownErr = err
	close(p.shutdownDone)
	p.shutdownMu.Unlock()
	return err
}

// shutdown performs the one-time staged termination: gate every live instance,
// stop the reaper, close subscriptions, signal-and-wait each runtime outside
// locks, drain leases, and record the runtimes for Confirm. A creation gated
// mid-I/O is awaited until its fresh runtime is torn down.
func (p *Proxy) shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.closed.Store(true)
	cancels := make([]context.CancelFunc, 0, len(p.spawnCancels))
	for _, cancel := range p.spawnCancels {
		cancels = append(cancels, cancel)
	}
	p.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	p.stopReaper()
	p.closeAllServerSubscriptions()

	type target struct {
		inst *instance
		rt   runtime
	}
	var targets []target
	var pending []*instance
	for _, inst := range p.snapshotLive() {
		lk := inst.lock
		lk.mu.Lock()
		if p.lookupLive(inst.serverID) != inst {
			lk.mu.Unlock()
			continue
		}
		inst.terminating = true
		inst.deleting = true
		inst.detached = true
		if inst.creating {
			// A creation is mid-I/O; its finalize observes these gates and
			// rolls back, killing the fresh runtime outside every lock. Wait
			// for that teardown after the live runtimes are signaled.
			pending = append(pending, inst)
			lk.mu.Unlock()
			continue
		}
		rt := inst.runtime
		lk.mu.Unlock()
		targets = append(targets, target{inst: inst, rt: rt})
	}

	for _, t := range targets {
		p.closeSubscriptions(t.inst)
	}
	// Signal every current runtime concurrently so one wedged Kill cannot
	// consume the caller's budget while other live runtimes remain
	// unsignaled. The lease-drain/wait phase starts only after all Kills
	// have returned.
	killErrs := make([]error, len(targets))
	var killWG sync.WaitGroup
	for i, t := range targets {
		killWG.Add(1)
		go func(i int, rt runtime) {
			defer killWG.Done()
			killErrs[i] = rt.Kill(ctx)
		}(i, t.rt)
	}
	killWG.Wait()
	var errs []error
	for _, err := range killErrs {
		if err != nil {
			errs = append(errs, err)
		}
	}
	for _, t := range targets {
		lk := t.inst.lock
		lk.mu.Lock()
		if p.lookupLive(t.inst.serverID) == t.inst {
			if err := t.inst.waitActivityZero(ctx); err != nil {
				errs = append(errs, err)
			}
			t.inst.closed = true
		}
		lk.mu.Unlock()
		if err := waitRuntime(ctx, t.rt); err != nil {
			errs = append(errs, err)
		}
		p.recordRetired(t.rt)
		p.dropLive(t.inst)
	}
	// Gated creations close ready once their fresh runtime is torn down; wait
	// so Shutdown never returns while a spawned process is still alive.
	for _, inst := range pending {
		select {
		case <-inst.ready:
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
		}
	}
	return errors.Join(errs...)
}

// Confirm idempotently re-waits the runtimes detached by Shutdown. It never
// signals a process; it only confirms pump completion before DB close.
func (p *Proxy) Confirm(ctx context.Context) error {
	p.mu.Lock()
	retired := append([]runtime(nil), p.retired...)
	p.mu.Unlock()

	var errs []error
	for _, rt := range retired {
		if err := waitRuntime(ctx, rt); err != nil {
			errs = append(errs, err)
		}
	}

	// A creation gated by Shutdown may still be inside its unlocked store or
	// spawn I/O; wait for it so SQLite is never closed under an active
	// creation. The bounded caller context keeps this from hanging.
	for _, inst := range p.snapshotLive() {
		inst.lock.mu.Lock()
		creating, ready := inst.creating, inst.ready
		inst.lock.mu.Unlock()
		if !creating {
			continue
		}
		select {
		case <-ready:
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
		}
	}
	return errors.Join(errs...)
}

// closeAllServerSubscriptions hard-closes every registered subscription,
// including those whose runtimes exited before shutdown.
func (p *Proxy) closeAllServerSubscriptions() {
	p.mu.Lock()
	p.subRegShutdown = true
	ids := make([]string, 0, len(p.subReg))
	for id := range p.subReg {
		ids = append(ids, id)
	}
	p.mu.Unlock()
	for _, id := range ids {
		p.closeSubscriptionsByServer(id)
	}
}

// retireDeadGeneration releases a dead gated generation's slot once its
// activity leases drain (or the bounded timeout expires), so a failed DELETE
// cannot permanently consume one of the 64 runtime slots. The durable row is
// left for a retry DELETE to prune. It never signals a process: the caller only
// schedules it after a successful Kill.
func (p *Proxy) retireDeadGeneration(inst *instance) {
	ctx, cancel := context.WithTimeout(context.Background(), p.retireLeaseTimeout)
	defer cancel()
	lk := inst.lock
	lk.mu.Lock()
	_ = inst.waitActivityZero(ctx)
	inst.closed = true
	lk.mu.Unlock()
	p.dropLive(inst)
}

// recordRetired remembers a runtime detached by Shutdown so Confirm can re-wait
// on it.
func (p *Proxy) recordRetired(rt runtime) {
	p.mu.Lock()
	p.retired = append(p.retired, rt)
	p.mu.Unlock()
}

// waitRuntime confirms runtime/pump completion under ctx without holding any
// lock. It is the idempotent Wait wrapper shared by Delete and Confirm.
func waitRuntime(ctx context.Context, rt runtime) error {
	done := make(chan struct{})
	go func() {
		_ = rt.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// persistenceFailure marks a store-operation failure as a persistence failure
// (HTTP 507) while leaving the lifecycle sentinels that carry their own HTTP
// status exactly untouched.
func persistenceFailure(err error) error {
	if err == nil ||
		errors.Is(err, acpstore.ErrNotFound) ||
		errors.Is(err, acpstore.ErrConflict) ||
		errors.Is(err, acpstore.ErrDeleted) {
		return err
	}
	return errors.Join(acpruntime.ErrPersistence, err)
}

// startupFailure classifies a runtime-factory failure: process spawn,
// cancellation, and exited failures stay process failures (502), while the
// startup live-row publication maps to 507. ponytail: spawn detection is
// type-based over exec.Cmd's error surface; an unknown spawn failure shape
// would be read as persistence.
func startupFailure(err error) error {
	var (
		execErr *exec.Error
		pathErr *os.PathError
	)
	if errors.As(err, &execErr) || errors.As(err, &pathErr) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, acpruntime.ErrExited) {
		return err
	}
	return persistenceFailure(err)
}

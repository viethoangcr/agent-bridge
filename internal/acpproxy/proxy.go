// Package acpproxy manages live ACP runtime creation, admission, subscriptions,
// reaping, deletion, and shutdown.
package acpproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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
	// ErrDeleting reports a server whose lifecycle is gated for deletion, or an
	// activity lease refused on a gated or still-creating instance.
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

// retireLeaseBound bounds how long a dead gated generation waits for its
// activity leases to drain before its slot is released anyway. It exists so a
// failed DELETE can never permanently consume runtime capacity.
const retireLeaseBound = 30 * time.Second

// runtime is the narrow subprocess surface the proxy consumes. Kill signals
// the direct child and waits while the sole waiter performs safe group cleanup;
// Wait confirms completion. Stderr exposes the redacted tail consulted for 502
// problem extensions.
type runtime interface {
	Post(ctx context.Context, payload json.RawMessage) (acpruntime.PostResult, error)
	Events() <-chan struct{}
	PID() int
	Stderr() string
	Wait() error
	Kill(ctx context.Context) error
}

// runtimeFactory creates one live runtime for a resolved launch spec. It is
// injected so tests can supply fake runtimes without spawning processes.
type runtimeFactory func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (runtime, error)

// Proxy is the per-server lifecycle owner. It keeps at most maxLiveRuntimes
// live instances and serializes creation, recreation, and termination per
// server ID with a keyed lifecycle lock. Every exported method is safe for
// concurrent use; Shutdown owns terminal closure and refuses new work once it
// starts.
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

// New constructs a production Proxy bound to acpruntime.Start. The caller
// retains ownership of store and resolver; the Proxy only calls them. A nil log
// discards log records.
func New(store *acpstore.Store, resolver acpruntime.Resolver, requestTimeout, idleTTL time.Duration, log *slog.Logger) *Proxy {
	return newWithFactory(store, resolver, requestTimeout, idleTTL, log,
		func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (runtime, error) {
			return acpruntime.Start(ctx, store, serverID, spec, requestTimeout, log)
		})
}

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
		retireLeaseTimeout: retireLeaseBound,
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

// killRuntime signals rt and reports the kill error. On failure it logs a
// bounded warning and the caller keeps the runtime in a retryable gated state
// instead of releasing its slot or capacity. It never signals an already-done
// runtime (Kill is idempotent).
func (p *Proxy) killRuntime(ctx context.Context, inst *instance, rt runtime) error {
	err := rt.Kill(ctx)
	if err != nil {
		p.log.Warn("runtime kill failed; retaining instance for retry",
			"server_id", inst.serverID, "error", err)
	}
	return err
}

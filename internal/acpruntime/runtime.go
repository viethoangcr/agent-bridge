package acpruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// waitDelay bounds how long Cmd.Wait may wait for wedged pipes before it
// forcibly closes them, so Wait can never hang on a descendant that inherited
// the stdio pipes.
const waitDelay = 2 * time.Second

// errNotRunning reports a write after the runtime's stdin was closed.
var errNotRunning = errors.New("acpruntime: process is not running")

// Runtime owns one ACP agent subprocess and its independent process group.
// Every exported method is safe for concurrent use. Wait and Kill are
// idempotent. The sole wait owner signals the group only while the child is
// unreaped; platforms without unreaped observation perform best-effort
// descendant cleanup, so a recycled PID or PGID is never signaled.
type Runtime struct {
	store    *acpstore.Store
	serverID string
	log      *slog.Logger

	cmd    *exec.Cmd
	cancel context.CancelFunc

	// pid is the live direct-child PID, cleared to zero after exit. External
	// Kill signals only this child's os.Process handle; the sole waiter owns
	// the negative-PGID group kill and runs it only while the child is
	// unreaped, so a recycled PGID is never targeted.
	pid atomic.Int64

	writeMu sync.Mutex
	stdin   io.WriteCloser

	// requestTimeout is the configured deadline shared by writer admission and
	// the complete write for every envelope kind; correlation and grace timers
	// build on it.
	requestTimeout time.Duration

	// The writer fields implement the single bounded JSONL serialization path.
	// writerQueue, writerStop, and writerStopped are created by startWriter;
	// post.go owns their lifecycle. streamWrite is the injected stdin-writer
	// seam: production uses writeRaw, tests replace it to block or fail.
	writerQueue    chan *writeItem
	writerStop     chan struct{}
	writerStopped  chan struct{}
	writerOnce     sync.Once
	writerStopOnce sync.Once
	poisonOnce     sync.Once
	poisoned       atomic.Bool
	streamWrite    func([]byte) (int, error)

	// stderr retains a bounded, redacted tail of the child's stderr.
	stderr stderrTail

	// The correlation fields guard the bounded waiting|grace|committing map.
	// corrMu is the only lock over corr and per-entry correlation state; it is
	// never held during SQL, signals, waits, or pump joins.
	corrMu sync.Mutex
	corr   map[string]*pendingRequest

	// statusMu serializes every runtime status write. Reconciliation acquires
	// statusMu, briefly reads corr under corrMu, releases corrMu, then writes
	// busy/idle. statusExited records terminal exit so no stale reconciliation
	// can overwrite it. exited is the lock-free fast path for Post.
	statusMu     sync.Mutex
	statusExited bool
	exited       atomic.Bool

	// terminal is the single terminal signal. It is closed exactly once,
	// before failPending clears correlations and before the process group is
	// killed; reserve checks it under corrMu and admit selects on it. The
	// admission gate serializes writer admission against this closure so no
	// record can be enqueued after closeTerminal returns.
	terminal     chan struct{}
	terminalOnce sync.Once
	gate         admissionGate

	// graceDuration is the fixed lifecycle late-response grace; it defaults to
	// defaultGraceDuration. newDeadlineTimer and newGraceTimer are the
	// injectable timer seams: production uses realRuntimeTimer, tests inject
	// controllable timers to advance deadlines/grace deterministically.
	graceDuration    time.Duration
	newDeadlineTimer func(time.Duration) *runtimeTimer
	newGraceTimer    func(time.Duration) *runtimeTimer

	// appendOutput is the persistence seam defaulting to store.AppendOutput so
	// tests can pause or fail a commit deterministically.
	appendOutput func(context.Context, string, acpstore.Output) (acpstore.Event, error)

	// beforeReconcileStatus runs after a commit releases corrMu but before
	// status reconciliation. It is nil in production and exists only so tests
	// can deterministically interleave a completion with a new reservation.
	beforeReconcileStatus func()

	// beforeTerminalClear runs after the terminal gate closes but before
	// pending correlations are cleared and terminal state is published to the
	// fast path. It is nil in production and exists only so tests can
	// deterministically interleave a Post with terminal teardown.
	beforeTerminalClear func()

	// afterReap runs in the sole waiter immediately after cmd.Wait returns,
	// before exit is published. It is nil in production and exists only so tests
	// can deterministically interleave an external signal with the reap-to-exit
	// transition.
	afterReap func()

	// observeExit reports whether the direct child's exit can be peeked without
	// reaping. It is nil in production, where procgroup.ObserveExit is used, and
	// exists only so tests can force the fallback reap-without-group-kill path
	// or pause the waiter around its group kill.
	observeExit func(*Runtime, int) bool

	// wake is the capacity-one coalesced output/termination wakeup channel.
	// wakeMu guards the closed flag so a late signal cannot panic after finish
	// closes the channel.
	wake       chan struct{}
	wakeMu     sync.Mutex
	wakeClosed bool

	// pumps tracks the runtime-owned stdio drains. done closes after the sole
	// wait owner has joined every pump and published exit.
	pumps sync.WaitGroup
	done  chan struct{}

	waitErr error
}

// Start launches the agent described by spec and returns a Runtime that owns the
// direct child and its process group. The caller retains ownership of store; a
// nil log discards log records. ctx governs the runtime's lifetime: Start
// derives its cancellable context from ctx, so canceling ctx kills the child and
// callers must detach ctx if the runtime should outlive it. A non-positive
// requestTimeout is rejected before any process is spawned. On failure after
// spawn, Start kills the direct child, attempts safe group cleanup, records the
// server exited, and returns a wrapped error; on success the durable row is
// marked live before Start returns.
func Start(ctx context.Context, store *acpstore.Store, serverID string, spec LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (*Runtime, error) {
	return start(ctx, store, serverID, spec, requestTimeout, log, nil, nil)
}

// start is Start with the test-only afterReap and observeExit hooks threaded
// through so they are set before the sole waiter goroutine launches.
func start(ctx context.Context, store *acpstore.Store, serverID string, spec LaunchSpec, requestTimeout time.Duration, log *slog.Logger, afterReap func(), observeExit func(*Runtime, int) bool) (*Runtime, error) {
	if requestTimeout <= 0 {
		return nil, fmt.Errorf("acpruntime: request timeout must be positive, got %s", requestTimeout)
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, spec.Program, spec.Args...)
	cmd.Env = spec.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = waitDelay

	stdin, stdout, stderr, err := openProcessPipes(cmd, spec.Program)
	if err != nil {
		cancel()
		return nil, err
	}

	r := newRuntime(store, serverID, log, cmd, cancel, stdin, requestTimeout)
	r.afterReap = afterReap
	r.observeExit = observeExit
	// Keep exec.CommandContext's default direct-child Cancel: a canceled start
	// kills only the leader, and the sole waiter then cleans up the group before
	// reaping. A group-killing Cancel would contend with the waiter and could
	// signal a PGID it no longer owns.

	if err := cmd.Start(); err != nil {
		cancel()
		closePipe(stdin)
		closePipe(stdout)
		closePipe(stderr)
		r.markExited()
		return nil, fmt.Errorf("acpruntime: start %q: %w", spec.Program, err)
	}

	r.pid.Store(int64(cmd.Process.Pid))

	// A cancellation that raced the spawn may not have been seen by the default
	// cancel watcher yet; observe it now before publishing live.
	if err := runCtx.Err(); err != nil {
		return nil, r.abortAfterSpawn(fmt.Errorf("acpruntime: spawn %q canceled: %w", spec.Program, err), stdout, stderr)
	}

	if err := r.publishLive(ctx, cmd.Process.Pid); err != nil {
		return nil, r.abortAfterSpawn(fmt.Errorf("acpruntime: mark %q live: %w", serverID, err), stdout, stderr)
	}

	r.pumps.Add(2)
	go r.readOutput(stdout)
	go r.readStderr(stderr)
	r.startWriter()
	go r.waitProcess()

	return r, nil
}

// newRuntime builds the Runtime for one spawned command and wires the
// store-backed append seam when a store is present. The terminal gate, done,
// and wakeup channels are created here; startWriter creates the writer channels.
func newRuntime(store *acpstore.Store, serverID string, log *slog.Logger, cmd *exec.Cmd, cancel context.CancelFunc, stdin io.WriteCloser, requestTimeout time.Duration) *Runtime {
	r := &Runtime{
		store:          store,
		serverID:       serverID,
		log:            log,
		cmd:            cmd,
		cancel:         cancel,
		stdin:          stdin,
		requestTimeout: requestTimeout,
		graceDuration:  defaultGraceDuration,
		corr:           make(map[string]*pendingRequest),
		terminal:       make(chan struct{}),
		done:           make(chan struct{}),
		wake:           make(chan struct{}, 1),
	}
	if store != nil {
		r.appendOutput = store.AppendOutput
	}
	return r
}

// PID returns the live direct-child process ID, or 0 after exit.
func (r *Runtime) PID() int {
	return int(r.pid.Load())
}

// Events returns the capacity-one coalesced wakeup channel. A value means
// committed output may be queryable; a closed channel means the runtime
// terminated. Callers must use the two-value receive form.
func (r *Runtime) Events() <-chan struct{} {
	return r.wake
}

// Wait blocks until the direct child has exited, safe group cleanup has been
// attempted, the pumps have joined, and server exit is published. Repeated
// calls return the same process error.
func (r *Runtime) Wait() error {
	<-r.done
	return r.waitErr
}

// Kill terminates the direct child and waits for runtime teardown. It is
// idempotent and safe for concurrent use. ctx bounds that wait. Kill signals
// only the os.Process handle; the sole wait owner performs safe group cleanup.
func (r *Runtime) Kill(ctx context.Context) error {
	select {
	case <-r.done:
		return nil
	default:
	}
	if err := r.killChild(); err != nil {
		return err
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// killChild SIGKILLs the direct child and treats an already-reaped process as
// success. It never targets a process group.
func (r *Runtime) killChild() error {
	if r.cmd == nil || r.cmd.Process == nil {
		return nil
	}
	if err := r.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

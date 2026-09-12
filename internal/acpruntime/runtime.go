package acpruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
//
// Start captures the child's pipes before spawn, records the row live only
// after a successful spawn, and starts a minimal stdout/stderr drain so the
// child cannot wedge. A single wait owner reaps the direct child; whenever the
// direct child exits, that owner immediately SIGKILLs the captured negative
// PGID before joining the pumps and publishing exit. Wait and repeated Kill are
// idempotent confirmations over the same completion.
type Runtime struct {
	store    *acpstore.Store
	serverID string
	log      *slog.Logger

	cmd    *exec.Cmd
	cancel context.CancelFunc

	// pgid is the captured process-group ID of the direct child; it is never
	// derived from persisted state. pid is cleared to zero after exit.
	pgid atomic.Int64
	pid  atomic.Int64

	writeMu sync.Mutex
	stdin   io.WriteCloser

	// requestTimeout is the configured deadline shared by writer admission and
	// the complete write for every envelope kind. Task 2.9b owns it; Task 2.9c
	// layers correlation/grace timers on top.
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

// Start launches the agent described by spec under a runtime-owned cancellable
// context and its own process group. A non-positive requestTimeout is rejected
// before any process is spawned. The row is marked live/idle with the PID only
// after a successful spawn; any post-spawn failure kills the group and marks
// the row exited.
func Start(ctx context.Context, store *acpstore.Store, serverID string, spec LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (*Runtime, error) {
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

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("acpruntime: stdin pipe for %q: %w", spec.Program, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		closePipe(stdin)
		return nil, fmt.Errorf("acpruntime: stdout pipe for %q: %w", spec.Program, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		closePipe(stdin)
		closePipe(stdout)
		return nil, fmt.Errorf("acpruntime: stderr pipe for %q: %w", spec.Program, err)
	}

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
	// CommandContext's default cancellation kills only the direct child;
	// replace it with a guarded negative-PGID SIGKILL.
	cmd.Cancel = func() error { return r.killProcessGroup() }

	if err := cmd.Start(); err != nil {
		cancel()
		closePipe(stdin)
		closePipe(stdout)
		closePipe(stderr)
		r.markExited()
		return nil, fmt.Errorf("acpruntime: start %q: %w", spec.Program, err)
	}

	// Setpgid guarantees the direct child leads its own group, so the PGID
	// equals the child PID even when the lookup itself fails.
	pgid := cmd.Process.Pid
	if captured, err := syscall.Getpgid(cmd.Process.Pid); err == nil && captured > 1 {
		pgid = captured
	}
	r.pgid.Store(int64(pgid))
	r.pid.Store(int64(cmd.Process.Pid))

	// A cancellation that raced the spawn was not seen by the guard above
	// (pgid was not yet captured); observe it now before publishing live.
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

// Wait blocks until the direct child has exited and the runtime has killed the
// group, joined its pumps, and marked the server exited. Repeated calls return
// the same process error.
func (r *Runtime) Wait() error {
	<-r.done
	return r.waitErr
}

// Kill sends SIGKILL to the captured negative PGID, then waits for the direct
// child and every runtime-owned goroutine. It is idempotent and safe to call
// concurrently or repeatedly.
func (r *Runtime) Kill(ctx context.Context) error {
	if err := r.killProcessGroup(); err != nil {
		return err
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitProcess is the sole cmd.Wait owner. It closes the terminal gate before
// killing the captured group, stops and joins the writer, and only then joins
// the pumps and publishes exit.
func (r *Runtime) waitProcess() {
	err := r.cmd.Wait()
	r.closeTerminal()
	_ = r.killProcessGroup()
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

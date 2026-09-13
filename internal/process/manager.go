package process

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MaxOutputBytes is the largest per-stream output a one-shot run may capture.
const MaxOutputBytes = 16 << 20

// MaxRetainedLogMemoryBytes is the fixed conservative aggregate budget for
// retained raw log chunks and their metadata. It is an admission/accounting
// control, not an exact Go heap guarantee.
const MaxRetainedLogMemoryBytes = 256 << 20

// MaxActiveRunPeakBytes is the fixed conservative aggregate reservation budget
// for active one-shot captures. It is an admission/accounting control, not an
// exact Go heap guarantee.
const MaxActiveRunPeakBytes = 512 << 20

// Sentinel errors the transport layer maps to HTTP statuses.
var (
	// ErrNotFound reports an unknown managed process ID.
	ErrNotFound = errors.New("process not found")
	// ErrConflict reports an operation that conflicts with the current state,
	// such as deleting a running process.
	ErrConflict = errors.New("process conflict")
	// ErrCapacity reports that the shared concurrency budget is exhausted.
	ErrCapacity = errors.New("process capacity reached")
	// ErrValidation reports invalid caller input.
	ErrValidation = errors.New("invalid process request")
	// ErrStart reports that a process group could not be created or spawned.
	ErrStart = errors.New("process start failed")
	// ErrPayloadTooLarge reports decoded input above the active limit.
	ErrPayloadTooLarge = errors.New("process input exceeds decoded limit")
	// ErrGateway reports a process I/O failure, such as a stdin write error.
	ErrGateway = errors.New("process i/o failure")
)

// errShuttingDown is returned once BlockNew or Shutdown marks the manager
// closing. It wraps ErrConflict so the transport maps it to 409.
var errShuttingDown = fmt.Errorf("%w: process manager is closing", ErrConflict)

// managerBudgets are the private admission budgets injected into a Manager.
// Production uses the package constants; tests inject small values so behavior
// can be exercised without large allocations.
type managerBudgets struct {
	retainedLogMemoryBytes int
	activeRunPeakBytes     int
}

// productionBudgets returns the fixed production admission budgets.
func productionBudgets() managerBudgets {
	return managerBudgets{
		retainedLogMemoryBytes: MaxRetainedLogMemoryBytes,
		activeRunPeakBytes:     MaxActiveRunPeakBytes,
	}
}

// Manager owns managed process groups and their in-memory lifecycle records.
// Exited records are retained until Delete. The zero value is not usable; call
// NewManager.
type Manager struct {
	mu        sync.Mutex
	processes map[string]*managedProcess
	nextID    atomic.Int64

	reserveMu       sync.Mutex
	closing         bool
	activeProcesses int
	activeRunPeak   int

	// wg counts active managed watchers and one-shot runs. Every reservation
	// adds one under reserveMu and releases it exactly once, so Shutdown can
	// await all watchers and pumps after marking the manager closing.
	wg sync.WaitGroup

	// runGroups tracks the live signal gate of every one-shot process group so
	// Shutdown can kill runs that are not retained as managed records without
	// ever targeting a reaped or recycled PGID.
	runGroupsMu sync.Mutex
	runGroups   map[int]*signalGate

	// observeExit and afterReap are private test seams for the unreaped-exit
	// observation. Production leaves both nil, so observedExit delegates to
	// procgroup.ObserveExit and afterReap is never called.
	observeExit func(pid int) bool
	afterReap   func(*managedProcess)

	shutdownOnce sync.Once
	shutdownDone chan struct{}

	// logMu is the single lock that orders every log append, global eviction,
	// query copy, and delete-time charge release. It is never held during pipe
	// reads, waits, base64 encoding, or HTTP writes.
	logMu          sync.Mutex
	globalSequence int64
	logFIFO        []*logEntry
	logCharge      int

	config  *configStore
	budgets managerBudgets
	baseEnv []string
	cwd     string
}

// NewManager returns a Manager that spawns processes with a defensively copied
// base environment and default cwd, using the production admission budgets.
func NewManager(baseEnv []string, cwd string) *Manager {
	m, err := newManagerWithBudgets(baseEnv, cwd, productionBudgets())
	if err != nil {
		// Production budgets are compile-time constants; reaching here is a
		// programming error, not a runtime condition.
		panic("process: invalid production budgets: " + err.Error())
	}
	return m
}

// newManagerWithBudgets is the private construction seam. It rejects
// non-positive budgets so tests cannot silently disable admission control.
func newManagerWithBudgets(baseEnv []string, cwd string, budgets managerBudgets) (*Manager, error) {
	if budgets.retainedLogMemoryBytes <= 0 {
		return nil, fmt.Errorf("process: retained log budget must be positive, got %d", budgets.retainedLogMemoryBytes)
	}
	if budgets.activeRunPeakBytes <= 0 {
		return nil, fmt.Errorf("process: active run peak budget must be positive, got %d", budgets.activeRunPeakBytes)
	}
	return &Manager{
		processes: make(map[string]*managedProcess),
		runGroups: make(map[int]*signalGate),
		config:    newConfigStore(DefaultConfig()),
		budgets:   budgets,
		baseEnv:   append([]string(nil), baseEnv...),
		cwd:       cwd,
	}, nil
}

// newID returns a process ID unique for the manager's lifetime. IDs are never
// reused, even after the record is deleted.
func (m *Manager) newID() string {
	return "proc_" + strconv.FormatInt(m.nextID.Add(1), 10)
}

// watch is the sole cmd.Wait owner for p. It waits out the direct child and
// both pumps, then publishes exit and releases concurrency capacity.
func (m *Manager) watch(p *managedProcess) {
	p.wait()
	m.publishExit(p)
	m.releaseProcess()
	close(p.done)
}

// publishExit records the direct child's exit code and timestamp under the
// registry lock unless the record already exited.
func (m *Manager) publishExit(p *managedProcess) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.status != StatusRunning {
		return
	}
	p.status = StatusExited
	if p.cmd.ProcessState != nil {
		if code := p.cmd.ProcessState.ExitCode(); code >= 0 {
			p.exitCode = &code
		}
	}
	exitedAt := time.Now().UnixMilli()
	p.exitedAtMs = &exitedAt
}

// mergeEnv applies overlay on top of base without mutating either input. Base
// entries keep their order; an overlay key replaces every base occurrence and
// overlay-only keys are appended in sorted order.
func mergeEnv(base []string, overlay map[string]string) []string {
	if len(overlay) == 0 {
		return append([]string(nil), base...)
	}
	out := make([]string, 0, len(base)+len(overlay))
	remaining := make(map[string]struct{}, len(overlay))
	for key := range overlay {
		remaining[key] = struct{}{}
	}
	for _, entry := range base {
		key, _, found := strings.Cut(entry, "=")
		value, overridden := overlay[key]
		if !found || !overridden {
			out = append(out, entry)
			continue
		}
		out = append(out, key+"="+value)
		delete(remaining, key)
	}
	keys := make([]string, 0, len(remaining))
	for key := range remaining {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		out = append(out, key+"="+overlay[key])
	}
	return out
}

// closePipe closes c best-effort. Pipes are closed on every pre-spawn failure
// path so no descriptor leaks.
func closePipe(c io.Closer) {
	_ = c.Close()
}

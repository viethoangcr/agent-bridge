package process

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

// Start validates req, reserves concurrency capacity, spawns the command in its
// own process group, and returns a running snapshot. On spawn failure it
// releases the reservation without creating a record.
func (m *Manager) Start(req StartRequest) (Snapshot, error) {
	command := strings.TrimSpace(req.Command)
	if command == "" {
		return Snapshot{}, fmt.Errorf("%w: command is required", ErrValidation)
	}
	effectiveCwd := m.cwd
	if req.Cwd != "" {
		effectiveCwd = req.Cwd
	}

	cmd := exec.Command(command, req.Args...)
	cmd.Dir = effectiveCwd
	cmd.Env = mergeEnv(m.baseEnv, req.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: stdin pipe: %v", ErrStart, err)
	}
	// Explicit pipes, not cmd.StdoutPipe/cmd.StderrPipe: cmd.Wait closes exec's
	// own parent pipes once the direct child is reaped, which would discard any
	// buffered output the pumps have not read yet. The pumps own these read
	// ends and close them after EOF.
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		closePipe(stdin)
		return Snapshot{}, fmt.Errorf("%w: stdout pipe: %v", ErrStart, err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		closePipe(stdin)
		closePipe(stdoutR)
		closePipe(stdoutW)
		return Snapshot{}, fmt.Errorf("%w: stderr pipe: %v", ErrStart, err)
	}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	if err := m.reserveProcess(); err != nil {
		closePipe(stdin)
		closePipe(stdoutR)
		closePipe(stdoutW)
		closePipe(stderrR)
		closePipe(stderrW)
		return Snapshot{}, err
	}
	if err := cmd.Start(); err != nil {
		closePipe(stdin)
		closePipe(stdoutR)
		closePipe(stdoutW)
		closePipe(stderrR)
		closePipe(stderrW)
		m.releaseProcess()
		return Snapshot{}, fmt.Errorf("%w: %v", ErrStart, err)
	}
	// The parent must drop its write ends so the pumps observe EOF once the
	// direct child and every descendant have closed theirs.
	closePipe(stdoutW)
	closePipe(stderrW)

	pid := cmd.Process.Pid
	p := &managedProcess{
		id:             m.newID(),
		command:        command,
		args:           append([]string(nil), req.Args...),
		cwd:            effectiveCwd,
		status:         StatusRunning,
		pid:            pid,
		signals:        signalGate{pid: pid},
		createdAtMs:    time.Now().UnixMilli(),
		manager:        m,
		cmd:            cmd,
		stdin:          stdin,
		inputAdmission: make(chan struct{}, 1),
		ring:           newLogRing(m.config.load().MaxLogBytesPerProcess),
		done:           make(chan struct{}),
	}

	m.mu.Lock()
	m.processes[p.id] = p
	snapshot := p.snapshot()
	m.mu.Unlock()

	// A concurrent Shutdown marks the manager closing after this start passed
	// its reservation check but before the group was registered. Self-kill the
	// freshly spawned group so it cannot outlive the shutdown snapshot.
	if m.isClosing() {
		_ = p.signalGroup(syscall.SIGKILL)
	}

	p.pumps.Add(2)
	go p.pump("stdout", stdoutR)
	go p.pump("stderr", stderrR)
	go m.watch(p)

	return snapshot, nil
}

// List returns independent snapshots of every retained process, sorted by ID.
func (m *Manager) List() []Snapshot {
	m.mu.Lock()
	out := make([]Snapshot, 0, len(m.processes))
	for _, p := range m.processes {
		out = append(out, p.snapshot())
	}
	m.mu.Unlock()

	slices.SortFunc(out, func(a, b Snapshot) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// Get returns a copied snapshot for id, or ErrNotFound.
func (m *Manager) Get(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.processes[id]
	if !ok {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return p.snapshot(), nil
}

// Delete removes an exited process record. A running process is ErrConflict and
// an unknown ID is ErrNotFound.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	p, ok := m.processes[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if p.status == StatusRunning {
		m.mu.Unlock()
		return fmt.Errorf("%w: process %s is still running", ErrConflict, id)
	}
	delete(m.processes, id)
	m.mu.Unlock()
	m.releaseLogCharge(p)
	return nil
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

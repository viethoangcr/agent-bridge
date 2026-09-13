package process

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/procgroup"
)

// runPeakMultiplier is the conservative per-run peak reservation factor applied
// to the effective per-stream output cap: 2x for two raw captures, up to 6x for
// UTF-8 replacement/result strings, and up to 12x for JSON escaping plus
// encoder buffers.
const runPeakMultiplier = 20

// Validate reports static RunRequest errors that do not depend on the active
// configuration: a required command, positive optional timeout/output caps, and
// a timeout representable as a time.Duration. Manager.Run additionally enforces
// the active maxima; requested values are never clamped.
func (r RunRequest) Validate() error {
	if strings.TrimSpace(r.Command) == "" {
		return fmt.Errorf("%w: command is required", ErrValidation)
	}
	if r.TimeoutMs != nil {
		if *r.TimeoutMs <= 0 {
			return fmt.Errorf("%w: timeoutMs must be positive", ErrValidation)
		}
		if *r.TimeoutMs > maxDurationMillis {
			return fmt.Errorf("%w: timeoutMs is too large to represent as a duration", ErrValidation)
		}
	}
	if r.MaxOutputBytes != nil && *r.MaxOutputBytes <= 0 {
		return fmt.Errorf("%w: maxOutputBytes must be positive", ErrValidation)
	}
	return nil
}

// resolveRunTimeout applies the active default and rejects a requested timeout
// above the active maximum instead of clamping it. RunRequest.Validate has
// already rejected non-positive requested values.
func resolveRunTimeout(cfg Config, requested *int64) (time.Duration, error) {
	ms := int64(cfg.DefaultRunTimeoutMs)
	if requested != nil {
		ms = *requested
	}
	if ms > int64(cfg.MaxRunTimeoutMs) {
		return 0, fmt.Errorf("%w: timeoutMs %d exceeds active maximum %d", ErrValidation, ms, cfg.MaxRunTimeoutMs)
	}
	if ms > maxDurationMillis {
		return 0, fmt.Errorf("%w: timeoutMs is too large to represent as a duration", ErrValidation)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// resolveRunOutputCap applies the active default and rejects a requested cap
// above the active maximum instead of clamping it. RunRequest.Validate has
// already rejected non-positive requested values, and the active maximum is
// bounded, so the int conversion is safe.
func resolveRunOutputCap(cfg Config, requested *int64) (int, error) {
	if requested == nil {
		return cfg.MaxOutputBytes, nil
	}
	if *requested > int64(cfg.MaxOutputBytes) {
		return 0, fmt.Errorf("%w: maxOutputBytes %d exceeds active maximum %d", ErrValidation, *requested, cfg.MaxOutputBytes)
	}
	return int(*requested), nil
}

// runPeakBytes returns the conservative per-run peak reservation of
// runPeakMultiplier times the effective per-stream output cap. The
// multiplication is checked so a pathological cap cannot wrap the reservation.
func runPeakBytes(maxOutputBytes int) (int, error) {
	if maxOutputBytes < 0 || maxOutputBytes > math.MaxInt/runPeakMultiplier {
		return 0, fmt.Errorf("%w: output cap %d overflows the %dx peak reservation", ErrValidation, maxOutputBytes, runPeakMultiplier)
	}
	return maxOutputBytes * runPeakMultiplier, nil
}

// capture reads r until EOF, retaining at most limit bytes and discarding the
// remainder so a child writing beyond its cap cannot deadlock on a full pipe.
// It sets truncated on the first discarded byte and deterministically replaces
// invalid UTF-8 using strings.ToValidUTF8 semantics.
func capture(r io.Reader, limit int) (string, bool) {
	var buf bytes.Buffer
	truncated := false
	chunk := make([]byte, pumpBufferSize)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			if remaining := limit - buf.Len(); remaining > 0 {
				take := min(n, remaining)
				buf.Write(chunk[:take])
				if take < n {
					truncated = true
				}
			} else {
				truncated = true
			}
		}
		if err != nil {
			break
		}
	}
	return strings.ToValidUTF8(buf.String(), "\uFFFD"), truncated
}

// runPlan is the validated, prepared state of one one-shot run before it is
// spawned. prepareRun creates the pipes; the caller owns closing the read ends.
type runPlan struct {
	cmd     *exec.Cmd
	stdoutR *os.File
	stderrR *os.File
	stdoutW *os.File
	stderrW *os.File
	timeout time.Duration
	outCap  int
	peak    int

	pid  int
	gate *signalGate
}

// captureResult is one stream's captured text and truncation flag.
type captureResult struct {
	text      string
	truncated bool
}

// runOutcome is the result of waiting out one spawned run.
type runOutcome struct {
	stdout   captureResult
	stderr   captureResult
	waitErr  error
	timedOut bool
}

// prepareRun validates req against a single configuration snapshot, resolves
// the effective timeout and output cap, checks ctx, and builds the command with
// explicit stdout/stderr pipes. On failure every created pipe is closed.
func (m *Manager) prepareRun(ctx context.Context, req RunRequest) (*runPlan, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	cfg := m.config.load()

	timeout, err := resolveRunTimeout(cfg, req.TimeoutMs)
	if err != nil {
		return nil, err
	}
	outCap, err := resolveRunOutputCap(cfg, req.MaxOutputBytes)
	if err != nil {
		return nil, err
	}
	peak, err := runPeakBytes(outCap)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGateway, err)
	}

	command := strings.TrimSpace(req.Command)
	effectiveCwd := m.cwd
	if req.Cwd != "" {
		effectiveCwd = req.Cwd
	}

	cmd := exec.Command(command, req.Args...)
	cmd.Dir = effectiveCwd
	cmd.Env = mergeEnv(m.baseEnv, req.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = nil

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("%w: stdout pipe: %v", ErrStart, err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		closePipe(stdoutR)
		closePipe(stdoutW)
		return nil, fmt.Errorf("%w: stderr pipe: %v", ErrStart, err)
	}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW
	return &runPlan{
		cmd:     cmd,
		stdoutR: stdoutR,
		stderrR: stderrR,
		stdoutW: stdoutW,
		stderrW: stderrW,
		timeout: timeout,
		outCap:  outCap,
		peak:    peak,
	}, nil
}

// closeReads closes both capture read ends. It is the caller's deferred cleanup.
func (p *runPlan) closeReads() {
	closePipe(p.stdoutR)
	closePipe(p.stderrR)
}

// closeWrites drops the parent's write ends so both captures observe EOF once
// the direct child and every descendant have closed theirs.
func (p *runPlan) closeWrites() {
	closePipe(p.stdoutW)
	closePipe(p.stderrW)
}

// spawnRun reserves capacity, starts plan's command, drops the parent's write
// ends, and registers the live signal gate. It returns release and unregister
// callbacks the caller must invoke exactly once. On failure it closes the
// write ends and leaves no reservation.
func (m *Manager) spawnRun(plan *runPlan) (release, unregister func(), err error) {
	if err := m.reserveRun(plan.peak); err != nil {
		plan.closeWrites()
		return nil, nil, err
	}
	released := false
	release = func() {
		if released {
			return
		}
		released = true
		m.releaseRun(plan.peak)
	}
	if err := plan.cmd.Start(); err != nil {
		plan.closeWrites()
		release()
		return nil, nil, fmt.Errorf("%w: %v", ErrStart, err)
	}
	plan.closeWrites()

	pid := plan.cmd.Process.Pid
	plan.pid = pid
	plan.gate = &signalGate{pid: pid}
	// Register the live group before checking closing so a concurrent Shutdown
	// either sees this gate in its snapshot or observes closing here.
	m.runGroupsMu.Lock()
	m.runGroups[pid] = plan.gate
	m.runGroupsMu.Unlock()
	unregister = func() {
		m.runGroupsMu.Lock()
		delete(m.runGroups, pid)
		m.runGroupsMu.Unlock()
	}
	if m.isClosing() {
		_ = plan.gate.signal(syscall.SIGKILL)
	}
	return release, unregister, nil
}

// awaitRun starts both captures and races the direct child's exit against the
// plan timeout and the caller context. A timeout or cancellation SIGKILLs the
// captured negative PGID and always reaps the direct child. It unregisters the
// group before returning so a later Shutdown cannot signal a reused PID.
func (m *Manager) awaitRun(ctx context.Context, plan *runPlan, unregister func()) runOutcome {
	killGroup := func() { _ = plan.gate.signal(syscall.SIGKILL) }

	stdoutCh := make(chan captureResult, 1)
	stderrCh := make(chan captureResult, 1)
	go func() {
		text, truncated := capture(plan.stdoutR, plan.outCap)
		stdoutCh <- captureResult{text: text, truncated: truncated}
	}()
	go func() {
		text, truncated := capture(plan.stderrR, plan.outCap)
		stderrCh <- captureResult{text: text, truncated: truncated}
	}()

	waitCh := make(chan error, 1)
	go func() {
		// The waiter is the group's sole owner. Observe the direct child's
		// exit without reaping it, SIGKILL the captured group and mark it
		// exited under the signal gate while the zombie still owns the PID,
		// then reap with cmd.Wait so the real exit status is published.
		// Platforms without an unreaped observation use the fallback order.
		var waitErr error
		if procgroup.ObserveExit(plan.pid) {
			plan.gate.exit()
			waitErr = plan.cmd.Wait()
		} else {
			waitErr = plan.cmd.Wait()
			plan.gate.exit()
		}
		waitCh <- waitErr
	}()

	timer := time.NewTimer(plan.timeout)
	defer timer.Stop()

	outcome := runOutcome{}
	select {
	case outcome.waitErr = <-waitCh:
		// The waiter already killed and marked the group.
	case <-timer.C:
		outcome.timedOut = true
		killGroup()
		outcome.waitErr = <-waitCh
	case <-ctx.Done():
		killGroup()
		outcome.waitErr = <-waitCh
	}

	// The direct child has been reaped; stop advertising this group so a later
	// Shutdown cannot signal a reused PID.
	unregister()

	outcome.stdout = <-stdoutCh
	outcome.stderr = <-stderrCh
	return outcome
}

// finishRun maps the outcome to a RunResult. A non-timeout context error or a
// non-exit wait error is an ErrGateway failure.
func finishRun(ctx context.Context, plan *runPlan, outcome runOutcome, started time.Time) (RunResult, error) {
	duration := time.Since(started).Milliseconds()

	if ctxErr := ctx.Err(); ctxErr != nil && !outcome.timedOut {
		return RunResult{}, fmt.Errorf("%w: %v", ErrGateway, ctxErr)
	}
	if outcome.waitErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(outcome.waitErr, &exitErr) {
			return RunResult{}, fmt.Errorf("%w: wait: %v", ErrGateway, outcome.waitErr)
		}
	}

	result := RunResult{
		TimedOut:        outcome.timedOut,
		Stdout:          outcome.stdout.text,
		Stderr:          outcome.stderr.text,
		StdoutTruncated: outcome.stdout.truncated,
		StderrTruncated: outcome.stderr.truncated,
		DurationMs:      duration,
	}
	if !outcome.timedOut && plan.cmd.ProcessState != nil {
		if code := plan.cmd.ProcessState.ExitCode(); code >= 0 {
			result.ExitCode = &code
		}
	}
	return result, nil
}

// Run executes one bounded one-shot command with null stdin, two output pipes,
// and an independent process group. It snapshots the active configuration once,
// validates requested timeout/output caps against the active maxima, and
// atomically reserves one shared process slot plus a checked
// runPeakMultiplier*effectiveMaxOutputBytes peak before spawning. Insufficient
// capacity is ErrCapacity with no partial reservation and no spawn.
//
// Both streams are captured concurrently up to their own effective cap while
// the remainder is drained and discarded. cmd.Wait races the requested timeout
// and the caller context; a timeout or cancellation SIGKILLs the captured
// negative PGID and always reaps the direct child. The sole waiter observes the
// direct child's exit unreaped (Linux), SIGKILLs the captured group and marks
// it exited under the group's signal gate while the zombie still owns the PID,
// and only then reaps the child with cmd.Wait, before publishing the result or
// joining captures. Descendants therefore cannot hold the inherited pipes open
// and no external signal path can ever target a recycled PGID. Platforms
// without an unreaped observation keep the reap-then-kill order.
func (m *Manager) Run(ctx context.Context, req RunRequest) (RunResult, error) {
	plan, err := m.prepareRun(ctx, req)
	if err != nil {
		return RunResult{}, err
	}
	defer plan.closeReads()

	started := time.Now()
	release, unregister, err := m.spawnRun(plan)
	if err != nil {
		return RunResult{}, err
	}
	defer release()
	defer unregister()

	outcome := m.awaitRun(ctx, plan, unregister)
	return finishRun(ctx, plan, outcome, started)
}

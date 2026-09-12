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
)

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

// runPeakBytes returns the conservative per-run peak reservation of 20 times
// the effective per-stream output cap. The multiplication is checked so a
// pathological cap cannot wrap the reservation. The 20x model is 2x for two raw
// captures, up to 6x for UTF-8 replacement/result strings, and up to 12x for
// JSON escaping plus encoder buffers.
func runPeakBytes(maxOutputBytes int) (int, error) {
	if maxOutputBytes < 0 || maxOutputBytes > math.MaxInt/20 {
		return 0, fmt.Errorf("%w: output cap %d overflows the 20x peak reservation", ErrValidation, maxOutputBytes)
	}
	return maxOutputBytes * 20, nil
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

// Run executes one bounded one-shot command with null stdin, two output pipes,
// and an independent process group. It snapshots the active configuration once,
// validates requested timeout/output caps against the active maxima, and
// atomically reserves one shared process slot plus a checked
// 20*effectiveMaxOutputBytes peak before spawning. Insufficient capacity is
// ErrCapacity with no partial reservation and no spawn.
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
	if err := req.Validate(); err != nil {
		return RunResult{}, err
	}
	cfg := m.config.load()

	timeout, err := resolveRunTimeout(cfg, req.TimeoutMs)
	if err != nil {
		return RunResult{}, err
	}
	outCap, err := resolveRunOutputCap(cfg, req.MaxOutputBytes)
	if err != nil {
		return RunResult{}, err
	}
	peak, err := runPeakBytes(outCap)
	if err != nil {
		return RunResult{}, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	if err := ctx.Err(); err != nil {
		return RunResult{}, fmt.Errorf("%w: %v", ErrGateway, err)
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
		return RunResult{}, fmt.Errorf("%w: stdout pipe: %v", ErrStart, err)
	}
	defer closePipe(stdoutR)
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		closePipe(stdoutW)
		return RunResult{}, fmt.Errorf("%w: stderr pipe: %v", ErrStart, err)
	}
	defer closePipe(stderrR)
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	if err := m.reserveRun(peak); err != nil {
		closePipe(stdoutW)
		closePipe(stderrW)
		return RunResult{}, err
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		m.releaseRun(peak)
	}
	defer release()

	start := time.Now()
	if err := cmd.Start(); err != nil {
		closePipe(stdoutW)
		closePipe(stderrW)
		return RunResult{}, fmt.Errorf("%w: %v", ErrStart, err)
	}
	// The parent must drop its write ends so the captures observe EOF once the
	// direct child and any descendants have exited.
	closePipe(stdoutW)
	closePipe(stderrW)

	pid := cmd.Process.Pid
	gate := &signalGate{pid: pid}
	// Register the live group before checking closing so a concurrent Shutdown
	// either sees this gate in its snapshot or observes closing here.
	m.runGroupsMu.Lock()
	m.runGroups[pid] = gate
	m.runGroupsMu.Unlock()
	unregister := func() {
		m.runGroupsMu.Lock()
		delete(m.runGroups, pid)
		m.runGroupsMu.Unlock()
	}
	defer unregister()

	// killGroup is the external signaling path; it becomes a no-op once the
	// waiter has SIGKILLed and marked the group exited.
	killGroup := func() {
		_ = gate.signal(syscall.SIGKILL)
	}
	if m.isClosing() {
		killGroup()
	}

	type captureResult struct {
		text      string
		truncated bool
	}
	stdoutCh := make(chan captureResult, 1)
	stderrCh := make(chan captureResult, 1)
	go func() {
		text, truncated := capture(stdoutR, outCap)
		stdoutCh <- captureResult{text: text, truncated: truncated}
	}()
	go func() {
		text, truncated := capture(stderrR, outCap)
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
		if observeExit(pid) {
			gate.exit()
			waitErr = cmd.Wait()
		} else {
			waitErr = cmd.Wait()
			gate.exit()
		}
		waitCh <- waitErr
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var waitErr error
	timedOut := false
	select {
	case waitErr = <-waitCh:
		// The waiter already killed and marked the group.
	case <-timer.C:
		timedOut = true
		killGroup()
		waitErr = <-waitCh
	case <-ctx.Done():
		killGroup()
		waitErr = <-waitCh
	}

	// The direct child has been reaped; stop advertising this group so a later
	// Shutdown cannot signal a reused PID.
	unregister()

	stdoutRes := <-stdoutCh
	stderrRes := <-stderrCh
	duration := time.Since(start).Milliseconds()

	if ctxErr := ctx.Err(); ctxErr != nil && !timedOut {
		return RunResult{}, fmt.Errorf("%w: %v", ErrGateway, ctxErr)
	}
	if waitErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			return RunResult{}, fmt.Errorf("%w: wait: %v", ErrGateway, waitErr)
		}
	}

	result := RunResult{
		TimedOut:        timedOut,
		Stdout:          stdoutRes.text,
		Stderr:          stderrRes.text,
		StdoutTruncated: stdoutRes.truncated,
		StderrTruncated: stderrRes.truncated,
		DurationMs:      duration,
	}
	if !timedOut && cmd.ProcessState != nil {
		if code := cmd.ProcessState.ExitCode(); code >= 0 {
			result.ExitCode = &code
		}
	}
	return result, nil
}

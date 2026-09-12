package process

import (
	"context"
	"syscall"
)

// BlockNew is the staged pre-drain hook: it marks the manager closing under the
// reservation lock so every new managed start and one-shot run is rejected
// before HTTP drain. It is idempotent and honors ctx cancellation.
func (m *Manager) BlockNew(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.reserveMu.Lock()
	m.closing = true
	m.reserveMu.Unlock()
	return nil
}

// Shutdown is the staged shutdown killer: it marks the manager closing, closes
// managed stdin, SIGKILLs every current managed and one-shot process group, and
// waits for all watchers and pumps to finish, releasing every reservation
// exactly once. It is idempotent and bounded by ctx; a caller that observes ctx
// expiry returns without re-running the kill.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.shutdownOnce.Do(func() {
		m.shutdownDone = make(chan struct{})
		go func() {
			defer close(m.shutdownDone)
			m.shutdown()
		}()
	})
	select {
	case <-m.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shutdown performs the one-time blocking kill and wait. Process groups are
// signaled outside every global lock, then the shared WaitGroup joins the
// managed watchers and one-shot runs.
func (m *Manager) shutdown() {
	m.reserveMu.Lock()
	m.closing = true
	m.reserveMu.Unlock()

	m.mu.Lock()
	managed := make([]*managedProcess, 0, len(m.processes))
	for _, p := range m.processes {
		if p.status == StatusRunning {
			managed = append(managed, p)
		}
	}
	m.mu.Unlock()

	m.runGroupsMu.Lock()
	runs := make([]*signalGate, 0, len(m.runGroups))
	for _, gate := range m.runGroups {
		runs = append(runs, gate)
	}
	m.runGroupsMu.Unlock()

	for _, p := range managed {
		closePipe(p.stdin)
	}
	for _, p := range managed {
		_ = p.signalGroup(syscall.SIGKILL)
	}
	for _, gate := range runs {
		_ = gate.signal(syscall.SIGKILL)
	}
	m.wg.Wait()
}

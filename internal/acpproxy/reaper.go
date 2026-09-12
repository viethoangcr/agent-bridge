package acpproxy

import (
	"context"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// reaperInterval is the fixed production cadence of the idle reaper. It is a
// policy sweep, not a per-instance timer: instances are reaped only once their
// durable idle deadline has passed.
const reaperInterval = time.Second

// reaperTicker abstracts the reaper cadence so tests advance a fake ticker
// instead of waiting in real time.
type reaperTicker interface {
	C() <-chan time.Time
	Stop()
}

// realReaperTicker is the production time.Ticker adapter.
type realReaperTicker struct{ ticker *time.Ticker }

func (t realReaperTicker) C() <-chan time.Time { return t.ticker.C }
func (t realReaperTicker) Stop()               { t.ticker.Stop() }

// StartReaper starts the single idle reaper. An idle TTL of zero disables
// reaping entirely. Repeated calls are no-ops.
func (p *Proxy) StartReaper() {
	if p.idleTTL <= 0 {
		return
	}
	p.reaperMu.Lock()
	if p.reaperStarted {
		p.reaperMu.Unlock()
		return
	}
	p.reaperStarted = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	ticker := p.newReaperTicker(reaperInterval)
	p.reaperCancel = cancel
	p.reaperDone = done
	p.reaperMu.Unlock()

	go p.reaperLoop(ctx, ticker, done)
}

// stopReaper cancels and joins the reaper if it was started. It is idempotent.
func (p *Proxy) stopReaper() {
	p.reaperMu.Lock()
	if !p.reaperStarted {
		p.reaperMu.Unlock()
		return
	}
	cancel := p.reaperCancel
	done := p.reaperDone
	p.reaperMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// reaperLoop sweeps on every tick and exits on cancellation, stopping its
// ticker on the way out.
func (p *Proxy) reaperLoop(ctx context.Context, ticker reaperTicker, done chan struct{}) {
	defer close(done)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			p.reapOnce(ctx)
		}
	}
}

// reapOnce sweeps every live instance once against the injected clock. An idle
// TTL of zero disables reaping.
func (p *Proxy) reapOnce(ctx context.Context) {
	if p.idleTTL <= 0 {
		return
	}
	now := p.now()
	for _, inst := range p.snapshotLive() {
		p.reapInstance(ctx, inst, now)
	}
}

// snapshotLive copies the current live instances under the map lock so sweeps
// never hold it during store I/O or kills.
func (p *Proxy) snapshotLive() []*instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	insts := make([]*instance, 0, len(p.live))
	for _, inst := range p.live {
		insts = append(insts, inst)
	}
	return insts
}

// reapInstance evaluates one instance against the idle deadline. It takes the
// instance's lifecycle lock just long enough to verify the current generation
// is ungated and to set the terminating gate, releases it for the durable
// status read, drains activity, re-reads the durable status unlocked, and only
// then signals the current runtime outside every lock. A stale generation or a
// refreshed/non-idle state clears the gate without killing.
func (p *Proxy) reapInstance(ctx context.Context, inst *instance, now time.Time) {
	lk := inst.lock
	lk.mu.Lock()
	if !p.reapEligibleLocked(inst) {
		lk.mu.Unlock()
		return
	}
	lk.mu.Unlock()

	// Confirm the durable row is still idle and expired BEFORE gating, so a
	// reaper pass never rejects concurrent POSTs for busy or non-expired
	// instances.
	if !p.idleExpired(ctx, inst.serverID, now) {
		return
	}

	lk.mu.Lock()
	if !p.reapEligibleLocked(inst) {
		lk.mu.Unlock()
		return
	}
	inst.terminating = true
	if err := inst.waitActivityZero(ctx); err != nil {
		inst.terminating = false
		lk.mu.Unlock()
		return
	}
	lk.mu.Unlock()

	// Re-read the durable status after the drain without holding any lock: a
	// busy row means the runtime still owns a grace-retained or committing
	// correlation, and a refreshed idle transition moved the deadline. Either
	// way the kill is abandoned.
	if !p.idleExpired(ctx, inst.serverID, p.now()) {
		p.clearTerminating(inst)
		return
	}

	lk.mu.Lock()
	if !p.reapGatedCurrentLocked(inst) {
		inst.terminating = false
		lk.mu.Unlock()
		return
	}
	rt := inst.runtime
	lk.mu.Unlock()
	if rt == nil {
		// Defensive: placeholders are never reaper-eligible.
		p.clearTerminating(inst)
		return
	}

	// Close streams first so the runtime's own termination fan-out cannot let a
	// final replay race past the hard close.
	p.closeSubscriptions(inst)
	_ = rt.Kill(ctx)
	p.dropLive(inst)
}

// reapEligibleLocked reports whether inst is the current live generation and
// carries no gate of its own. The caller holds inst's lifecycle lock.
func (p *Proxy) reapEligibleLocked(inst *instance) bool {
	current := p.lookupLive(inst.serverID)
	return current == inst && !inst.terminating && !inst.deleting && !inst.detached &&
		!inst.closed && !inst.creating
}

// reapGatedCurrentLocked reports whether a reaper-gated inst is still the
// current live generation and has not been claimed by another terminator while
// the lifecycle lock was released. The caller holds inst's lifecycle lock.
func (p *Proxy) reapGatedCurrentLocked(inst *instance) bool {
	current := p.lookupLive(inst.serverID)
	return current == inst && !inst.deleting && !inst.detached && !inst.closed && !inst.creating
}

// clearTerminating releases the reaper's gate after an abandoned kill.
func (p *Proxy) clearTerminating(inst *instance) {
	lk := inst.lock
	lk.mu.Lock()
	inst.terminating = false
	lk.mu.Unlock()
}

// idleExpired reports whether the server's durable idle deadline is at or
// before now. An unreadable row or any non-idle status is never expired.
func (p *Proxy) idleExpired(ctx context.Context, serverID string, now time.Time) bool {
	server, err := p.storeServer(ctx, serverID)
	if err != nil || server.Status != acpstore.StatusIdle || server.IdleSinceMs == nil {
		return false
	}
	return !now.Before(time.UnixMilli(*server.IdleSinceMs).Add(p.idleTTL))
}

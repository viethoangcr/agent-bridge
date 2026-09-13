package acpproxy

import (
	"context"
	"errors"
	"sync"
)

// shutdownTarget pairs a gated live instance with its runtime for shutdown's
// signal-and-wait phase.
type shutdownTarget struct {
	inst *instance
	rt   runtime
}

// Shutdown terminates every current runtime and closes all subscriptions while
// preserving durable rows. It blocks new work and stops the reaper. Concurrent
// or repeated calls block on the first call and share its result; ctx bounds
// the process waits. It never signals a PID read only from durable state.
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

	targets, pending := p.gateForShutdown()

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
	errs = append(errs, p.finishPendingCreates(ctx, pending)...)
	return errors.Join(errs...)
}

// gateForShutdown marks every current live instance terminating and returns the
// non-creating generations with their runtimes plus the mid-I/O creations whose
// rollback must be awaited after the live runtimes are signaled.
func (p *Proxy) gateForShutdown() (targets []shutdownTarget, pending []*instance) {
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
		targets = append(targets, shutdownTarget{inst: inst, rt: rt})
	}
	return targets, pending
}

// finishPendingCreates waits for gated creations to tear down their fresh
// runtimes so Shutdown never returns while a spawned process is still alive. A
// rollback whose kill failed retains the runtime, so it is terminated here too.
func (p *Proxy) finishPendingCreates(ctx context.Context, pending []*instance) []error {
	var errs []error
	for _, inst := range pending {
		select {
		case <-inst.ready:
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			continue
		}
		lk := inst.lock
		lk.mu.Lock()
		rt := inst.runtime
		if p.lookupLive(inst.serverID) != inst || rt == nil {
			rt = nil
		} else {
			inst.closed = true
		}
		lk.mu.Unlock()
		if rt == nil {
			continue
		}
		if err := p.killRuntime(ctx, inst, rt); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := waitRuntime(ctx, rt); err != nil {
			errs = append(errs, err)
		}
		p.recordRetired(rt)
		p.dropLive(inst)
	}
	return errs
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

func (p *Proxy) recordRetired(rt runtime) {
	p.mu.Lock()
	p.retired = append(p.retired, rt)
	p.mu.Unlock()
}

// waitRuntime confirms runtime/pump completion under ctx without holding any
// lock. It is the idempotent Wait wrapper shared by Delete and Confirm. Wait's
// own error is intentionally discarded: Kill has already reported the signal
// outcome, and callers only need to know whether the pumps finished before the
// store closes, not why the child exited.
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

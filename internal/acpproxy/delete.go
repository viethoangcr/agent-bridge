package acpproxy

import (
	"context"
	"errors"
)

// Delete marks the server deleting, blocks new leases, immediately
// signal-and-waits the current runtime outside every lock, drains activity
// leases, confirms completion, and prunes the durable row. A creation in
// flight is awaited without any lock, then terminated like any live runtime. A
// prune failure leaves the row exited, removes the live instance, clears the
// deleting gate, and is retryable by a later Delete without restarting a
// process.
func (p *Proxy) Delete(ctx context.Context, serverID string) error {
	lk := p.keyedLock(serverID)
	defer p.releaseKeyedLock(serverID, lk)
	defer p.clearRetainedStderr(serverID)

	p.markDeleting(serverID)
	if hook := p.afterDeleteMark; hook != nil {
		hook()
	}
	p.closeSubscriptionsByServer(serverID)
	for {
		lk.mu.Lock()
		inst := p.lookupLive(serverID)
		if inst == nil {
			lk.mu.Unlock()
			if _, err := p.store.Server(ctx, serverID); err != nil {
				p.clearDeleting(serverID)
				return err
			}
			err := p.deleteServer(ctx, serverID)
			p.clearDeleting(serverID)
			return err
		}
		if inst.creating {
			// A first POST is mid-creation; wait for it without holding any
			// lock, then re-evaluate and terminate whatever it published.
			ready := inst.ready
			lk.mu.Unlock()
			select {
			case <-ready:
			case <-ctx.Done():
				p.clearDeleting(serverID)
				return ctx.Err()
			}
			continue
		}
		inst.deleting = true
		inst.terminating = true
		inst.detached = true
		rt := inst.runtime
		lk.mu.Unlock()

		// Close streams and signal immediately, outside every lock, so active
		// Runtime.Post calls return without waiting out the request timeout.
		p.closeSubscriptions(inst)
		killErr := rt.Kill(ctx)
		lk.mu.Lock()
		waitErr := inst.waitActivityZero(ctx)
		lk.mu.Unlock()
		var confirmErr error
		if killErr == nil {
			confirmErr = waitRuntime(ctx, rt)
		}
		if err := errors.Join(killErr, waitErr, confirmErr); err != nil {
			// Termination did not complete: keep the deleting gate and the live
			// instance so a later DELETE retries without pruning durable rows
			// while a process or lease may still be active. If the process is
			// already dead, a bounded background retire releases the slot once
			// the leases drain so capacity cannot leak without a retry.
			if killErr == nil {
				go p.retireDeadGeneration(inst)
			}
			return err
		}
		lk.mu.Lock()
		inst.closed = true
		lk.mu.Unlock()
		p.dropLive(inst)

		if delErr := p.deleteServer(ctx, serverID); delErr != nil {
			// Prune failure after completed termination: the live instance is
			// gone and the durable row stays exited for a retryable DELETE.
			p.clearDeleting(serverID)
			return delErr
		}
		p.clearDeleting(serverID)
		return nil
	}
}

// retireDeadGeneration releases a dead gated generation's slot once its
// activity leases drain (or the bounded timeout expires), so a failed DELETE
// cannot permanently consume one of the 64 runtime slots. The durable row is
// left for a retry DELETE to prune. It never signals a process: the caller only
// schedules it after a successful Kill.
func (p *Proxy) retireDeadGeneration(inst *instance) {
	ctx, cancel := context.WithTimeout(context.Background(), p.retireLeaseTimeout)
	defer cancel()
	lk := inst.lock
	lk.mu.Lock()
	_ = inst.waitActivityZero(ctx)
	inst.closed = true
	lk.mu.Unlock()
	p.dropLive(inst)
}

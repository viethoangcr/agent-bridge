package acpproxy

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// Post routes one client envelope to the server's current runtime, creating or
// recreating that runtime when necessary. Concurrent callers for the same
// server ID are serialized on the runtime lifecycle so exactly one creation
// wins; callers for other servers proceed independently. The payload is passed
// through unchanged for the runtime to validate and compact. Lifecycle failures
// are returned as the package sentinel errors the HTTP layer maps to problem
// responses.
func (p *Proxy) Post(ctx context.Context, serverID string, agent *string, method string, payload json.RawMessage) (acpruntime.PostResult, error) {
	if p.closed.Load() {
		return acpruntime.PostResult{}, ErrClosed
	}

	lk := p.keyedLock(serverID)
	defer p.releaseKeyedLock(serverID, lk)

	inst, err := p.acquireForPost(ctx, lk, serverID, agent, method)
	if err != nil {
		return acpruntime.PostResult{}, err
	}
	defer inst.releaseActivity()
	return inst.runtime.Post(ctx, payload)
}

// acquireForPost returns a live instance with one activity lease held. When no
// instance is live it claims a creating placeholder under the lifecycle lock,
// runs the creation I/O unlocked, and rechecks. A caller that finds another
// goroutine's placeholder waits on ready without any lock; it uses the live
// replacement when one was published and inherits the placeholder's error when
// the creation was abandoned.
func (p *Proxy) acquireForPost(ctx context.Context, lk *lifecycleLock, serverID string, agent *string, method string) (*instance, error) {
	for {
		lk.mu.Lock()
		if p.closed.Load() {
			lk.mu.Unlock()
			return nil, ErrClosed
		}
		if p.isDeleting(serverID) {
			lk.mu.Unlock()
			return nil, ErrDeleting
		}
		live := p.lookupLive(serverID)
		if live == nil {
			placeholder := p.newInstance(serverID, "", lk)
			placeholder.creating = true
			placeholder.ready = make(chan struct{})
			if err := p.reserveLive(placeholder); err != nil {
				lk.mu.Unlock()
				return nil, err
			}
			lk.mu.Unlock()

			if err := p.create(ctx, lk, placeholder, serverID, agent, method); err != nil {
				return nil, err
			}
			// create published the placeholder. Re-check and take the lease
			// under the same lock so a concurrent DELETE cannot slip between
			// the health check and the re-acquisition and make this caller
			// recreate the server the DELETE just pruned. A natural exit drops
			// the instance without setting the terminal flags, so the existing
			// retry/reinitialize path is preserved.
			lk.mu.Lock()
			if p.lookupLive(serverID) == placeholder && !placeholder.creating && !placeholder.gated() {
				if err := placeholder.acquireActivity(); err != nil {
					lk.mu.Unlock()
					return nil, err
				}
				lk.mu.Unlock()
				return placeholder, nil
			}
			gated := placeholder.gated()
			lk.mu.Unlock()
			if gated {
				return nil, ErrDeleting
			}
			continue
		}
		if live.creating {
			current, retry, err := p.awaitCreating(ctx, lk, serverID, agent, live)
			if err != nil {
				return nil, err
			}
			if retry {
				continue
			}
			return current, nil
		}
		if live.gated() {
			lk.mu.Unlock()
			return nil, ErrDeleting
		}
		if agent != nil && *agent != "" && *agent != live.agent {
			lk.mu.Unlock()
			return nil, ErrAgentConflict
		}
		if err := live.acquireActivity(); err != nil {
			lk.mu.Unlock()
			return nil, err
		}
		lk.mu.Unlock()
		return live, nil
	}
}

// awaitCreating blocks on a placeholder's ready channel without holding any
// lock and resolves the outcome under the lifecycle lock. It returns retry=true
// when the acquisition loop must run again because the placeholder was replaced
// without an error to inherit (a published replacement or a stale generation).
// The caller must hold the lifecycle lock on entry.
func (p *Proxy) awaitCreating(ctx context.Context, lk *lifecycleLock, serverID string, agent *string, placeholder *instance) (inst *instance, retry bool, err error) {
	ready := placeholder.ready
	lk.mu.Unlock()
	if hook := p.beforeCreateWait; hook != nil {
		hook()
	}
	select {
	case <-ready:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	lk.mu.Lock()
	current := p.lookupLive(serverID)
	if current != nil && !current.creating && !current.gated() {
		// Re-check the requested agent in the same critical section as
		// the lease so a waiter for a different agent can never be
		// dispatched to the instance the creation just published.
		if agent != nil && *agent != "" && *agent != current.agent {
			lk.mu.Unlock()
			return nil, false, ErrAgentConflict
		}
		if err := current.acquireActivity(); err != nil {
			lk.mu.Unlock()
			return nil, false, err
		}
		lk.mu.Unlock()
		return current, false, nil
	}
	gated := placeholder.gated()
	createErr := placeholder.createErr
	lk.mu.Unlock()
	// A recorded abandonment error is the creator's outcome; it wins over the
	// gate state so every waiter observes the same cause as the creator. It
	// applies when the waiter's own generation failed and was not replaced:
	// either it was dropped (current == nil) or its failed teardown retained it
	// (current == placeholder). A genuinely replaced generation falls through
	// to the retry/lease path below.
	if createErr != nil && (current == nil || current == placeholder) {
		return nil, false, createErr
	}
	// A DELETE that gated the placeholder while this waiter slept wins:
	// never recreate a server the DELETE just pruned, even after the
	// global deleting gate cleared.
	if gated {
		return nil, false, ErrDeleting
	}
	return nil, true, nil
}

// create performs the durable read, resolution, row transition, and spawn for a
// reserved placeholder without holding any lifecycle or global lock. On
// success it publishes the runtime via publishCreate; on failure it abandons
// the placeholder and returns the error.
func (p *Proxy) create(ctx context.Context, lk *lifecycleLock, inst *instance, serverID string, agent *string, method string) error {
	// A new generation supersedes any tail retained for an older failure, so a
	// resolution or spawn failure never attaches stale agent stderr.
	p.clearRetainedStderr(serverID)
	if p.closed.Load() {
		return p.abandonCreate(ctx, inst, nil, ErrClosed)
	}
	server, err := p.storeServer(ctx, serverID)
	newServer := false
	switch {
	case errors.Is(err, acpstore.ErrNotFound):
		if agent == nil || *agent == "" {
			return p.abandonCreate(ctx, inst, nil, ErrMissingAgent)
		}
		newServer = true
		server = acpstore.Server{ServerID: serverID, Agent: *agent}
	case err != nil:
		return p.abandonCreate(ctx, inst, nil, persistenceFailure(err))
	default:
		if agent != nil && *agent != "" && *agent != server.Agent {
			return p.abandonCreate(ctx, inst, nil, ErrAgentConflict)
		}
		if method != initializeMethod {
			return p.abandonCreate(ctx, inst, nil, ErrReinitialize)
		}
	}

	spec, err := p.resolver.Resolve(server.Agent)
	if err != nil {
		return p.abandonCreate(ctx, inst, nil, err)
	}

	if newServer {
		if _, err := p.createServer(ctx, serverID, server.Agent); err != nil {
			return p.abandonCreate(ctx, inst, nil, persistenceFailure(err))
		}
	} else if err := p.setStatus(ctx, serverID, acpstore.StatusCreating); err != nil {
		return p.abandonCreate(ctx, inst, nil, persistenceFailure(err))
	}

	// A runtime outlives the HTTP request that created it: detach the spawn
	// context so request completion/cancellation cannot kill the subprocess.
	// The proxy owns termination through Kill, DELETE, the reaper, and shutdown.
	// Shutdown cancels this spawn context, so an in-flight spawn is aborted
	// before publication; the cancel is deregistered before the runtime is
	// published so a live runtime is never killed by shutdown's sweep.
	if p.closed.Load() {
		return p.abandonCreate(ctx, inst, nil, ErrClosed)
	}
	if hook := p.beforeSpawnRegister; hook != nil {
		hook()
	}
	spawnCtx, cancelSpawn := context.WithCancel(context.WithoutCancel(ctx))
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		cancelSpawn()
		return p.abandonCreate(ctx, inst, nil, ErrClosed)
	}
	p.spawnCancels[inst] = cancelSpawn
	p.mu.Unlock()
	rt, err := p.factory(spawnCtx, p.store, serverID, spec, p.requestTimeout, p.log)
	p.mu.Lock()
	delete(p.spawnCancels, inst)
	p.mu.Unlock()
	if err != nil {
		return p.abandonCreate(ctx, inst, nil, startupFailure(err))
	}

	return p.publishCreate(ctx, inst, serverID, rt, server.Agent)
}

// publishCreate attaches a successfully spawned runtime to its placeholder
// under the lifecycle lock and wakes its waiters. A DELETE or shutdown that
// gated the placeholder while the spawn was in flight wins: the fresh runtime
// is killed outside every lock and the placeholder abandoned. The caller holds
// no lock.
func (p *Proxy) publishCreate(ctx context.Context, inst *instance, serverID string, rt runtime, agent string) error {
	lk := inst.lock
	lk.mu.Lock()
	if p.live[serverID] != inst || inst.gated() || p.closed.Load() {
		closed := p.closed.Load()
		lk.mu.Unlock()
		gateErr := ErrDeleting
		if closed {
			gateErr = ErrClosed
		}
		return p.abandonCreate(ctx, inst, rt, gateErr)
	}
	if hook := p.beforeAllowServerSubs; hook != nil {
		hook()
	}
	if !p.allowServerSubs(serverID) {
		lk.mu.Unlock()
		return p.abandonCreate(ctx, inst, rt, ErrDeleting)
	}
	inst.agent = agent
	inst.runtime = rt
	inst.creating = false
	close(inst.ready)
	lk.mu.Unlock()

	go p.watch(inst)
	go p.watchEvents(inst)
	return nil
}

// abandonCreate removes a placeholder, releases exactly one capacity slot, and
// wakes its waiters, which inherit err. A spawn that could not be published is
// killed first, outside every lock, so no process survives a delete or shutdown
// that gated the placeholder mid-spawn. If that kill fails, the runtime stays
// tracked as a retained generation so DELETE, shutdown, or the idle reaper can
// retry terminating it: its capacity stays consumed until it is confirmed gone.
// It is marked terminating to gate activity but not deleting or detached, so
// the reaper still considers it eligible.
func (p *Proxy) abandonCreate(ctx context.Context, inst *instance, rt runtime, err error) error {
	retained := rt != nil && p.killRuntime(context.WithoutCancel(ctx), inst, rt) != nil
	lk := inst.lock
	lk.mu.Lock()
	if retained {
		inst.runtime = rt
		inst.retained = true
		inst.terminating = true
	} else {
		p.dropLive(inst)
	}
	inst.createErr = err
	inst.creating = false
	close(inst.ready)
	lk.mu.Unlock()
	if retained {
		// Observe a natural exit so the slot is released even if no retry
		// reclaims the runtime.
		go p.watch(inst)
	}
	return err
}

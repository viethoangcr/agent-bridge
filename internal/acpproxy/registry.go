package acpproxy

// LivePID reports the direct-child PID of serverID's current live generation.
// It returns false for an unknown server or one that is gated for termination,
// so durable PIDs are never exposed as live.
func (p *Proxy) LivePID(serverID string) (int, bool) {
	inst := p.lookupLive(serverID)
	if inst == nil {
		return 0, false
	}
	lk := inst.lock
	lk.mu.Lock()
	defer lk.mu.Unlock()
	if p.lookupLive(serverID) != inst || inst.runtime == nil {
		return 0, false
	}
	if inst.gated() {
		return 0, false
	}
	return inst.runtime.PID(), true
}

// Stderr returns the current live generation's retained, redacted agent stderr
// tail for the HTTP 502 problem extension. When the watcher already released an
// exited generation, it falls back to that generation's tail so error mapping
// after a failing Post still sees it; the retained tail is consumed by the
// read. It returns "" when no generation exists or none exposed a stderr tail.
func (p *Proxy) Stderr(serverID string) string {
	inst := p.lookupLive(serverID)
	if inst != nil {
		lk := inst.lock
		lk.mu.Lock()
		live := p.lookupLive(serverID) == inst && inst.runtime != nil
		stderr := ""
		if live {
			stderr = inst.runtime.Stderr()
		}
		lk.mu.Unlock()
		if live {
			return stderr
		}
	}
	return p.takeRetainedStderr(serverID)
}

// retainStderr stores the exiting generation's redacted tail for the error
// mapping that follows its removal, unless a newer generation already owns the
// server's live slot.
func (p *Proxy) retainStderr(inst *instance) {
	stderr := inst.runtime.Stderr()
	if stderr == "" {
		return
	}
	p.mu.Lock()
	if current, ok := p.live[inst.serverID]; !ok || current == inst {
		p.retainedStderr[inst.serverID] = stderr
	}
	p.mu.Unlock()
}

// takeRetainedStderr releases and returns the retained tail so a mapped
// process failure consumes it exactly once.
func (p *Proxy) takeRetainedStderr(serverID string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	stderr := p.retainedStderr[serverID]
	delete(p.retainedStderr, serverID)
	return stderr
}

func (p *Proxy) clearRetainedStderr(serverID string) {
	p.mu.Lock()
	delete(p.retainedStderr, serverID)
	p.mu.Unlock()
}

func (p *Proxy) isDeleting(serverID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.deleting[serverID]
	return ok
}

// markDeleting gates new leases and recreation for serverID during prune.
func (p *Proxy) markDeleting(serverID string) {
	p.mu.Lock()
	p.deleting[serverID] = struct{}{}
	// Mark SSE closed in the same critical section as the deleting gate so a
	// create cannot publish and allow subscriptions between the two.
	p.subRegClosed[serverID] = struct{}{}
	p.mu.Unlock()
}

// clearDeleting releases the prune gate once pruning has succeeded or failed;
// a failed prune leaves the durable row exited and retryable by a later DELETE.
func (p *Proxy) clearDeleting(serverID string) {
	p.mu.Lock()
	delete(p.deleting, serverID)
	p.mu.Unlock()
}

func (p *Proxy) lookupLive(serverID string) *instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.live[serverID]
}

func (p *Proxy) reserveLive(inst *instance) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return ErrClosed
	}
	if len(p.live) >= maxLiveRuntimes {
		return ErrRuntimeCapacity
	}
	p.live[inst.serverID] = inst
	return nil
}

// dropLive removes inst from the live map and prunes the keyed lock once it has
// no live owner and no waiters. An instance replaced by a newer generation is
// left untouched. Attached subscribers are terminated so a spawn failure or
// runtime exit cannot strand them.
func (p *Proxy) dropLive(inst *instance) {
	p.mu.Lock()
	if p.live[inst.serverID] == inst {
		delete(p.live, inst.serverID)
	}
	if lk, ok := p.locks[inst.serverID]; ok && lk == inst.lock && lk.refs == 0 {
		delete(p.locks, inst.serverID)
	}
	p.mu.Unlock()
	p.terminateSubscribers(inst)
	inst.watchedOnce.Do(func() { close(inst.watched) })
}

// watch releases the instance's live slot exactly once when its runtime exits
// naturally. A generation owned by DELETE or shutdown keeps its slot and
// lifecycle state until that owner finishes draining and pruning, so a failed
// termination can always be retried on the same instance. Natural exit
// terminates subscribers through dropLive's soft runtimeDown path, preserving
// their one final replay; the redacted stderr tail is retained for error
// mapping. DELETE/shutdown hard-closes subscriptions before it releases the
// slot.
func (p *Proxy) watch(inst *instance) {
	_ = inst.runtime.Wait()
	inst.lock.mu.Lock()
	gated := inst.deleting || inst.closed
	inst.lock.mu.Unlock()
	if gated {
		return
	}
	p.retainStderr(inst)
	p.dropLive(inst)
}

// keyedLock returns the lifecycle lock for serverID, creating it on demand and
// counting the caller as a holder. releaseKeyedLock must pair with it.
func (p *Proxy) keyedLock(serverID string) *lifecycleLock {
	p.mu.Lock()
	defer p.mu.Unlock()
	lk := p.locks[serverID]
	if lk == nil {
		lk = &lifecycleLock{}
		p.locks[serverID] = lk
	}
	lk.refs++
	return lk
}

// releaseKeyedLock drops one holder and prunes the lock table when the server
// has no live instance and no remaining holders.
func (p *Proxy) releaseKeyedLock(serverID string, lk *lifecycleLock) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if lk.refs > 0 {
		lk.refs--
	}
	if lk.refs != 0 || p.locks[serverID] != lk {
		return
	}
	if _, live := p.live[serverID]; !live {
		delete(p.locks, serverID)
	}
}

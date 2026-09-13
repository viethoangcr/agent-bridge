package acpproxy

import (
	"context"
	"sync"
)

// lifecycleLock serializes creation, recreation, and termination for one
// server ID. refs counts holders and waiters and is guarded by Proxy.mu so the
// table can be pruned when a server has no live instance and no waiters.
type lifecycleLock struct {
	mu   sync.Mutex
	refs int
}

// instance is one live runtime and its activity/gate state. activity,
// generation, terminating, deleting, detached, closed, and zero are guarded by
// the server's lifecycle lock. terminating/deleting/detached/closed plus
// waitActivityZero are the gate/lease primitives the reaper, DELETE, and
// shutdown drive.
type instance struct {
	serverID    string
	agent       string
	runtime     runtime
	lock        *lifecycleLock
	generation  uint64
	activity    int
	terminating bool
	deleting    bool
	detached    bool
	closed      bool

	// creating marks a placeholder reserved while its durable-row read,
	// resolution, and spawn I/O are still in flight. ready closes when the
	// creation publishes its runtime or is abandoned, and createErr records an
	// abandonment cause so waiters inherit it. All three are guarded by the
	// server's lifecycle lock.
	creating  bool
	ready     chan struct{}
	createErr error

	// zero is closed whenever activity is zero and replaced by acquireActivity
	// on the next lease, giving termination waits a cancellation-aware signal.
	zero chan struct{}

	// watched is closed by dropLive once the live slot has been released,
	// giving tests an explicit cleanup signal instead of polling.
	watched     chan struct{}
	watchedOnce sync.Once

	// subMu guards the attached subscribers and the two terminal flags.
	// runtimeDown is set when the runtime closes its wakeup channel; subsClosed
	// is set by DELETE/shutdown. Both stop new registrations.
	subMu       sync.Mutex
	subs        map[*subscription]struct{}
	runtimeDown bool
	subsClosed  bool
}

// acquireActivity registers one active lease. The caller holds the lifecycle
// lock. It refuses leases once the instance is terminating, detached, or still
// a creation placeholder without a runtime.
func (i *instance) acquireActivity() error {
	if i.terminating || i.detached || i.creating {
		return ErrDeleting
	}
	if i.activity == 0 {
		i.zero = make(chan struct{})
	}
	i.activity++
	return nil
}

// releaseActivity drops one active lease, waking termination waiters when the
// count reaches zero. It takes the lifecycle lock itself and is safe against an
// unmatched release.
func (i *instance) releaseActivity() {
	i.lock.mu.Lock()
	defer i.lock.mu.Unlock()
	if i.activity == 0 {
		return
	}
	i.activity--
	if i.activity == 0 {
		close(i.zero)
	}
}

// waitActivityZero blocks until no active leases remain or ctx ends. The caller
// must hold the lifecycle lock; it is released while waiting and reacquired
// before returning. This is the 3.3 reaper/delete drain primitive.
func (i *instance) waitActivityZero(ctx context.Context) error {
	for i.activity > 0 {
		drained := i.zero
		i.lock.mu.Unlock()
		select {
		case <-ctx.Done():
			i.lock.mu.Lock()
			return ctx.Err()
		case <-drained:
			i.lock.mu.Lock()
		}
	}
	return nil
}

// newInstance builds a drained, ungated instance bound to lk with the next
// generation. A newer generation's presence in the live map is what makes an
// in-flight reaper or delete decision for an older instance stale.
func (p *Proxy) newInstance(serverID, agent string, lk *lifecycleLock) *instance {
	inst := &instance{
		serverID:   serverID,
		agent:      agent,
		lock:       lk,
		generation: p.nextGen.Add(1),
		zero:       make(chan struct{}),
		watched:    make(chan struct{}),
	}
	close(inst.zero)
	return inst
}

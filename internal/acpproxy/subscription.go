package acpproxy

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// subscriptionFallbackInterval is the bounded production fallback poll. A
// dropped or coalesced runtime wakeup delays an event by at most this long,
// after which the subscription re-queries authoritative SQLite state.
const subscriptionFallbackInterval = time.Second

// Subscription is one replay-then-live event reader for a server. Next returns
// persisted acpstore.Event values in ascending sequence order; io.EOF means the
// runtime terminated or the subscription closed. Close is idempotent and does
// not affect other subscriptions of the same server.
type Subscription interface {
	Next(context.Context) (acpstore.Event, error)
	Close()
}

// subscriptionTicker abstracts the bounded fallback poll so tests can advance a
// fake ticker without sleeping.
type subscriptionTicker interface {
	C() <-chan time.Time
	Stop()
}

// realSubscriptionTicker is the production time.Ticker adapter.
type realSubscriptionTicker struct{ ticker *time.Ticker }

func (t realSubscriptionTicker) C() <-chan time.Time { return t.ticker.C }
func (t realSubscriptionTicker) Stop()               { t.ticker.Stop() }

// subscription is one subscriber attached to a live instance, or a replay-only
// reader of an exited server. Wakeups never carry event data: each only prompts
// a query after lastSeq, so a dropped wakeup can never create a gap.
type subscription struct {
	proxy    *Proxy
	serverID string
	inst     *instance
	ticker   subscriptionTicker

	// lastSeq is the last sequence emitted by Next; it is owned by the single
	// consumer goroutine.
	lastSeq int64

	// wake is the capacity-one coalesced notification from the instance
	// watcher. Its closure signals runtime termination.
	wake chan struct{}
	// closed is closed by Close and stops Next with io.EOF.
	closed chan struct{}

	// wakeMu guards wakeClosed so a notify racing termination can never send on
	// a closed channel.
	wakeMu     sync.Mutex
	wakeClosed bool

	terminateOnce sync.Once
	closeOnce     sync.Once
}

// Subscribe returns a subscription that replays persisted events after after
// and then follows committed wakeups. An unknown server fails before any
// subscription is created. An exited server with no live runtime still replays
// its persisted events and then terminates.
func (p *Proxy) Subscribe(ctx context.Context, serverID string, after int64) (Subscription, error) {
	if after < 0 {
		return nil, acpstore.ErrValidation
	}
	if p.closed.Load() {
		return nil, ErrClosed
	}
	if _, err := p.storeServer(ctx, serverID); err != nil {
		return nil, err
	}

	sub := &subscription{
		proxy:    p,
		serverID: serverID,
		lastSeq:  after,
		wake:     make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	// Create the fallback ticker before the subscription is published to the
	// instance registry so a concurrent DELETE/shutdown Close always has a
	// ticker to stop.
	sub.ticker = p.newSubscriptionTicker(subscriptionFallbackInterval)

	// Register under the server ID first so DELETE/shutdown can hard-close even
	// a subscription whose runtime exited before it attached. A registration
	// refused by a concurrent DELETE/shutdown is hard-closed immediately, so no
	// late subscriber can escape the close.
	if !p.registerServerSub(serverID, sub) {
		sub.Close()
		return sub, nil
	}

	// Register before the replay query so a commit racing subscription setup
	// is either signaled or observed directly; either way it cannot be missed.
	inst := p.lookupLive(serverID)
	if inst != nil {
		sub.inst = inst
		p.registerSubscriber(inst, sub)
	} else {
		sub.terminate()
	}
	return sub, nil
}

// Next returns the next persisted event after the last emitted sequence. It
// queries authoritative SQLite on entry and after every wakeup or fallback
// tick, so a dropped wakeup cannot create a gap or duplicate. When the runtime
// wakeup channel closes it performs one final replay and then returns io.EOF.
func (s *subscription) Next(ctx context.Context) (acpstore.Event, error) {
	for {
		select {
		case <-s.closed:
			return acpstore.Event{}, io.EOF
		case <-ctx.Done():
			return acpstore.Event{}, ctx.Err()
		default:
		}

		events, err := s.query(ctx)
		if err != nil {
			return acpstore.Event{}, err
		}
		if len(events) > 0 {
			if s.hardClosed() {
				return acpstore.Event{}, io.EOF
			}
			s.lastSeq = events[0].Seq
			return events[0], nil
		}

		select {
		case <-ctx.Done():
			return acpstore.Event{}, ctx.Err()
		case <-s.closed:
			return acpstore.Event{}, io.EOF
		case <-s.ticker.C():
		case _, ok := <-s.wake:
			if !ok {
				// Runtime terminated: replay once more and stop, unless a hard
				// close (DELETE/shutdown) raced the exit and takes precedence.
				// Returning here instead of re-selecting a closed channel
				// prevents a permanently ready case from busy-looping SQLite.
				if s.hardClosed() {
					return acpstore.Event{}, io.EOF
				}
				final, err := s.query(ctx)
				if err != nil {
					return acpstore.Event{}, err
				}
				if len(final) > 0 {
					if s.hardClosed() {
						return acpstore.Event{}, io.EOF
					}
					s.lastSeq = final[0].Seq
					return final[0], nil
				}
				return acpstore.Event{}, io.EOF
			}
		}
	}
}

// hardClosed reports whether Close has completed. Once it has, Next never
// replays another event.
func (s *subscription) hardClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// query reads the next persisted event strictly after lastSeq. The store
// returns copied acpstore.Event values; the proxy never caches or reorders
// them.
func (s *subscription) query(ctx context.Context) ([]acpstore.Event, error) {
	return s.proxy.store.Events(ctx, s.serverID, acpstore.EventQuery{
		After: s.lastSeq,
		Limit: 1,
	})
}

// Close stops the subscription and detaches it from its instance.
func (s *subscription) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.ticker != nil {
			s.ticker.Stop()
		}
		if s.inst != nil {
			s.proxy.unregisterSubscriber(s.inst, s)
		}
		s.proxy.unregisterServerSub(s.serverID, s)
	})
}

// watchEvents fans one runtime's coalesced commit wakeups out to every attached
// subscriber. It exits when the runtime closes its wakeup channel, terminating
// each subscriber after its final replay.
func (p *Proxy) watchEvents(inst *instance) {
	wake := inst.runtime.Events()
	for {
		_, ok := <-wake
		if !ok {
			p.terminateSubscribers(inst)
			return
		}
		p.notifySubscribers(inst)
	}
}

// notifySubscribers attempts a non-blocking capacity-one wakeup per subscriber.
func (p *Proxy) notifySubscribers(inst *instance) {
	inst.subMu.Lock()
	subs := make([]*subscription, 0, len(inst.subs))
	for sub := range inst.subs {
		subs = append(subs, sub)
	}
	inst.subMu.Unlock()
	for _, sub := range subs {
		sub.notify()
	}
}

// registerSubscriber attaches sub to inst unless DELETE/shutdown already gated
// the instance, in which case the late registrant is hard-closed. A natural
// exit still registers the subscriber before terminating it, so a racing
// DELETE/shutdown can hard-close it instead of letting its final replay race
// the durable prune.
func (p *Proxy) registerSubscriber(inst *instance, sub *subscription) {
	inst.subMu.Lock()
	subsClosed := inst.subsClosed
	runtimeDown := inst.runtimeDown
	if !subsClosed {
		if inst.subs == nil {
			inst.subs = make(map[*subscription]struct{})
		}
		inst.subs[sub] = struct{}{}
	}
	inst.subMu.Unlock()
	switch {
	case subsClosed:
		sub.Close()
	case runtimeDown:
		sub.terminate()
	}
}

// unregisterSubscriber detaches sub from inst so a closed subscription stops
// receiving wakeups.
func (p *Proxy) unregisterSubscriber(inst *instance, sub *subscription) {
	inst.subMu.Lock()
	delete(inst.subs, sub)
	inst.subMu.Unlock()
}

// terminateSubscribers marks the runtime down and terminates every subscriber
// so each performs one final replay before Next returns io.EOF. Registrations
// stay attached so a later DELETE/shutdown hard close can still take precedence
// over that replay; each subscription's own Close unregisters it.
func (p *Proxy) terminateSubscribers(inst *instance) {
	inst.subMu.Lock()
	inst.runtimeDown = true
	subs := make([]*subscription, 0, len(inst.subs))
	for sub := range inst.subs {
		subs = append(subs, sub)
	}
	inst.subMu.Unlock()
	for _, sub := range subs {
		sub.terminate()
	}
}

// closeSubscriptions is the DELETE/shutdown seam (Task 3.3): it stops every
// subscription attached to the current generation and blocks new ones. Each
// subscription's Close removes its own registration, so a natural-exit
// terminate that raced ahead cannot hide subscribers from this hard close.
func (p *Proxy) closeSubscriptions(inst *instance) {
	inst.subMu.Lock()
	inst.subsClosed = true
	subs := make([]*subscription, 0, len(inst.subs))
	for sub := range inst.subs {
		subs = append(subs, sub)
	}
	inst.subMu.Unlock()
	for _, sub := range subs {
		sub.Close()
	}
}

// registerServerSub records a subscription under its server so DELETE and
// shutdown can hard-close SSE even after the runtime exited and the live
// instance was released.
func (p *Proxy) registerServerSub(serverID string, sub *subscription) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.subRegShutdown {
		return false
	}
	if _, closed := p.subRegClosed[serverID]; closed {
		return false
	}
	set := p.subReg[serverID]
	if set == nil {
		set = make(map[*subscription]struct{})
		p.subReg[serverID] = set
	}
	set[sub] = struct{}{}
	return true
}

// allowServerSubs atomically rejects an active DELETE for serverID and clears
// any stale SSE-closed marker for the freshly published generation, so a
// registration can never slip between the two.
func (p *Proxy) allowServerSubs(serverID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, deleting := p.deleting[serverID]; deleting {
		return false
	}
	delete(p.subRegClosed, serverID)
	return true
}

// unregisterServerSub drops a closed subscription from the registry.
func (p *Proxy) unregisterServerSub(serverID string, sub *subscription) {
	p.mu.Lock()
	if set := p.subReg[serverID]; set != nil {
		delete(set, sub)
		if len(set) == 0 {
			delete(p.subReg, serverID)
		}
	}
	p.mu.Unlock()
}

// closeSubscriptionsByServer hard-closes every subscription registered for a
// server, live or exited.
func (p *Proxy) closeSubscriptionsByServer(serverID string) {
	p.mu.Lock()
	p.subRegClosed[serverID] = struct{}{}
	subs := make([]*subscription, 0, len(p.subReg[serverID]))
	for sub := range p.subReg[serverID] {
		subs = append(subs, sub)
	}
	p.mu.Unlock()
	for _, sub := range subs {
		sub.Close()
	}
}

// notify attempts a coalesced capacity-one wakeup and never blocks. It never
// sends after terminate has closed the channel.
func (s *subscription) notify() {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	if s.wakeClosed {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// terminate closes the wakeup channel exactly once to signal runtime exit.
func (s *subscription) terminate() {
	s.terminateOnce.Do(func() {
		s.wakeMu.Lock()
		s.wakeClosed = true
		close(s.wake)
		s.wakeMu.Unlock()
	})
}

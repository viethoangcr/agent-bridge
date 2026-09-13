package acpproxy

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestSubscriptionUnknownServerCreatesNothing(t *testing.T) {
	p, _, _ := newSubscriptionProxy(t)
	created := false
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker {
		created = true
		return newFakeTicker()
	}

	if _, err := p.Subscribe(t.Context(), "missing", 0); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("Subscribe unknown = %v, want ErrNotFound", err)
	}
	if created {
		t.Fatal("Subscribe created a subscription for an unknown server")
	}
}

func TestSubscriptionExitedServerReplay(t *testing.T) {
	p, store, _ := newSubscriptionProxy(t)
	const id = "exited"
	if _, err := store.CreateServer(t.Context(), id, "alpha"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	seeded := commitEvents(t, store, id, 3)
	if err := store.MarkExited(t.Context(), id); err != nil {
		t.Fatalf("mark exited: %v", err)
	}

	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	requireEvents(t, sub, seeded)
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("after replay = %v, want io.EOF", err)
	}
}

func TestSubscriptionRegistrationWatermarkRace(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "race"
	rt := startServer(t, p, f, id)

	ticker := newFakeTicker()
	var committed []acpstore.Event
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker {
		// Commit after the subscriber registered but before Subscribe returns:
		// the event lands between registration and the first replay query.
		committed = commitEvents(t, store, id, 1)
		return ticker
	}

	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	requireEvents(t, sub, committed)
	// A later commit still arrives in ascending order with no duplicate of the
	// racing sequence.
	more := commitEvents(t, store, id, 1)
	rt.notify()
	requireEvents(t, sub, more)
}

func TestSubscriptionCoalescedWakeupsNoGaps(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "coalesce"
	rt := startServer(t, p, f, id)

	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	const n = 64
	seeded := commitEvents(t, store, id, n)
	// Flood both capacity-one channels without a consumer: the runtime wakeup
	// coalesces and the producer never blocks.
	for i := 0; i < n; i++ {
		rt.notify()
	}
	// SQLite replay must still yield every event once, in order.
	requireEvents(t, sub, seeded)
}

func TestSubscriptionDroppedWakeupFallbackTicker(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "fallback"
	startServer(t, p, f, id)

	ticker := newFakeTicker()
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return ticker }
	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// No wakeup at all: only the bounded fallback tick surfaces the commits.
	seeded := commitEvents(t, store, id, 3)
	ticker.tick()
	requireEvents(t, sub, seeded)
}

func TestSubscriptionNaturalExitFinalReplay(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "natural"
	rt := startServer(t, p, f, id)

	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	first := commitEvents(t, store, id, 1)
	rt.notify()
	requireEvents(t, sub, first)

	// Commit final events then let the agent exit. The runtime closes its
	// wakeup channel; the subscription must replay the final events and then
	// terminate rather than spin on the closed channel.
	final := commitEvents(t, store, id, 2)
	rt.exit(nil)
	requireEvents(t, sub, final)

	for i := 0; i < 3; i++ {
		if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
			t.Fatalf("Next after exit #%d = %v, want io.EOF", i, err)
		}
	}
}

func TestSubscriptionMultipleSubscribersIndependent(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "multi"
	rt := startServer(t, p, f, id)

	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	early, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe early: %v", err)
	}
	defer early.Close()
	late, err := p.Subscribe(t.Context(), id, 2)
	if err != nil {
		t.Fatalf("subscribe late: %v", err)
	}
	defer late.Close()

	seeded := commitEvents(t, store, id, 4)
	rt.notify()

	// early starts at 0 and late starts after 2; they advance independently.
	requireEvents(t, early, seeded[:1])
	requireEvents(t, late, seeded[2:3])
	requireEvents(t, early, seeded[1:2])
	requireEvents(t, late, seeded[3:4])
	requireEvents(t, early, seeded[2:4])
}

func TestSubscriptionCancellationIsolation(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "cancel"
	rt := startServer(t, p, f, id)

	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	kept, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe kept: %v", err)
	}
	defer kept.Close()
	dropped, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe dropped: %v", err)
	}

	// Closing one subscription must not disturb the other.
	dropped.Close()
	if _, err := nextWithin(t, dropped); !errors.Is(err, io.EOF) {
		t.Fatalf("closed subscription = %v, want io.EOF", err)
	}

	seeded := commitEvents(t, store, id, 2)
	rt.notify()
	requireEvents(t, kept, seeded)

	// Request cancellation is isolated to the subscription it is passed to.
	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := kept.Next(cancelCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Next = %v, want context.Canceled", err)
	}
}

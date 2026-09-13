package acpproxy

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestSubscriptionSpawnFailureTerminates(t *testing.T) {
	p, _, f := newSubscriptionProxy(t)
	const id = "spawn-fail-sub"
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }

	var sub Subscription
	f.setOnCreate(func(_ context.Context, _ *acpstore.Store, _ string, _ acpruntime.LaunchSpec) error {
		created, err := p.Subscribe(t.Context(), id, 0)
		if err != nil {
			return err
		}
		sub = created
		return errBoom
	})

	agent := "alpha"
	if _, err := p.Post(t.Context(), id, &agent, "initialize", initPayload); !errors.Is(err, errBoom) {
		t.Fatalf("post = %v, want errBoom", err)
	}
	if sub == nil {
		t.Fatal("subscription was not created")
	}
	defer sub.Close()
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after spawn failure = %v, want io.EOF", err)
	}
}

func TestSubscriptionCloseSubscriptionsSeam(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "closed"
	startServer(t, p, f, id)

	ticker := newFakeTicker()
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return ticker }
	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	commitEvents(t, store, id, 1)
	inst := requireInstance(t, p, id)
	p.closeSubscriptions(inst)

	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after closeSubscriptions = %v, want io.EOF", err)
	}
	if !ticker.isStopped() {
		t.Fatal("closeSubscriptions did not stop the fallback ticker")
	}
}

// TestSubscriptionHardCloseBeatsNaturalExitReplay proves DELETE closure takes
// precedence over the runtime's natural-exit final replay: a subscription hard
// closed by a real DELETE returns io.EOF without replaying the remaining
// persisted events.
func TestSubscriptionHardCloseBeatsNaturalExitReplay(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "hard-close"
	startServer(t, p, f, id)

	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	commitEvents(t, store, id, 2)

	// DELETE closes SSE, kills the runtime, and prunes the durable row; the
	// subscription must see the hard close instead of the pending replay.
	if err := p.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
			t.Fatalf("Next after DELETE #%d = %v, want io.EOF without replay", i, err)
		}
	}
}

// TestSubscriptionCloseDuringRegistrationStopsTicker proves the subscription
// owns its ticker before it is published to the instance registry, so a
// DELETE/shutdown Close racing registration always stops the ticker exactly
// once.
func TestSubscriptionCloseDuringRegistrationStopsTicker(t *testing.T) {
	p, _, f := newSubscriptionProxy(t)
	const id = "registration-close"
	startServer(t, p, f, id)

	ticker := newFakeTicker()
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker {
		// Simulate DELETE/shutdown arriving while Subscribe is still wiring the
		// subscription: every close from here on must find a ticker to stop.
		if inst := liveInstance(p, id); inst != nil {
			p.closeSubscriptions(inst)
		}
		return ticker
	}

	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	sub.Close()
	sub.Close()
	if got := ticker.stopCount(); got != 1 {
		t.Fatalf("ticker stops = %d, want exactly 1 (Close before ticker creation leaks it)", got)
	}
}

// TestProxyDeleteAfterNaturalExitClosesSubscriptions proves a real DELETE
// still hard-closes SSE after the runtime exited: registered subscriptions
// stay reachable by server ID, so Next returns io.EOF instead of replaying.
func TestProxyDeleteAfterNaturalExitClosesSubscriptions(t *testing.T) {
	p, store, f := newSubscriptionProxy(t)
	const id = "delete-after-exit"
	rt := startServer(t, p, f, id)

	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), id, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	inst := requireInstance(t, p, id)
	commitEvents(t, store, id, 2)

	watched := inst.watched
	rt.exit(nil)
	awaitSignal(t, watched, "natural-exit cleanup")

	// The subscription is in its final-replay window; a real DELETE must win.
	if err := p.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
			t.Fatalf("Next after DELETE #%d = %v, want io.EOF without replay", i, err)
		}
	}
}

// TestSubscribeRacingDeleteIsHardClosed proves a subscription whose existence
// check passed before a DELETE cannot register after the hard close: the late
// registration is refused and returned already closed.
func TestSubscribeRacingDeleteIsHardClosed(t *testing.T) {
	p, _, f := newSubscriptionProxy(t)
	const id = "subscribe-race"
	startServer(t, p, f, id)

	entered := make(chan struct{})
	release := make(chan struct{})
	realServer := p.storeServer
	p.storeServer = func(ctx context.Context, serverID string) (acpstore.Server, error) {
		server, err := realServer(ctx, serverID)
		close(entered)
		<-release
		return server, err
	}

	subCh := make(chan Subscription, 1)
	errCh := make(chan error, 1)
	go func() {
		sub, err := p.Subscribe(t.Context(), id, 0)
		if err != nil {
			errCh <- err
			return
		}
		subCh <- sub
	}()

	<-entered
	if err := p.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	close(release)

	select {
	case err := <-errCh:
		t.Fatalf("subscribe after racing DELETE: %v", err)
	case sub := <-subCh:
		defer sub.Close()
		if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
			t.Fatalf("Next = %v, want io.EOF for a refused late registration", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscribe did not return")
	}
}

// TestSubscribeAfterShutdownReturnsErrClosed proves shutdown refuses new SSE
// subscriptions instead of registering one that shutdown would never close.
func TestSubscribeAfterShutdownReturnsErrClosed(t *testing.T) {
	p, _, f := newSubscriptionProxy(t)
	const id = "shutdown-sub"
	startServer(t, p, f, id)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, err := p.Subscribe(t.Context(), id, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after shutdown = %v, want ErrClosed", err)
	}
}

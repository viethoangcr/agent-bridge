package acpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// fakeTicker is a manually advanced fallback ticker. Tests advance it without
// sleeping.
type fakeTicker struct {
	ch chan time.Time

	mu    sync.Mutex
	stops int
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time, 1)}
}

func (f *fakeTicker) C() <-chan time.Time { return f.ch }

func (f *fakeTicker) Stop() {
	f.mu.Lock()
	f.stops++
	f.mu.Unlock()
}

func (f *fakeTicker) tick() {
	select {
	case f.ch <- time.Now():
	default:
	}
}

func (f *fakeTicker) isStopped() bool { return f.stopCount() > 0 }

func (f *fakeTicker) stopCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stops
}

func newSubscriptionProxy(t *testing.T) (*Proxy, *acpstore.Store, *testFactory) {
	t.Helper()
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	return p, store, f
}

func startServer(t *testing.T, p *Proxy, f *testFactory, id string) *fakeRuntime {
	t.Helper()
	agent := "alpha"
	if _, err := p.Post(t.Context(), id, &agent, "initialize", initPayload); err != nil {
		t.Fatalf("start server %q: %v", id, err)
	}
	return requireRuntime(t, f)
}

// commitEvents appends n events directly to SQLite, proving the subscription's
// source of truth is the store rather than any wakeup payload.
func commitEvents(t *testing.T, store *acpstore.Store, serverID string, n int) []acpstore.Event {
	t.Helper()
	events := make([]acpstore.Event, 0, n)
	for i := 0; i < n; i++ {
		payload := json.RawMessage(fmt.Sprintf(`{"n":%d}`, i+1))
		event, err := store.AppendOutput(t.Context(), serverID, acpstore.Output{
			Kind:    "notification",
			Payload: payload,
		})
		if err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
		events = append(events, event)
	}
	return events
}

// nextWithin reads one event under a bounded guard so a broken subscription
// fails instead of hanging the suite.
func nextWithin(t *testing.T, sub Subscription) (acpstore.Event, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	return sub.Next(ctx)
}

func requireEvents(t *testing.T, sub Subscription, want []acpstore.Event) {
	t.Helper()
	for i, expected := range want {
		event, err := nextWithin(t, sub)
		if err != nil {
			t.Fatalf("event %d (seq %d): %v", i, expected.Seq, err)
		}
		if event.Seq != expected.Seq {
			t.Fatalf("event %d seq = %d, want %d", i, event.Seq, expected.Seq)
		}
		if !bytes.Equal(event.Payload, expected.Payload) {
			t.Fatalf("event %d payload = %s, want %s", i, event.Payload, expected.Payload)
		}
	}
}

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

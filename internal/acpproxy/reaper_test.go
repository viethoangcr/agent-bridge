package acpproxy

import (
	"context"
	"errors"
	"io"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// waitForNextMillisecond spins until the wall clock leaves ms so a following
// store write is guaranteed to record a strictly later idle timestamp.
func waitForNextMillisecond(t *testing.T, ms int64) {
	t.Helper()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().UnixMilli() <= ms {
		if time.Now().After(deadline) {
			t.Fatal("wall clock did not advance a millisecond")
		}
		goruntime.Gosched()
	}
}

// waitFor polls a condition with a bounded deadline. It synchronizes tests on
// asynchronous reaper state that exposes no channel.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

// ptr returns a pointer to value for nullable store fields.
func ptr[T any](value T) *T { return &value }

// TestReaperDisabledWhenIdleTTLZero asserts a zero idle TTL disables reaping
// entirely: StartReaper creates no ticker and a sweep kills nothing.
func TestReaperDisabledWhenIdleTTLZero(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyWithTTL(t, f, 0)
	startServer(t, p, f, "disabled")
	rt := requireRuntime(t, f)

	called := false
	p.newReaperTicker = func(time.Duration) reaperTicker {
		called = true
		return newFakeReaperTicker()
	}
	p.StartReaper()
	if called {
		t.Fatal("StartReaper created a ticker with a zero idle TTL")
	}

	fixedNow(p, time.Now().Add(24*time.Hour))
	p.reapOnce(t.Context())
	if got := rt.kills(); got != 0 {
		t.Fatalf("kills = %d, want 0 with reaping disabled", got)
	}
	if liveInstance(p, "disabled") == nil {
		t.Fatal("instance was reaped with a zero idle TTL")
	}
}

// TestReaperReapsIdleAtDeadline asserts only a current idle instance at or past
// its durable deadline is killed, its instance removed, its row left exited,
// and its subscriptions closed while events and sessions survive.
func TestReaperReapsIdleAtDeadline(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	rt := startServer(t, p, f, "idle-deadline")
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }

	sub, err := p.Subscribe(t.Context(), "idle-deadline", 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	if _, err := store.AppendOutput(t.Context(), "idle-deadline", acpstore.Output{
		Kind:    "notification",
		Payload: []byte(`{"n":1}`),
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if _, err := store.AppendOutput(t.Context(), "idle-deadline", acpstore.Output{
		Kind:      "notification",
		Payload:   []byte(`{"n":2}`),
		SessionID: ptr("session-a"),
		Mutation:  &acpstore.SessionMutation{Lifecycle: "new", SessionID: "session-a", CWD: "/tmp"},
	}); err != nil {
		t.Fatalf("append session event: %v", err)
	}

	fixedNow(p, idleSince(t, store, "idle-deadline").Add(time.Minute))
	p.reapOnce(t.Context())

	waitFor(t, func() bool { return liveInstance(p, "idle-deadline") == nil })
	if got := rt.kills(); got != 1 {
		t.Fatalf("kills = %d, want 1", got)
	}
	server, err := store.Server(t.Context(), "idle-deadline")
	if err != nil {
		t.Fatalf("server after reap: %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Fatalf("status = %q, want exited", server.Status)
	}
	events, err := store.Events(t.Context(), "idle-deadline", acpstore.EventQuery{})
	if err != nil {
		t.Fatalf("events after reap: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2 preserved", len(events))
	}
	if _, err := store.Session(t.Context(), "idle-deadline", "session-a"); err != nil {
		t.Fatalf("session after reap: %v", err)
	}
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription after reap = %v, want io.EOF", err)
	}

	// Only initialize may recreate the exited server.
	if _, err := p.Post(t.Context(), "idle-deadline", nil, "session/load", initPayload); !errors.Is(err, ErrReinitialize) {
		t.Fatalf("non-initialize after reap = %v, want ErrReinitialize", err)
	}
	agent := "alpha"
	if _, err := p.Post(t.Context(), "idle-deadline", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("initialize recreation after reap: %v", err)
	}
}

// TestReaperSkipsBeforeDeadline asserts an idle instance whose deadline has not
// yet arrived is left untouched.
func TestReaperSkipsBeforeDeadline(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	rt := startServer(t, p, f, "not-yet")
	fixedNow(p, idleSince(t, store, "not-yet").Add(time.Minute-time.Millisecond))

	p.reapOnce(t.Context())
	if got := rt.kills(); got != 0 {
		t.Fatalf("kills = %d, want 0 before the deadline", got)
	}
	if liveInstance(p, "not-yet") == nil {
		t.Fatal("instance reaped before its deadline")
	}
}

// TestReaperSkipsNonIdleStates asserts creating, busy, and exited durable rows
// are never reaped regardless of elapsed time.
func TestReaperSkipsNonIdleStates(t *testing.T) {
	for _, status := range []acpstore.Status{acpstore.StatusCreating, acpstore.StatusBusy, acpstore.StatusExited} {
		t.Run(string(status), func(t *testing.T) {
			f := newTestFactory(t)
			f.setOnCreate(liveOnCreate)
			p, store := newProxyWithTTL(t, f, time.Minute)
			rt := startServer(t, p, f, "state")
			if err := store.SetStatus(t.Context(), "state", status); err != nil {
				t.Fatalf("set status %q: %v", status, err)
			}

			fixedNow(p, time.Now().Add(24*time.Hour))
			p.reapOnce(t.Context())
			if got := rt.kills(); got != 0 {
				t.Fatalf("kills = %d, want 0 for %q", got, status)
			}
			if liveInstance(p, "state") == nil {
				t.Fatalf("instance with %q status was reaped", status)
			}
		})
	}
}

// TestReaperSkipsRefreshedIdle asserts a refreshed idle_since_ms pushes the
// deadline forward so an instance is not reaped at the old deadline.
func TestReaperSkipsRefreshedIdle(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	rt := startServer(t, p, f, "refreshed")
	original := idleSince(t, store, "refreshed")

	waitForNextMillisecond(t, original.UnixMilli())
	if err := store.SetStatus(t.Context(), "refreshed", acpstore.StatusIdle); err != nil {
		t.Fatalf("refresh idle: %v", err)
	}
	fixedNow(p, original.Add(time.Minute))
	p.reapOnce(t.Context())

	if got := rt.kills(); got != 0 {
		t.Fatalf("kills = %d, want 0 for a refreshed idle transition", got)
	}
	if liveInstance(p, "refreshed") == nil {
		t.Fatal("refreshed idle instance was reaped at the old deadline")
	}
}

// TestReaperBusyCorrelationUnreapable proves a durable busy row stays
// unreapable even after the HTTP waiter returned and released its lease: busy
// remains runtime-owned until grace/commit work completes.
func TestReaperBusyCorrelationUnreapable(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	startServer(t, p, f, "busy-grace")
	rt := requireRuntime(t, f)

	// Runtime-owned busy with a grace-retained correlation: the Post waiter
	// returns and the lease drops, but the row stays busy.
	rt.setHook(func() {
		if err := store.SetStatus(t.Context(), "busy-grace", acpstore.StatusBusy); err != nil {
			t.Errorf("set busy: %v", err)
		}
	})
	if _, err := p.Post(t.Context(), "busy-grace", nil, "session/prompt", initPayload); err != nil {
		t.Fatalf("post: %v", err)
	}
	inst := requireInstance(t, p, "busy-grace")
	inst.lock.mu.Lock()
	activity := inst.activity
	inst.lock.mu.Unlock()
	if activity != 0 {
		t.Fatalf("activity = %d, want 0 after the waiter returned", activity)
	}

	fixedNow(p, time.Now().Add(24*time.Hour))
	p.reapOnce(t.Context())
	if got := rt.kills(); got != 0 {
		t.Fatalf("kills = %d, want 0 while a correlation is retained", got)
	}
	if liveInstance(p, "busy-grace") == nil {
		t.Fatal("busy instance was reaped")
	}
}

// TestReaperRefreshedStateClearsGate asserts that if the durable status becomes
// non-idle while the reaper is draining activity, the reaper clears its gate
// without killing.
func TestReaperRefreshedStateClearsGate(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	startServer(t, p, f, "race")
	inst := requireInstance(t, p, "race")
	rt := requireRuntime(t, f)

	entered := make(chan struct{})
	release := make(chan struct{})
	rt.setHook(func() {
		close(entered)
		<-release
	})

	postDone := make(chan struct{})
	go func() {
		defer close(postDone)
		_, _ = p.Post(t.Context(), "race", nil, "session/prompt", initPayload)
	}()
	<-entered

	fixedNow(p, idleSince(t, store, "race").Add(time.Minute))
	reaped := make(chan struct{})
	go func() {
		p.reapOnce(t.Context())
		close(reaped)
	}()

	waitFor(t, func() bool {
		inst.lock.mu.Lock()
		defer inst.lock.mu.Unlock()
		return inst.terminating
	})
	// The runtime reconciles to busy during the drain; the reaper must abandon
	// the kill and clear the gate.
	if err := store.SetStatus(t.Context(), "race", acpstore.StatusBusy); err != nil {
		t.Fatalf("set busy: %v", err)
	}
	close(release)
	<-postDone
	<-reaped

	if got := rt.kills(); got != 0 {
		t.Fatalf("kills = %d, want 0 after the state changed", got)
	}
	inst.lock.mu.Lock()
	terminating := inst.terminating
	inst.lock.mu.Unlock()
	if terminating {
		t.Fatal("reaper gate was not cleared after the state changed")
	}
}

// TestReaperKillOnlyCurrentGeneration asserts a stale generation decision never
// kills the replacement runtime.
func TestReaperKillOnlyCurrentGeneration(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyWithTTL(t, f, time.Minute)
	rtOld := startServer(t, p, f, "generation")
	old := requireInstance(t, p, "generation")

	replacement := newFakeRuntime()
	t.Cleanup(func() { replacement.exit(nil) })
	next := p.newInstance("generation", "alpha", old.lock)
	next.runtime = replacement
	p.mu.Lock()
	p.live["generation"] = next
	p.mu.Unlock()

	p.reapInstance(t.Context(), old, time.Now().Add(24*time.Hour))
	if got := rtOld.kills(); got != 0 {
		t.Fatalf("stale generation killed its runtime %d times", got)
	}
	if liveInstance(p, "generation") != next {
		t.Fatal("reaper removed the replacement generation")
	}
}

// TestReaperStatusChecksRunWithoutLifecycleLock proves both durable status
// reads (before the gate drain and after it) execute without holding the
// lifecycle lock: while each read is blocked in the store seam, TryLock on the
// instance's lifecycle lock succeeds.
func TestReaperStatusChecksRunWithoutLifecycleLock(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyWithTTL(t, f, time.Minute)
	rt := startServer(t, p, f, "reap-lock")
	inst := requireInstance(t, p, "reap-lock")

	realServer := p.storeServer
	firstEntered := make(chan struct{})
	firstRelease := make(chan struct{})
	secondEntered := make(chan struct{})
	secondRelease := make(chan struct{})
	var calls atomic.Int32
	p.storeServer = func(ctx context.Context, serverID string) (acpstore.Server, error) {
		switch calls.Add(1) {
		case 1:
			close(firstEntered)
			<-firstRelease
		case 2:
			close(secondEntered)
			<-secondRelease
		}
		return realServer(ctx, serverID)
	}

	fixedNow(p, time.Now().Add(24*time.Hour))
	reaped := make(chan struct{})
	go func() {
		p.reapOnce(t.Context())
		close(reaped)
	}()

	awaitSignal(t, firstEntered, "initial durable status read")
	if inst.lock.mu.TryLock() {
		inst.lock.mu.Unlock()
	} else {
		t.Fatal("lifecycle lock held during the initial durable status read")
	}
	close(firstRelease)

	awaitSignal(t, secondEntered, "post-drain durable status read")
	if inst.lock.mu.TryLock() {
		inst.lock.mu.Unlock()
	} else {
		t.Fatal("lifecycle lock held during the post-drain durable status read")
	}
	close(secondRelease)

	awaitSignal(t, reaped, "reaper sweep")
	if got := rt.kills(); got != 1 {
		t.Fatalf("kills = %d, want 1", got)
	}
	if liveInstance(p, "reap-lock") != nil {
		t.Fatal("instance survived the reaper")
	}
}

// TestReaperStartWithFakeTicker asserts StartReaper sweeps on its injected
// ticker and Shutdown stops and joins it.
func TestReaperStartWithFakeTicker(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	rt := startServer(t, p, f, "tick")
	ticker := newFakeReaperTicker()
	p.newReaperTicker = func(time.Duration) reaperTicker { return ticker }

	fixedNow(p, idleSince(t, store, "tick").Add(time.Minute))
	p.StartReaper()
	ticker.tick()
	waitFor(t, func() bool { return liveInstance(p, "tick") == nil })
	if got := rt.kills(); got != 1 {
		t.Fatalf("kills = %d, want 1", got)
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if !ticker.isStopped() {
		t.Fatal("Shutdown did not stop the reaper ticker")
	}
}

// TestReaperDoesNotGateBusyInstance proves a reaper pass confirms the durable
// status BEFORE gating, so a busy instance never rejects concurrent POSTs with
// a transient terminating conflict.
func TestReaperDoesNotGateBusyInstance(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	startServer(t, p, f, "busy-gate")
	inst := requireInstance(t, p, "busy-gate")
	if err := store.SetStatus(t.Context(), "busy-gate", acpstore.StatusBusy); err != nil {
		t.Fatalf("set busy: %v", err)
	}
	fixedNow(p, time.Now().Add(2*time.Minute))

	entered := make(chan struct{})
	release := make(chan struct{})
	realServer := p.storeServer
	p.storeServer = func(ctx context.Context, serverID string) (acpstore.Server, error) {
		close(entered)
		<-release
		return realServer(ctx, serverID)
	}

	done := make(chan struct{})
	go func() {
		p.reapOnce(t.Context())
		close(done)
	}()
	<-entered

	inst.lock.mu.Lock()
	gated := inst.terminating
	inst.lock.mu.Unlock()
	if gated {
		t.Fatal("reaper gated a busy instance before confirming its durable status")
	}
	if _, err := p.Post(t.Context(), "busy-gate", nil, "session/prompt", initPayload); err != nil {
		t.Fatalf("post during reaper pass = %v, want success", err)
	}

	close(release)
	<-done
	if got := requireRuntime(t, f).kills(); got != 0 {
		t.Fatalf("kills = %d, want 0 for a busy row", got)
	}
}

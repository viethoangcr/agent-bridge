package acpproxy

import (
	"errors"
	"io"
	"testing"
	"time"
)

// TestReaperKillFailureRetainsInstanceForRetry proves a failed kill does not
// drop ownership: the instance stays live and reap-eligible (capacity stays
// consumed), subscriptions stay closed, and a later sweep retries the kill.
func TestReaperKillFailureRetainsInstanceForRetry(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	rt := startServer(t, p, f, "reap-retry")
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), "reap-retry", 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	rt.setKillErr(errBoom)
	fixedNow(p, idleSince(t, store, "reap-retry").Add(time.Minute))
	p.reapOnce(t.Context())

	if got := rt.kills(); got != 1 {
		t.Fatalf("kills = %d, want 1", got)
	}
	inst := liveInstance(p, "reap-retry")
	if inst == nil {
		t.Fatal("instance dropped after a failed kill")
	}
	if got := liveCount(p); got != 1 {
		t.Fatalf("live = %d, want 1 (capacity stays consumed)", got)
	}
	inst.lock.mu.Lock()
	terminating := inst.terminating
	inst.lock.mu.Unlock()
	if terminating {
		t.Fatal("terminating gate not cleared; reap is not retryable")
	}
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription after failed kill = %v, want io.EOF", err)
	}

	rt.setKillErr(nil)
	p.reapOnce(t.Context())
	waitFor(t, func() bool { return liveInstance(p, "reap-retry") == nil })
	if got := rt.kills(); got != 2 {
		t.Fatalf("kills after retry = %d, want 2", got)
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live after retry = %d, want 0", got)
	}
}

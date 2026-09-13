package acpproxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestAbandonCreateRetainedRuntimeReapable proves a spawn that lost its
// publication race and whose teardown kill failed is still retryable by the
// idle reaper, even when no DELETE or shutdown ever reclaims it: the next gated
// sweep kills the retained runtime and releases exactly one capacity slot. It
// also proves the retained generation is not replaceable while its runtime is
// alive.
func TestAbandonCreateRetainedRuntimeReapable(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyWithTTL(t, f, time.Minute)
	id := "retained-reap"
	agent := "alpha"

	// Every teardown kill fails so abandonCreate retains the fresh runtime.
	inner := p.factory
	p.factory = func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, timeout time.Duration, log *slog.Logger) (runtime, error) {
		rt, err := inner(ctx, store, serverID, spec, timeout, log)
		if err == nil {
			rt.(*fakeRuntime).setKillErr(errBoom)
		}
		return rt, err
	}
	// Losing the publication race to a delete mark forces the rollback path
	// without a DELETE goroutine that would retry the kill itself.
	p.beforeAllowServerSubs = func() { p.markDeleting(id) }

	if _, err := p.Post(t.Context(), id, &agent, "initialize", initPayload); !errors.Is(err, ErrDeleting) {
		t.Fatalf("Post = %v, want ErrDeleting", err)
	}
	if inst := liveInstance(p, id); inst == nil {
		t.Fatal("failed kill orphaned the spawned runtime")
	}
	rt := requireRuntime(t, f)

	// The delete mark is cleared as a completed DELETE would, leaving only the
	// retained runtime to gate the server.
	p.clearDeleting(id)
	if _, err := p.Post(t.Context(), id, &agent, "initialize", initPayload); !errors.Is(err, ErrDeleting) {
		t.Fatalf("Post on retained generation = %v, want ErrDeleting", err)
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns = %d, want 1 (retained generation must not be replaced)", got)
	}

	// The runtime is confirmed gone on the next retry; no DELETE or shutdown.
	rt.setKillErr(nil)
	fixedNow(p, idleSince(t, store, id).Add(time.Minute))
	p.reapOnce(t.Context())
	waitFor(t, func() bool { return liveInstance(p, id) == nil })
	if got := rt.kills(); got != 2 {
		t.Fatalf("kills = %d, want 2 (abandon plus reaper retry)", got)
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live after reaper retry = %d, want 0", got)
	}
}

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

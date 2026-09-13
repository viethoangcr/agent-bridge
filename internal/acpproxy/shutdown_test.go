package acpproxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestProxyShutdownIdempotentUnblocksPosts proves Shutdown signals active
// runtimes before waiting leases, never re-runs, preserves durable rows, and
// blocks later work.
func TestProxyShutdownIdempotentUnblocksPosts(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	startServer(t, p, f, "shutdown")
	rt := requireRuntime(t, f)

	entered := make(chan struct{})
	release := make(chan struct{})
	rt.setHook(func() {
		close(entered)
		<-release
	})
	rt.setKillHook(func() { close(release) })

	go func() {
		_, _ = p.Post(t.Context(), "shutdown", nil, "session/prompt", initPayload)
	}()
	<-entered

	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if got := rt.kills(); got != 1 {
		t.Fatalf("kills = %d, want 1 (idempotent)", got)
	}
	if _, err := store.Server(t.Context(), "shutdown"); err != nil {
		t.Fatalf("durable row not preserved: %v", err)
	}
	if liveCount(p) != 0 {
		t.Fatalf("live instances after shutdown = %d, want 0", liveCount(p))
	}
	agent := "alpha"
	if _, err := p.Post(t.Context(), "later", &agent, "initialize", initPayload); !errors.Is(err, ErrClosed) {
		t.Fatalf("post after shutdown = %v, want ErrClosed", err)
	}
}

// TestProxyShutdownSignalsRuntimesConcurrently proves shutdown starts every
// runtime's Kill before waiting out a wedged one: both live runtimes must enter
// Kill while neither has completed, so one blocked Kill cannot starve the rest.
func TestProxyShutdownSignalsRuntimesConcurrently(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	for _, id := range []string{"shutdown-a", "shutdown-b"} {
		if _, err := p.Post(ctx, id, &agent, "initialize", initPayload); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	f.mu.Lock()
	runtimes := append([]*fakeRuntime(nil), f.runtimes...)
	f.mu.Unlock()
	if len(runtimes) != 2 {
		t.Fatalf("spawns = %d, want 2", len(runtimes))
	}

	started := make(chan struct{}, len(runtimes))
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	for _, rt := range runtimes {
		rt.setKillHook(func() {
			started <- struct{}{}
			<-release
		})
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown(ctx) }()

	// Both Kills must begin while the first is still blocked.
	for i := range runtimes {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d Kill calls started while the first was blocked", i, len(runtimes))
		}
	}
	releaseAll()
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for i, rt := range runtimes {
		if got := rt.kills(); got != 1 {
			t.Fatalf("runtime %d kills = %d, want 1", i, got)
		}
	}
}

// TestProxyShutdownClosesSubscriptions proves shutdown stops streaming.
func TestProxyShutdownClosesSubscriptions(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	startServer(t, p, f, "shutdown-sub")
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), "shutdown-sub", 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription after shutdown = %v, want io.EOF", err)
	}
}

// TestProxyShutdownNeverSignalsDbOnlyPid proves shutdown leaves persisted-state
// PIDs alone: a durable row with a PID but no live runtime spawns and signals
// nothing.
func TestProxyShutdownNeverSignalsDbOnlyPid(t *testing.T) {
	f := newTestFactory(t)
	p, store := newProxyForTest(t, f)
	if _, err := store.CreateServer(t.Context(), "db-only", "alpha"); err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := store.SetLive(t.Context(), "db-only", 9999); err != nil {
		t.Fatalf("set live: %v", err)
	}

	if err := p.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := f.spawnCount(); got != 0 {
		t.Fatalf("spawns = %d, want 0 for a DB-only PID", got)
	}
	server, err := store.Server(t.Context(), "db-only")
	if err != nil {
		t.Fatalf("server after shutdown: %v", err)
	}
	if server.PID == nil || *server.PID != 9999 {
		t.Fatalf("durable PID = %v, want 9999 preserved", server.PID)
	}
}

// TestProxyShutdownTearsDownCreatingPlaceholder proves shutdown racing a
// mid-I/O creation leaves no live instance and kills exactly the fresh runtime,
// whether it wins the gate (rollback) or observes the published instance.
func TestProxyShutdownTearsDownCreatingPlaceholder(t *testing.T) {
	f := newTestFactory(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	f.setOnCreate(func(_ context.Context, _ *acpstore.Store, _ string, _ acpruntime.LaunchSpec) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	})
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	id := "shutdown-creating"
	agent := "alpha"

	created := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &agent, "initialize", initPayload)
		created <- err
	}()
	awaitSignal(t, entered, "creation start")

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown(ctx) }()
	close(release)

	if err := <-created; err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrDeleting) {
		t.Fatalf("creation during shutdown = %v, want success, ErrClosed, or ErrDeleting", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := requireRuntime(t, f).kills(); got != 1 {
		t.Fatalf("kills = %d, want 1", got)
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live after shutdown = %d, want 0", got)
	}
}

// TestProxyShutdownAbortsInFlightSpawn proves shutdown cancels a creation's
// spawn context so no child can be spawned after shutdown began, even while
// the creation is inside its detached factory I/O.
func TestProxyShutdownAbortsInFlightSpawn(t *testing.T) {
	f := newTestFactory(t)
	entered := make(chan struct{})
	f.setOnCreate(func(ctx context.Context, _ *acpstore.Store, _ string, _ acpruntime.LaunchSpec) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	p, _ := newProxyForTest(t, f)

	agent := "alpha"
	postErr := make(chan error, 1)
	go func() {
		_, err := p.Post(t.Context(), "spawn-abort", &agent, "initialize", initPayload)
		postErr <- err
	}()
	<-entered

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-postErr:
		if err == nil {
			t.Fatal("Post succeeded after shutdown aborted its spawn")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Post did not return after the spawn abort")
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
}

// TestShutdownLatchWinsOverSpawnRegistration proves that once shutdown latches,
// a create that already passed its first closed check still cannot register a
// spawn context or reach the factory.
func TestShutdownLatchWinsOverSpawnRegistration(t *testing.T) {
	f := newTestFactory(t)
	registerGate := make(chan struct{})
	releaseRegister := make(chan struct{})
	p, _ := newProxyForTest(t, f)
	p.beforeSpawnRegister = func() {
		close(registerGate)
		<-releaseRegister
	}

	agent := "alpha"
	postErr := make(chan error, 1)
	go func() {
		_, err := p.Post(t.Context(), "spawn-latch", &agent, "initialize", initPayload)
		postErr <- err
	}()
	<-registerGate

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- p.Shutdown(t.Context()) }()
	waitFor(t, p.closed.Load)
	close(releaseRegister)

	if err := <-postErr; !errors.Is(err, ErrClosed) {
		t.Fatalf("Post = %v, want ErrClosed", err)
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if got := f.spawnCount(); got != 0 {
		t.Fatalf("factory calls = %d, want 0 after shutdown latched", got)
	}
}

// TestProxyConfirmWaitsForCreatingPlaceholder proves post-drain confirmation
// does not return while a gated creation is still inside its unlocked I/O, so
// SQLite cannot be closed under an active creation.
func TestProxyConfirmWaitsForCreatingPlaceholder(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)

	entered := make(chan struct{})
	release := make(chan struct{})
	realServer := p.storeServer
	p.storeServer = func(ctx context.Context, serverID string) (acpstore.Server, error) {
		close(entered)
		<-release
		return realServer(ctx, serverID)
	}

	agent := "alpha"
	go func() {
		_, _ = p.Post(t.Context(), "confirm-create", &agent, "initialize", initPayload)
	}()
	<-entered
	p.closed.Store(true)

	done := make(chan error, 1)
	go func() { done <- p.Confirm(t.Context()) }()

	select {
	case err := <-done:
		t.Fatalf("Confirm returned while creation was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Confirm = %v, want nil after the creation abandoned", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Confirm did not return after the creation finished")
	}
}

// TestProxyShutdownRetriesRetainedSpawn proves Shutdown does not return while a
// spawn whose rollback kill failed is still alive: it retries the kill, reports
// the failure, and leaves the runtime tracked rather than orphaning it.
func TestProxyShutdownRetriesRetainedSpawn(t *testing.T) {
	f := newTestFactory(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	f.setOnCreate(func(_ context.Context, _ *acpstore.Store, _ string, _ acpruntime.LaunchSpec) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	})
	p, _ := newProxyForTest(t, f)

	// Force every kill to fail so the spawn rollback retains the runtime and
	// Shutdown must retry terminating it.
	inner := p.factory
	p.factory = func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, timeout time.Duration, log *slog.Logger) (runtime, error) {
		rt, err := inner(ctx, store, serverID, spec, timeout, log)
		if err == nil {
			rt.(*fakeRuntime).setKillErr(errBoom)
		}
		return rt, err
	}

	agent := "alpha"
	created := make(chan error, 1)
	go func() {
		_, err := p.Post(t.Context(), "shutdown-retain", &agent, "initialize", initPayload)
		created <- err
	}()
	awaitSignal(t, entered, "creation start")

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown(t.Context()) }()
	// Wait until Shutdown has latched closed and gated the in-flight creation,
	// then release the spawn so its rollback loses the race deterministically.
	waitFor(t, p.closed.Load)
	waitFor(t, func() bool {
		inst := liveInstance(p, "shutdown-retain")
		if inst == nil {
			return false
		}
		inst.lock.mu.Lock()
		defer inst.lock.mu.Unlock()
		return inst.terminating
	})
	close(release)

	if err := <-created; err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrDeleting) {
		t.Fatalf("creation during shutdown = %v, want success, ErrClosed, or ErrDeleting", err)
	}
	if err := <-shutdownDone; !errors.Is(err, errBoom) {
		t.Fatalf("Shutdown = %v, want errBoom from the retained kill", err)
	}
	rt := requireRuntime(t, f)
	if got := rt.kills(); got < 2 {
		t.Fatalf("kills = %d, want >= 2 (abandon plus shutdown retry)", got)
	}
	if liveInstance(p, "shutdown-retain") == nil {
		t.Fatal("Shutdown orphaned the retained spawn")
	}
}

package acpproxy

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestProxyWaiterRechecksAgentAfterCreationPublishes proves a waiter for a
// different agent than the in-flight creation is not dispatched to the
// published instance: it must observe ErrAgentConflict, exactly as if it had
// arrived after creation completed.
func TestProxyWaiterRechecksAgentAfterCreationPublishes(t *testing.T) {
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
	id := "waiter-agent-conflict"
	alpha, beta := "alpha", "beta"

	creatorDone := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &alpha, "initialize", initPayload)
		creatorDone <- err
	}()
	awaitSignal(t, entered, "creation start")

	waiterArrived := make(chan struct{}, 1)
	p.beforeCreateWait = func() { waiterArrived <- struct{}{} }
	waiterDone := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &beta, "initialize", initPayload)
		waiterDone <- err
	}()
	awaitSignal(t, waiterArrived, "conflicting waiter arrival")

	close(release)
	if err := <-creatorDone; err != nil {
		t.Fatalf("creator: %v", err)
	}
	if err := <-waiterDone; !errors.Is(err, ErrAgentConflict) {
		t.Fatalf("waiter error = %v, want ErrAgentConflict", err)
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns = %d, want 1 (waiter must not spawn its own runtime)", got)
	}
}

// TestProxySpawnFailureRollsBackPlaceholder proves a failed spawn removes the
// placeholder, releases its capacity slot, records the error for waiters, and
// leaves no live instance or duplicate spawn behind.
func TestProxySpawnFailureRollsBackPlaceholder(t *testing.T) {
	f := newTestFactory(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	f.setOnCreate(func(ctx context.Context, store *acpstore.Store, serverID string, _ acpruntime.LaunchSpec) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		if err := store.MarkExited(ctx, serverID); err != nil {
			return err
		}
		return errBoom
	})
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	id := "spawn-fail-waiters"
	agent := "alpha"

	firstDone := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &agent, "initialize", initPayload)
		firstDone <- err
	}()
	awaitSignal(t, entered, "creation start")

	waiterArrived := make(chan struct{}, 1)
	p.beforeCreateWait = func() { waiterArrived <- struct{}{} }
	waiterDone := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, nil, "initialize", initPayload)
		waiterDone <- err
	}()
	awaitSignal(t, waiterArrived, "placeholder waiter arrival")

	placeholder := liveInstance(p, id)
	if placeholder == nil || !placeholder.creating {
		t.Fatal("expected a creating placeholder")
	}

	close(release)
	if err := <-firstDone; !errors.Is(err, errBoom) {
		t.Fatalf("creator error = %v, want errBoom", err)
	}
	if err := <-waiterDone; !errors.Is(err, errBoom) {
		t.Fatalf("waiter error = %v, want errBoom", err)
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns = %d, want 1 (waiters must not retry)", got)
	}
	if liveInstance(p, id) != nil {
		t.Fatal("placeholder survived the failed spawn")
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live = %d, want 0", got)
	}
	server, err := store.Server(ctx, id)
	if err != nil {
		t.Fatalf("spawn-failed row missing: %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Fatalf("status = %q, want exited", server.Status)
	}
	select {
	case <-placeholder.ready:
	default:
		t.Fatal("abandoned placeholder did not close ready")
	}

	// The released slot admits a fresh creation.
	f.setOnCreate(nil)
	if _, err := p.Post(ctx, "after-fail", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("creation after spawn failure: %v", err)
	}
	if got := liveCount(p); got != 1 {
		t.Fatalf("live after recovery = %d, want 1", got)
	}
}

func TestProxyOwnershipSpawnFailureKeepsExitedRowAndReleasesCapacity(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(func(ctx context.Context, store *acpstore.Store, serverID string, _ acpruntime.LaunchSpec) error {
		if err := store.MarkExited(ctx, serverID); err != nil {
			return err
		}
		return errBoom
	})
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "spawn-fail", &agent, "initialize", initPayload); !errors.Is(err, errBoom) {
		t.Fatalf("spawn failure: got %v, want errBoom", err)
	}
	server, err := store.Server(ctx, "spawn-fail")
	if err != nil {
		t.Fatalf("spawn-failed row missing: %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Fatalf("spawn-failed status = %q, want exited", server.Status)
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live after spawn failure = %d, want 0", got)
	}

	f.setOnCreate(nil)
	if _, err := p.Post(ctx, "after-fail", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("creation after spawn failure: %v", err)
	}
	if got := liveCount(p); got != 1 {
		t.Fatalf("live after recovery = %d, want 1", got)
	}
}

func TestProxyOwnershipResolverFailureCreatesNoRow(t *testing.T) {
	f := newTestFactory(t)
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	unknown := "unknown"

	if _, err := p.Post(ctx, "resolve-fail", &unknown, "initialize", initPayload); err == nil {
		t.Fatal("expected resolver failure")
	}
	if _, err := store.Server(ctx, "resolve-fail"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("resolver failure created a row: %v", err)
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live after resolver failure = %d, want 0", got)
	}
	if got := f.spawnCount(); got != 0 {
		t.Fatalf("spawns after resolver failure = %d, want 0", got)
	}
}

func TestProxyOwnershipClosedGate(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)
	p.closed.Store(true)
	agent := "alpha"

	if _, err := p.Post(t.Context(), "closed", &agent, "initialize", initPayload); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed proxy: got %v, want ErrClosed", err)
	}
}

// TestCreateFinalizeLosesToDeleteMark proves an in-flight create cannot publish
// (or allow subscriptions) once DELETE has marked the server, and that a
// subscription racing that window is refused instead of escaping the close.
func TestCreateFinalizeLosesToDeleteMark(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)

	finalizeGate := make(chan struct{})
	releaseFinalize := make(chan struct{})
	p.beforeAllowServerSubs = func() {
		close(finalizeGate)
		<-releaseFinalize
	}
	deleteMarked := make(chan struct{})
	releaseDelete := make(chan struct{})
	p.afterDeleteMark = func() {
		close(deleteMarked)
		<-releaseDelete
	}

	agent := "alpha"
	postErr := make(chan error, 1)
	go func() {
		_, err := p.Post(t.Context(), "finalize-loses", &agent, "initialize", initPayload)
		postErr <- err
	}()
	<-finalizeGate

	deleteErr := make(chan error, 1)
	go func() { deleteErr <- p.Delete(t.Context(), "finalize-loses") }()
	<-deleteMarked

	sub, err := p.Subscribe(t.Context(), "finalize-loses", 0)
	if err != nil {
		t.Fatalf("subscribe during delete: %v", err)
	}
	defer sub.Close()
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("Next during delete = %v, want io.EOF", err)
	}

	close(releaseFinalize)
	if err := <-postErr; !errors.Is(err, ErrDeleting) {
		t.Fatalf("Post = %v, want ErrDeleting", err)
	}
	// The create must have aborted, not published: DELETE still owns the ID.
	if liveInstance(p, "finalize-loses") != nil {
		t.Fatal("create published an instance despite an active DELETE")
	}
	close(releaseDelete)
	if err := <-deleteErr; err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// TestAwaitCreatingInheritsShutdownCreationError proves a waiter woken by a
// creation abandoned during shutdown observes the creator's recorded cause
// (ErrClosed), not the transient gated-state ErrDeleting.
func TestAwaitCreatingInheritsShutdownCreationError(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	id := "shutdown-waiter-error"
	agent := "alpha"

	entered := make(chan struct{})
	release := make(chan struct{})
	realServer := p.storeServer
	p.storeServer = func(ctx context.Context, serverID string) (acpstore.Server, error) {
		close(entered)
		<-release
		return realServer(ctx, serverID)
	}

	creatorErr := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &agent, "initialize", initPayload)
		creatorErr <- err
	}()
	awaitSignal(t, entered, "creation store read")

	waiterArrived := make(chan struct{}, 1)
	p.beforeCreateWait = func() { waiterArrived <- struct{}{} }
	waiterErr := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, nil, "initialize", initPayload)
		waiterErr <- err
	}()
	awaitSignal(t, waiterArrived, "waiter arrival")

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown(ctx) }()
	waitFor(t, p.closed.Load)
	close(release)

	creator := <-creatorErr
	waiter := <-waiterErr
	if !errors.Is(creator, ErrClosed) {
		t.Fatalf("creator error = %v, want ErrClosed", creator)
	}
	if !errors.Is(waiter, creator) {
		t.Fatalf("waiter error = %v, want the creator's %v", waiter, creator)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestAwaitCreatingInheritsDeleteCreationError proves a waiter woken by a
// creation abandoned by DELETE observes the same cause as the creator.
func TestAwaitCreatingInheritsDeleteCreationError(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	id := "delete-waiter-error"
	agent := "alpha"

	entered := make(chan struct{})
	release := make(chan struct{})
	realServer := p.storeServer
	p.storeServer = func(ctx context.Context, serverID string) (acpstore.Server, error) {
		close(entered)
		<-release
		return realServer(ctx, serverID)
	}

	creatorErr := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &agent, "initialize", initPayload)
		creatorErr <- err
	}()
	awaitSignal(t, entered, "creation store read")

	waiterArrived := make(chan struct{}, 1)
	p.beforeCreateWait = func() { waiterArrived <- struct{}{} }
	waiterErr := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, nil, "initialize", initPayload)
		waiterErr <- err
	}()
	awaitSignal(t, waiterArrived, "waiter arrival")

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- p.Delete(ctx, id) }()
	waitFor(t, func() bool { return p.isDeleting(id) })
	close(release)

	creator := <-creatorErr
	waiter := <-waiterErr
	if !errors.Is(waiter, creator) {
		t.Fatalf("waiter error = %v, want the creator's %v", waiter, creator)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// TestAbandonCreateKillFailureRetainsRuntime proves a spawned runtime that lost
// its publication race is not orphaned when its teardown kill fails: the
// instance stays tracked with the runtime so a retrying DELETE can terminate
// it, and capacity is released only once it is gone.
func TestAbandonCreateKillFailureRetainsRuntime(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)

	finalizeGate := make(chan struct{})
	releaseFinalize := make(chan struct{})
	p.beforeAllowServerSubs = func() {
		close(finalizeGate)
		<-releaseFinalize
	}
	deleteMarked := make(chan struct{})
	releaseDelete := make(chan struct{})
	p.afterDeleteMark = func() {
		close(deleteMarked)
		<-releaseDelete
	}

	agent := "alpha"
	postErr := make(chan error, 1)
	go func() {
		_, err := p.Post(t.Context(), "abandon-retain", &agent, "initialize", initPayload)
		postErr <- err
	}()
	<-finalizeGate
	rt := requireRuntime(t, f)
	rt.setKillErr(errBoom)

	deleteErr := make(chan error, 1)
	go func() { deleteErr <- p.Delete(t.Context(), "abandon-retain") }()
	awaitSignal(t, deleteMarked, "delete mark")
	close(releaseFinalize)

	if err := <-postErr; !errors.Is(err, ErrDeleting) {
		t.Fatalf("Post = %v, want ErrDeleting", err)
	}
	inst := liveInstance(p, "abandon-retain")
	if inst == nil {
		t.Fatal("failed teardown orphaned the spawned runtime")
	}
	inst.lock.mu.Lock()
	got := inst.runtime
	inst.lock.mu.Unlock()
	if got != rt {
		t.Fatal("retained instance does not own the spawned runtime")
	}
	if liveCount(p) != 1 {
		t.Fatalf("live = %d, want 1 (capacity stays consumed)", liveCount(p))
	}

	rt.setKillErr(nil)
	close(releaseDelete)
	if err := <-deleteErr; err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if liveInstance(p, "abandon-retain") != nil {
		t.Fatal("retained runtime survived the retry delete")
	}
	if _, err := store.Server(t.Context(), "abandon-retain"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("server after retry delete = %v, want ErrNotFound", err)
	}
	if got := rt.kills(); got != 2 {
		t.Fatalf("kills = %d, want 2", got)
	}
}

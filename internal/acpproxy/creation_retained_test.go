package acpproxy

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

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

// TestAwaitCreatingInheritsRetainedTeardownError proves a waiter woken by a
// creation whose teardown kill failed observes the creator's recorded cause
// (ErrClosed), not the transient gated-state ErrDeleting. The retained
// placeholder is still the current generation, so the recorded error wins over
// the gate because the waiter's own generation was not replaced.
func TestAwaitCreatingInheritsRetainedTeardownError(t *testing.T) {
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
	id := "retained-waiter-error"
	agent := "alpha"

	// Every kill fails so the shutdown rollback retains the fresh runtime.
	inner := p.factory
	p.factory = func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, timeout time.Duration, log *slog.Logger) (runtime, error) {
		rt, err := inner(ctx, store, serverID, spec, timeout, log)
		if err == nil {
			rt.(*fakeRuntime).setKillErr(errBoom)
		}
		return rt, err
	}

	creatorErr := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &agent, "initialize", initPayload)
		creatorErr <- err
	}()
	awaitSignal(t, entered, "creation start")

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
	waitFor(t, func() bool {
		inst := liveInstance(p, id)
		if inst == nil {
			return false
		}
		inst.lock.mu.Lock()
		defer inst.lock.mu.Unlock()
		return inst.terminating
	})
	close(release)

	creator := <-creatorErr
	waiter := <-waiterErr
	if !errors.Is(creator, ErrClosed) {
		t.Fatalf("creator error = %v, want ErrClosed", creator)
	}
	if !errors.Is(waiter, creator) {
		t.Fatalf("waiter error = %v, want the creator's %v", waiter, creator)
	}
	inst := liveInstance(p, id)
	if inst == nil {
		t.Fatal("retained generation was dropped")
	}
	inst.lock.mu.Lock()
	recorded := inst.createErr
	inst.lock.mu.Unlock()
	if !errors.Is(recorded, creator) {
		t.Fatalf("recorded createErr = %v, want the creator's %v", recorded, creator)
	}
	// The retained kill fails again, so Shutdown reports it.
	if err := <-shutdownDone; !errors.Is(err, errBoom) {
		t.Fatalf("Shutdown = %v, want errBoom from the retained kill", err)
	}
}

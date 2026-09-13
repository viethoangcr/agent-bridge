package acpproxy

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestProxyDeleteUnknownServer(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)

	if err := p.Delete(t.Context(), "ghost"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("Delete unknown = %v, want ErrNotFound", err)
	}
}

func TestProxyDeleteRemovesDurableStateAndClosesSubscriptions(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	rt := startServer(t, p, f, "delete")
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), "delete", 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()
	if _, err := store.AppendOutput(t.Context(), "delete", acpstore.Output{
		Kind:    "notification",
		Payload: []byte(`{"n":1}`),
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}

	if err := p.Delete(t.Context(), "delete"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Server(t.Context(), "delete"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("server row after delete = %v, want ErrNotFound", err)
	}
	if liveInstance(p, "delete") != nil {
		t.Fatal("live instance survived delete")
	}
	if _, err := store.Events(t.Context(), "delete", acpstore.EventQuery{}); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("events after delete = %v, want ErrNotFound", err)
	}
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription after delete = %v, want io.EOF", err)
	}

	// A late committed wakeup cannot restore the row.
	rt.notify()
	if _, err := store.Server(t.Context(), "delete"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("late wakeup restored a row: %v", err)
	}

	agent := "alpha"
	if _, err := p.Post(t.Context(), "delete", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("first post after delete: %v", err)
	}
	if liveInstance(p, "delete") == nil {
		t.Fatal("fresh post after delete created no instance")
	}
}

// TestProxyDeleteConcurrentPostConflict proves a concurrent POST during delete
// observes ErrDeleting rather than blocking until a new row is possible.
func TestProxyDeleteConcurrentPostConflict(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	startServer(t, p, f, "delete-conflict")
	rt := requireRuntime(t, f)

	entered := make(chan struct{})
	release := make(chan struct{})
	killStarted := make(chan struct{})
	allowDelete := make(chan struct{})
	rt.setHook(func() {
		close(entered)
		<-release
	})
	rt.setKillHook(func() {
		close(release)
		close(killStarted)
		<-allowDelete
	})

	postDone := make(chan struct{})
	go func() {
		defer close(postDone)
		_, _ = p.Post(t.Context(), "delete-conflict", nil, "session/prompt", initPayload)
	}()
	<-entered

	deleteDone := make(chan error, 1)
	go func() { deleteDone <- p.Delete(t.Context(), "delete-conflict") }()
	awaitSignal(t, killStarted, "delete kill")

	if _, err := p.Post(t.Context(), "delete-conflict", nil, "initialize", initPayload); !errors.Is(err, ErrDeleting) {
		t.Fatalf("concurrent post = %v, want ErrDeleting", err)
	}

	close(allowDelete)
	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete: %v", err)
	}
	<-postDone
}

// TestProxyDeleteKillsBeforeWaitingLease proves Delete signals the runtime
// while the active lease is still held, so the blocked Post releases rather
// than waiting out the request timeout.
func TestProxyDeleteKillsBeforeWaitingLease(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	startServer(t, p, f, "delete-lease")
	inst := requireInstance(t, p, "delete-lease")
	rt := requireRuntime(t, f)

	entered := make(chan struct{})
	release := make(chan struct{})
	var activeAtKill atomic.Bool
	rt.setHook(func() {
		close(entered)
		<-release
	})
	rt.setKillHook(func() {
		inst.lock.mu.Lock()
		active := inst.activity
		inst.lock.mu.Unlock()
		if active > 0 {
			activeAtKill.Store(true)
		}
		close(release)
	})

	go func() {
		_, _ = p.Post(t.Context(), "delete-lease", nil, "session/prompt", initPayload)
	}()
	<-entered

	if err := p.Delete(t.Context(), "delete-lease"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !activeAtKill.Load() {
		t.Fatal("Delete waited for the lease before killing")
	}
}

// TestProxyDeletePruneFailureRetry proves a failed prune leaves an exited row,
// removes the live instance, clears the deleting gate, keeps subscriptions
// closed, and lets a later DELETE retry without another process.
func TestProxyDeletePruneFailureRetry(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	startServer(t, p, f, "prune")
	p.newSubscriptionTicker = func(time.Duration) subscriptionTicker { return newFakeTicker() }
	sub, err := p.Subscribe(t.Context(), "prune", 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	realDelete := p.deleteServer
	var calls atomic.Int32
	p.deleteServer = func(ctx context.Context, serverID string) error {
		if calls.Add(1) == 1 {
			return errBoom
		}
		return realDelete(ctx, serverID)
	}

	if err := p.Delete(t.Context(), "prune"); !errors.Is(err, errBoom) {
		t.Fatalf("failed prune = %v, want errBoom", err)
	}
	server, err := store.Server(t.Context(), "prune")
	if err != nil {
		t.Fatalf("server after failed prune: %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Fatalf("status after failed prune = %q, want exited", server.Status)
	}
	if liveInstance(p, "prune") != nil {
		t.Fatal("live instance survived the failed prune")
	}
	if p.isDeleting("prune") {
		t.Fatal("deleting gate was not cleared after the failed prune")
	}
	if _, err := nextWithin(t, sub); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription after failed prune = %v, want io.EOF", err)
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns after failed prune = %d, want 1 (no restart)", got)
	}

	if err := p.Delete(t.Context(), "prune"); err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if _, err := store.Server(t.Context(), "prune"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("server after retry = %v, want ErrNotFound", err)
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns after retry = %d, want 1", got)
	}
}

// TestProxyDeleteFailureRetainsStateForRetry proves a failed Kill does not
// prune durable rows or drop the live instance, and that a later DELETE retries
// successfully without restarting a process.
func TestProxyDeleteFailureRetainsStateForRetry(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	startServer(t, p, f, "killfail")
	rt := requireRuntime(t, f)
	rt.setKillErr(errBoom)

	if err := p.Delete(t.Context(), "killfail"); !errors.Is(err, errBoom) {
		t.Fatalf("delete with failing kill = %v, want errBoom", err)
	}
	if _, err := store.Server(t.Context(), "killfail"); err != nil {
		t.Fatalf("durable row after failed kill = %v, want retained", err)
	}
	if liveInstance(p, "killfail") == nil {
		t.Fatal("live instance dropped despite failed kill")
	}
	if !p.isDeleting("killfail") {
		t.Fatal("deleting gate cleared despite failed kill")
	}

	rt.setKillErr(nil)
	if err := p.Delete(t.Context(), "killfail"); err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if _, err := store.Server(t.Context(), "killfail"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("server after retry = %v, want ErrNotFound", err)
	}
}

// TestProxyDeleteWaitsForCreatingPlaceholder proves DELETE waits for a
// mid-flight creation without holding any lock, then terminates the fresh
// runtime and prunes the durable row.
func TestProxyDeleteWaitsForCreatingPlaceholder(t *testing.T) {
	f := newTestFactory(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	f.setOnCreate(func(_ context.Context, _ *acpstore.Store, _ string, _ acpruntime.LaunchSpec) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	})
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	id := "delete-creating"
	agent := "alpha"

	created := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &agent, "initialize", initPayload)
		created <- err
	}()
	awaitSignal(t, entered, "creation start")

	deleteMarked := make(chan struct{})
	p.afterDeleteMark = func() { close(deleteMarked) }
	deleted := make(chan error, 1)
	go func() { deleted <- p.Delete(ctx, id) }()
	awaitSignal(t, deleteMarked, "delete mark")
	close(release)

	if err := <-created; !errors.Is(err, ErrDeleting) {
		t.Fatalf("creation during delete = %v, want ErrDeleting", err)
	}
	if err := <-deleted; err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if liveInstance(p, id) != nil {
		t.Fatal("live instance survived Delete")
	}
	if _, err := store.Server(ctx, id); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("server after Delete = %v, want ErrNotFound", err)
	}
	if got := requireRuntime(t, f).kills(); got != 1 {
		t.Fatalf("kills = %d, want 1", got)
	}
}

// TestProxyDeleteRetryAfterLeaseDrainTimeout proves a Delete whose Kill
// succeeded but whose lease drain expired keeps the gated generation reachable;
// the watcher must not release the slot, and a retry drains and prunes.
func TestProxyDeleteRetryAfterLeaseDrainTimeout(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	startServer(t, p, f, "drain-retry")
	inst := requireInstance(t, p, "drain-retry")
	rt := requireRuntime(t, f)

	entered := make(chan struct{})
	release := make(chan struct{})
	rt.setHook(func() {
		close(entered)
		<-release
	})
	go func() {
		_, _ = p.Post(t.Context(), "drain-retry", nil, "session/prompt", initPayload)
	}()
	<-entered

	shortCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.Delete(shortCtx, "drain-retry"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delete with expired drain = %v, want deadline exceeded", err)
	}

	// The watcher ran when Kill completed, but the generation is gated: it
	// must keep its live slot so the retry can drain the lease.
	if liveInstance(p, "drain-retry") != inst {
		t.Fatal("gated instance was dropped by the watcher after a failed drain")
	}
	if !p.isDeleting("drain-retry") {
		t.Fatal("deleting gate was cleared after a failed drain")
	}

	close(release)
	if err := p.Delete(t.Context(), "drain-retry"); err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if _, err := store.Server(t.Context(), "drain-retry"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("server after retry = %v, want ErrNotFound", err)
	}
}

// TestProxyRetireDeadGenerationReleasesCapacity proves a failed DELETE whose
// Kill already succeeded releases the live slot once leases drain, without
// requiring a retry, while leaving the durable row for pruning.
func TestProxyRetireDeadGenerationReleasesCapacity(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	startServer(t, p, f, "retire-dead")
	inst := requireInstance(t, p, "retire-dead")
	rt := requireRuntime(t, f)

	entered := make(chan struct{})
	release := make(chan struct{})
	rt.setHook(func() {
		close(entered)
		<-release
	})
	go func() {
		_, _ = p.Post(t.Context(), "retire-dead", nil, "session/prompt", initPayload)
	}()
	<-entered

	shortCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.Delete(shortCtx, "retire-dead"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delete with expired drain = %v, want deadline exceeded", err)
	}
	if liveInstance(p, "retire-dead") != inst {
		t.Fatal("instance released before its leases drained")
	}

	close(release)
	awaitSignal(t, inst.watched, "retire release")

	if liveInstance(p, "retire-dead") != nil {
		t.Fatal("dead generation kept its live slot")
	}
	if _, err := store.Server(t.Context(), "retire-dead"); err != nil {
		t.Fatalf("durable row after retire = %v, want retained for retry", err)
	}
	if err := p.Delete(t.Context(), "retire-dead"); err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if _, err := store.Server(t.Context(), "retire-dead"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("server after retry = %v, want ErrNotFound", err)
	}
}

package acpproxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestProxyOwnershipAgentRules(t *testing.T) {
	f := newTestFactory(t)
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	id := "server-a"

	if _, err := p.Post(ctx, id, nil, "initialize", initPayload); !errors.Is(err, ErrMissingAgent) {
		t.Fatalf("missing agent: got %v, want ErrMissingAgent", err)
	}
	if _, err := store.Server(ctx, id); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("missing agent created a row: %v", err)
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live runtimes after missing agent = %d, want 0", got)
	}

	agent := "alpha"
	if _, err := p.Post(ctx, id, &agent, "initialize", initPayload); err != nil {
		t.Fatalf("first post: %v", err)
	}
	if got := liveCount(p); got != 1 {
		t.Fatalf("live runtimes after first post = %d, want 1", got)
	}

	other := "beta"
	if _, err := p.Post(ctx, id, &other, "initialize", initPayload); !errors.Is(err, ErrAgentConflict) {
		t.Fatalf("conflicting agent: got %v, want ErrAgentConflict", err)
	}
	if _, err := p.Post(ctx, id, nil, "initialize", initPayload); err != nil {
		t.Fatalf("omitted later agent: %v", err)
	}
	if _, err := p.Post(ctx, id, &agent, "initialize", initPayload); err != nil {
		t.Fatalf("matching later agent: %v", err)
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns = %d, want 1", got)
	}
	if spec := f.lastSpec(); spec.Program != "/usr/bin/alpha-bin" {
		t.Fatalf("spec = %+v, want the resolver-resolved alpha binary", spec)
	}
}

func TestProxyOwnershipConcurrentFirstPostsCreateOneRuntime(t *testing.T) {
	f := newTestFactory(t)
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	id := "concurrent"
	agent := "alpha"
	const n = 20

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.Post(ctx, id, &agent, "initialize", initPayload)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns = %d, want 1", got)
	}
	servers, err := store.Servers(ctx)
	if err != nil {
		t.Fatalf("list servers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("rows = %d, want 1", len(servers))
	}
	if got := requireRuntime(t, f).calls(); got != n {
		t.Fatalf("runtime posts = %d, want %d", got, n)
	}
}

func TestProxyOwnershipDifferentServersCreateConcurrently(t *testing.T) {
	f := newTestFactory(t)
	const n = 8
	started := make(chan struct{}, n)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()

	f.setOnCreate(func(_ context.Context, _ *acpstore.Store, _ string, _ acpruntime.LaunchSpec) error {
		started <- struct{}{}
		<-release
		return nil
	})
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := p.Post(ctx, fmt.Sprintf("srv-%d", i), &agent, "initialize", initPayload); err != nil {
				t.Errorf("post srv-%d: %v", i, err)
			}
		}(i)
	}

	for i := 0; i < n; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d creations ran concurrently", i, n)
		}
	}
	releaseAll()
	wg.Wait()
}

// TestProxyCreationHoldsNoLifecycleOrGlobalLock proves the per-ID lifecycle
// lock and the global live-map lock are released before the durable read,
// resolution, and spawn: while one server's creation is blocked in a seam,
// TryLock on its lifecycle lock succeeds and a POST for a different server
// completes.
func TestProxyCreationHoldsNoLifecycleOrGlobalLock(t *testing.T) {
	tests := []struct {
		name  string
		block func(t *testing.T, f *testFactory, p *Proxy, store *acpstore.Store, entered, release chan struct{})
	}{
		{
			name: "store read",
			block: func(_ *testing.T, _ *testFactory, p *Proxy, store *acpstore.Store, entered, release chan struct{}) {
				realServer := p.storeServer
				var once sync.Once
				p.storeServer = func(ctx context.Context, serverID string) (acpstore.Server, error) {
					if serverID == "creating" {
						once.Do(func() { close(entered) })
						<-release
					}
					return realServer(ctx, serverID)
				}
			},
		},
		{
			name: "resolution",
			block: func(_ *testing.T, _ *testFactory, p *Proxy, _ *acpstore.Store, entered, release chan struct{}) {
				var calls atomic.Int32
				p.resolver.LookPath = func(name string) (string, error) {
					if calls.Add(1) == 1 {
						close(entered)
						<-release
					}
					return "/usr/bin/" + name, nil
				}
			},
		},
		{
			name: "spawn",
			block: func(_ *testing.T, f *testFactory, _ *Proxy, _ *acpstore.Store, entered, release chan struct{}) {
				f.setOnCreate(func(_ context.Context, _ *acpstore.Store, serverID string, _ acpruntime.LaunchSpec) error {
					if serverID == "creating" {
						close(entered)
						<-release
					}
					return nil
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTestFactory(t)
			f.setOnCreate(liveOnCreate)
			p, store := newProxyForTest(t, f)
			entered := make(chan struct{})
			release := make(chan struct{})
			tc.block(t, f, p, store, entered, release)

			created := make(chan error, 1)
			go func() {
				agent := "alpha"
				_, err := p.Post(t.Context(), "creating", &agent, "initialize", initPayload)
				created <- err
			}()
			awaitSignal(t, entered, "creation I/O")

			placeholder := liveInstance(p, "creating")
			if placeholder == nil {
				t.Fatal("no instance reserved while creation I/O is in flight")
			}
			if placeholder.lock.mu.TryLock() {
				placeholder.lock.mu.Unlock()
			} else {
				t.Fatal("lifecycle lock held during creation I/O")
			}

			other := make(chan error, 1)
			go func() {
				agent := "alpha"
				_, err := p.Post(t.Context(), "creating-other", &agent, "initialize", initPayload)
				other <- err
			}()
			select {
			case err := <-other:
				if err != nil {
					t.Fatalf("post for a different server: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("post for a different server blocked during creation I/O")
			}

			close(release)
			if err := <-created; err != nil {
				t.Fatalf("blocked creation: %v", err)
			}
		})
	}
}

// TestProxyConcurrentFirstPostsWaitOnCreatingPlaceholder proves concurrent
// first POSTs that find a creating placeholder wait without holding any lock,
// share the one creation, and never spawn a second runtime or row.
func TestProxyConcurrentFirstPostsWaitOnCreatingPlaceholder(t *testing.T) {
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
	id := "waiters"
	agent := "alpha"
	const n = 8

	firstDone := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, id, &agent, "initialize", initPayload)
		firstDone <- err
	}()
	awaitSignal(t, entered, "creation start")

	waiterArrived := make(chan struct{}, n)
	p.beforeCreateWait = func() { waiterArrived <- struct{}{} }

	waiterErrs := make([]error, n-1)
	var wg sync.WaitGroup
	for i := 0; i < n-1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, waiterErrs[i] = p.Post(ctx, id, nil, "initialize", initPayload)
		}(i)
	}
	for i := 0; i < n-1; i++ {
		awaitSignal(t, waiterArrived, "placeholder waiter arrival")
	}

	placeholder := liveInstance(p, id)
	if placeholder == nil || !placeholder.creating {
		t.Fatal("expected one creating placeholder")
	}
	if got := liveCount(p); got != 1 {
		t.Fatalf("live while creating = %d, want 1", got)
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns while creating = %d, want 1", got)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first post: %v", err)
	}
	wg.Wait()
	for i, err := range waiterErrs {
		if err != nil {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
	if got := f.spawnCount(); got != 1 {
		t.Fatalf("spawns = %d, want 1", got)
	}
	servers, err := store.Servers(ctx)
	if err != nil {
		t.Fatalf("list servers: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("rows = %d, want 1", len(servers))
	}
	if got := requireRuntime(t, f).calls(); got != n {
		t.Fatalf("runtime posts = %d, want %d", got, n)
	}
}

func TestProxyOwnershipCapacity65(t *testing.T) {
	f := newTestFactory(t)
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"
	const n = maxLiveRuntimes + 1

	type outcome struct {
		id  string
		err error
	}
	results := make([]outcome, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		results[i].id = fmt.Sprintf("cap-%02d", i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i].err = p.Post(ctx, results[i].id, &agent, "initialize", initPayload)
		}(i)
	}
	wg.Wait()

	var ok, capacity int
	var failedID string
	for _, r := range results {
		switch {
		case r.err == nil:
			ok++
		case errors.Is(r.err, ErrRuntimeCapacity):
			capacity++
			failedID = r.id
		default:
			t.Fatalf("unexpected error for %s: %v", r.id, r.err)
		}
	}
	if ok != maxLiveRuntimes || capacity != 1 {
		t.Fatalf("ok=%d capacity=%d, want %d/1", ok, capacity, maxLiveRuntimes)
	}
	if got := liveCount(p); got != maxLiveRuntimes {
		t.Fatalf("live = %d, want %d", got, maxLiveRuntimes)
	}
	if got := f.spawnCount(); got != maxLiveRuntimes {
		t.Fatalf("spawns = %d, want %d", got, maxLiveRuntimes)
	}
	if _, err := store.Server(ctx, failedID); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("capacity-rejected server %q created a row: %v", failedID, err)
	}
}

func TestProxyOwnershipExitReleasesCapacity(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "first", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("first post: %v", err)
	}
	watched := requireInstance(t, p, "first").watched
	requireRuntime(t, f).exit(nil)
	awaitSignal(t, watched, "first watcher cleanup")
	if liveInstance(p, "first") != nil {
		t.Fatal("live instance survived runtime exit")
	}

	if _, err := p.Post(ctx, "second", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("creation after exit: %v", err)
	}
	if got := liveCount(p); got != 1 {
		t.Fatalf("live after exit = %d, want 1", got)
	}
}

func TestProxyOwnershipKeyedLocksCleanedUp(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "lock-clean", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("post: %v", err)
	}
	if got := keyedLockCount(p); got != 1 {
		t.Fatalf("keyed locks while live = %d, want 1", got)
	}
	watched := requireInstance(t, p, "lock-clean").watched
	requireRuntime(t, f).exit(nil)
	awaitSignal(t, watched, "keyed lock cleanup")
	if got := keyedLockCount(p); got != 0 {
		t.Fatalf("keyed locks after exit = %d, want 0", got)
	}
	if got := liveCount(p); got != 0 {
		t.Fatalf("live after exit = %d, want 0", got)
	}
}

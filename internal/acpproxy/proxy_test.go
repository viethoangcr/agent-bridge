package acpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/config"
)

var (
	initPayload = json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	errBoom     = errors.New("boom")
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeRuntime is a controllable in-memory runtime implementing the narrow
// private runtime interface. store/serverID let exit model the runtime-owned
// exited transition that the real process/pump teardown performs.
type fakeRuntime struct {
	mu          sync.Mutex
	postResult  acpruntime.PostResult
	postErr     error
	postCalls   int
	killCalls   int
	pid         int
	lastPayload []byte
	onPost      func()
	onKill      func()
	done        chan struct{}
	waitErr     error
	killErr     error
	wake        chan struct{}
	wakeOnce    sync.Once
	wakeMu      sync.Mutex
	wakeClosed  bool
	stderr      string

	store    *acpstore.Store
	serverID string
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{done: make(chan struct{}), wake: make(chan struct{}, 1)}
}

func (f *fakeRuntime) Post(_ context.Context, payload json.RawMessage) (acpruntime.PostResult, error) {
	f.mu.Lock()
	f.postCalls++
	f.lastPayload = append([]byte(nil), payload...)
	hook := f.onPost
	result, err := f.postResult, f.postErr
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return result, err
}

// Events returns the capacity-one coalesced commit wakeup channel.
func (f *fakeRuntime) Events() <-chan struct{} {
	return f.wake
}

// PID returns the fake's live direct-child PID.
func (f *fakeRuntime) PID() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pid
}

func (f *fakeRuntime) setPID(pid int) {
	f.mu.Lock()
	f.pid = pid
	f.mu.Unlock()
}

// notify simulates a committed-output wakeup without blocking on a full
// capacity-one channel and without sending on a closed one.
func (f *fakeRuntime) notify() {
	f.wakeMu.Lock()
	defer f.wakeMu.Unlock()
	if f.wakeClosed {
		return
	}
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *fakeRuntime) Wait() error {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.waitErr
}

// Kill records the signal, runs any hook, then simulates the process and pump
// completion that a real signal-and-wait Kill performs. A configured killErr
// simulates a signal failure without completing the runtime.
func (f *fakeRuntime) Kill(ctx context.Context) error {
	f.mu.Lock()
	f.killCalls++
	hook := f.onKill
	killErr := f.killErr
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if killErr != nil {
		return killErr
	}
	f.exit(nil)
	select {
	case <-f.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeRuntime) exit(err error) {
	f.mu.Lock()
	f.waitErr = err
	f.mu.Unlock()
	select {
	case <-f.done:
	default:
		close(f.done)
	}
	f.wakeOnce.Do(func() {
		f.wakeMu.Lock()
		f.wakeClosed = true
		close(f.wake)
		f.wakeMu.Unlock()
	})
	if f.store != nil {
		_ = f.store.MarkExited(context.Background(), f.serverID)
	}
}

func (f *fakeRuntime) setResult(result acpruntime.PostResult) {
	f.mu.Lock()
	f.postResult = result
	f.mu.Unlock()
}

func (f *fakeRuntime) setErr(err error) {
	f.mu.Lock()
	f.postErr = err
	f.mu.Unlock()
}

func (f *fakeRuntime) setHook(hook func()) {
	f.mu.Lock()
	f.onPost = hook
	f.mu.Unlock()
}

func (f *fakeRuntime) setKillHook(hook func()) {
	f.mu.Lock()
	f.onKill = hook
	f.mu.Unlock()
}

func (f *fakeRuntime) setKillErr(err error) {
	f.mu.Lock()
	f.killErr = err
	f.mu.Unlock()
}

// Stderr exposes the configured redacted tail so the fake satisfies the
// optional stderr provider the proxy consults for 502 problem extensions.
func (f *fakeRuntime) Stderr() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stderr
}

func (f *fakeRuntime) setStderr(stderr string) {
	f.mu.Lock()
	f.stderr = stderr
	f.mu.Unlock()
}

func (f *fakeRuntime) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.postCalls
}

func (f *fakeRuntime) kills() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.killCalls
}

func (f *fakeRuntime) payload() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.lastPayload...)
}

// testFactory records every creation and hands out fake runtimes.
type testFactory struct {
	mu       sync.Mutex
	runtimes []*fakeRuntime
	specs    []acpruntime.LaunchSpec
	servers  []string
	onCreate func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec) error
}

func newTestFactory(t *testing.T) *testFactory {
	f := &testFactory{}
	t.Cleanup(func() {
		f.mu.Lock()
		runtimes := append([]*fakeRuntime(nil), f.runtimes...)
		f.mu.Unlock()
		for _, rt := range runtimes {
			rt.exit(nil)
		}
	})
	return f
}

func (f *testFactory) factory(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec, _ time.Duration, _ *slog.Logger) (runtime, error) {
	f.mu.Lock()
	f.specs = append(f.specs, spec)
	f.servers = append(f.servers, serverID)
	onCreate := f.onCreate
	f.mu.Unlock()

	if onCreate != nil {
		if createErr := onCreate(ctx, store, serverID, spec); createErr != nil {
			return nil, createErr
		}
	}
	rt := newFakeRuntime()
	rt.store = store
	rt.serverID = serverID
	f.mu.Lock()
	f.runtimes = append(f.runtimes, rt)
	f.mu.Unlock()
	return rt, nil
}

func (f *testFactory) setOnCreate(hook func(ctx context.Context, store *acpstore.Store, serverID string, spec acpruntime.LaunchSpec) error) {
	f.mu.Lock()
	f.onCreate = hook
	f.mu.Unlock()
}

func (f *testFactory) spawnCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.servers)
}

func (f *testFactory) lastRuntime() *fakeRuntime {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.runtimes) == 0 {
		return nil
	}
	return f.runtimes[len(f.runtimes)-1]
}

func (f *testFactory) lastSpec() acpruntime.LaunchSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.specs) == 0 {
		return acpruntime.LaunchSpec{}
	}
	return f.specs[len(f.specs)-1]
}

// liveOnCreate simulates Phase 02 Start's creating-to-idle transition.
func liveOnCreate(ctx context.Context, store *acpstore.Store, serverID string, _ acpruntime.LaunchSpec) error {
	return store.SetLive(ctx, serverID, 4242)
}

func newProxyForTest(t *testing.T, f *testFactory) (*Proxy, *acpstore.Store) {
	t.Helper()
	store, err := acpstore.Open(t.Context(), filepath.Join(t.TempDir(), "proxy.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	resolver := acpruntime.Resolver{
		Executable: "/bin/true",
		Commands:   map[string]config.AgentCommand{"alpha": {Binary: "alpha-bin"}},
		LookPath:   func(name string) (string, error) { return "/usr/bin/" + name, nil },
	}
	return newWithFactory(store, resolver, time.Minute, time.Hour, testLogger(), f.factory), store
}

// newProxyWithTTL builds a test proxy with a specific idle TTL so reaping can
// be enabled or disabled independently of the runtime request timeout.
func newProxyWithTTL(t *testing.T, f *testFactory, idleTTL time.Duration) (*Proxy, *acpstore.Store) {
	t.Helper()
	p, store := newProxyForTest(t, f)
	p.idleTTL = idleTTL
	return p, store
}

// fakeReaperTicker is a manually advanced reaper cadence. Tests drive ticks
// without sleeping.
type fakeReaperTicker struct {
	ch chan time.Time

	mu      sync.Mutex
	stopped bool
}

func newFakeReaperTicker() *fakeReaperTicker {
	return &fakeReaperTicker{ch: make(chan time.Time, 1)}
}

func (f *fakeReaperTicker) C() <-chan time.Time { return f.ch }

func (f *fakeReaperTicker) Stop() {
	f.mu.Lock()
	f.stopped = true
	f.mu.Unlock()
}

func (f *fakeReaperTicker) tick() {
	select {
	case f.ch <- time.Now():
	default:
	}
}

func (f *fakeReaperTicker) isStopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

// idleSince returns the durable idle timestamp that the reaper compares against
// its injected clock.
func idleSince(t *testing.T, store *acpstore.Store, serverID string) time.Time {
	t.Helper()
	server, err := store.Server(t.Context(), serverID)
	if err != nil {
		t.Fatalf("Server(%q): %v", serverID, err)
	}
	if server.IdleSinceMs == nil {
		t.Fatalf("Server(%q) has no idle_since_ms", serverID)
	}
	return time.UnixMilli(*server.IdleSinceMs)
}

// fixedNow pins the proxy clock to a single instant for deadline assertions.
func fixedNow(p *Proxy, at time.Time) {
	p.now = func() time.Time { return at }
}

func liveCount(p *Proxy) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.live)
}

func keyedLockCount(p *Proxy) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.locks)
}

func liveInstance(p *Proxy, serverID string) *instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.live[serverID]
}

func requireInstance(t *testing.T, p *Proxy, serverID string) *instance {
	t.Helper()
	inst := liveInstance(p, serverID)
	if inst == nil {
		t.Fatalf("no live instance for %q", serverID)
	}
	return inst
}

func requireRuntime(t *testing.T, f *testFactory) *fakeRuntime {
	t.Helper()
	rt := f.lastRuntime()
	if rt == nil {
		t.Fatal("no runtime was created")
	}
	return rt
}

// awaitSignal fails rather than hanging when an expected lifecycle signal never
// arrives.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

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

	deleted := make(chan error, 1)
	go func() { deleted <- p.Delete(ctx, id) }()
	close(release)

	if err := <-created; err != nil && !errors.Is(err, ErrDeleting) {
		t.Fatalf("creation during delete = %v, want success or ErrDeleting", err)
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

func TestProxyPostDelegationReturnsExactResult(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "delegation", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("create: %v", err)
	}
	rt := requireRuntime(t, f)

	response := json.RawMessage(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`)
	request := json.RawMessage(`{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":{"sessionId":"s"}}`)
	rt.setResult(acpruntime.PostResult{Response: response})
	got, err := p.Post(ctx, "delegation", nil, "session/prompt", request)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if !bytes.Equal(got.Response, response) || got.Accepted {
		t.Fatalf("result = %+v, want exact response bytes", got)
	}
	if !bytes.Equal(rt.payload(), request) {
		t.Fatalf("payload = %s, want %s", rt.payload(), request)
	}
	if got := rt.calls(); got != 2 {
		t.Fatalf("runtime posts = %d, want 2", got)
	}

	rt.setResult(acpruntime.PostResult{Accepted: true})
	got, err = p.Post(ctx, "delegation", nil, "session/cancel", initPayload)
	if err != nil {
		t.Fatalf("accepted post: %v", err)
	}
	if !got.Accepted || got.Response != nil {
		t.Fatalf("accepted result = %+v", got)
	}

	rt.setErr(acpruntime.ErrRequestTimeout)
	if _, err := p.Post(ctx, "delegation", nil, "session/prompt", request); !errors.Is(err, acpruntime.ErrRequestTimeout) {
		t.Fatalf("runtime error: got %v, want ErrRequestTimeout", err)
	}

	server, err := store.Server(ctx, "delegation")
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	if server.Status != acpstore.StatusIdle {
		t.Fatalf("status = %q, want idle (proxy must not write busy/idle)", server.Status)
	}
}

func TestProxyPostDelegationRunsWithoutLifecycleLock(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "lock-free", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("create: %v", err)
	}
	inst := liveInstance(p, "lock-free")
	if inst == nil {
		t.Fatal("no live instance")
	}

	var lockFree atomic.Bool
	requireRuntime(t, f).setHook(func() {
		if inst.lock.mu.TryLock() {
			inst.lock.mu.Unlock()
			lockFree.Store(true)
		}
	})
	if _, err := p.Post(ctx, "lock-free", nil, "initialize", initPayload); err != nil {
		t.Fatalf("post: %v", err)
	}
	if !lockFree.Load() {
		t.Fatal("Runtime.Post ran while the lifecycle lock was held")
	}
}

func TestProxyRecreateExitedServerPolicy(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	ctx := t.Context()
	id := "recreate"
	agent := "alpha"

	if _, err := p.Post(ctx, id, &agent, "initialize", initPayload); err != nil {
		t.Fatalf("create: %v", err)
	}
	watched := requireInstance(t, p, id).watched
	requireRuntime(t, f).exit(nil)
	if err := store.MarkExited(ctx, id); err != nil {
		t.Fatalf("mark exited: %v", err)
	}
	awaitSignal(t, watched, "recreate watcher cleanup")
	creations := f.spawnCount()

	if _, err := p.Post(ctx, id, nil, "session/load", initPayload); !errors.Is(err, ErrReinitialize) {
		t.Fatalf("non-initialize on exited server: got %v, want ErrReinitialize", err)
	}
	if got := f.spawnCount(); got != creations {
		t.Fatalf("non-initialize spawned: creations = %d, want %d", got, creations)
	}

	other := "beta"
	if _, err := p.Post(ctx, id, &other, "initialize", initPayload); !errors.Is(err, ErrAgentConflict) {
		t.Fatalf("conflicting agent on exited server: got %v, want ErrAgentConflict", err)
	}
	server, err := store.Server(ctx, id)
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Fatalf("status after conflict = %q, want exited", server.Status)
	}

	var statusAtSpawn acpstore.Status
	var statusMu sync.Mutex
	f.setOnCreate(func(ctx context.Context, store *acpstore.Store, serverID string, _ acpruntime.LaunchSpec) error {
		row, readErr := store.Server(ctx, serverID)
		if readErr != nil {
			return readErr
		}
		statusMu.Lock()
		statusAtSpawn = row.Status
		statusMu.Unlock()
		return store.SetLive(ctx, serverID, 4242)
	})

	recPayload := json.RawMessage(`{"jsonrpc":"2.0","id":9,"method":"initialize","params":{"clientInfo":{}}}`)
	if _, err := p.Post(ctx, id, nil, "initialize", recPayload); err != nil {
		t.Fatalf("initialize recreation: %v", err)
	}
	if got := f.spawnCount(); got != creations+1 {
		t.Fatalf("creations = %d, want %d", got, creations+1)
	}
	statusMu.Lock()
	creating := statusAtSpawn
	statusMu.Unlock()
	if creating != acpstore.StatusCreating {
		t.Fatalf("status at spawn = %q, want creating", creating)
	}
	if spec := f.lastSpec(); spec.Program != "/usr/bin/alpha-bin" {
		t.Fatalf("recreated spec = %+v, want stored agent alpha", spec)
	}
	server, err = store.Server(ctx, id)
	if err != nil {
		t.Fatalf("server after recreation: %v", err)
	}
	if server.Agent != agent {
		t.Fatalf("agent = %q, want %q", server.Agent, agent)
	}
	if server.Status != acpstore.StatusIdle {
		t.Fatalf("status after recreation = %q, want idle", server.Status)
	}
	if !bytes.Equal(requireRuntime(t, f).payload(), recPayload) {
		t.Fatalf("recreation payload = %s, want %s", requireRuntime(t, f).payload(), recPayload)
	}
	if liveInstance(p, id) == nil {
		t.Fatal("no live instance after recreation")
	}
}

func TestActivityLeaseReleaseOnSuccessAndError(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "lease", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("create: %v", err)
	}
	inst := requireInstance(t, p, "lease")
	rt := requireRuntime(t, f)

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "error", err: acpruntime.ErrRequestTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt.setErr(tc.err)
			_, err := p.Post(ctx, "lease", nil, "session/prompt", initPayload)
			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want %v", err, tc.err)
			}
			inst.lock.mu.Lock()
			activity := inst.activity
			inst.lock.mu.Unlock()
			if activity != 0 {
				t.Fatalf("activity = %d, want 0 after Post returns", activity)
			}
		})
	}
}

func TestActivityLeaseDeleteGateWaitsForRelease(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "gate", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("create: %v", err)
	}
	inst := requireInstance(t, p, "gate")
	entered := make(chan struct{})
	release := make(chan struct{})
	requireRuntime(t, f).setHook(func() {
		close(entered)
		<-release
	})

	done := make(chan error, 1)
	go func() {
		_, err := p.Post(ctx, "gate", nil, "session/prompt", initPayload)
		done <- err
	}()
	<-entered

	inst.lock.mu.Lock()
	activity := inst.activity
	inst.terminating = true
	inst.lock.mu.Unlock()
	if activity != 1 {
		t.Fatalf("activity = %d, want 1 while Post is in flight", activity)
	}

	waited := make(chan error, 1)
	go func() {
		inst.lock.mu.Lock()
		defer inst.lock.mu.Unlock()
		waited <- inst.waitActivityZero(ctx)
	}()

	select {
	case err := <-waited:
		t.Fatalf("waitActivityZero returned early: %v", err)
	default:
	}

	if _, err := p.Post(ctx, "gate", &agent, "initialize", initPayload); !errors.Is(err, ErrDeleting) {
		t.Fatalf("gated post: got %v, want ErrDeleting", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight post: %v", err)
	}
	if err := <-waited; err != nil {
		t.Fatalf("waitActivityZero: %v", err)
	}

	inst.lock.mu.Lock()
	activity = inst.activity
	inst.lock.mu.Unlock()
	if activity != 0 {
		t.Fatalf("activity = %d, want 0", activity)
	}
	select {
	case <-inst.zero:
	default:
		t.Fatal("drained channel is not closed at zero activity")
	}
}

func TestActivityLeaseCountsConcurrentPosts(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, err := p.Post(ctx, "concurrent-lease", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("create: %v", err)
	}
	inst := requireInstance(t, p, "concurrent-lease")

	const n = 4
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()

	requireRuntime(t, f).setHook(func() {
		entered <- struct{}{}
		<-release
	})

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Post(ctx, "concurrent-lease", nil, "session/prompt", initPayload)
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d posts became active", i, n)
		}
	}
	inst.lock.mu.Lock()
	activity := inst.activity
	inst.lock.mu.Unlock()
	if activity != n {
		t.Fatalf("activity = %d, want %d", activity, n)
	}
	releaseAll()
	wg.Wait()
}

func TestProxyDeleteUnknownServer(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)

	if err := p.Delete(t.Context(), "ghost"); !errors.Is(err, acpstore.ErrNotFound) {
		t.Fatalf("Delete unknown = %v, want ErrNotFound", err)
	}
}

// TestProxyDeleteRemovesDurableStateAndClosesSubscriptions proves the happy
// path prunes server/session/event rows, removes the live instance, closes the
// subscription, and permits a fresh first POST with a newly supplied agent.
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

// TestProxyLivePID proves LivePID returns the current generation's runtime PID,
// never the stale durable PID, and refuses gated or exited generations.
func TestProxyLivePID(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	ctx := t.Context()
	agent := "alpha"

	if _, ok := p.LivePID("ghost"); ok {
		t.Fatal("LivePID reported an unknown server as live")
	}

	if _, err := p.Post(ctx, "live-pid", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("create: %v", err)
	}
	inst := requireInstance(t, p, "live-pid")
	// The durable PID written by liveOnCreate is 4242; the runtime's own PID is
	// the only value LivePID may expose.
	requireRuntime(t, f).setPID(7777)

	if pid, ok := p.LivePID("live-pid"); !ok || pid != 7777 {
		t.Fatalf("LivePID = (%d,%v), want (7777,true)", pid, ok)
	}

	t.Run("gated generations are not live", func(t *testing.T) {
		gates := []struct {
			name  string
			set   func()
			clear func()
		}{
			{name: "terminating", set: func() { inst.terminating = true }, clear: func() { inst.terminating = false }},
			{name: "deleting", set: func() { inst.deleting = true }, clear: func() { inst.deleting = false }},
			{name: "detached", set: func() { inst.detached = true }, clear: func() { inst.detached = false }},
			{name: "closed", set: func() { inst.closed = true }, clear: func() { inst.closed = false }},
		}
		for _, gate := range gates {
			t.Run(gate.name, func(t *testing.T) {
				inst.lock.mu.Lock()
				gate.set()
				inst.lock.mu.Unlock()
				if _, ok := p.LivePID("live-pid"); ok {
					t.Fatalf("LivePID reported a %s generation as live", gate.name)
				}
				inst.lock.mu.Lock()
				gate.clear()
				inst.lock.mu.Unlock()
			})
		}
	})

	t.Run("current generation owns the PID", func(t *testing.T) {
		replacement := p.newInstance("live-pid", agent, inst.lock)
		rt := newFakeRuntime()
		rt.setPID(8888)
		replacement.runtime = rt
		p.mu.Lock()
		p.live["live-pid"] = replacement
		p.mu.Unlock()
		t.Cleanup(func() {
			p.mu.Lock()
			p.live["live-pid"] = inst
			p.mu.Unlock()
			rt.exit(nil)
		})
		if pid, ok := p.LivePID("live-pid"); !ok || pid != 8888 {
			t.Fatalf("LivePID = (%d,%v), want (8888,true)", pid, ok)
		}
	})

	t.Run("exited runtime is not live", func(t *testing.T) {
		watched := requireInstance(t, p, "live-pid").watched
		requireRuntime(t, f).exit(nil)
		awaitSignal(t, watched, "live-pid watcher cleanup")
		if _, ok := p.LivePID("live-pid"); ok {
			t.Fatal("LivePID reported an exited runtime as live")
		}
	})
}

// TestProxyStderrRetainedAfterRuntimeExit proves the exiting generation's
// redacted stderr tail outlives the watcher's live-slot release long enough for
// HTTP error mapping, and is released once consumed.
func TestProxyStderrRetainedAfterRuntimeExit(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	startServer(t, p, f, "stderr-retain")
	rt := requireRuntime(t, f)
	const tail = "fatal: password=[REDACTED]\n"
	rt.setStderr(tail)

	watched := requireInstance(t, p, "stderr-retain").watched
	rt.exit(nil)
	awaitSignal(t, watched, "watcher cleanup")

	if got := p.Stderr("stderr-retain"); got != tail {
		t.Fatalf("Stderr after exit = %q, want retained %q", got, tail)
	}
	if got := p.Stderr("stderr-retain"); got != "" {
		t.Fatalf("Stderr after consumption = %q, want empty", got)
	}
}

// TestProxyStderrReplacementDropsRetainedTail proves publishing a replacement
// generation releases the prior generation's retained tail.
func TestProxyStderrReplacementDropsRetainedTail(t *testing.T) {
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, _ := newProxyForTest(t, f)
	startServer(t, p, f, "stderr-replace")
	rt := requireRuntime(t, f)
	rt.setStderr("old generation failure\n")

	watched := requireInstance(t, p, "stderr-replace").watched
	rt.exit(nil)
	awaitSignal(t, watched, "watcher cleanup")

	agent := "alpha"
	if _, err := p.Post(t.Context(), "stderr-replace", &agent, "initialize", initPayload); err != nil {
		t.Fatalf("reinitialize: %v", err)
	}
	replacement := requireInstance(t, p, "stderr-replace")
	watched = replacement.watched
	f.lastRuntime().exit(nil)
	awaitSignal(t, watched, "replacement watcher cleanup")

	if got := p.Stderr("stderr-replace"); got != "" {
		t.Fatalf("Stderr after replacement exit = %q, want the old tail released", got)
	}
}

// TestProxyPersistenceFailuresClassified proves proxy-owned persistence
// failures carry acpruntime.ErrPersistence (HTTP 507) while lifecycle
// sentinels and process-spawn failures stay untouched.
func TestProxyPersistenceFailuresClassified(t *testing.T) {
	agent := "alpha"
	tests := []struct {
		name   string
		setup  func(t *testing.T, p *Proxy, f *testFactory)
		want   error
		reject error
	}{
		{
			name: "server read",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(context.Context, string) (acpstore.Server, error) {
					return acpstore.Server{}, errBoom
				}
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "create server",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(context.Context, string) (acpstore.Server, error) {
					return acpstore.Server{}, acpstore.ErrNotFound
				}
				p.createServer = func(context.Context, string, string) (acpstore.Server, error) {
					return acpstore.Server{}, errBoom
				}
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "set status",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(_ context.Context, serverID string) (acpstore.Server, error) {
					return acpstore.Server{ServerID: serverID, Agent: agent}, nil
				}
				p.setStatus = func(context.Context, string, acpstore.Status) error {
					return errBoom
				}
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "startup live publication",
			setup: func(_ *testing.T, _ *Proxy, f *testFactory) {
				f.setOnCreate(func(context.Context, *acpstore.Store, string, acpruntime.LaunchSpec) error {
					return errBoom
				})
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "spawn failure stays a process failure",
			setup: func(_ *testing.T, _ *Proxy, f *testFactory) {
				f.setOnCreate(func(context.Context, *acpstore.Store, string, acpruntime.LaunchSpec) error {
					return &exec.Error{Name: "alpha-bin", Err: exec.ErrNotFound}
				})
			},
			want:   exec.ErrNotFound,
			reject: acpruntime.ErrPersistence,
		},
		{
			name: "create conflict preserved",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(context.Context, string) (acpstore.Server, error) {
					return acpstore.Server{}, acpstore.ErrNotFound
				}
				p.createServer = func(context.Context, string, string) (acpstore.Server, error) {
					return acpstore.Server{}, fmt.Errorf("%w: duplicate", acpstore.ErrConflict)
				}
			},
			want:   acpstore.ErrConflict,
			reject: acpruntime.ErrPersistence,
		},
		{
			name: "set status not found preserved",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(_ context.Context, serverID string) (acpstore.Server, error) {
					return acpstore.Server{ServerID: serverID, Agent: agent}, nil
				}
				p.setStatus = func(context.Context, string, acpstore.Status) error {
					return fmt.Errorf("%w: gone", acpstore.ErrNotFound)
				}
			},
			want:   acpstore.ErrNotFound,
			reject: acpruntime.ErrPersistence,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTestFactory(t)
			p, _ := newProxyForTest(t, f)
			tc.setup(t, p, f)

			_, err := p.Post(t.Context(), "persistence", &agent, "initialize", initPayload)
			if err == nil {
				t.Fatal("Post succeeded, want a classified failure")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("Post error = %v, want %v", err, tc.want)
			}
			if tc.reject != nil && errors.Is(err, tc.reject) {
				t.Fatalf("Post error = %v, must not match %v", err, tc.reject)
			}
		})
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

// TestAllowServerSubsRejectsActiveDelete pins the atomic marker/gate contract:
// while DELETE is active no new subscription may be allowed, and once the
// delete gate clears the fresh generation may accept subscriptions again.
func TestAllowServerSubsRejectsActiveDelete(t *testing.T) {
	f := newTestFactory(t)
	p, _ := newProxyForTest(t, f)
	const id = "allow-delete"

	p.markDeleting(id)
	if p.allowServerSubs(id) {
		t.Fatal("allowServerSubs accepted a server with an active DELETE")
	}
	p.clearDeleting(id)
	if !p.allowServerSubs(id) {
		t.Fatal("allowServerSubs rejected after the delete gate cleared")
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

package acpproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
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

// Stderr exposes the configured redacted tail required by the runtime
// interface the proxy consults for 502 problem extensions.
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

func startServer(t *testing.T, p *Proxy, f *testFactory, id string) *fakeRuntime {
	t.Helper()
	agent := "alpha"
	if _, err := p.Post(t.Context(), id, &agent, "initialize", initPayload); err != nil {
		t.Fatalf("start server %q: %v", id, err)
	}
	return requireRuntime(t, f)
}

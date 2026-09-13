package acpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

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

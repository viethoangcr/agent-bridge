package acpproxy

import (
	"testing"
)

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

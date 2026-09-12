package acpruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestPostRequestTimeoutBroadcastStaysConsistent proves the one-shot request
// deadline reaches the write waiter even when the manager consumes it, so a
// blocked writer cannot keep Post past the deadline and the runtime stays
// consistent with the correlation released.
func TestPostRequestTimeoutBroadcastStaysConsistent(t *testing.T) {
	gate := make(chan struct{})
	gateEntered := make(chan struct{})
	var once sync.Once
	var r *Runtime
	r = newWriterRuntime(t, 150*time.Millisecond, func(p []byte) (int, error) {
		if bytes.Equal(p, []byte("gate\n")) {
			once.Do(func() { close(gateEntered) })
			select {
			case <-gate:
				return len(p), nil
			case <-r.writerStop:
				return 0, ErrExited
			}
		}
		return len(p), nil
	})

	r.writerQueue <- newWriteItem([]byte("gate\n"))
	<-gateEntered

	start := time.Now()
	_, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
	if !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("Post with blocked writer error = %v, want request timeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Post returned after %s, want at the request deadline", elapsed)
	}

	r.corrMu.Lock()
	pending := len(r.corr)
	r.corrMu.Unlock()
	if pending != 0 {
		t.Fatalf("correlations = %d, want 0 after the deadline", pending)
	}
	if r.poisoned.Load() {
		t.Fatal("pre-write deadline poisoned the runtime")
	}

	close(gate)
	res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"b"}`))
	if err != nil || !res.Accepted {
		t.Fatalf("Post after the deadline = (%+v, %v), want accepted", res, err)
	}
}

// TestPostRequestDeadlineBroadcast proves the manager closes the broadcast
// deadline channel, so admission/write waiters observe the deadline even when
// the manager consumed the one-shot timer value.
func TestPostRequestDeadlineBroadcast(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	entry, err := h.r.reserve(Pending{ID: json.RawMessage("1")})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	h.deadlines.last().fire()

	select {
	case <-entry.expired:
	case <-time.After(5 * time.Second):
		t.Fatal("the manager did not broadcast the request deadline")
	}
}

func TestLifecycleGraceRetainsReservation(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	done := goRequest(h, context.Background(),
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}`)
	entry := h.waitWritten(t, numericKey(1))

	deadlineTimer := h.deadlines.last()
	if deadlineTimer == nil {
		t.Fatal("no deadline timer was created")
	}
	deadlineTimer.fire()

	if got := <-done; !errors.Is(got.err, ErrRequestTimeout) {
		t.Fatalf("lifecycle timeout error = %v, want ErrRequestTimeout", got.err)
	}
	if entry.state != corrGrace {
		t.Fatalf("state = %v, want grace", entry.state)
	}
	if entry.cwd == nil || *entry.cwd != "/w" {
		t.Errorf("grace entry lost cwd metadata: %v", entry.cwd)
	}
	if entry.sessionID != nil {
		t.Errorf("grace entry invented a session id: %v", *entry.sessionID)
	}
	if _, err := h.r.reserve(Pending{ID: json.RawMessage("1"), Lifecycle: LifecycleNew}); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("duplicate during grace error = %v, want ErrDuplicateID", err)
	}
	if h.corrLen() != 1 {
		t.Fatalf("corr len = %d, want 1 (grace retained)", h.corrLen())
	}
	h.waitStatus(t, acpstore.StatusBusy)

	// A late successful response within grace still commits session state.
	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{"sessionId":"late-1"}}`))
	if _, err := h.store.Session(context.Background(), "srv", "late-1"); err != nil {
		t.Fatalf("late lifecycle response did not mutate the session: %v", err)
	}
	h.waitStatus(t, acpstore.StatusIdle)
}

func TestNonLifecycleTimeoutReleasesImmediately(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	done := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	h.waitWritten(t, numericKey(1))

	h.deadlines.last().fire()
	if got := <-done; !errors.Is(got.err, ErrRequestTimeout) {
		t.Fatalf("timeout error = %v, want ErrRequestTimeout", got.err)
	}
	if h.corrLen() != 0 {
		t.Fatalf("corr len = %d, want 0 after non-lifecycle timeout", h.corrLen())
	}
	if _, err := h.r.reserve(Pending{ID: json.RawMessage("1")}); err != nil {
		t.Fatalf("capacity not released after timeout: %v", err)
	}
}

func TestGraceCommitCancelsExpiry(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	done := goRequest(h, context.Background(),
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}`)
	entry := h.waitWritten(t, numericKey(1))
	h.deadlines.last().fire()
	if got := <-done; !errors.Is(got.err, ErrRequestTimeout) {
		t.Fatalf("timeout error = %v", got.err)
	}

	appendGate := make(chan struct{})
	appendEntered := make(chan struct{})
	var once sync.Once
	h.r.appendOutput = func(context.Context, string, acpstore.Output) (acpstore.Event, error) {
		once.Do(func() { close(appendEntered) })
		<-appendGate
		return acpstore.Event{Seq: 1}, nil
	}
	go func() { h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s"}}`)) }()
	<-appendEntered

	// The grace timer created on timeout must be stopped by the commit.
	if gt := h.graces.last(); gt == nil || !gt.isStopped() {
		t.Fatal("grace timer was not canceled when the response committed")
	}
	if entry.state != corrCommitting {
		t.Fatalf("state = %v, want committing", entry.state)
	}
	close(appendGate)
}

// TestLifecycleGraceExpiryClosesGateBeforeClearing proves grace expiry closes
// the admission gate before clearing the retained grace entry, so concurrent
// posts cannot reuse the ID or write through the teardown.
func TestLifecycleGraceExpiryClosesGateBeforeClearing(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	close(h.r.done) // Kill must not wait for a process that never exists.

	done := goRequest(h, context.Background(),
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}`)
	h.waitWritten(t, numericKey(1))
	h.deadlines.last().fire()
	if got := <-done; !errors.Is(got.err, ErrRequestTimeout) {
		t.Fatalf("lifecycle timeout error = %v, want ErrRequestTimeout", got.err)
	}

	stop := startPostHammer(h.r, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}`, 8)
	h.graces.last().fire()

	deadline := time.Now().Add(5 * time.Second)
	for !h.r.exited.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !h.r.exited.Load() {
		t.Fatal("grace expiry did not close the admission gate")
	}

	for _, err := range stop() {
		if !errors.Is(err, ErrDuplicateID) && !errors.Is(err, ErrExited) {
			t.Fatalf("concurrent post error = %v, want ErrDuplicateID or ErrExited", err)
		}
	}
	if h.corrLen() != 0 {
		t.Fatalf("correlations not cleared: %d", h.corrLen())
	}
	if records := h.writer.all(); len(records) != 1 {
		t.Fatalf("writer records = %d, want 1 (no ID reuse write)", len(records))
	}
	if got := h.status(t); got != acpstore.StatusExited {
		t.Fatalf("status = %q, want exited", got)
	}
	select {
	case <-h.r.Events():
		t.Fatal("wakeup signaled for uncommitted output")
	default:
	}
	if _, err := h.r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/w"}}`)); !errors.Is(err, ErrExited) {
		t.Fatalf("post-terminal post = %v, want ErrExited", err)
	}
	if records := h.writer.all(); len(records) != 1 {
		t.Fatalf("post-terminal writer records = %d, want 1", len(records))
	}
}

package acpruntime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestCommittingHoldsAccountingUntilAppendCommits(t *testing.T) {
	tests := []struct {
		name      string
		lifecycle Lifecycle
		response  string
		wantMut   bool
	}{
		{name: "success lifecycle", lifecycle: LifecycleNew, response: `{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s-late"}}`, wantMut: true},
		{name: "jsonrpc error", lifecycle: LifecycleNew, response: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603}}`},
		{name: "malformed lifecycle success", lifecycle: LifecycleNew, response: `{"jsonrpc":"2.0","id":1,"result":{}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newCorrelationHarness(t, time.Minute)
			payload := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/c"}}`
			if tt.lifecycle == LifecycleNone {
				payload = `{"jsonrpc":"2.0","id":1,"method":"initialize"}`
			}
			done := goRequest(h, context.Background(), payload)
			entry := h.waitWritten(t, numericKey(1))

			appendGate := make(chan struct{})
			appendEntered := make(chan struct{})
			var once sync.Once
			h.r.appendOutput = func(context.Context, string, acpstore.Output) (acpstore.Event, error) {
				once.Do(func() { close(appendEntered) })
				<-appendGate
				return acpstore.Event{Seq: 1}, nil
			}

			handleDone := make(chan struct{})
			go func() {
				h.r.handleOutput([]byte(tt.response))
				close(handleDone)
			}()

			select {
			case <-appendEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("append was never entered")
			}

			h.r.corrMu.Lock()
			state := entry.state
			h.r.corrMu.Unlock()
			if state != corrCommitting {
				t.Fatalf("state = %v, want committing", state)
			}
			if _, err := h.r.reserve(Pending{ID: json.RawMessage("1")}); !errors.Is(err, ErrDuplicateID) {
				t.Fatalf("duplicate id during commit error = %v, want ErrDuplicateID", err)
			}
			if h.corrLen() != 1 {
				t.Fatalf("committing entry released its slot early: corr=%d", h.corrLen())
			}
			h.waitStatus(t, acpstore.StatusBusy)

			select {
			case got := <-done:
				t.Fatalf("waiter returned before commit: %+v", got)
			case <-time.After(30 * time.Millisecond):
			}
			select {
			case <-h.r.Events():
				t.Fatal("wakeup signaled before the output committed")
			default:
			}

			close(appendGate)
			select {
			case <-handleDone:
			case <-time.After(5 * time.Second):
				t.Fatal("commit did not finish")
			}

			got := <-done
			if got.err != nil {
				t.Fatalf("Post error = %v", got.err)
			}
			if h.corrLen() != 0 {
				t.Fatalf("corr len = %d, want 0 after commit", h.corrLen())
			}
			h.waitStatus(t, acpstore.StatusIdle)
			select {
			case <-h.r.Events():
			default:
				t.Fatal("wakeup was not signaled after commit")
			}
		})
	}
}

func TestStatusReconcileNeverWritesStaleIdle(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)

	doneA := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	h.waitWritten(t, numericKey(1))

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.r.beforeReconcileStatus = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	go func() { h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("completion never reached the pre-reconcile hook")
	}

	// A new Post reserves the only free slot and reconciles busy while the
	// older completion is paused before its own reconcile.
	doneB := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":2,"method":"initialize"}`)
	h.waitWritten(t, numericKey(2))
	close(release)

	if got := <-doneA; got.err != nil {
		t.Fatalf("Post A error = %v", got.err)
	}
	if h.entry(t, numericKey(2)) == nil {
		t.Fatal("Post B entry disappeared while waiting")
	}
	h.waitStatus(t, acpstore.StatusBusy)

	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))
	<-doneB
	h.waitStatus(t, acpstore.StatusIdle)
}

func TestStatusPersistenceFailureFailsWaiters(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	if err := h.store.Close(context.Background()); err != nil {
		t.Fatalf("close store: %v", err)
	}

	_, err := h.r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
	if !errors.Is(err, ErrPersistence) {
		t.Fatalf("Post after status failure = %v, want ErrPersistence", err)
	}
	if !h.r.exited.Load() {
		t.Fatal("runtime was not marked exited after status persistence failure")
	}
	if h.corrLen() != 0 {
		t.Fatalf("correlations not cleared: %d", h.corrLen())
	}
}

func TestAppendFailureFailsWaiterWithPersistence(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	done := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	if entry := h.waitWritten(t, numericKey(1)); entry == nil {
		t.Fatal("no entry")
	}
	h.r.appendOutput = func(context.Context, string, acpstore.Output) (acpstore.Event, error) {
		return acpstore.Event{}, errors.New("boom")
	}
	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))

	got := <-done
	if !errors.Is(got.err, ErrPersistence) {
		t.Fatalf("append failure error = %v, want ErrPersistence", got.err)
	}
	if !h.r.exited.Load() {
		t.Fatal("runtime was not marked exited after append failure")
	}
	if h.corrLen() != 0 {
		t.Fatalf("correlations not cleared: %d", h.corrLen())
	}
}

// TestPostAppendFailureClosesGateBeforeClearing proves a failed AppendOutput
// closes the admission gate before the committing entry is cleared: concurrent
// posts cannot reuse the ID or write, no wakeup exposes uncommitted output, and
// the waiter observes ErrPersistence.
func TestPostAppendFailureClosesGateBeforeClearing(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	done := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	h.waitWritten(t, numericKey(1))

	appendGate := make(chan struct{})
	appendEntered := make(chan struct{})
	var once sync.Once
	h.r.appendOutput = func(context.Context, string, acpstore.Output) (acpstore.Event, error) {
		once.Do(func() { close(appendEntered) })
		<-appendGate
		return acpstore.Event{}, errors.New("boom")
	}
	go func() { h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`)) }()
	select {
	case <-appendEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("append was never entered")
	}

	stop := startPostHammer(h.r, `{"jsonrpc":"2.0","id":1,"method":"initialize"}`, 8)

	close(appendGate)
	if got := <-done; !errors.Is(got.err, ErrPersistence) {
		t.Fatalf("append failure error = %v, want ErrPersistence", got.err)
	}
	if !h.r.exited.Load() {
		t.Fatal("admission gate was not closed before the committing entry cleared")
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
	select {
	case <-h.r.Events():
		t.Fatal("wakeup signaled for uncommitted output")
	default:
	}
	for _, id := range []string{"1", "2"} {
		payload := `{"jsonrpc":"2.0","id":` + id + `,"method":"initialize"}`
		if _, err := h.r.Post(context.Background(), []byte(payload)); !errors.Is(err, ErrExited) {
			t.Fatalf("post-terminal Post(%s) = %v, want ErrExited", id, err)
		}
	}
	if records := h.writer.all(); len(records) != 1 {
		t.Fatalf("post-terminal writer records = %d, want 1", len(records))
	}
}

package acpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// fakeTicker is a manually advanced fallback ticker. Tests advance it without
// sleeping.
type fakeTicker struct {
	ch chan time.Time

	mu    sync.Mutex
	stops int
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time, 1)}
}

func (f *fakeTicker) C() <-chan time.Time { return f.ch }

func (f *fakeTicker) Stop() {
	f.mu.Lock()
	f.stops++
	f.mu.Unlock()
}

func (f *fakeTicker) tick() {
	select {
	case f.ch <- time.Now():
	default:
	}
}

func (f *fakeTicker) isStopped() bool { return f.stopCount() > 0 }

func (f *fakeTicker) stopCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stops
}

func newSubscriptionProxy(t *testing.T) (*Proxy, *acpstore.Store, *testFactory) {
	t.Helper()
	f := newTestFactory(t)
	f.setOnCreate(liveOnCreate)
	p, store := newProxyForTest(t, f)
	return p, store, f
}

// commitEvents appends n events directly to SQLite, proving the subscription's
// source of truth is the store rather than any wakeup payload.
func commitEvents(t *testing.T, store *acpstore.Store, serverID string, n int) []acpstore.Event {
	t.Helper()
	events := make([]acpstore.Event, 0, n)
	for i := 0; i < n; i++ {
		payload := json.RawMessage(fmt.Sprintf(`{"n":%d}`, i+1))
		event, err := store.AppendOutput(t.Context(), serverID, acpstore.Output{
			Kind:    "notification",
			Payload: payload,
		})
		if err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
		events = append(events, event)
	}
	return events
}

// nextWithin reads one event under a bounded guard so a broken subscription
// fails instead of hanging the suite.
func nextWithin(t *testing.T, sub Subscription) (acpstore.Event, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	return sub.Next(ctx)
}

func requireEvents(t *testing.T, sub Subscription, want []acpstore.Event) {
	t.Helper()
	for i, expected := range want {
		event, err := nextWithin(t, sub)
		if err != nil {
			t.Fatalf("event %d (seq %d): %v", i, expected.Seq, err)
		}
		if event.Seq != expected.Seq {
			t.Fatalf("event %d seq = %d, want %d", i, event.Seq, expected.Seq)
		}
		if !bytes.Equal(event.Payload, expected.Payload) {
			t.Fatalf("event %d payload = %s, want %s", i, event.Payload, expected.Payload)
		}
	}
}

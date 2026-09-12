package acpstore

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestEventsQuery(t *testing.T) {
	clock := newFakeClock(1_000)
	store := openWithClock(t, filepath.Join(t.TempDir(), "events.db"), clock.Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	seedSession(t, store, "srv", "s1", "/one", 1)
	seedSession(t, store, "srv", "s2", "/two", 1)

	seed := []Output{
		{Kind: "notification", Payload: json.RawMessage(`{"n":1}`), SessionID: stringPtr("s1")},
		{Kind: "request", Method: stringPtr("fs/read"), Payload: json.RawMessage(`{"n":2}`), SessionID: stringPtr("s1")},
		{Kind: "response", Payload: json.RawMessage(`{"n":3}`), SessionID: stringPtr("s2")},
		{Kind: "notification", Payload: json.RawMessage(`{"n":4}`)},
	}
	for i, output := range seed {
		if _, err := store.AppendOutput(t.Context(), "srv", output); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	seqOf := func(events []Event) []int64 {
		got := make([]int64, len(events))
		for i, event := range events {
			got[i] = event.Seq
		}
		return got
	}

	t.Run("AscendingAfter", func(t *testing.T) {
		events, err := store.Events(t.Context(), "srv", EventQuery{After: 2})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if want := []int64{3, 4}; !reflect.DeepEqual(seqOf(events), want) {
			t.Errorf("asc after 2 = %v, want %v", seqOf(events), want)
		}
	})

	t.Run("Descending", func(t *testing.T) {
		events, err := store.Events(t.Context(), "srv", EventQuery{Desc: true})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if want := []int64{4, 3, 2, 1}; !reflect.DeepEqual(seqOf(events), want) {
			t.Errorf("desc = %v, want %v", seqOf(events), want)
		}
	})

	t.Run("Limit", func(t *testing.T) {
		events, err := store.Events(t.Context(), "srv", EventQuery{Limit: 2})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if want := []int64{1, 2}; !reflect.DeepEqual(seqOf(events), want) {
			t.Errorf("limit 2 = %v, want %v", seqOf(events), want)
		}
	})

	t.Run("AfterAndLimit", func(t *testing.T) {
		events, err := store.Events(t.Context(), "srv", EventQuery{After: 1, Limit: 1})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if want := []int64{2}; !reflect.DeepEqual(seqOf(events), want) {
			t.Errorf("after 1 limit 1 = %v, want %v", seqOf(events), want)
		}
	})

	t.Run("SessionFilter", func(t *testing.T) {
		events, err := store.Events(t.Context(), "srv", EventQuery{SessionID: stringPtr("s1")})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if want := []int64{1, 2}; !reflect.DeepEqual(seqOf(events), want) {
			t.Errorf("session s1 = %v, want %v", seqOf(events), want)
		}
	})

	t.Run("Deterministic", func(t *testing.T) {
		first, err := store.Events(t.Context(), "srv", EventQuery{})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		second, err := store.Events(t.Context(), "srv", EventQuery{})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Errorf("Events not deterministic: %v vs %v", first, second)
		}
	})

	t.Run("DefensivePayloadCopy", func(t *testing.T) {
		events, err := store.Events(t.Context(), "srv", EventQuery{After: 0, Limit: 1})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		events[0].Payload[0] = 'X'
		again, err := store.Events(t.Context(), "srv", EventQuery{After: 0, Limit: 1})
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		if string(again[0].Payload) != `{"n":1}` {
			t.Errorf("stored payload mutated through returned copy: %q", again[0].Payload)
		}
	})

	t.Run("UnknownServer", func(t *testing.T) {
		if _, err := store.Events(t.Context(), "nope", EventQuery{}); !errors.Is(err, ErrNotFound) {
			t.Errorf("Events(unknown server) error = %v, want ErrNotFound", err)
		}
	})

	t.Run("UnknownSession", func(t *testing.T) {
		if _, err := store.Events(t.Context(), "srv", EventQuery{SessionID: stringPtr("missing")}); !errors.Is(err, ErrNotFound) {
			t.Errorf("Events(unknown session) error = %v, want ErrNotFound", err)
		}
	})

	t.Run("NegativeAfter", func(t *testing.T) {
		if _, err := store.Events(t.Context(), "srv", EventQuery{After: -1}); !errors.Is(err, ErrValidation) {
			t.Errorf("Events(after -1) error = %v, want ErrValidation", err)
		}
	})

	t.Run("NegativeLimit", func(t *testing.T) {
		if _, err := store.Events(t.Context(), "srv", EventQuery{Limit: -1}); !errors.Is(err, ErrValidation) {
			t.Errorf("Events(limit -1) error = %v, want ErrValidation", err)
		}
	})
}

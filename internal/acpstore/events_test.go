package acpstore

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func stringPtr(v string) *string { return &v }

func notificationPayload(body string) Output {
	return Output{Kind: "notification", Payload: json.RawMessage(body)}
}

func TestAppendOutputSequenceAndWatermark(t *testing.T) {
	clock := newFakeClock(10_000)
	store := openWithClock(t, filepath.Join(t.TempDir(), "append.db"), clock.Now)

	for _, id := range []string{"srv-a", "srv-b"} {
		if _, err := store.CreateServer(t.Context(), id, "claude"); err != nil {
			t.Fatalf("CreateServer(%q): %v", id, err)
		}
	}

	clock.advanceMs(5)
	firstA, err := store.AppendOutput(t.Context(), "srv-a", notificationPayload(`{"jsonrpc":"2.0"}`))
	if err != nil {
		t.Fatalf("first AppendOutput(srv-a): %v", err)
	}
	if firstA.Seq != 1 {
		t.Errorf("first srv-a seq = %d, want 1", firstA.Seq)
	}
	if firstA.ServerID != "srv-a" || firstA.CreatedAtMs != 10_005 {
		t.Errorf("first srv-a event = %+v, want server srv-a created 10005", firstA)
	}

	firstB, err := store.AppendOutput(t.Context(), "srv-b", notificationPayload(`{"jsonrpc":"2.0"}`))
	if err != nil {
		t.Fatalf("first AppendOutput(srv-b): %v", err)
	}
	if firstB.Seq != 1 {
		t.Errorf("first srv-b seq = %d, want 1 (independent from srv-a)", firstB.Seq)
	}

	clock.advanceMs(5)
	secondA, err := store.AppendOutput(t.Context(), "srv-a", notificationPayload(`{"jsonrpc":"2.0"}`))
	if err != nil {
		t.Fatalf("second AppendOutput(srv-a): %v", err)
	}
	if secondA.Seq != 2 {
		t.Errorf("second srv-a seq = %d, want 2", secondA.Seq)
	}

	srv, err := store.Server(t.Context(), "srv-a")
	if err != nil {
		t.Fatalf("Server(srv-a): %v", err)
	}
	if srv.LastEventSeq != 2 {
		t.Errorf("srv-a last_event_seq = %d, want 2", srv.LastEventSeq)
	}
	if srv.UpdatedAtMs != 10_010 {
		t.Errorf("srv-a updated_at_ms = %d, want 10010", srv.UpdatedAtMs)
	}
	if srv.CreatedAtMs != 10_000 {
		t.Errorf("srv-a created_at_ms = %d, want unchanged 10000", srv.CreatedAtMs)
	}
}

func TestAppendOutputPayloadRoundTrip(t *testing.T) {
	store := openWithClock(t, filepath.Join(t.TempDir(), "payload.db"), newFakeClock(1_000).Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}

	cases := []struct {
		name    string
		output  Output
		wantRaw string
	}{
		{
			name: "request",
			output: Output{
				Kind:      "request",
				Method:    stringPtr("fs/read"),
				SessionID: stringPtr("sess-1"),
				Payload:   json.RawMessage("{\n  \"method\" : \"fs/read\"\n}"),
			},
			wantRaw: "{\n  \"method\" : \"fs/read\"\n}",
		},
		{
			name: "response",
			output: Output{
				Kind:    "response",
				Payload: json.RawMessage(`{"id":1,"result":{"sessionId":"s"}}`),
			},
			wantRaw: `{"id":1,"result":{"sessionId":"s"}}`,
		},
		{
			name: "notification",
			output: Output{
				Kind:   "notification",
				Method: stringPtr("session/update"),
				Payload: json.RawMessage(
					`{"method":"session/update","params":{"sessionId":"s"}}`),
			},
			wantRaw: `{"method":"session/update","params":{"sessionId":"s"}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.output
			got, err := store.AppendOutput(t.Context(), "srv", want)
			if err != nil {
				t.Fatalf("AppendOutput: %v", err)
			}
			if got.Kind != want.Kind {
				t.Errorf("kind = %q, want %q", got.Kind, want.Kind)
			}
			if !rawEqual(got.Method, want.Method) {
				t.Errorf("method = %v, want %v", ptrDeref(got.Method), ptrDeref(want.Method))
			}
			if !rawEqual(got.SessionID, want.SessionID) {
				t.Errorf("sessionID = %v, want %v", ptrDeref(got.SessionID), ptrDeref(want.SessionID))
			}
			if string(got.Payload) != tc.wantRaw {
				t.Errorf("payload = %q, want exact %q", got.Payload, tc.wantRaw)
			}

			events, err := store.Events(t.Context(), "srv", EventQuery{After: got.Seq - 1, Limit: 1})
			if err != nil {
				t.Fatalf("Events: %v", err)
			}
			if len(events) != 1 {
				t.Fatalf("Events returned %d events, want 1", len(events))
			}
			if string(events[0].Payload) != tc.wantRaw {
				t.Errorf("stored payload = %q, want exact %q", events[0].Payload, tc.wantRaw)
			}
		})
	}

	if _, err := store.AppendOutput(t.Context(), "srv", Output{Kind: "notification", Payload: []byte{0xff, 0xfe}}); !errors.Is(err, ErrValidation) {
		t.Errorf("invalid UTF-8 payload error = %v, want ErrValidation", err)
	}
	assertRowCount(t, store, "events", len(cases))
}

func TestSequenceOverflow(t *testing.T) {
	var (
		_ int64 = Event{}.Seq
		_ int64 = EventQuery{}.After
		_ int64 = Server{}.LastEventSeq
	)

	clock := newFakeClock(1_000)
	store := openWithClock(t, filepath.Join(t.TempDir(), "overflow.db"), clock.Now)

	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	if _, err := store.db.ExecContext(t.Context(),
		`UPDATE servers SET last_event_seq = ? WHERE server_id = ?`,
		int64(math.MaxInt64-1), "srv"); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}

	clock.advanceMs(1)
	last, err := store.AppendOutput(t.Context(), "srv", notificationPayload(`{"last":true}`))
	if err != nil {
		t.Fatalf("append at MaxInt64: %v", err)
	}
	if last.Seq != math.MaxInt64 {
		t.Fatalf("append seq = %d, want %d", last.Seq, int64(math.MaxInt64))
	}

	before, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server: %v", err)
	}

	clock.advanceMs(1)
	_, err = store.AppendOutput(t.Context(), "srv", Output{
		Kind:     "response",
		Payload:  json.RawMessage(`{"overflow":true}`),
		Mutation: &SessionMutation{Lifecycle: "new", SessionID: "sess-x", CWD: "/x"},
	})
	if !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("overflow error = %v, want ErrSequenceExhausted", err)
	}

	after, err := store.Server(t.Context(), "srv")
	if err != nil {
		t.Fatalf("Server after overflow: %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Errorf("server mutated by rejected append: %+v, want %+v", after, before)
	}
	if after.LastEventSeq != math.MaxInt64 {
		t.Errorf("watermark = %d, want unchanged %d", after.LastEventSeq, int64(math.MaxInt64))
	}
	assertRowCount(t, store, "events", 1)
	assertRowCount(t, store, "server_sessions", 0)
}

func TestLifecycleCommit(t *testing.T) {
	t.Run("NewCreatesSession", func(t *testing.T) {
		clock := newFakeClock(2_000)
		store := openWithClock(t, filepath.Join(t.TempDir(), "new.db"), clock.Now)
		if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
			t.Fatalf("CreateServer: %v", err)
		}

		event, err := store.AppendOutput(t.Context(), "srv", Output{
			Kind:     "response",
			Payload:  json.RawMessage(`{"result":{"sessionId":"sess-new"}}`),
			Mutation: &SessionMutation{Lifecycle: "new", SessionID: "sess-new", CWD: "/work"},
		})
		if err != nil {
			t.Fatalf("AppendOutput: %v", err)
		}
		if event.Seq != 1 {
			t.Errorf("event seq = %d, want 1", event.Seq)
		}

		session, err := store.Session(t.Context(), "srv", "sess-new")
		if err != nil {
			t.Fatalf("Session: %v", err)
		}
		want := Session{ServerID: "srv", SessionID: "sess-new", CWD: "/work", CreatedAtMs: 2_000, UpdatedAtMs: 2_000}
		if session != want {
			t.Errorf("session = %+v, want %+v", session, want)
		}
	})

	t.Run("LoadResumeUpdateAndPreserveCreated", func(t *testing.T) {
		clock := newFakeClock(3_000)
		store := openWithClock(t, filepath.Join(t.TempDir(), "load.db"), clock.Now)
		if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
			t.Fatalf("CreateServer: %v", err)
		}
		seedSession(t, store, "srv", "s-load", "/old-load", 500)
		seedSession(t, store, "srv", "s-resume", "/old-resume", 600)

		if _, err := store.AppendOutput(t.Context(), "srv", Output{
			Kind:     "response",
			Payload:  json.RawMessage(`{"result":{}}`),
			Mutation: &SessionMutation{Lifecycle: "load", SessionID: "s-load", CWD: "/new-load"},
		}); err != nil {
			t.Fatalf("load AppendOutput: %v", err)
		}
		clock.advanceMs(10)
		if _, err := store.AppendOutput(t.Context(), "srv", Output{
			Kind:     "response",
			Payload:  json.RawMessage(`{"result":{}}`),
			Mutation: &SessionMutation{Lifecycle: "resume", SessionID: "s-resume", CWD: "/new-resume"},
		}); err != nil {
			t.Fatalf("resume AppendOutput: %v", err)
		}

		load, err := store.Session(t.Context(), "srv", "s-load")
		if err != nil {
			t.Fatalf("Session(load): %v", err)
		}
		if load.CWD != "/new-load" || load.CreatedAtMs != 500 || load.UpdatedAtMs != 3_000 {
			t.Errorf("load session = %+v, want cwd /new-load created 500 updated 3000", load)
		}
		resume, err := store.Session(t.Context(), "srv", "s-resume")
		if err != nil {
			t.Fatalf("Session(resume): %v", err)
		}
		if resume.CWD != "/new-resume" || resume.CreatedAtMs != 600 || resume.UpdatedAtMs != 3_010 {
			t.Errorf("resume session = %+v, want cwd /new-resume created 600 updated 3010", resume)
		}
	})

	t.Run("NilMutationCreatesNoSession", func(t *testing.T) {
		store := openWithClock(t, filepath.Join(t.TempDir(), "nil-mutation.db"), newFakeClock(4_000).Now)
		if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
			t.Fatalf("CreateServer: %v", err)
		}
		if _, err := store.AppendOutput(t.Context(), "srv", Output{
			Kind:      "response",
			Payload:   json.RawMessage(`{"result":{}}`),
			SessionID: stringPtr("ghost"),
		}); err != nil {
			t.Fatalf("AppendOutput: %v", err)
		}
		assertRowCount(t, store, "server_sessions", 0)
	})

	t.Run("FailureRollsBackEverything", func(t *testing.T) {
		clock := newFakeClock(5_000)
		store := openWithClock(t, filepath.Join(t.TempDir(), "rollback.db"), clock.Now)
		if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
			t.Fatalf("CreateServer: %v", err)
		}
		seedEvent(t, store, "srv", 2)
		if _, err := store.db.ExecContext(t.Context(),
			`UPDATE servers SET last_event_seq = 1 WHERE server_id = ?`, "srv"); err != nil {
			t.Fatalf("seed watermark: %v", err)
		}
		before, err := store.Server(t.Context(), "srv")
		if err != nil {
			t.Fatalf("Server: %v", err)
		}

		clock.advanceMs(1)
		_, err = store.AppendOutput(t.Context(), "srv", Output{
			Kind:     "response",
			Payload:  json.RawMessage(`{"result":{"sessionId":"sess-fail"}}`),
			Mutation: &SessionMutation{Lifecycle: "new", SessionID: "sess-fail", CWD: "/fail"},
		})
		if err == nil {
			t.Fatal("AppendOutput with colliding sequence succeeded, want constraint failure")
		}

		after, err := store.Server(t.Context(), "srv")
		if err != nil {
			t.Fatalf("Server after failure: %v", err)
		}
		if !reflect.DeepEqual(after, before) {
			t.Errorf("server mutated after rollback: %+v, want %+v", after, before)
		}
		assertRowCount(t, store, "events", 1)
		assertRowCount(t, store, "server_sessions", 0)
	})
}

func TestConcurrentAppend(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "concurrent.db"))
	if _, err := store.CreateServer(t.Context(), "srv", "claude"); err != nil {
		t.Fatalf("CreateServer: %v", err)
	}

	const writers = 50
	ctx := context.Background()
	seqs := make([]int64, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			event, err := store.AppendOutput(ctx, "srv", notificationPayload(`{"concurrent":true}`))
			seqs[i] = event.Seq
			errs[i] = err
		})
	}
	wg.Wait()

	seen := make(map[int64]bool, writers)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("AppendOutput(%d): %v", i, err)
		}
		if seen[seqs[i]] {
			t.Fatalf("duplicate sequence %d", seqs[i])
		}
		seen[seqs[i]] = true
	}
	for want := int64(1); want <= writers; want++ {
		if !seen[want] {
			t.Errorf("missing sequence %d", want)
		}
	}
}

func rawEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func ptrDeref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

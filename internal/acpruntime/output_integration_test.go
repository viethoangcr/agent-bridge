package acpruntime

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func TestRuntimeOutputCommitBeforeWakeup(t *testing.T) {
	r, store := startMockRuntime(t, time.Minute)

	sendInvalidStdout(t, r, 1, "not json")

	select {
	case <-r.Events():
	case <-time.After(5 * time.Second):
		t.Fatal("no wakeup after committed agent output")
	}

	events := listServerEvents(t, store)
	if len(events) == 0 {
		t.Fatal("wakeup delivered before any output was committed")
	}
	first := events[0]
	if first.Kind != "notification" || eventMethod(first) != invalidStdoutMethod {
		t.Errorf("first committed event = kind %q method %q, want invalid_stdout notification", first.Kind, eventMethod(first))
	}
}

func TestWakeupCoalescingNeverBlocks(t *testing.T) {
	r, store := startMockRuntime(t, time.Minute)

	const hooks = 5
	for i := 1; i <= hooks; i++ {
		sendInvalidStdout(t, r, i, "garbage")
	}

	// Each hook yields one invalid synthetic and one response.
	events := waitForEventCount(t, store, hooks*2)
	if len(events) < hooks*2 {
		t.Fatalf("events = %d, want %d (pump blocked on a full wakeup channel)", len(events), hooks*2)
	}

	// One coalesced wakeup must be buffered; consuming it is enough to observe
	// every committed sequence.
	select {
	case <-r.Events():
	default:
		t.Fatal("capacity-one wakeup channel was never signaled")
	}
	if after := listServerEvents(t, store); len(after) < hooks*2 {
		t.Fatalf("events after one wakeup = %d, want %d", len(after), hooks*2)
	}
}

func TestRuntimeOutputInvalidLinesRecover(t *testing.T) {
	r, store := startMockRuntime(t, time.Minute)

	sendInvalidStdout(t, r, 1, "{malformed")
	sendInvalidStdout(t, r, 2, `"non-object"`)
	// A subsequent valid control request must still be persisted.
	writeClientJSON(t, r, map[string]any{"jsonrpc": "2.0", "id": 3, "method": "_mock/delay", "params": map[string]any{"ms": 0}})

	events := waitForEventCount(t, store, 5)
	invalid := 0
	lateValid := false
	for _, e := range events {
		if e.Kind == "notification" && eventMethod(e) == invalidStdoutMethod {
			invalid++
		}
		if e.Kind == "response" && bytes.Contains(e.Payload, []byte(`"id":3`)) {
			lateValid = true
		}
	}
	if invalid != 2 {
		t.Errorf("invalid_stdout events = %d, want 2", invalid)
	}
	if !lateValid {
		t.Error("valid output after invalid lines was not committed")
	}
}

// TestRuntimeOutputWhitespaceLinePersistsOnce proves every non-empty line that
// trims to empty persists exactly one invalid-stdout event, later valid lines
// still commit, and a truly empty final read adds no event.
func TestRuntimeOutputWhitespaceLinePersistsOnce(t *testing.T) {
	r, store := newOutputRuntime(t)
	valid := []byte(`{"jsonrpc":"2.0","id":2,"result":{}}`)

	input := append([]byte(" \t \n\r\n"), valid...)
	input = append(input, '\n')
	runReadOutput(t, r, input)

	events := listServerEvents(t, store)
	if len(events) != 3 {
		t.Fatalf("events = %d, want 2 invalid_stdout plus 1 committed response", len(events))
	}
	for i := 0; i < 2; i++ {
		if events[i].Kind != "notification" || eventMethod(events[i]) != invalidStdoutMethod {
			t.Errorf("event[%d] = kind %q method %q, want invalid_stdout notification", i, events[i].Kind, eventMethod(events[i]))
		}
	}
	last := events[2]
	if last.Kind != "response" || !bytes.Equal(last.Payload, valid) {
		t.Errorf("last event = kind %q payload %q, want committed valid response", last.Kind, last.Payload)
	}
}

func TestRuntimeOutputPersistsOnlyAgentEvents(t *testing.T) {
	r, store := startMockRuntime(t, time.Minute)

	const clientSentinel = "CLIENT_PAYLOAD_SENTINEL"
	writeClientJSON(t, r, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "_mock/invalid_stdout",
		"params":  map[string]any{"line": clientSentinel + " appears only in the client request"},
	})

	events := waitForEventCount(t, store, 2)
	for _, e := range events {
		if bytes.Contains(e.Payload, []byte(clientSentinel)) {
			t.Errorf("client payload leaked into persisted event seq %d: %s", e.Seq, e.Payload)
		}
		if bytes.Contains(e.Payload, []byte(`"_mock/`)) {
			t.Errorf("client method name leaked into persisted event seq %d: %s", e.Seq, e.Payload)
		}
	}
}

func TestRuntimeOutputNaturalExit(t *testing.T) {
	r, store := startMockRuntime(t, time.Minute)

	writeClientJSON(t, r, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "_mock/exit"})
	if err := r.Wait(); err != nil {
		t.Fatalf("Wait after natural mock exit = %v, want nil", err)
	}

	events := listServerEvents(t, store)
	if len(events) < 2 {
		t.Fatalf("events = %d, want at least 2", len(events))
	}
	last := events[len(events)-1]
	if last.Kind != "notification" || eventMethod(last) != agentExitedMethod {
		t.Errorf("final event = kind %q method %q, want agent_exited notification", last.Kind, eventMethod(last))
	}

	server, err := store.Server(context.Background(), "srv")
	if err != nil {
		t.Fatalf("Server(srv): %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Errorf("status = %q, want %q", server.Status, acpstore.StatusExited)
	}
	if server.PID != nil {
		t.Errorf("stored PID = %d, want nil", *server.PID)
	}
	waitWakeClosed(t, r)
}

func TestRuntimeOutputStoreFailureKillsGroup(t *testing.T) {
	requireLinux(t)

	store := newRuntimeStore(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	env := withEnv(os.Environ(), mockEnvVar, "1")
	spec := LaunchSpec{Program: os.Args[0], Env: env}

	r, err := Start(t.Context(), store, "srv", spec, time.Minute, logger)
	if err != nil {
		t.Fatalf("Start(mock): %v", err)
	}
	pid := r.PID()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Kill(ctx)
	})

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("close store: %v", err)
	}

	sendInvalidStdout(t, r, 1, "garbage")

	if err := r.Wait(); err != nil {
		t.Logf("process wait error after storage failure: %v", err)
	}
	waitReaped(t, pid)

	if !strings.Contains(logs.String(), "persist") {
		t.Errorf("logs did not report the storage failure: %s", logs.String())
	}

	// No successful commit happened, so the wakeup channel must close without
	// ever delivering an uncommitted wakeup.
	select {
	case _, ok := <-r.Events():
		if ok {
			t.Fatal("received a wakeup for output that was never committed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wakeup channel was not closed after storage failure")
	}
}

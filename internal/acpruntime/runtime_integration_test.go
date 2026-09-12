package acpruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

func startMockRuntime(t *testing.T, timeout time.Duration) (*Runtime, *acpstore.Store) {
	t.Helper()
	store := newRuntimeStore(t)
	env := withEnv(os.Environ(), mockEnvVar, "1")
	spec := LaunchSpec{Program: os.Args[0], Env: env}

	r, err := Start(t.Context(), store, "srv", spec, timeout, testLogger())
	if err != nil {
		t.Fatalf("Start(mock): %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Kill(ctx)
	})
	return r, store
}

func writeClientJSON(t *testing.T, r *Runtime, v any) {
	t.Helper()
	line, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal client line: %v", err)
	}
	if err := r.writeLine(line); err != nil {
		t.Fatalf("writeLine: %v", err)
	}
}

func sendInvalidStdout(t *testing.T, r *Runtime, id int, line string) {
	t.Helper()
	writeClientJSON(t, r, map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "_mock/invalid_stdout",
		"params":  map[string]any{"line": line},
	})
}

// waitForEventCount polls the store until at least want events are committed.
func waitForEventCount(t *testing.T, store *acpstore.Store, want int) []acpstore.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		events := listServerEvents(t, store)
		if len(events) >= want {
			return events
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d committed events", want)
	return nil
}

// waitWakeClosed drains any buffered wakeup token and then requires the channel
// to be closed within the deadline.
func waitWakeClosed(t *testing.T, r *Runtime) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case _, ok := <-r.Events():
			if !ok {
				return
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}
	t.Fatal("wakeup channel was not closed within deadline")
}

// largeNotification builds a valid non-session notification whose payload
// exceeds a typical pipe buffer.
func largeNotification(method string, size int) []byte {
	pad := strings.Repeat("a", size)
	return []byte(`{"jsonrpc":"2.0","method":"` + method + `","params":{"pad":"` + pad + `"}}`)
}

// TestPostWritePartialTimeoutNeverReadingChild blocks a real pipe with a child
// that never reads stdin, then proves the timeout after a partial record kills
// and reaps the process group and rejects later posts.
func TestPostWritePartialTimeoutNeverReadingChild(t *testing.T) {
	requireLinux(t)
	r, _, pidPath := startHelperRuntime(t, helperProcessGroup, 200*time.Millisecond)
	pids := readHelperPIDs(t, pidPath)
	cleanupReportedPIDs(t, pids)

	_, err := r.Post(context.Background(), largeNotification("x", 256*1024))
	if !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("Post(large) error = %v, want request timeout", err)
	}
	if _, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"y"}`)); !errors.Is(err, ErrExited) {
		t.Fatalf("Post after partial record error = %v, want poisoned writer rejection", err)
	}

	waitReaped(t, pids.Direct)
	waitGoneOrZombie(t, pids.Grandchild)
}

// waitCorrState polls until the correlation for key reaches want.
func waitCorrState(t *testing.T, r *Runtime, key string, want correlationState) *pendingRequest {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.corrMu.Lock()
		entry := r.corr[key]
		state := corrWaiting
		if entry != nil {
			state = entry.state
		}
		r.corrMu.Unlock()
		if entry != nil && state == want {
			return entry
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("correlation %q never reached state %v", key, want)
	return nil
}

func corrCount(r *Runtime) int {
	r.corrMu.Lock()
	defer r.corrMu.Unlock()
	return len(r.corr)
}

// initializeMock performs the ACP initialize handshake against a mock runtime.
func initializeMock(t *testing.T, r *Runtime) {
	t.Helper()
	res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`))
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if !bytes.Contains(res.Response, []byte(`"protocolVersion":1`)) {
		t.Fatalf("initialize response = %s", res.Response)
	}
}

func TestRuntimePostSessionNewCommitsSession(t *testing.T) {
	r, store := startMockRuntime(t, time.Minute)
	initializeMock(t, r)

	res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/work","mcpServers":[]}}`))
	if err != nil {
		t.Fatalf("session/new: %v", err)
	}
	var body struct {
		Result struct {
			SessionID string `json:"sessionId"`
		} `json:"result"`
	}
	if err := json.Unmarshal(res.Response, &body); err != nil || body.Result.SessionID == "" {
		t.Fatalf("session/new response = %s (%v)", res.Response, err)
	}
	session, err := store.Session(context.Background(), "srv", body.Result.SessionID)
	if err != nil {
		t.Fatalf("session not committed: %v", err)
	}
	if session.CWD != "/work" {
		t.Errorf("session cwd = %q, want /work", session.CWD)
	}
}

func TestRuntimePostFreshSubprocessUnknownSessionPassthrough(t *testing.T) {
	first, _ := startMockRuntime(t, time.Minute)
	initializeMock(t, first)
	res, err := first.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/one"}}`))
	if err != nil {
		t.Fatalf("session/new: %v", err)
	}
	var created struct {
		Result struct {
			SessionID string `json:"sessionId"`
		} `json:"result"`
	}
	if err := json.Unmarshal(res.Response, &created); err != nil || created.Result.SessionID == "" {
		t.Fatalf("session/new response = %s", res.Response)
	}
	formerID := created.Result.SessionID

	second, store := startMockRuntime(t, time.Minute)
	initializeMock(t, second)

	for _, method := range []string{"session/load", "session/resume"} {
		payload := `{"jsonrpc":"2.0","id":"` + method + `","method":"` + method +
			`","params":{"sessionId":"` + formerID + `","cwd":"/two"}}`
		res, err := second.Post(context.Background(), []byte(payload))
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		if !bytes.Contains(res.Response, []byte(`-32002`)) {
			t.Errorf("%s response = %s, want unknown-session -32002", method, res.Response)
		}
		if sessions, err := store.Sessions(context.Background(), "srv"); err == nil && len(sessions) != 0 {
			t.Errorf("%s synthesized sessions: %+v", method, sessions)
		}
	}
}

func TestRuntimeExitFailsPendingWithErrExited(t *testing.T) {
	r, _ := startMockRuntime(t, time.Minute)
	initializeMock(t, r)

	done := make(chan error, 1)
	go func() {
		_, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":"delay","method":"_mock/delay","params":{"ms":60000}}`))
		done <- err
	}()
	waitCorrState(t, r, `s:delay`, corrWaiting)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Kill(ctx); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrExited) {
			t.Fatalf("pending Post after exit = %v, want ErrExited", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending Post was not failed on exit")
	}
	if corrCount(r) != 0 {
		t.Fatalf("correlations not cleared on exit: %d", corrCount(r))
	}
}

func TestRuntimeCallerCancellationReleasesSlotAfterTimeout(t *testing.T) {
	r, _ := startMockRuntime(t, 150*time.Millisecond)
	initializeMock(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Post(ctx, []byte(`{"jsonrpc":"2.0","id":"delay","method":"_mock/delay","params":{"ms":60000}}`))
		done <- err
	}()
	entry := waitCorrState(t, r, `s:delay`, corrWaiting)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.corrMu.Lock()
		written := entry.written
		r.corrMu.Unlock()
		if written {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Post error = %v, want context.Canceled", err)
	}
	if corrCount(r) == 0 {
		t.Fatal("caller cancellation released the slot before timeout")
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && corrCount(r) != 0 {
		time.Sleep(2 * time.Millisecond)
	}
	if corrCount(r) != 0 {
		t.Fatalf("non-lifecycle slot not released after timeout: %d", corrCount(r))
	}
}

func TestRuntimeGraceExpiryKillsAndExits(t *testing.T) {
	requireLinux(t)
	// The helper never reads its stdin, so the record is written but no
	// response ever arrives, forcing a lifecycle timeout into grace.
	r, store, _ := startHelperRuntime(t, helperProcessGroup, 50*time.Millisecond)
	graces := newFakeTimerSource()
	r.newGraceTimer = graces.create

	done := make(chan error, 1)
	go func() {
		_, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/grace"}}`))
		done <- err
	}()
	waitCorrState(t, r, numericKey(1), corrGrace)
	if err := <-done; !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("lifecycle timeout = %v, want ErrRequestTimeout", err)
	}
	if _, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/grace"}}`)); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("duplicate during grace = %v, want ErrDuplicateID", err)
	}

	graces.last().fire()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("grace expiry did not terminate the runtime")
	}
	waitWakeClosed(t, r)
	if corrCount(r) != 0 {
		t.Fatalf("grace expiry left correlations: %d", corrCount(r))
	}
	server, err := store.Server(context.Background(), "srv")
	if err != nil {
		t.Fatalf("Server(srv): %v", err)
	}
	if server.Status != acpstore.StatusExited {
		t.Errorf("status = %q, want exited", server.Status)
	}
}

package acpruntime

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// --- Task 2.9c correlation harness -----------------------------------------

// fakeTimerSource records every runtimeTimer it creates so tests can fire a
// specific deadline or grace deterministically.
type fakeTimerSource struct {
	mu     sync.Mutex
	timers []*fakeTimer
}

type fakeTimer struct {
	ch chan time.Time

	mu      sync.Mutex
	stopped bool
}

func (f *fakeTimer) stop() {
	f.mu.Lock()
	f.stopped = true
	f.mu.Unlock()
}

func (f *fakeTimer) isStopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

func (f *fakeTimer) fire() { f.ch <- time.Now() }

func newFakeTimerSource() *fakeTimerSource { return &fakeTimerSource{} }

func (s *fakeTimerSource) create(time.Duration) *runtimeTimer {
	ft := &fakeTimer{ch: make(chan time.Time, 1)}
	s.mu.Lock()
	s.timers = append(s.timers, ft)
	s.mu.Unlock()
	return &runtimeTimer{ch: ft.ch, stop: ft.stop}
}

func (s *fakeTimerSource) last() *fakeTimer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.timers) == 0 {
		return nil
	}
	return s.timers[len(s.timers)-1]
}

// correlationHarness runs the writer and correlation map against a real store
// with injected deadline/grace timers and no subprocess. Tests drive responses
// by calling handleOutput directly.
type correlationHarness struct {
	r         *Runtime
	store     *acpstore.Store
	writer    *recordWriter
	deadlines *fakeTimerSource
	graces    *fakeTimerSource
}

func newCorrelationHarness(t *testing.T, timeout time.Duration) *correlationHarness {
	t.Helper()
	store := newRuntimeStore(t)
	writer := &recordWriter{}
	h := &correlationHarness{
		store:     store,
		writer:    writer,
		deadlines: newFakeTimerSource(),
		graces:    newFakeTimerSource(),
	}
	h.r = &Runtime{
		store:            store,
		serverID:         "srv",
		log:              testLogger(),
		requestTimeout:   timeout,
		graceDuration:    defaultGraceDuration,
		corr:             make(map[string]*pendingRequest),
		terminal:         make(chan struct{}),
		wake:             make(chan struct{}, 1),
		done:             make(chan struct{}),
		streamWrite:      writer.write,
		newDeadlineTimer: h.deadlines.create,
		newGraceTimer:    h.graces.create,
	}
	h.r.startWriter()
	t.Cleanup(func() {
		h.r.stopWriter()
		h.r.closeStdin()
		h.r.awaitWriterStopped()
		h.r.failPending(ErrExited)
	})
	return h
}

func (h *correlationHarness) status(t *testing.T) acpstore.Status {
	t.Helper()
	server, err := h.store.Server(context.Background(), "srv")
	if err != nil {
		t.Fatalf("Server(srv): %v", err)
	}
	return server.Status
}

func (h *correlationHarness) waitStatus(t *testing.T, want acpstore.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.status(t) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("status = %q, want %q", h.status(t), want)
}

func (h *correlationHarness) entry(t *testing.T, key string) *pendingRequest {
	t.Helper()
	h.r.corrMu.Lock()
	defer h.r.corrMu.Unlock()
	return h.r.corr[key]
}

func (h *correlationHarness) waitWritten(t *testing.T, key string) *pendingRequest {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.r.corrMu.Lock()
		entry := h.r.corr[key]
		written := entry != nil && entry.written
		h.r.corrMu.Unlock()
		if written {
			return entry
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("entry %q never reached the written waiting state", key)
	return nil
}

func (h *correlationHarness) corrLen() int {
	h.r.corrMu.Lock()
	defer h.r.corrMu.Unlock()
	return len(h.r.corr)
}

type postCall struct {
	res PostResult
	err error
}

func goRequest(h *correlationHarness, ctx context.Context, payload string) chan postCall {
	done := make(chan postCall, 1)
	go func() {
		res, err := h.r.Post(ctx, []byte(payload))
		done <- postCall{res, err}
	}()
	return done
}

func numericKey(i int) string {
	key, _ := idKey(json.RawMessage(strconv.Itoa(i)))
	return key
}

// startPostHammer spins n goroutines posting payload until the returned stop
// function is called, which then returns every observed outcome. Caller
// cancellation bounds any post that a regression would otherwise strand.
func startPostHammer(r *Runtime, payload string, n int) (stop func() []error) {
	stopCh := make(chan struct{})
	var mu sync.Mutex
	var observed []error
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				_, err := r.Post(ctx, []byte(payload))
				mu.Lock()
				if len(observed) < 4096 {
					observed = append(observed, err)
				}
				mu.Unlock()
			}
		}()
	}
	return func() []error {
		close(stopCh)
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		return append([]error(nil), observed...)
	}
}

func TestPostPendingCanonicalIDWritten(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)

	done := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	entry := h.waitWritten(t, numericKey(1))

	if entry.lifecycle != LifecycleNone {
		t.Errorf("lifecycle = %q, want none", entry.lifecycle)
	}
	if string(entry.id) != "1" {
		t.Errorf("id = %q, want %q", entry.id, "1")
	}
	if h.status(t) != acpstore.StatusBusy {
		t.Errorf("status = %q, want busy after reservation", h.status(t))
	}
	if records := h.writer.all(); len(records) != 1 {
		t.Fatalf("records = %d, want exactly one compact JSONL record", len(records))
	}

	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`))
	got := <-done
	if got.err != nil {
		t.Fatalf("Post error = %v, want nil", got.err)
	}
	if string(got.res.Response) != `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}` {
		t.Errorf("response = %s, want exact committed bytes", got.res.Response)
	}
	h.waitStatus(t, acpstore.StatusIdle)
}

func TestPostBusyStaysBusyUntilLastReservation(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)

	done1 := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	h.waitWritten(t, numericKey(1))
	done2 := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":2,"method":"initialize"}`)
	h.waitWritten(t, numericKey(2))
	h.waitStatus(t, acpstore.StatusBusy)

	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	if got := <-done1; got.err != nil {
		t.Fatalf("Post 1 error = %v", got.err)
	}
	h.waitStatus(t, acpstore.StatusBusy)

	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))
	if got := <-done2; got.err != nil {
		t.Fatalf("Post 2 error = %v", got.err)
	}
	h.waitStatus(t, acpstore.StatusIdle)
}

func TestPostDuplicateCanonicalNumericIDs(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	done := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	h.waitWritten(t, numericKey(1))

	for _, raw := range []string{"1", "1.0", "1e0"} {
		payload := `{"jsonrpc":"2.0","id":` + raw + `,"method":"initialize"}`
		if _, err := h.r.Post(context.Background(), []byte(payload)); !errors.Is(err, ErrDuplicateID) {
			t.Errorf("duplicate id %s error = %v, want ErrDuplicateID", raw, err)
		}
	}
	if len(h.writer.all()) != 1 {
		t.Fatalf("duplicate posts wrote %d records, want 1", len(h.writer.all()))
	}

	done2 := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":"1","method":"initialize"}`)
	h.waitWritten(t, `s:1`)

	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}`))
	<-done
	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	<-done2
}

func TestCapacityRejects257thBeforeAdmission(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)

	for i := 0; i < maxCorrelations; i++ {
		id := json.RawMessage(strconv.Itoa(i + 1))
		if _, err := h.r.reserve(Pending{ID: id, Lifecycle: LifecycleNone}); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	before := len(h.writer.all())
	_, err := h.r.reserve(Pending{ID: json.RawMessage("9999"), Lifecycle: LifecycleNone})
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("257th reserve error = %v, want ErrCapacity", err)
	}
	if got := len(h.writer.all()); got != before {
		t.Fatalf("capacity rejection wrote %d records, want none", got-before)
	}
	if h.corrLen() != maxCorrelations {
		t.Fatalf("corr len = %d, want %d", h.corrLen(), maxCorrelations)
	}
}

func TestCapacityCountsGraceAndCommitting(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)

	for i := 0; i < maxCorrelations-2; i++ {
		if _, err := h.r.reserve(Pending{ID: json.RawMessage(strconv.Itoa(i + 1))}); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	grace, err := h.r.reserve(Pending{ID: json.RawMessage("900"), Lifecycle: LifecycleNew})
	if err != nil {
		t.Fatalf("reserve grace: %v", err)
	}
	committing, err := h.r.reserve(Pending{ID: json.RawMessage("901")})
	if err != nil {
		t.Fatalf("reserve committing: %v", err)
	}
	h.r.corrMu.Lock()
	grace.state = corrGrace
	committing.state = corrCommitting
	h.r.corrMu.Unlock()

	if _, err := h.r.reserve(Pending{ID: json.RawMessage("902")}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("reserve over grace+committing = %v, want ErrCapacity", err)
	}
}

func TestPendingSessionNewMetadata(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	cwd := "/work/project"
	done := goRequest(h, context.Background(),
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"`+cwd+`","mcpServers":[]}}`)
	entry := h.waitWritten(t, numericKey(1))

	if entry.lifecycle != LifecycleNew {
		t.Errorf("lifecycle = %q, want new", entry.lifecycle)
	}
	if entry.sessionID != nil {
		t.Errorf("sessionID = %q, want nil until success", *entry.sessionID)
	}
	if entry.cwd == nil || *entry.cwd != cwd {
		t.Errorf("cwd = %v, want %q", entry.cwd, cwd)
	}

	h.r.handleOutput([]byte(`{"jsonrpc":"2.0","id":1,"result":{"sessionId":"new-1"}}`))
	got := <-done
	if got.err != nil {
		t.Fatalf("Post error = %v", got.err)
	}

	session, err := h.store.Session(context.Background(), "srv", "new-1")
	if err != nil {
		t.Fatalf("session was not committed: %v", err)
	}
	if session.CWD != cwd {
		t.Errorf("session cwd = %q, want %q", session.CWD, cwd)
	}
}

// TestTerminalGateClosesBeforeClearingReservations proves the terminal gate
// closes before failPending clears correlations: while teardown is paused
// between those two steps, a same-ID request, a new-ID reserve, and a
// notification must all be rejected with ErrExited without reserving or
// writing anything, and the pre-existing reservation must be cleared after
// teardown. The pause runs before terminal state reaches the Post fast path so
// the reserve and admit gates themselves are exercised.
func TestTerminalGateClosesBeforeClearingReservations(t *testing.T) {
	h := newCorrelationHarness(t, time.Minute)
	close(h.r.done) // markTerminal must not wait for a process that never exists.

	done := goRequest(h, context.Background(), `{"jsonrpc":"2.0","id":1,"method":"initialize"}`)
	h.waitWritten(t, numericKey(1))

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.r.beforeTerminalClear = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	teardownDone := make(chan struct{})
	go func() {
		defer close(teardownDone)
		h.r.markTerminal(ErrExited)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal teardown never paused after closing the gate")
	}

	// The gate is closed but correlations are not cleared yet: nothing may
	// reserve or enqueue through the window.
	if _, err := h.r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`)); !errors.Is(err, ErrExited) {
		t.Fatalf("same-ID post during teardown = %v, want ErrExited", err)
	}
	if _, err := h.r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"session/cancel"}`)); !errors.Is(err, ErrExited) {
		t.Fatalf("notification post during teardown = %v, want ErrExited", err)
	}
	if _, err := h.r.reserve(Pending{ID: json.RawMessage("2")}); !errors.Is(err, ErrExited) {
		t.Fatalf("new-ID reserve during teardown = %v, want ErrExited", err)
	}
	if records := h.writer.all(); len(records) != 1 {
		t.Fatalf("writer records during teardown = %d, want 1", len(records))
	}

	close(release)
	select {
	case <-teardownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal teardown did not finish")
	}

	if got := <-done; !errors.Is(got.err, ErrExited) {
		t.Fatalf("pre-existing post error = %v, want ErrExited", got.err)
	}
	if h.corrLen() != 0 {
		t.Fatalf("correlations after teardown = %d, want 0", h.corrLen())
	}
	if records := h.writer.all(); len(records) != 1 {
		t.Fatalf("writer records after teardown = %d, want 1", len(records))
	}
	if !h.r.terminalClosed() {
		t.Fatal("terminal gate is not closed after teardown")
	}
}

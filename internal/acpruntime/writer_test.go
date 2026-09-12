package acpruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordWriter captures every complete record handed to the writer seam.
type recordWriter struct {
	mu      sync.Mutex
	records [][]byte
}

func (w *recordWriter) write(p []byte) (int, error) {
	w.mu.Lock()
	w.records = append(w.records, append([]byte(nil), p...))
	w.mu.Unlock()
	return len(p), nil
}

func (w *recordWriter) all() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.records...)
}

// newWriterRuntime builds a runtime with only the writer path running: no
// process and no store. The supplied write function is the injected writer seam.
func newWriterRuntime(t *testing.T, timeout time.Duration, write func([]byte) (int, error)) *Runtime {
	t.Helper()
	r := &Runtime{
		serverID:       "srv",
		log:            testLogger(),
		requestTimeout: timeout,
		graceDuration:  defaultGraceDuration,
		corr:           make(map[string]*pendingRequest),
		terminal:       make(chan struct{}),
		streamWrite:    write,
		wake:           make(chan struct{}, 1),
		done:           make(chan struct{}),
	}
	r.startWriter()
	t.Cleanup(func() {
		r.stopWriter()
		r.closeStdin()
		r.awaitWriterStopped()
	})
	return r
}

// TestAdmitRejectsAfterTerminalClose proves no writer admission can succeed
// once the terminal gate is closed, even while the queue has free capacity and
// both the queue send and the terminal signal would be ready.
func TestAdmitRejectsAfterTerminalClose(t *testing.T) {
	r := newWriterRuntime(t, time.Minute, (&recordWriter{}).write)
	r.closeTerminal()

	for i := 0; i < 100; i++ {
		err := r.admit(context.Background(), newWriteItem([]byte("{}\n")), time.After(time.Second))
		if !errors.Is(err, ErrExited) {
			t.Fatalf("admit after terminal = %v, want ErrExited", err)
		}
	}
}

// TestAdmitBlockedQueueReturnsOnTerminal proves a writer admission blocked on
// a full queue observes terminal closure promptly instead of waiting for its
// deadline.
func TestAdmitBlockedQueueReturnsOnTerminal(t *testing.T) {
	blocked := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	r := newWriterRuntime(t, time.Hour, func(p []byte) (int, error) {
		once.Do(func() { close(entered) })
		<-blocked
		return len(p), nil
	})
	t.Cleanup(func() { close(blocked) })

	// Occupy the single writer, then fill the queue to capacity so the next
	// admission blocks on the send.
	r.writerQueue <- newWriteItem([]byte("{}\n"))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never started")
	}
	for i := 0; i < writerQueueDepth; i++ {
		r.writerQueue <- newWriteItem([]byte("{}\n"))
	}

	result := make(chan error, 1)
	go func() {
		result <- r.admit(context.Background(), newWriteItem([]byte("{}\n")), time.After(time.Hour))
	}()

	r.closeTerminal()

	select {
	case err := <-result:
		if !errors.Is(err, ErrExited) {
			t.Fatalf("blocked admit = %v, want ErrExited", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked admit did not observe terminal closure")
	}
}

func TestWriterAllKindsShareQueue(t *testing.T) {
	recorder := &recordWriter{}
	r := newWriterRuntime(t, time.Minute, recorder.write)
	ctx := context.Background()

	notif := `{"jsonrpc":"2.0","method":"session/cancel"}`
	resp := `{"jsonrpc":"2.0","id":9,"result":{"ok":true}}`
	req := `{"jsonrpc":"2.0","id":10,"method":"initialize"}`

	if res, err := r.Post(ctx, []byte(notif)); err != nil || !res.Accepted {
		t.Fatalf("notification Post = (%+v, %v), want accepted", res, err)
	}
	if res, err := r.Post(ctx, []byte(resp)); err != nil || !res.Accepted {
		t.Fatalf("response Post = (%+v, %v), want accepted", res, err)
	}

	// A request reserves a correlation and waits for a response; cancel the
	// caller after the record is written and confirm the write still happened.
	reqCtx, cancelReq := context.WithCancel(ctx)
	reqDone := make(chan error, 1)
	go func() {
		_, err := r.Post(reqCtx, []byte(req))
		reqDone <- err
	}()
	waitForRecords(t, recorder, 3)
	cancelReq()
	if err := <-reqDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("request Post error = %v, want context canceled", err)
	}

	records := recorder.all()
	want := []string{notif, resp, req}
	if len(records) != len(want) {
		t.Fatalf("records = %d, want %d", len(records), len(want))
	}
	for i, raw := range want {
		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(raw)); err != nil {
			t.Fatalf("compact expectation[%d]: %v", i, err)
		}
		wantLine := append(compact.Bytes(), '\n')
		if !bytes.Equal(records[i], wantLine) {
			t.Errorf("record[%d] = %q, want %q", i, records[i], wantLine)
		}
	}
}

// waitForRecords blocks until the recorder has at least n records.
func waitForRecords(t *testing.T, w *recordWriter, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(w.all()) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("writer captured %d records, want %d", len(w.all()), n)
}

func TestWriterAcceptedOnlyAfterCompleteWrite(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	r := newWriterRuntime(t, time.Minute, func(p []byte) (int, error) {
		once.Do(func() { close(entered) })
		<-release
		return len(p), nil
	})

	type result struct {
		res PostResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"session/cancel"}`))
		done <- result{res, err}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never received the record")
	}
	select {
	case got := <-done:
		t.Fatalf("Post returned %+v before the record completed", got)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case got := <-done:
		if got.err != nil || !got.res.Accepted {
			t.Fatalf("Post = (%+v, %v), want accepted", got.res, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Post did not return after the record completed")
	}
}

func TestWriterAdmissionBounded(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	var r *Runtime
	r = newWriterRuntime(t, time.Minute, func(p []byte) (int, error) {
		once.Do(func() { close(entered) })
		select {
		case <-gate:
			return len(p), nil
		case <-r.writerStop:
			return 0, ErrExited
		}
	})

	r.writerQueue <- newWriteItem([]byte("gate\n"))
	<-entered

	if got := cap(r.writerQueue); got != writerQueueDepth {
		t.Fatalf("writer queue capacity = %d, want %d", got, writerQueueDepth)
	}
	for i := 0; i < writerQueueDepth; i++ {
		select {
		case r.writerQueue <- newWriteItem([]byte("queued\n")):
		default:
			t.Fatalf("queue rejected item %d, want capacity %d", i, writerQueueDepth)
		}
	}
	select {
	case r.writerQueue <- newWriteItem([]byte("overflow\n")):
		t.Fatal("writer admitted beyond the 256-slot bound")
	default:
	}
	close(gate)
}

func TestPostTimeoutBeforeWrite(t *testing.T) {
	gate := make(chan struct{})
	gateEntered := make(chan struct{})
	var once sync.Once
	var r *Runtime
	r = newWriterRuntime(t, 200*time.Millisecond, func(p []byte) (int, error) {
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

	_, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"a"}`))
	if !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("Post with blocked writer error = %v, want request timeout", err)
	}
	if r.poisoned.Load() {
		t.Fatal("pre-write timeout poisoned the runtime")
	}

	close(gate)
	res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"b"}`))
	if err != nil || !res.Accepted {
		t.Fatalf("Post after pre-write timeout = (%+v, %v), want accepted", res, err)
	}
}

func TestPostWriteFailureBeforeBytes(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	r := newWriterRuntime(t, time.Minute, func(p []byte) (int, error) {
		if fail.Load() {
			return 0, errors.New("boom")
		}
		return len(p), nil
	})

	_, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"a"}`))
	if !errors.Is(err, ErrWrite) {
		t.Fatalf("Post with failing writer error = %v, want write failure", err)
	}
	if r.poisoned.Load() {
		t.Fatal("zero-byte write failure poisoned the runtime")
	}

	fail.Store(false)
	res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"b"}`))
	if err != nil || !res.Accepted {
		t.Fatalf("Post after zero-byte failure = (%+v, %v), want accepted", res, err)
	}
}

func TestPostWritePartialTimeout(t *testing.T) {
	partialWritten := make(chan struct{})
	var partialOnce sync.Once
	var mu sync.Mutex
	var partial []byte
	var r *Runtime
	r = newWriterRuntime(t, 200*time.Millisecond, func(p []byte) (int, error) {
		half := len(p) / 2
		mu.Lock()
		partial = append(partial, p[:half]...)
		mu.Unlock()
		partialOnce.Do(func() { close(partialWritten) })
		<-r.writerStop
		return half, errors.New("stream broken")
	})

	type result struct {
		res PostResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"a"}`))
		done <- result{res, err}
	}()

	select {
	case <-partialWritten:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never emitted a partial record")
	}
	select {
	case got := <-done:
		if !errors.Is(got.err, ErrRequestTimeout) {
			t.Fatalf("Post after partial write error = %v, want request timeout", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Post did not return after the partial-write timeout")
	}

	mu.Lock()
	emitted := len(partial)
	mu.Unlock()
	if emitted == 0 {
		t.Fatal("no partial bytes were emitted")
	}

	if _, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"b"}`)); !errors.Is(err, ErrExited) {
		t.Fatalf("Post after partial write = %v, want poisoned writer rejection", err)
	}
	if !r.terminalClosed() {
		t.Fatal("fatal-write poison did not close the terminal gate")
	}
	select {
	case <-r.writerStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("writer was not joined after the partial-record failure")
	}
}

// TestPostWritePartialPoisonsBeforeReturn proves a partial write poisons the
// runtime before the outcome is released, and joins the writer goroutine before
// Post returns. The stdin lock parks the writer inside teardown so the early
// release is observable deterministically.
func TestPostWritePartialPoisonsBeforeReturn(t *testing.T) {
	r := newWriterRuntime(t, time.Minute, func(p []byte) (int, error) {
		return len(p) / 2, nil
	})
	r.writeMu.Lock()

	done := make(chan postCall, 1)
	go func() {
		res, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"session/cancel"}`))
		done <- postCall{res, err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !r.poisoned.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !r.poisoned.Load() {
		r.writeMu.Unlock()
		t.Fatal("writer never poisoned the runtime")
	}
	runtime.Gosched()
	select {
	case got := <-done:
		r.writeMu.Unlock()
		t.Fatalf("Post returned %v before the writer teardown settled", got.err)
	default:
	}

	r.writeMu.Unlock()
	select {
	case got := <-done:
		if !errors.Is(got.err, ErrWrite) {
			t.Fatalf("Post after partial write error = %v, want write failure", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Post did not return after the writer teardown settled")
	}
	select {
	case <-r.writerStopped:
	default:
		t.Fatal("Post returned before the writer goroutine settled")
	}
	if _, err := r.Post(context.Background(), []byte(`{"jsonrpc":"2.0","method":"b"}`)); !errors.Is(err, ErrExited) {
		t.Fatalf("Post after partial write = %v, want poisoned writer rejection", err)
	}
	if !r.terminalClosed() {
		t.Fatal("fatal-write poison did not close the terminal gate")
	}
}

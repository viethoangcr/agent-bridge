package acpruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"
)

// writerQueueDepth is the fixed admission capacity shared by requests,
// notifications, and client responses.
const writerQueueDepth = 256

// writerJoinTimeout bounds how long a failing Post waits for the writer
// goroutine to observe stdin closure and stop.
const writerJoinTimeout = 5 * time.Second

// writeItem is one complete compact JSONL record queued for serialization.
type writeItem struct {
	line []byte
	done chan struct{}

	mu      sync.Mutex
	started bool
	closed  bool
	fatal   bool
	err     error
}

func newWriteItem(line []byte) *writeItem {
	return &writeItem{line: line, done: make(chan struct{})}
}

// begin reports whether the writer should serialize this item. A false result
// means the item was cancelled before any byte was emitted.
func (it *writeItem) begin() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed {
		return false
	}
	it.started = true
	return true
}

// complete records the write outcome exactly once and releases any waiter.
func (it *writeItem) complete(err error) {
	it.mu.Lock()
	if it.closed {
		it.mu.Unlock()
		return
	}
	it.closed = true
	it.err = err
	it.mu.Unlock()
	close(it.done)
}

// completeFatal records a partial-write failure and marks the outcome fatal so
// a waiter joins the writer and process teardown before returning.
func (it *writeItem) completeFatal(err error) {
	it.mu.Lock()
	it.fatal = true
	if it.closed {
		it.mu.Unlock()
		return
	}
	it.closed = true
	it.err = err
	it.mu.Unlock()
	close(it.done)
}

// isFatal reports whether the item failed after emitting partial bytes, so the
// stream was poisoned and the writer must be joined before returning.
func (it *writeItem) isFatal() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.fatal
}

// cancelBeforeWrite cancels an item that has not begun writing. It reports
// whether the item had already begun or finished, in which case the caller must
// inspect finished and possibly contain a partial record.
func (it *writeItem) cancelBeforeWrite(err error) (begun bool) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed || it.started {
		return true
	}
	it.closed = true
	it.err = err
	close(it.done)
	return false
}

// result returns the terminal write error.
func (it *writeItem) result() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

// finished reports the terminal write error once the writer has completed.
func (it *writeItem) finished() (error, bool) {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err, it.closed
}

// admit queues one item under the supplied deadline. The deadline is shared
// with awaitWrite and starts before admission for every envelope kind. The
// admission gate makes terminal closure win deterministically: an admit that
// entered before closure either enqueues before closeTerminal returns or
// observes the closed signal, and no admit enters afterwards.
func (r *Runtime) admit(ctx context.Context, item *writeItem, deadline <-chan time.Time) error {
	if !r.gate.enter() {
		return ErrExited
	}
	defer r.gate.leave()

	select {
	case r.writerQueue <- item:
		return nil
	case <-r.terminal:
		return ErrExited
	case <-r.writerStop:
		return ErrExited
	case <-deadline:
		return ErrRequestTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

// awaitWrite blocks until the queued record is completely written, the deadline
// expires, or the caller cancels. A fatal partial-write outcome joins the
// writer and process teardown before returning.
func (r *Runtime) awaitWrite(ctx context.Context, item *writeItem, deadline <-chan time.Time) error {
	select {
	case <-item.done:
		if item.isFatal() {
			r.awaitWriterStopped()
		}
		return item.result()
	case <-r.writerStop:
		return r.cancelWriterStopped(item)
	case <-deadline:
		return r.cancelQueued(item, ErrRequestTimeout)
	case <-ctx.Done():
		return r.cancelQueued(item, ctx.Err())
	}
}

// cancelQueued handles a deadline or cancellation that raced the writer. An
// item that never began writing is removed alone, leaving the runtime usable.
// An item that had begun may be a partial record, so the runtime is failed and
// the writer joined before returning.
func (r *Runtime) cancelQueued(item *writeItem, cause error) error {
	if !item.cancelBeforeWrite(cause) {
		return cause
	}
	if err, ok := item.finished(); ok {
		if item.isFatal() {
			r.awaitWriterStopped()
		}
		return err
	}
	r.poison()
	r.awaitWriterStopped()
	return cause
}

// cancelWriterStopped handles a writer teardown racing a queued write. It
// joins a fatal partial write and reports the item's own outcome; an item that
// never began is ErrExited.
func (r *Runtime) cancelWriterStopped(item *writeItem) error {
	if !item.cancelBeforeWrite(ErrExited) {
		return ErrExited
	}
	if err, finished := item.finished(); finished {
		if item.isFatal() {
			r.awaitWriterStopped()
		}
		return err
	}
	r.poison()
	r.awaitWriterStopped()
	if err, finished := item.finished(); finished && item.isFatal() {
		return err
	}
	return ErrExited
}

// startWriter lazily creates the queue and launches the single serializer
// goroutine. Start calls it once; tests call it directly.
func (r *Runtime) startWriter() {
	r.writerOnce.Do(func() {
		if r.writerQueue == nil {
			r.writerQueue = make(chan *writeItem, writerQueueDepth)
		}
		if r.writerStop == nil {
			r.writerStop = make(chan struct{})
		}
		if r.writerStopped == nil {
			r.writerStopped = make(chan struct{})
		}
		if r.streamWrite == nil {
			r.streamWrite = r.writeRaw
		}
		go r.writerLoop()
	})
}

// stopWriter signals the serializer to stop. It is idempotent.
func (r *Runtime) stopWriter() {
	if r.writerStop == nil {
		return
	}
	r.writerStopOnce.Do(func() { close(r.writerStop) })
}

// poison marks the stream unusable and tears down the process group. It closes
// the terminal gate before the kill, then stops the serializer and fails every
// retained correlation before signaling, so fatal-write teardown follows the
// same gate -> stop -> clear -> kill ordering as every other terminal path. It
// is idempotent and safe to call from the writer goroutine.
func (r *Runtime) poison() {
	r.poisonOnce.Do(func() {
		r.closeTerminal()
		r.poisoned.Store(true)
		r.stopWriter()
		r.failPending(ErrWrite)
		_ = r.killProcessGroup()
		r.closeStdin()
	})
}

// awaitWriterStopped joins the serializer after a failure.
func (r *Runtime) awaitWriterStopped() {
	if r.writerStopped == nil {
		return
	}
	select {
	case <-r.writerStopped:
	case <-time.After(writerJoinTimeout):
	}
}

// writerLoop is the single goroutine that serializes every envelope record. A
// partial record poisons the runtime; a zero-byte failure drops only that
// envelope and keeps the stream usable.
func (r *Runtime) writerLoop() {
	defer close(r.writerStopped)
	for {
		select {
		case <-r.writerStop:
			r.drainWrites(ErrExited)
			return
		case item := <-r.writerQueue:
			if !item.begin() {
				continue
			}
			n, err := r.streamWrite(item.line)
			if err == nil && n == len(item.line) {
				item.complete(nil)
				continue
			}
			if n == 0 {
				item.complete(ErrWrite)
				continue
			}
			// A partial record leaves the stream unusable: poison and tear down
			// the process group before the fatal outcome is released, and have
			// awaitWrite join writerStopped before returning.
			r.poison()
			item.completeFatal(ErrWrite)
			r.drainWrites(ErrExited)
			return
		}
	}
}

// drainWrites fails every queued item with err.
func (r *Runtime) drainWrites(err error) {
	for {
		select {
		case item := <-r.writerQueue:
			item.complete(err)
		default:
			return
		}
	}
}

// compactLine frames one validated payload as a complete JSONL record equal to
// json.Compact(payload) plus a newline. Decode-and-re-marshal is deliberately
// avoided: it HTML-escapes and normalizes number lexemes.
func compactLine(payload json.RawMessage) ([]byte, error) {
	var compacted bytes.Buffer
	compacted.Grow(len(payload))
	if err := json.Compact(&compacted, payload); err != nil {
		return nil, invalidEnvelope("payload cannot be compacted")
	}
	line := make([]byte, 0, compacted.Len()+1)
	line = append(line, compacted.Bytes()...)
	line = append(line, '\n')
	return line, nil
}

package acpruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

const (
	// synthetic notification methods.
	invalidStdoutMethod = "_adapter/invalid_stdout"
	agentExitedMethod   = "_adapter/agent_exited"
)

// readOutput consumes the child's stdout as newline-delimited JSON objects and
// persists every agent envelope before publication. It strips only surrounding
// JSONL framing whitespace and never decodes, re-marshals, or compacts the
// remaining object bytes. An oversized, malformed, non-object, batch,
// invalid-UTF-8, or whitespace-only line becomes exactly one synthetic
// invalid-stdout notification and the loop continues. A truly empty read
// produces no event.
func (r *Runtime) readOutput(pipe io.ReadCloser) {
	defer r.pumps.Done()
	defer closePipe(pipe)

	reader := bufio.NewReaderSize(pipe, outputReadBuffer)
	for {
		line, oversized, err := readRawLine(reader, maxOutputLine)
		switch {
		case oversized:
			r.persistSynthetic(invalidStdoutMethod, nil)
		case len(line) > 0:
			if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
				r.handleOutput(trimmed)
			} else {
				r.persistSynthetic(invalidStdoutMethod, nil)
			}
		}
		if err != nil {
			return
		}
	}
}

// handleOutput classifies and commits one framed agent-output object. A
// response atomically transitions its matching correlation to committing (and
// cancels lifecycle grace) before the output is persisted; the entry is
// released only after AppendOutput commits.
func (r *Runtime) handleOutput(raw []byte) {
	var matched *pendingRequest
	lookup := func(id json.RawMessage) (pendingMeta, bool) {
		entry, meta, ok := r.beginCommit(id)
		if !ok {
			return pendingMeta{}, false
		}
		matched = entry
		return meta, true
	}

	out, ok := classifyOutput(raw, lookup)
	if !ok {
		if matched != nil {
			r.rollbackCommit(matched)
		}
		r.persistSynthetic(invalidStdoutMethod, nil)
		return
	}
	r.commitClassified(out, matched)
}

// beginCommit atomically moves a matching waiting or grace entry to committing
// and cancels its grace timer while retaining its metadata, waiter, slot,
// duplicate reservation, and accounting. A committing entry is not matched
// twice, and a missing entry means an unmatched response.
func (r *Runtime) beginCommit(id json.RawMessage) (*pendingRequest, pendingMeta, bool) {
	key, err := idKey(id)
	if err != nil {
		return nil, pendingMeta{}, false
	}
	r.corrMu.Lock()
	defer r.corrMu.Unlock()
	entry := r.corr[key]
	if entry == nil || entry.state == corrCommitting {
		return nil, pendingMeta{}, false
	}
	entry.state = corrCommitting
	if entry.grace != nil {
		entry.grace.Stop()
	}
	return entry, pendingMeta{
		Lifecycle: entry.lifecycle,
		SessionID: entry.sessionID,
		CWD:       entry.cwd,
	}, true
}

// rollbackCommit returns a committing entry to waiting when classification
// failed after the transition but before AppendOutput.
func (r *Runtime) rollbackCommit(entry *pendingRequest) {
	r.corrMu.Lock()
	if r.corr[entry.key] == entry && entry.state == corrCommitting {
		entry.state = corrWaiting
	}
	r.corrMu.Unlock()
}

// persistSynthetic commits one synthetic notification for invalid stdout or
// agent exit. Raw invalid content is never persisted.
func (r *Runtime) persistSynthetic(method string, sessionID *string) {
	payload := []byte(`{"jsonrpc":"2.0","method":` + strconv.Quote(method) + `}`)
	r.commitClassified(classifiedOutput{
		Kind:      "notification",
		Method:    &method,
		Payload:   payload,
		SessionID: sessionID,
	}, nil)
}

// commitClassified persists one classified output through the injected
// AppendOutput seam and only then attempts a coalesced wakeup. A matched entry
// is released through completeResponse; a storage failure fails the committing
// waiter with ErrPersistence and kills the runtime so no uncommitted waiter or
// wakeup is ever exposed.
func (r *Runtime) commitClassified(out classifiedOutput, entry *pendingRequest) {
	if _, err := r.append(out); err != nil {
		r.log.Error("persist agent output", "server_id", r.serverID, "error", err)
		// The committing entry, its waiter, and its accounting are retained
		// until the single terminal path clears them and delivers ErrPersistence.
		r.failPersistence(err)
		return
	}
	if entry == nil {
		r.signalCommitted()
		return
	}
	r.completeResponse(entry, out.Payload)
}

// append persists one classified output through the injected appendOutput seam
// or the store. A runtime with neither is a persistence failure.
func (r *Runtime) append(out classifiedOutput) (acpstore.Event, error) {
	if r.appendOutput != nil {
		return r.appendOutput(context.Background(), r.serverID, out.storeOutput())
	}
	if r.store == nil {
		return acpstore.Event{}, errors.Join(ErrPersistence, errors.New("no store"))
	}
	return r.store.AppendOutput(context.Background(), r.serverID, out.storeOutput())
}

// signalCommitted attempts a non-blocking send to the capacity-one wakeup
// channel; a full channel coalesces the signal and never blocks the pump. It
// never sends after finish closes the channel.
func (r *Runtime) signalCommitted() {
	r.wakeMu.Lock()
	defer r.wakeMu.Unlock()
	if r.wakeClosed {
		return
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// closeWake closes the wakeup channel exactly once.
func (r *Runtime) closeWake() {
	r.wakeMu.Lock()
	defer r.wakeMu.Unlock()
	if r.wakeClosed {
		return
	}
	r.wakeClosed = true
	close(r.wake)
}

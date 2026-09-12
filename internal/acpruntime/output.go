package acpruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

const (
	// maxOutputLine matches the bridge's 10 MiB JSON body hard ceiling.
	maxOutputLine = 10 * 1024 * 1024

	// outputReadBuffer sizes the bufio.Reader below maxOutputLine so an overlong
	// line surfaces as bufio.ErrBufferFull and can be drained.
	outputReadBuffer = 32 * 1024

	// maxSessionIDBytes and maxCWDBytes bound agent-supplied session metadata.
	maxSessionIDBytes = 1024
	maxCWDBytes       = 4096

	// synthetic notification methods.
	invalidStdoutMethod = "_adapter/invalid_stdout"
	agentExitedMethod   = "_adapter/agent_exited"
)

// Lifecycle is the bounded lifecycle kind retained for a client request.
type Lifecycle string

const (
	LifecycleNone   Lifecycle = "none"
	LifecycleNew    Lifecycle = "new"
	LifecycleLoad   Lifecycle = "load"
	LifecycleResume Lifecycle = "resume"
)

// pendingMeta is the bounded request metadata the output classifier reads for a
// matching response. Task 2.9c owns the correlation map and supplies this seam.
type pendingMeta struct {
	Lifecycle Lifecycle
	SessionID *string
	CWD       *string
}

// pendingLookup resolves a response ID to its reserved request metadata.
type pendingLookup func(id json.RawMessage) (pendingMeta, bool)

// classifiedOutput is one classified agent-output envelope ready to persist.
type classifiedOutput struct {
	Kind      string
	Method    *string
	Payload   json.RawMessage
	SessionID *string
	Mutation  *acpstore.SessionMutation
}

func (c classifiedOutput) storeOutput() acpstore.Output {
	return acpstore.Output{
		Kind:      c.Kind,
		Method:    c.Method,
		Payload:   c.Payload,
		SessionID: c.SessionID,
		Mutation:  c.Mutation,
	}
}

// sessionScopedMethods is the authoritative §5 field map: sessionId is
// inspected only on these agent-originated methods. elicitation/create is
// deliberately absent because it carries no sessionId.
var sessionScopedMethods = map[string]bool{
	"session/prompt":             true,
	"session/cancel":             true,
	"session/update":             true,
	"session/request_permission": true,
	"fs/read_text_file":          true,
	"fs/write_text_file":         true,
	"terminal/create":            true,
	"terminal/output":            true,
	"terminal/wait_for_exit":     true,
	"terminal/kill":              true,
	"terminal/release":           true,
	"session/set_mode":           true,
	"session/set_config_option":  true,
	"session/close":              true,
	"session/delete":             true,
}

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

// classifyOutput inspects one agent-output object without decoding or
// re-marshaling its bytes. It classifies kind and method, validates the ID
// shape, and inspects only bounded session metadata for session-scoped
// methods. Unrelated objects carrying sessionId/cwd keys are left untouched.
func classifyOutput(raw []byte, lookup pendingLookup) (classifiedOutput, bool) {
	if !utf8.Valid(raw) {
		return classifiedOutput{}, false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return classifiedOutput{}, false
	}

	methodRaw, hasMethod := obj["method"]
	idRaw, hasID := obj["id"]
	_, hasResult := obj["result"]
	_, hasError := obj["error"]

	var method string
	if hasMethod {
		if isJSONNull(methodRaw) || json.Unmarshal(methodRaw, &method) != nil || method == "" {
			return classifiedOutput{}, false
		}
	}
	if hasID && !validOutputID(idRaw) {
		return classifiedOutput{}, false
	}

	out := classifiedOutput{Payload: raw}
	switch {
	case hasMethod && hasID:
		out.Kind = "request"
	case hasMethod:
		out.Kind = "notification"
	case hasID:
		if hasResult == hasError {
			return classifiedOutput{}, false
		}
		out.Kind = "response"
	default:
		return classifiedOutput{}, false
	}
	if hasMethod {
		out.Method = &method
	}

	if out.Kind == "response" {
		out.attachResponse(obj, lookup)
		return out, true
	}
	if sessionScopedMethods[method] {
		if sessionID, ok := paramsString(obj["params"], "sessionId", maxSessionIDBytes); ok {
			out.SessionID = &sessionID
		}
	}
	return out, true
}

// attachResponse attributes a response event and, for a successful lifecycle
// response, derives its session roster/cwd mutation from the matching pending
// request. A session/new response has no prior session ID, so its result
// sessionId is used with the pending cwd.
func (o *classifiedOutput) attachResponse(obj map[string]json.RawMessage, lookup pendingLookup) {
	if lookup == nil {
		return
	}
	_, hasError := obj["error"]
	result := obj["result"]

	pending, ok := lookup(obj["id"])
	if !ok {
		return
	}
	if pending.SessionID != nil {
		o.SessionID = pending.SessionID
	}
	o.Mutation = sessionMutation(pending, result, hasError)
	if pending.Lifecycle == LifecycleNew && !hasError {
		if sessionID, ok := resultSessionID(result); ok {
			o.SessionID = &sessionID
		}
	}
}

// sessionMutation derives the roster/cwd change of a successful lifecycle
// response. Error responses and malformed or over-limit success payloads never
// mutate; session/load and session/resume use the retained request identity.
func sessionMutation(pending pendingMeta, result json.RawMessage, hasError bool) *acpstore.SessionMutation {
	if hasError {
		return nil
	}
	switch pending.Lifecycle {
	case LifecycleNew:
		sessionID, ok := resultSessionID(result)
		if !ok {
			return nil
		}
		return &acpstore.SessionMutation{Lifecycle: string(LifecycleNew), SessionID: sessionID, CWD: boundedCWD(pending.CWD)}
	case LifecycleLoad, LifecycleResume:
		if pending.SessionID == nil {
			return nil
		}
		return &acpstore.SessionMutation{Lifecycle: string(pending.Lifecycle), SessionID: *pending.SessionID, CWD: boundedCWD(pending.CWD)}
	default:
		return nil
	}
}

// resultSessionID extracts a bounded result.sessionId from a lifecycle success.
func resultSessionID(result json.RawMessage) (string, bool) {
	if len(result) == 0 {
		return "", false
	}
	var body struct {
		SessionID json.RawMessage `json:"sessionId"`
	}
	if json.Unmarshal(result, &body) != nil {
		return "", false
	}
	return boundedString(body.SessionID, maxSessionIDBytes)
}

// paramsString extracts a bounded string field from a JSON object params.
func paramsString(params json.RawMessage, key string, limit int) (string, bool) {
	if len(params) == 0 || isJSONNull(params) {
		return "", false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(params, &fields) != nil {
		return "", false
	}
	return boundedString(fields[key], limit)
}

// boundedString returns the decoded JSON string when it is a string no longer
// than limit UTF-8 bytes.
func boundedString(raw json.RawMessage, limit int) (string, bool) {
	if len(raw) == 0 || isJSONNull(raw) {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	if len(s) > limit {
		return "", false
	}
	return s, true
}

// validOutputID reports whether raw is a non-null JSON string or number ID.
func validOutputID(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false
	}
	switch raw[0] {
	case '"':
		return json.Valid(raw)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return json.Valid(raw)
	default:
		return false
	}
}

func isJSONNull(raw json.RawMessage) bool {
	return string(bytes.TrimSpace(raw)) == "null"
}

// boundedCWD returns the retained cwd within its byte cap, or an empty string
// when it is absent or over-limit.
func boundedCWD(p *string) string {
	if p == nil || len(*p) > maxCWDBytes {
		return ""
	}
	return *p
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

// readRawLine reads one newline-terminated record without letting a single
// record grow past limit bytes. It returns the retained bytes (never longer
// than limit), whether the record exceeded limit, and the read error. A nil
// error means the record ended at a newline; on error the caller processes any
// retained bytes and then stops.
func readRawLine(reader *bufio.Reader, limit int) (line []byte, oversized bool, err error) {
	for {
		fragment, readErr := reader.ReadSlice('\n')
		newline := bytes.IndexByte(fragment, '\n')
		if newline < 0 {
			if !oversized {
				if len(line)+len(fragment) > limit {
					oversized = true
				} else {
					line = append(line, fragment...)
				}
			}
			if readErr == bufio.ErrBufferFull {
				continue
			}
			return line, oversized, readErr
		}
		if !oversized {
			if len(line)+newline > limit {
				oversized = true
			} else {
				line = append(line, fragment[:newline]...)
			}
		}
		return line, oversized, nil
	}
}

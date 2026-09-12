package acpruntime

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Exported typed/sentinel errors that Phase 03 maps to HTTP statuses. The
// runtime never surfaces a raw internal error through Post.
var (
	// ErrInvalidEnvelope reports a client payload that is not one of the three
	// authoritative client envelope forms or exceeds a bounded ID/session limit.
	ErrInvalidEnvelope = errors.New("acpruntime: invalid client envelope")

	// ErrDuplicateID reports a canonical request ID already reserved by a
	// waiting, grace-retained, or committing correlation.
	ErrDuplicateID = errors.New("acpruntime: duplicate request id")

	// ErrCapacity reports that the runtime already holds the maximum 256
	// waiting, grace-retained, or committing correlations.
	ErrCapacity = errors.New("acpruntime: correlation capacity exhausted")

	// ErrRequestTimeout reports that the configured request deadline expired.
	ErrRequestTimeout = errors.New("acpruntime: request timeout")

	// ErrExited reports that the process is not running, so no envelope can be
	// admitted or answered.
	ErrExited = errors.New("acpruntime: process is not running")

	// ErrWrite reports a stdin write failed before a complete record was
	// emitted, or that a partial record made the stream unusable.
	ErrWrite = errors.New("acpruntime: stdin write failed")

	// ErrPersistence reports that a store operation failed; the runtime is
	// failed and terminated rather than exposing uncommitted state.
	ErrPersistence = errors.New("acpruntime: persistence failure")
)

// PostResult is the outcome of one client post: a matched response body or an
// accepted (notification/client response) acknowledgement.
type PostResult struct {
	Response json.RawMessage
	Accepted bool
}

// Post validates payload as one client envelope and forwards it to the agent.
// Only ClassifyClientEnvelope validates client envelopes; the invalid corpus is
// rejected with ErrInvalidEnvelope before any reservation or stdin write.
//
// Every envelope enters the same capacity-256 writer queue under the configured
// deadline. A request first reserves one correlation slot (rejecting duplicate
// canonical IDs and overflow with ErrDuplicateID/ErrCapacity), which reconciles
// durable status to busy. Notifications and client responses reserve no
// correlation. A request is answered only after its matching agent response is
// committed to SQLite.
func (r *Runtime) Post(ctx context.Context, payload json.RawMessage) (PostResult, error) {
	kind, pending, err := ClassifyClientEnvelope(payload)
	if err != nil {
		return PostResult{}, err
	}
	if r.exited.Load() || r.poisoned.Load() {
		return PostResult{}, ErrExited
	}
	if r.writerQueue == nil {
		return PostResult{}, ErrExited
	}

	line, err := compactLine(payload)
	if err != nil {
		return PostResult{}, err
	}

	if kind == ClientRequest {
		return r.postRequest(ctx, line, pending)
	}

	item := newWriteItem(line)
	deadline := time.NewTimer(r.requestTimeout)
	defer deadline.Stop()

	if err := r.admit(ctx, item, deadline.C); err != nil {
		return PostResult{}, err
	}
	if err := r.awaitWrite(ctx, item, deadline.C); err != nil {
		return PostResult{}, err
	}
	return PostResult{Accepted: true}, nil
}

// postRequest reserves a correlation, writes the request, and waits for its
// committed response.
func (r *Runtime) postRequest(ctx context.Context, line []byte, pending Pending) (PostResult, error) {
	entry, err := r.reserve(pending)
	if err != nil {
		return PostResult{}, err
	}

	item := newWriteItem(line)
	if err := r.admit(ctx, item, entry.expired); err != nil {
		r.releaseEntry(entry)
		return PostResult{}, err
	}
	if err := r.awaitWrite(ctx, item, entry.expired); err != nil {
		r.releaseEntry(entry)
		return PostResult{}, err
	}
	r.markWritten(entry)
	return r.awaitResponse(ctx, entry)
}

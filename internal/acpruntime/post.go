package acpruntime

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Sentinels the HTTP layer maps to problem responses. The runtime never
// surfaces a raw internal error through Post.
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

// PostResult is the outcome of one client post: either a matched response body
// or an accepted acknowledgement.
type PostResult struct {
	// Response is the matching agent response body. It is set only when
	// Accepted is false.
	Response json.RawMessage
	// Accepted reports that the envelope needed no response and its record was
	// written. It is true only when Response is nil.
	Accepted bool
}

// Post validates payload as one client envelope and forwards it to the agent.
// Invalid envelopes are rejected with ErrInvalidEnvelope before any write.
//
// A notification or client response is acknowledged once its record is written.
// A request reserves one bounded correlation slot, rejecting duplicate IDs with
// ErrDuplicateID and exhaustion with ErrCapacity, and is answered only after its
// matching agent response is committed to durable storage. The configured
// request timeout bounds every envelope and returns ErrRequestTimeout; caller
// cancellation returns ctx.Err(). A written lifecycle request that times out
// keeps its correlation through the grace window so a late response still
// commits.
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

package acpstore

import (
	"encoding/json"
	"errors"
)

// Status is the durable lifecycle state of an ACP server.
type Status string

const (
	StatusCreating Status = "creating"
	StatusIdle     Status = "idle"
	StatusBusy     Status = "busy"
	StatusExited   Status = "exited"
)

// Sentinel errors that the transport layer maps to HTTP statuses.
var (
	// ErrNotFound reports an unknown server or session.
	ErrNotFound = errors.New("not found")
	// ErrConflict reports a duplicate server or a conflicting agent.
	ErrConflict = errors.New("conflict")
	// ErrDeleted reports an operation on a server being deleted.
	ErrDeleted = errors.New("deleted")
	// ErrValidation reports invalid caller input such as negative query values,
	// an unknown event kind, or a payload that is not valid UTF-8.
	ErrValidation = errors.New("validation")
	// ErrSequenceExhausted reports that a server's event sequence reached
	// math.MaxInt64, so no further event can be allocated without overflow.
	ErrSequenceExhausted = errors.New("event sequence exhausted")
)

// Output is one classified agent-output envelope supplied by the runtime for
// persistence. Payload carries the exact agent bytes after JSONL framing
// whitespace removal; the store never interprets or rewrites it.
type Output struct {
	Kind      string
	Method    *string
	Payload   json.RawMessage
	SessionID *string
	Mutation  *SessionMutation
}

// SessionMutation is the roster and cwd change produced by a successful
// lifecycle response. Lifecycle is one of "new", "load", or "resume".
type SessionMutation struct {
	Lifecycle string
	SessionID string
	CWD       string
}

// Event is one persisted, sequenced agent-output envelope.
type Event struct {
	ServerID    string
	Seq         int64
	Kind        string
	Method      *string
	Payload     json.RawMessage
	SessionID   *string
	CreatedAtMs int64
}

// EventQuery selects events of one server. After is exclusive; a nil SessionID
// selects every session. A Limit of zero or less returns every matching event.
type EventQuery struct {
	SessionID *string
	After     int64
	Limit     int
	Desc      bool
}

// Server is one persisted row of the servers table.
type Server struct {
	ServerID     string
	Agent        string
	Status       Status
	CreatedAtMs  int64
	UpdatedAtMs  int64
	IdleSinceMs  *int64
	LastEventSeq int64
	PID          *int
	ExitedAtMs   *int64
}

// Session is one persisted row of the server_sessions table.
type Session struct {
	ServerID    string
	SessionID   string
	CWD         string
	CreatedAtMs int64
	UpdatedAtMs int64
}

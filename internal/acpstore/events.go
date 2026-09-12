package acpstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

const eventColumns = `server_id, seq, kind, method, payload, session_id, created_at_ms`

// AppendOutput persists one classified agent-output envelope. It allocates the
// server's next int64 sequence, inserts the event, applies any successful
// lifecycle session mutation, and advances the server watermark and timestamp
// in a single transaction. It returns the event only after that transaction
// commits.
func (s *Store) AppendOutput(ctx context.Context, serverID string, output Output) (Event, error) {
	if !output.valid() {
		return Event{}, fmt.Errorf("%w: invalid output for server %q", ErrValidation, serverID)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, fmt.Errorf("begin append for server %q: %w", serverID, err)
	}
	defer func() { _ = tx.Rollback() }()

	var lastSeq int64
	err = tx.QueryRowContext(ctx,
		`SELECT last_event_seq FROM servers WHERE server_id = ?`, serverID).Scan(&lastSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, fmt.Errorf("%w: server %q", ErrNotFound, serverID)
	}
	if err != nil {
		return Event{}, fmt.Errorf("read watermark for server %q: %w", serverID, err)
	}
	if lastSeq == math.MaxInt64 {
		return Event{}, fmt.Errorf("%w: server %q at sequence %d", ErrSequenceExhausted, serverID, lastSeq)
	}
	seq := lastSeq + 1

	now := s.nowMs()
	if output.Mutation != nil {
		if err := upsertSession(ctx, tx, serverID, output.Mutation, now); err != nil {
			return Event{}, err
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO events (`+eventColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		serverID, seq, output.Kind, nullString(output.Method), []byte(output.Payload),
		nullString(output.SessionID), now); err != nil {
		return Event{}, fmt.Errorf("insert event for server %q: %w", serverID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE servers SET last_event_seq = ?, updated_at_ms = ? WHERE server_id = ?`,
		seq, now, serverID); err != nil {
		return Event{}, fmt.Errorf("advance server %q: %w", serverID, err)
	}

	event := Event{
		ServerID:    serverID,
		Seq:         seq,
		Kind:        output.Kind,
		Method:      cloneString(output.Method),
		Payload:     bytes.Clone(output.Payload),
		SessionID:   cloneString(output.SessionID),
		CreatedAtMs: now,
	}
	if err := tx.Commit(); err != nil {
		return Event{}, fmt.Errorf("commit append for server %q: %w", serverID, err)
	}
	return event, nil
}

// Events returns the events of one server ordered by sequence. After is
// exclusive; a nil SessionID selects every session, and Limit <= 0 returns
// every matching event. An unknown server or session filter returns ErrNotFound.
func (s *Store) Events(ctx context.Context, serverID string, q EventQuery) ([]Event, error) {
	if q.After < 0 {
		return nil, fmt.Errorf("%w: after must be nonnegative", ErrValidation)
	}
	if q.Limit < 0 {
		return nil, fmt.Errorf("%w: limit must be nonnegative", ErrValidation)
	}
	if _, err := s.Server(ctx, serverID); err != nil {
		return nil, err
	}
	if q.SessionID != nil {
		if _, err := s.Session(ctx, serverID, *q.SessionID); err != nil {
			return nil, err
		}
	}

	query := `SELECT ` + eventColumns + ` FROM events WHERE server_id = ? AND seq > ?`
	args := []any{serverID, q.After}
	if q.SessionID != nil {
		query += ` AND session_id = ?`
		args = append(args, *q.SessionID)
	}
	if q.Desc {
		query += ` ORDER BY seq DESC`
	} else {
		query += ` ORDER BY seq ASC`
	}
	if q.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, q.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list events for server %q: %w", serverID, err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event for server %q: %w", serverID, err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list events for server %q: %w", serverID, err)
	}
	return events, nil
}

// valid reports whether the output is structurally persistable. Classification
// itself is the runtime's responsibility; the store only rejects values the
// schema and UTF-8 contract cannot hold.
func (o Output) valid() bool {
	switch o.Kind {
	case "request", "response", "notification":
	default:
		return false
	}
	return utf8.Valid(o.Payload)
}

// upsertSession applies one successful lifecycle roster mutation. A new session
// is inserted with created_at_ms; an existing row keeps created_at_ms and has
// its cwd and updated_at_ms replaced.
func upsertSession(ctx context.Context, tx *sql.Tx, serverID string, mutation *SessionMutation, now int64) error {
	switch mutation.Lifecycle {
	case "new", "load", "resume":
	default:
		return nil
	}
	if mutation.SessionID == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO server_sessions (server_id, session_id, cwd, created_at_ms, updated_at_ms)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (server_id, session_id) DO UPDATE SET
		     cwd = excluded.cwd,
		     updated_at_ms = excluded.updated_at_ms`,
		serverID, mutation.SessionID, mutation.CWD, now, now); err != nil {
		return fmt.Errorf("upsert session %q in server %q: %w", mutation.SessionID, serverID, err)
	}
	return nil
}

// scanEvent reads one event row with defensive copies of raw and nullable data.
func scanEvent(row rowScanner) (Event, error) {
	var (
		event     Event
		method    sql.NullString
		payload   []byte
		sessionID sql.NullString
	)
	if err := row.Scan(&event.ServerID, &event.Seq, &event.Kind, &method, &payload,
		&sessionID, &event.CreatedAtMs); err != nil {
		return Event{}, err
	}
	event.Method = nullStringPtr(method)
	event.SessionID = nullStringPtr(sessionID)
	event.Payload = bytes.Clone(payload)
	return event, nil
}

func nullString(p *string) sql.NullString {
	if p == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *p, Valid: true}
}

func nullStringPtr(n sql.NullString) *string {
	if !n.Valid {
		return nil
	}
	value := n.String
	return &value
}

func cloneString(p *string) *string {
	if p == nil {
		return nil
	}
	value := *p
	return &value
}

package acpstore

import (
	"context"
	"fmt"
)

// schema is the authoritative DDL from the specification's "ACP state
// endpoints and schema" section. Every statement is idempotent so opening an
// existing database re-applies it without error or data loss.
const schema = `
CREATE TABLE IF NOT EXISTS servers (
    server_id      TEXT PRIMARY KEY,
    agent          TEXT NOT NULL,
    status         TEXT NOT NULL,
    created_at_ms  INTEGER NOT NULL,
    updated_at_ms  INTEGER NOT NULL,
    idle_since_ms  INTEGER,
    last_event_seq INTEGER NOT NULL DEFAULT 0,
    pid            INTEGER,
    exited_at_ms   INTEGER
);

CREATE TABLE IF NOT EXISTS server_sessions (
    server_id     TEXT NOT NULL,
    session_id    TEXT NOT NULL,
    cwd           TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (server_id, session_id),
    FOREIGN KEY (server_id) REFERENCES servers (server_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
    server_id     TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    kind          TEXT NOT NULL CHECK (kind IN ('request', 'response', 'notification')),
    method        TEXT,
    payload       BLOB NOT NULL,
    session_id    TEXT,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (server_id, seq),
    FOREIGN KEY (server_id) REFERENCES servers (server_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS events_server_session_seq
    ON events (server_id, session_id, seq);
`

// applySchema creates the authoritative tables and index. It is safe to call
// on every open because every statement is guarded by IF NOT EXISTS.
func (s *Store) applySchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

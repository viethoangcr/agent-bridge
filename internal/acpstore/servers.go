package acpstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const serverColumns = `server_id, agent, status, created_at_ms, updated_at_ms,
	idle_since_ms, last_event_seq, pid, exited_at_ms`

const sessionColumns = `server_id, session_id, cwd, created_at_ms, updated_at_ms`

// nowMs returns the current time in Unix milliseconds from the store clock.
func (s *Store) nowMs() int64 {
	if s.clock == nil {
		return time.Now().UnixMilli()
	}
	return s.clock().UnixMilli()
}

// CreateServer inserts a new server in the creating state with zeroed
// counters and null live metadata, using the store clock for both timestamps.
func (s *Store) CreateServer(ctx context.Context, serverID, agent string) (Server, error) {
	now := s.nowMs()
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO servers (server_id, agent, status, created_at_ms, updated_at_ms, last_event_seq)
		 VALUES (?, ?, ?, ?, ?, 0)
		 ON CONFLICT (server_id) DO NOTHING`,
		serverID, agent, string(StatusCreating), now, now)
	if err != nil {
		return Server{}, fmt.Errorf("create server %q: %w", serverID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return Server{}, fmt.Errorf("create server %q: %w", serverID, err)
	}
	if affected == 0 {
		return Server{}, fmt.Errorf("%w: server %q", ErrConflict, serverID)
	}
	return Server{
		ServerID:    serverID,
		Agent:       agent,
		Status:      StatusCreating,
		CreatedAtMs: now,
		UpdatedAtMs: now,
	}, nil
}

// Server returns one server by ID, or ErrNotFound.
func (s *Store) Server(ctx context.Context, serverID string) (Server, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+serverColumns+` FROM servers WHERE server_id = ?`, serverID)
	server, err := scanServer(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Server{}, fmt.Errorf("%w: server %q", ErrNotFound, serverID)
	}
	if err != nil {
		return Server{}, fmt.Errorf("get server %q: %w", serverID, err)
	}
	return server, nil
}

// Servers returns every server ordered by server ID.
func (s *Store) Servers(ctx context.Context) ([]Server, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+serverColumns+` FROM servers ORDER BY server_id`)
	if err != nil {
		return nil, fmt.Errorf("list servers: %w", err)
	}
	defer rows.Close()

	var servers []Server
	for rows.Next() {
		server, err := scanServer(rows)
		if err != nil {
			return nil, fmt.Errorf("scan server: %w", err)
		}
		servers = append(servers, server)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list servers: %w", err)
	}
	return servers, nil
}

// SetLive transitions a server from creating to idle and records its PID.
func (s *Store) SetLive(ctx context.Context, serverID string, pid int) error {
	now := s.nowMs()
	res, err := s.db.ExecContext(ctx,
		`UPDATE servers
		 SET status = ?, pid = ?, idle_since_ms = ?, updated_at_ms = ?
		 WHERE server_id = ?`,
		string(StatusIdle), pid, now, now, serverID)
	if err != nil {
		return fmt.Errorf("set server %q live: %w", serverID, err)
	}
	if err := requireAffected(res); err != nil {
		return fmt.Errorf("set server %q live: %w", serverID, err)
	}
	return nil
}

// SetStatus transitions a server between idle and busy, setting idle_since_ms
// when it enters idle and clearing it when it leaves.
func (s *Store) SetStatus(ctx context.Context, serverID string, status Status) error {
	now := s.nowMs()
	var idleSince sql.NullInt64
	if status == StatusIdle {
		idleSince = sql.NullInt64{Int64: now, Valid: true}
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE servers
		 SET status = ?, idle_since_ms = ?, updated_at_ms = ?
		 WHERE server_id = ?`,
		string(status), idleSince, now, serverID)
	if err != nil {
		return fmt.Errorf("set server %q status: %w", serverID, err)
	}
	if err := requireAffected(res); err != nil {
		return fmt.Errorf("set server %q status: %w", serverID, err)
	}
	return nil
}

// MarkExited marks a server exited, clearing live metadata. The exit
// timestamp is set once; repeated calls keep the original value.
func (s *Store) MarkExited(ctx context.Context, serverID string) error {
	now := s.nowMs()
	res, err := s.db.ExecContext(ctx,
		`UPDATE servers
		 SET status = ?, pid = NULL, idle_since_ms = NULL,
		     exited_at_ms = COALESCE(exited_at_ms, ?), updated_at_ms = ?
		 WHERE server_id = ?`,
		string(StatusExited), now, now, serverID)
	if err != nil {
		return fmt.Errorf("mark server %q exited: %w", serverID, err)
	}
	if err := requireAffected(res); err != nil {
		return fmt.Errorf("mark server %q exited: %w", serverID, err)
	}
	return nil
}

// DeleteServer removes a server, cascading its sessions and events.
func (s *Store) DeleteServer(ctx context.Context, serverID string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM servers WHERE server_id = ?`, serverID)
	if err != nil {
		return fmt.Errorf("delete server %q: %w", serverID, err)
	}
	if err := requireAffected(res); err != nil {
		return fmt.Errorf("delete server %q: %w", serverID, err)
	}
	return nil
}

// Session returns one session by server and session ID, or ErrNotFound.
func (s *Store) Session(ctx context.Context, serverID, sessionID string) (Session, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+sessionColumns+` FROM server_sessions WHERE server_id = ? AND session_id = ?`,
		serverID, sessionID)
	session, err := scanSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, fmt.Errorf("%w: session %q in server %q", ErrNotFound, sessionID, serverID)
	}
	if err != nil {
		return Session{}, fmt.Errorf("get session %q in server %q: %w", sessionID, serverID, err)
	}
	return session, nil
}

// Sessions returns every session of a server ordered by session ID. An unknown
// server is ErrNotFound.
func (s *Store) Sessions(ctx context.Context, serverID string) ([]Session, error) {
	if _, err := s.Server(ctx, serverID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+sessionColumns+` FROM server_sessions WHERE server_id = ? ORDER BY session_id`,
		serverID)
	if err != nil {
		return nil, fmt.Errorf("list sessions for server %q: %w", serverID, err)
	}
	defer rows.Close()

	var sessions []Session
	for rows.Next() {
		session, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("scan session for server %q: %w", serverID, err)
		}
		sessions = append(sessions, session)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list sessions for server %q: %w", serverID, err)
	}
	return sessions, nil
}

// rowScanner is satisfied by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanServer reads one server row and defensively copies its nullable values.
func scanServer(row rowScanner) (Server, error) {
	var (
		server    Server
		idleSince sql.NullInt64
		pid       sql.NullInt64
		exitedAt  sql.NullInt64
	)
	if err := row.Scan(&server.ServerID, &server.Agent, &server.Status, &server.CreatedAtMs,
		&server.UpdatedAtMs, &idleSince, &server.LastEventSeq, &pid, &exitedAt); err != nil {
		return Server{}, err
	}
	server.IdleSinceMs = nullInt64Ptr(idleSince)
	server.PID = nullIntPtr(pid)
	server.ExitedAtMs = nullInt64Ptr(exitedAt)
	return server, nil
}

// scanSession reads one session row.
func scanSession(row rowScanner) (Session, error) {
	var session Session
	if err := row.Scan(&session.ServerID, &session.SessionID, &session.CWD,
		&session.CreatedAtMs, &session.UpdatedAtMs); err != nil {
		return Session{}, err
	}
	return session, nil
}

func nullInt64Ptr(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	value := n.Int64
	return &value
}

func nullIntPtr(n sql.NullInt64) *int {
	if !n.Valid {
		return nil
	}
	value := int(n.Int64)
	return &value
}

// requireAffected maps an update or delete that matched no row to ErrNotFound.
func requireAffected(res sql.Result) error {
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

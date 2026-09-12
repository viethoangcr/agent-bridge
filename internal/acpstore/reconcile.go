package acpstore

import (
	"context"
	"errors"
	"fmt"
)

// Reconcile marks every persisted live row (creating, idle, or busy) as exited
// and clears its PID and idle timestamp, using the store clock for the exit and
// update timestamps. Already-exited rows are untouched, so their original
// exited_at_ms and metadata are retained. It runs exactly one update
// transaction.
//
// Reconcile never signals a persisted PID: process ownership belongs to the
// runtime that created it, and startup recovery only rewrites durable state.
func (s *Store) Reconcile(ctx context.Context) error {
	now := s.nowMs()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reconcile: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`UPDATE servers
		 SET status = ?, pid = NULL, idle_since_ms = NULL,
		     exited_at_ms = ?, updated_at_ms = ?
		 WHERE status IN (?, ?, ?)`,
		string(StatusExited), now, now,
		string(StatusCreating), string(StatusIdle), string(StatusBusy)); err != nil {
		return fmt.Errorf("reconcile servers: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reconcile: %w", err)
	}
	return nil
}

// Close checkpoints the WAL and closes the database exactly once. The
// checkpoint uses TRUNCATE and its result columns are read back so a busy
// (non-clean) checkpoint is surfaced as an error instead of being assumed
// clean. Repeated calls are idempotent and return the first result; checkpoint
// and close errors are joined when both occur. A store method called after
// Close returns the database/sql closed error rather than panicking.
func (s *Store) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.closeErr = errors.Join(s.checkpoint(ctx), s.db.Close())
	})
	return s.closeErr
}

// checkpoint truncates the write-ahead log and reports a nonzero busy status.
func (s *Store) checkpoint(ctx context.Context) error {
	var busy, logPages, checkpointed int
	if err := s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).
		Scan(&busy, &logPages, &checkpointed); err != nil {
		return fmt.Errorf("wal checkpoint: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("wal checkpoint: database busy")
	}
	return nil
}

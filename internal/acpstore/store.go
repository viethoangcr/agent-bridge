// Package acpstore owns the durable SQLite state for ACP servers, sessions,
// and events.
//
// Open creates the authoritative schema and returns a Store backed by a single
// configured connection. Persisted agent-output bytes are stored exactly as
// received after JSONL framing removal; this package never interprets
// conversation content.
package acpstore

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store is the durable ACP state database.
type Store struct {
	db    *sql.DB
	clock func() time.Time

	closeOnce sync.Once
	closeErr  error
}

// Open opens (creating when necessary) the SQLite database at path, applies
// the connection pragmas, and creates the schema. On any partial failure the
// underlying pool is closed before the error is returned.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	configure(db)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	store := &Store{db: db, clock: time.Now}
	if err := store.applySchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// dsn builds the escaped file: URI for path. The path is carried in the URL
// path field and the pragmas in RawQuery, so characters such as '?', '#', and
// '%' stay filename bytes instead of being read as query or fragment syntax.
// OmitHost keeps a relative path on the file: scheme rather than turning its
// first component into a URI authority.
func dsn(path string) string {
	query := url.Values{}
	query.Add("_pragma", "journal_mode(wal)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Set("_txlock", "immediate")

	u := url.URL{
		Scheme:   "file",
		Path:     path,
		RawQuery: query.Encode(),
		OmitHost: true,
	}
	return u.String()
}

// configure limits db to a single connection so database/sql serializes all
// SQL and driver-applied connection pragmas cannot be lost to pool recycling.
func configure(db *sql.DB) {
	db.SetMaxOpenConns(1)
}

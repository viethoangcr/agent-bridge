package acpstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// queryer is satisfied by *sql.DB and *sql.Conn, so pragma and schema
// assertions can run against the pool or one specific physical connection.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// columnSpec mirrors one PRAGMA table_info row.
type columnSpec struct {
	name    string
	typ     string
	notNull bool
	dflt    string
	pk      int
}

func TestOpenSchema(t *testing.T) {
	t.Run("tables, keys, cascades, and index", func(t *testing.T) {
		store := openTestStore(t, filepath.Join(t.TempDir(), "schema.db"))

		assertTables(t, store.db, []string{"events", "server_sessions", "servers"})
		assertTable(t, store.db, "servers", []columnSpec{
			{"server_id", "TEXT", false, "", 1},
			{"agent", "TEXT", true, "", 0},
			{"status", "TEXT", true, "", 0},
			{"created_at_ms", "INTEGER", true, "", 0},
			{"updated_at_ms", "INTEGER", true, "", 0},
			{"idle_since_ms", "INTEGER", false, "", 0},
			{"last_event_seq", "INTEGER", true, "0", 0},
			{"pid", "INTEGER", false, "", 0},
			{"exited_at_ms", "INTEGER", false, "", 0},
		})
		assertTable(t, store.db, "server_sessions", []columnSpec{
			{"server_id", "TEXT", true, "", 1},
			{"session_id", "TEXT", true, "", 2},
			{"cwd", "TEXT", true, "", 0},
			{"created_at_ms", "INTEGER", true, "", 0},
			{"updated_at_ms", "INTEGER", true, "", 0},
		})
		assertTable(t, store.db, "events", []columnSpec{
			{"server_id", "TEXT", true, "", 1},
			{"seq", "INTEGER", true, "", 2},
			{"kind", "TEXT", true, "", 0},
			{"method", "TEXT", false, "", 0},
			{"payload", "BLOB", true, "", 0},
			{"session_id", "TEXT", false, "", 0},
			{"created_at_ms", "INTEGER", true, "", 0},
		})

		assertForeignKey(t, store.db, "server_sessions", "server_id", "servers", "server_id", "CASCADE")
		assertForeignKey(t, store.db, "events", "server_id", "servers", "server_id", "CASCADE")
		assertEventsIndex(t, store.db)

		assertCascade(t, store.db)
		assertEventKindConstraint(t, store.db)
	})

	t.Run("connection pragmas and pool", func(t *testing.T) {
		store := openTestStore(t, filepath.Join(t.TempDir(), "pragmas.db"))

		assertPragmas(t, store.db)
		if got := store.db.Stats().MaxOpenConnections; got != 1 {
			t.Errorf("DB.Stats().MaxOpenConnections = %d, want 1", got)
		}
	})

	t.Run("idempotent reopen", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "idempotent.db")
		first := openTestStore(t, path)
		if _, err := first.db.ExecContext(t.Context(),
			`INSERT INTO servers (server_id, agent, status, created_at_ms, updated_at_ms)
			 VALUES ('srv-1', 'claude', 'idle', 1, 1)`); err != nil {
			t.Fatalf("seed server: %v", err)
		}

		// Opening again while the first store is live must not fail or drop data.
		second, err := Open(t.Context(), path)
		if err != nil {
			t.Fatalf("second Open: %v", err)
		}
		assertTable(t, second.db, "servers", []columnSpec{
			{"server_id", "TEXT", false, "", 1},
			{"agent", "TEXT", true, "", 0},
			{"status", "TEXT", true, "", 0},
			{"created_at_ms", "INTEGER", true, "", 0},
			{"updated_at_ms", "INTEGER", true, "", 0},
			{"idle_since_ms", "INTEGER", false, "", 0},
			{"last_event_seq", "INTEGER", true, "0", 0},
			{"pid", "INTEGER", false, "", 0},
			{"exited_at_ms", "INTEGER", false, "", 0},
		})
		if err := second.db.Close(); err != nil {
			t.Fatalf("close second store: %v", err)
		}

		// A full close/reopen must keep the schema and committed data.
		if err := first.db.Close(); err != nil {
			t.Fatalf("close first store: %v", err)
		}
		reopened, err := Open(t.Context(), path)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		t.Cleanup(func() { _ = reopened.db.Close() })
		assertTables(t, reopened.db, []string{"events", "server_sessions", "servers"})
		assertPragmas(t, reopened.db)

		var count int
		if err := reopened.db.QueryRowContext(t.Context(),
			`SELECT count(*) FROM servers WHERE server_id = 'srv-1'`).Scan(&count); err != nil {
			t.Fatalf("count servers: %v", err)
		}
		if count != 1 {
			t.Errorf("server count after reopen = %d, want 1", count)
		}
	})

	t.Run("pragmas survive connection recycling", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "recycle.db")
		store := openTestStore(t, path)

		conn, err := store.db.Conn(t.Context())
		if err != nil {
			t.Fatalf("acquire connection: %v", err)
		}
		if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("Conn.Raw error = %v, want driver.ErrBadConn", err)
		}
		// database/sql marks the connection bad and closes it, so Close
		// reports ErrConnDone rather than nil.
		if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			t.Fatalf("close discarded connection: %v", err)
		}

		// The pool must hand out a brand-new physical connection whose DSN
		// pragmas were re-applied by the driver.
		replacement, err := store.db.Conn(t.Context())
		if err != nil {
			t.Fatalf("replacement connection: %v", err)
		}
		assertPragmas(t, replacement)
		if err := replacement.Close(); err != nil {
			t.Fatalf("close replacement connection: %v", err)
		}

		if err := store.db.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		reopened, err := Open(t.Context(), path)
		if err != nil {
			t.Fatalf("reopen store: %v", err)
		}
		t.Cleanup(func() { _ = reopened.db.Close() })
		assertPragmas(t, reopened.db)
	})

	t.Run("txlock immediate", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "txlock.db")
		store := openTestStore(t, path)

		// _txlock=immediate makes Begin acquire the write lock at BEGIN, before
		// any write, so an independent connection cannot BEGIN IMMEDIATE.
		tx, err := store.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		defer func() { _ = tx.Rollback() }()

		probe, err := sql.Open("sqlite", dsn(path))
		if err != nil {
			t.Fatalf("open probe: %v", err)
		}
		defer probe.Close()
		probe.SetMaxOpenConns(1)

		probeConn, err := probe.Conn(t.Context())
		if err != nil {
			t.Fatalf("probe connection: %v", err)
		}
		defer probeConn.Close()
		if _, err := probeConn.ExecContext(t.Context(), "PRAGMA busy_timeout(50)"); err != nil {
			t.Fatalf("set probe busy_timeout: %v", err)
		}

		_, err = probeConn.ExecContext(t.Context(), "BEGIN IMMEDIATE")
		if err == nil {
			t.Fatal("independent connection acquired BEGIN IMMEDIATE while the store transaction held the write lock; _txlock=immediate is not applied")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "locked") && !strings.Contains(strings.ToLower(err.Error()), "busy") {
			t.Fatalf("probe BEGIN IMMEDIATE error = %v, want a busy/locked database error", err)
		}
	})

	t.Run("escaped filename", func(t *testing.T) {
		for _, name := range []string{"a?b.db", "a#b.db", "a%b.db"} {
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, name)

				store := openTestStore(t, path)
				assertPragmas(t, store.db)
				assertNoAlternateFiles(t, dir, name)
				if err := store.db.Close(); err != nil {
					t.Fatalf("close store: %v", err)
				}

				reopened, err := Open(t.Context(), path)
				if err != nil {
					t.Fatalf("reopen %q: %v", path, err)
				}
				t.Cleanup(func() { _ = reopened.db.Close() })
				assertPragmas(t, reopened.db)
				assertNoAlternateFiles(t, dir, name)
			})
		}
	})
}

// openTestStore opens a store and closes it when the test ends.
func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = store.db.Close() })
	return store
}

func assertTables(t *testing.T, q queryer, want []string) {
	t.Helper()
	rows, err := q.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite_master: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tables = %v, want %v", got, want)
	}
}

func assertTable(t *testing.T, q queryer, table string, want []columnSpec) {
	t.Helper()
	rows, err := q.QueryContext(context.Background(), "PRAGMA table_info("+table+")")
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer rows.Close()

	var got []columnSpec
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			t.Fatalf("scan %s column: %v", table, err)
		}
		got = append(got, columnSpec{name: name, typ: typ, notNull: notNull != 0, dflt: dflt.String, pk: pk})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s columns: %v", table, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PRAGMA table_info(%s) =\n  %+v\nwant\n  %+v", table, got, want)
	}
}

func assertForeignKey(t *testing.T, q queryer, table, from, refTable, to, onDelete string) {
	t.Helper()
	rows, err := q.QueryContext(context.Background(), "PRAGMA foreign_key_list("+table+")")
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_list(%s): %v", table, err)
	}
	defer rows.Close()

	type foreignKey struct {
		table, from, to, onUpdate, onDelete string
	}
	var got []foreignKey
	for rows.Next() {
		var id, seq int
		var ref, fromCol, toCol, onUpdate, onDeleteCol, match string
		if err := rows.Scan(&id, &seq, &ref, &fromCol, &toCol, &onUpdate, &onDeleteCol, &match); err != nil {
			t.Fatalf("scan %s foreign key: %v", table, err)
		}
		got = append(got, foreignKey{table: ref, from: fromCol, to: toCol, onUpdate: onUpdate, onDelete: onDeleteCol})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s foreign keys: %v", table, err)
	}
	want := []foreignKey{{table: refTable, from: from, to: to, onUpdate: "NO ACTION", onDelete: onDelete}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PRAGMA foreign_key_list(%s) = %+v, want %+v", table, got, want)
	}
}

func assertEventsIndex(t *testing.T, q queryer) {
	t.Helper()
	const index = "events_server_session_seq"

	rows, err := q.QueryContext(context.Background(), "PRAGMA index_list(events)")
	if err != nil {
		t.Fatalf("PRAGMA index_list(events): %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan events index list: %v", err)
		}
		if name == index {
			found = true
			if unique != 0 {
				t.Errorf("index %s must not be unique", index)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate events index list: %v", err)
	}
	if !found {
		t.Fatalf("index %s is missing", index)
	}

	infoRows, err := q.QueryContext(context.Background(), "PRAGMA index_info("+index+")")
	if err != nil {
		t.Fatalf("PRAGMA index_info(%s): %v", index, err)
	}
	defer infoRows.Close()

	var columns []string
	for infoRows.Next() {
		var seqno, cid int
		var name string
		if err := infoRows.Scan(&seqno, &cid, &name); err != nil {
			t.Fatalf("scan index info: %v", err)
		}
		columns = append(columns, name)
	}
	if err := infoRows.Err(); err != nil {
		t.Fatalf("iterate index info: %v", err)
	}
	want := []string{"server_id", "session_id", "seq"}
	if !reflect.DeepEqual(columns, want) {
		t.Errorf("index %s columns = %v, want %v", index, columns, want)
	}
}

func assertPragmas(t *testing.T, q queryer) {
	t.Helper()
	tests := []struct {
		pragma string
		want   string
	}{
		{"journal_mode", "wal"},
		{"foreign_keys", "1"},
		{"busy_timeout", "5000"},
		{"synchronous", "2"}, // 2 == FULL
	}
	for _, tc := range tests {
		var got string
		if err := q.QueryRowContext(context.Background(), "PRAGMA "+tc.pragma).Scan(&got); err != nil {
			t.Fatalf("PRAGMA %s: %v", tc.pragma, err)
		}
		if got != tc.want {
			t.Errorf("PRAGMA %s = %q, want %q", tc.pragma, got, tc.want)
		}
	}
}

func assertCascade(t *testing.T, q *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := q.ExecContext(ctx,
		`INSERT INTO servers (server_id, agent, status, created_at_ms, updated_at_ms)
		 VALUES ('cascade', 'claude', 'idle', 1, 1)`); err != nil {
		t.Fatalf("insert server: %v", err)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO server_sessions (server_id, session_id, cwd, created_at_ms, updated_at_ms)
		 VALUES ('cascade', 'sess-1', '/tmp', 1, 1)`); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO events (server_id, seq, kind, payload, created_at_ms)
		 VALUES ('cascade', 1, 'notification', X'7B7D', 1)`); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM servers WHERE server_id = 'cascade'`); err != nil {
		t.Fatalf("delete server: %v", err)
	}
	for _, table := range []string{"server_sessions", "events"} {
		var count int
		if err := q.QueryRowContext(ctx, `SELECT count(*) FROM `+table+` WHERE server_id = 'cascade'`).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s rows after cascade delete = %d, want 0", table, count)
		}
	}
}

func assertEventKindConstraint(t *testing.T, q *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := q.ExecContext(ctx,
		`INSERT INTO servers (server_id, agent, status, created_at_ms, updated_at_ms)
		 VALUES ('kinds', 'claude', 'idle', 1, 1)`); err != nil {
		t.Fatalf("insert server: %v", err)
	}
	for seq, kind := range []string{"request", "response", "notification"} {
		if _, err := q.ExecContext(ctx,
			`INSERT INTO events (server_id, seq, kind, payload, created_at_ms)
			 VALUES ('kinds', ?, ?, X'7B7D', 1)`, seq+1, kind); err != nil {
			t.Errorf("insert %s event: %v", kind, err)
		}
	}
	if _, err := q.ExecContext(ctx,
		`INSERT INTO events (server_id, seq, kind, payload, created_at_ms)
		 VALUES ('kinds', 100, 'bogus', X'7B7D', 1)`); err == nil {
		t.Error("insert of an event with an unknown kind succeeded, want CHECK constraint failure")
	}
}

// assertNoAlternateFiles verifies the exact intended filename exists and no
// truncated or escaped alternate database was created in dir.
func assertNoAlternateFiles(t *testing.T, dir, name string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	allowed := map[string]bool{name: true, name + "-wal": true, name + "-shm": true}
	sawDatabase := false
	for _, entry := range entries {
		if entry.Name() == name {
			sawDatabase = true
		}
		if !allowed[entry.Name()] {
			t.Errorf("unexpected file %q in %s; database path was truncated or escaped incorrectly", entry.Name(), dir)
		}
	}
	if !sawDatabase {
		t.Errorf("intended database file %q was not created in %s", name, dir)
	}
}

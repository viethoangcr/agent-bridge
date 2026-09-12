# Plan: Phase 02 - ACP Persistence and Stdio Runtime

**Date:** 2026-08-23
**Status:** DRAFT
**Risk Level:** High

---

## Phase Goal

Add the durable SQLite state layer and complete private stdio ACP subprocess runtime beneath the future HTTP proxy: create and reconcile the exact schema, resolve and launch configured agents, correlate JSON-RPC posts with committed responses, persist all agent output, retain/redact stderr, and provide a deterministic strict mock agent. This phase exposes no ACP HTTP handlers, SSE, idle reaper, or proxy manager.

## Assumptions and Specification References

- `docs/plans/20260815-agent-bridge.md` remains authoritative. This plan depends on the completed Phase 01 repository/config/lifecycle foundation and does not redefine the HTTP contract.
- Apply the master specification sections **Platform and dependencies**, **Authentication and errors**, **ACP HTTP contract** where it defines stdio post outcomes/correlation, **ACP lifecycle and persistence**, **ACP state endpoints and schema**, **Agent resolution**, **Non-ACP endpoints** process-group rule, and **Environment, shutdown, and image**.
- This plan is independently executable from a clean checkout after completing `docs/plans/20260823-01-scaffolding.md`; no unfinished Phase 03 code is required.
- Pin `modernc.org/sqlite` v1.57.0, which requires Go 1.25+ and is compatible with the repository's Go 1.26.7 pin, in `go.mod`/`go.sum`; no test-only modules are allowed.
- Database timestamps use Unix milliseconds from an injected `func() time.Time` in tests and `time.Now` in production.
- Persisted event `kind` is one of `request`, `response`, or `notification`; synthetic `_adapter/agent_exited` and `_adapter/invalid_stdout` envelopes are notifications. `request` means only an agent-originated reverse-call. Inbound client messages are never passed to `Store.AppendOutput` and therefore are never persisted.
- Runtime stdout accepts one valid-UTF-8 JSON object per line. A non-object, malformed JSON, invalid-UTF-8, batch array, or oversized line becomes one `_adapter/invalid_stdout` synthetic notification; raw invalid content is not persisted. The pump then continues when framing permits.
- Set the stdout line ceiling to the authoritative ACP maximum, 10 MiB. Use `bufio.Reader`, not `bufio.Scanner`, so an oversized line can be drained safely and converted into one synthetic event without permanently stopping the stream.
- Sequence values are limited everywhere to `0..math.MaxInt64`, matching SQLite `INTEGER`. Store models and queries use `int64`; allocation rejects a watermark at `math.MaxInt64` without inserting an event or mutating server/session state. Future HTTP `after`, `Last-Event-ID`, and DTOs use nonnegative decimal `int64`, never `uint64`.
- Process tests must verify process-group behavior on Linux with helper test subprocesses: the helper starts a grandchild, reports both PIDs, and blocks; killing negative PGID must terminate both. Tests may skip only when not running on Linux, although production itself is Linux-only.
- Runtime status transitions in this phase include `creating -> idle`, `idle <-> busy`, and `idle|busy -> exited`. Public `busy` means at least one reserved correlation in `waiting`, lifecycle `grace`, or `committing`; only zero reserved correlations is idle. Notifications and client responses reserve no correlation and never affect busy/idle. Initialize-only recreation, DELETE atomicity, idle TTL, and HTTP-driven instance lifecycle belong to Phase 03.
- `Runtime.Post` owns stdio JSON-RPC classification, semantic ID correlation, duplicate detection, pending metadata, configured timeout, compact JSONL forwarding, and accepted forwarding. Phase 03 delegates an already HTTP-validated raw envelope to `Runtime.Post` and only maps its typed result/errors to HTTP; it must not recreate correlation or persistence.
- Each runtime permits at most 256 total waiting, grace-retained, or committing correlations. Every raw ID token, including a string token's quotes/escapes, is at most 128 bytes. Retained metadata uses a fixed lifecycle enum rather than an arbitrary method string, with session IDs capped at 1024 UTF-8 bytes and cwd at 4096 UTF-8 bytes. A lifecycle timeout returns the typed timeout immediately but reserves its ID/minimal metadata for a fixed 30-second late-response grace. A matched response atomically changes the correlation to `committing` and cancels grace expiry, but does not release its slot, duplicate reservation, capacity/busy accounting, or metadata until `AppendOutput` commits. Posts requiring a correlation over capacity fail before writer admission with a typed capacity error.
- The runtime completes a waiter and signals a committed-output wakeup only after the SQLite transaction commits. A timed-out request detaches its waiter; for lifecycle requests only, it retains the minimal correlation record during the bounded grace so a late success can mutate session state. Its late response is persisted and may produce a wakeup. `Runtime.Events` is a capacity-1 non-blocking/coalesced wakeup, never an event queue: SQLite is authoritative and dropped wakeups are safe. Phase 03 queries the store after wakeups and on a bounded fallback ticker.
- SQLite checkpoint/close occurs only after runtime termination and pump completion. Phase 02 tests runtime shutdown directly and wires only application DB reconciliation/close; Phase 03 owns live-runtime pre-drain registration and its app-level ordering tests because it owns the proxy/runtime registry.
- For every task, missing package/symbol output is setup evidence only. Once the smallest compiling seam exists, retain a failing behavioral assertion as RED wherever feasible.

## Scope Boundaries

**Included:** SQLite dependency/driver, exact schema and pragmas, one-connection store, server/session/event persistence, transactional event sequences, startup reconciliation, agent binary resolution using Phase 01 `internal/childenv`, independent process-group spawn/kill, strict stdio JSON-RPC `Post` correlation and timeout, pending metadata, JSONL pumps, synthetic output/exit events, stderr tail/redaction, full private mock ACP flow, and store/runtime shutdown tests.

**Excluded:** all `/v1/acp*` handlers, HTTP content negotiation/body/server-ID validation and status mapping, SSE/replay/live subscription, proxy instance registry and server creation/recreation policy, DELETE semantics, idle reaping, process API, and any public mock command or documentation. Stdio request matching, duplicate IDs, timeout, reverse-call response forwarding, and persistence are explicitly included in `Runtime.Post`.

## Target Files and Interfaces

### `internal/acpstore`

```go
type Status string

const (
	StatusCreating Status = "creating"
	StatusIdle     Status = "idle"
	StatusBusy     Status = "busy"
	StatusExited   Status = "exited"
)

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

type Session struct {
	ServerID   string
	SessionID  string
	CWD        string
	CreatedAtMs int64
	UpdatedAtMs int64
}

type Event struct {
	ServerID   string
	Seq        int64
	Kind       string
	Method     *string
	Payload    json.RawMessage
	SessionID  *string
	CreatedAtMs int64
}

type EventQuery struct {
	SessionID *string
	After     int64
	Limit     int
	Desc      bool
}

type Store struct { /* private *sql.DB and clock */ }

func Open(ctx context.Context, path string) (*Store, error)
func (s *Store) Close(ctx context.Context) error
func (s *Store) Reconcile(ctx context.Context) error
func (s *Store) CreateServer(ctx context.Context, serverID, agent string) (Server, error)
func (s *Store) Server(ctx context.Context, serverID string) (Server, error)
func (s *Store) Servers(ctx context.Context) ([]Server, error)
func (s *Store) SetLive(ctx context.Context, serverID string, pid int) error
func (s *Store) SetStatus(ctx context.Context, serverID string, status Status) error
func (s *Store) MarkExited(ctx context.Context, serverID string) error
func (s *Store) DeleteServer(ctx context.Context, serverID string) error
func (s *Store) Session(ctx context.Context, serverID, sessionID string) (Session, error)
func (s *Store) Sessions(ctx context.Context, serverID string) ([]Session, error)
func (s *Store) AppendOutput(ctx context.Context, serverID string, output Output) (Event, error)
func (s *Store) Events(ctx context.Context, serverID string, q EventQuery) ([]Event, error)
```

`Open` uses one `database/sql` connection and applies WAL, foreign keys, and 5s busy timeout. `Close` runs `PRAGMA wal_checkpoint(TRUNCATE)` before closing. Export sentinel errors `ErrNotFound`, `ErrConflict`, and `ErrDeleted` for future transport mapping.

`Output` contains the validated agent-output envelope bytes exactly as emitted after stripping only surrounding JSONL framing whitespace, plus classified `Kind`, optional `Method`, optional `SessionID`, and optional lifecycle session mutation. `AppendOutput` allocates an `int64` sequence, rejects overflow at `math.MaxInt64`, inserts the event, applies session upsert metadata, and updates the server watermark/timestamp in one transaction. A successful lifecycle response's event, session roster mutation, and cwd mutation commit together; an error response or timeout creates no session, while a late successful response may create/update one when it eventually commits.

### `internal/acpruntime`

```go
type LaunchSpec struct {
	Program string
	Args    []string
	Env     []string
}

type Resolver struct {
	Executable string
	Commands   map[string]config.AgentCommand
	Environ    []string
	LookPath   func(string) (string, error)
}

func (r Resolver) Resolve(agent string) (LaunchSpec, error)

type PostResult struct {
	Response json.RawMessage
	Accepted bool
}

type Pending struct {
	ID        json.RawMessage
	Lifecycle Lifecycle
	SessionID *string
	CWD       *string
}

type Runtime struct { /* private process/pumps/state */ }

func Start(ctx context.Context, store *acpstore.Store, serverID string, spec LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (*Runtime, error)
func (r *Runtime) PID() int
func (r *Runtime) Post(ctx context.Context, payload json.RawMessage) (PostResult, error)
func (r *Runtime) Events() <-chan struct{}
func (r *Runtime) Stderr() string
func (r *Runtime) Wait() error
func (r *Runtime) Kill(ctx context.Context) error
```

`Start` rejects a non-positive request timeout, launches with `syscall.SysProcAttr{Setpgid:true}`, pipes stdio, marks the row live/idle only after successful spawn, and starts pumps. `Post` accepts exactly the three authoritative client envelope forms and alone compacts each validated raw object with stdlib `json.Compact` immediately before JSONL framing; decode-and-re-marshal is forbidden because it changes payload bytes. Every raw string or numeric ID token is at most 128 bytes. String IDs retain their exact decoded value. Numeric IDs additionally have exponent magnitude at most 1,000,000 and use a bounded lexical tuple of sign, normalized significant digits, and decimal scale, without `float64`, arbitrary-precision arithmetic, or power expansion. A request atomically reserves one of 256 correlation slots before writer admission, rejects duplicate canonical IDs, reconciles durable status to busy, and waits for its committed response. Invalid/over-limit IDs or lifecycle metadata return `ErrInvalidEnvelope` before reservation/admission. Pending state stores only canonical ID, lifecycle enum `none|new|load|resume`, optional session ID up to 1024 UTF-8 bytes, and optional cwd up to 4096 UTF-8 bytes. Notifications/client responses reserve no correlation but use the same writer path.

The 256 cap and public busy count include correlations in `waiting`, lifecycle `grace`, and `committing` states. A request over the cap returns typed `ErrCapacity` before writer admission; Phase 03 maps it to 429. Non-lifecycle timeout removes its correlation and reconciles runtime-owned busy/idle state. Lifecycle timeout detaches the waiter and returns `ErrRequestTimeout` for HTTP 504, but moves the correlation to `grace` and retains its slot, metadata, duplicate-ID reservation, capacity accounting, and busy accounting for exactly 30 seconds using an injected timer seam in unit tests.

All requests, notifications, and client responses enter one fixed-capacity 256 writer queue; one writer goroutine serializes complete compact JSONL records. The configured request timeout starts before queue admission for every envelope type. If timeout/cancellation wins before any bytes are emitted, remove only that queued item and any correlation it reserved, leaving the runtime usable. If any partial line was emitted, cancel the runtime, close stdin, kill/wait the process group and runtime-owned goroutines, and fail/clear every correlation so the stream is never reused. Notifications/client responses return accepted only after a complete write. After a complete request line is written, timeout/grace and caller-detachment follow the correlation rules above.

`Runtime` has a dedicated status-transition mutex separate from its correlation mutex. After every correlation mutation that can change the map between zero and nonzero, call one reconciliation path: acquire the status mutex; while holding it, briefly acquire the correlation mutex and re-read the current total `waiting+grace+committing` count; release the correlation mutex; persist `busy` when count is positive or `idle` when zero; then release the status mutex. Never hold the correlation mutex during SQL. All runtime status writes, including initial live/idle and exit, use the status mutex so a stale completion cannot write idle after a concurrent insertion has reconciled busy. Exit marks terminal while serialized by this mutex, and any queued reconciliation observes terminal state and performs no later busy/idle write. Any status persistence failure follows the persistence-failure path: fail attached/all waiters as applicable, kill/exit the runtime, and emit no false successful completion.

When stdout matches a response, the pump acquires the correlation mutex and atomically changes `waiting|grace -> committing`, cancels the grace timer, and copies the attached waiter reference without removing anything from the map. Grace expiry only wins from `grace`; it is a no-op after `committing`. The pump then classifies lifecycle mutation from the still-reserved metadata and calls `AppendOutput` without holding the correlation mutex. On commit success it removes the same committing generation under the correlation mutex, releases that mutex, runs serialized status reconciliation (which re-reads the current map rather than using a stale snapshot), then completes an attached waiter and signals the wakeup. Duplicate posts and capacity checks continue to observe the committing entry until successful removal, and status remains busy throughout grace/commit. If status persistence fails after the output commit, the copied waiter is still available for `ErrPersistence` before kill. On `AppendOutput` failure the map retains enough committing state/waiter reference to fail with `ErrPersistence`; the pump invokes idempotent `Runtime.Kill` outside the mutex, and kill clears every correlation/timer. Error responses and malformed lifecycle success payloads follow exactly the same commit-before-release path because their agent-output event is still durable even though they do not mutate sessions. Response, expiry, concurrent insertion/completion, and kill races use the correlation and status mutexes in that fixed order; store/signal/wait operations never occur under the correlation mutex. Export typed/sentinel errors for invalid envelope, duplicate ID, capacity, timeout, exited process, write failure, and persistence failure so Phase 03 only maps outcomes to HTTP.

For `session/new`, pending metadata has lifecycle cwd but no session ID until a successful matching response supplies `result.sessionId`. Before persistence, the output classifier uses the committing entry's lifecycle enum to identify that successful lifecycle response and assigns its returned session ID and pending cwd to the event/session mutation. An over-limit agent-supplied session ID, error, or malformed success creates no session but the valid output envelope still commits before correlation release. For `session/load` and `session/resume`, correlation uses the bounded request `sessionId` and cwd retained through committing, and mutates the roster/cwd only on a successful matching response. A timed-out lifecycle request creates no session at timeout, but its retained metadata lets a response within 30 seconds do so; late error/malformed output persists before releasing without mutation. No response by grace expiry kills/exits the runtime instead of retaining metadata indefinitely. The runtime never invents an ID, synthesizes a request, or replays a prompt.

The stdout pump strips only JSONL framing whitespace around the one-line object and persists the remaining agent-output JSON bytes without re-marshaling or compacting them. It commits before completing a matching `Post` waiter or attempting a non-blocking send to the capacity-1 wakeup channel. SQLite failure kills the process group, marks exited where possible, closes/fails every pending waiter with the persistence error, and signals no uncommitted output. `Kill` signals the captured negative process-group ID with SIGKILL and waits for process/writer/stdout/stderr goroutines before returning; `Wait` and repeated `Kill` are idempotent confirmations over the same completion. On every direct-child exit, including natural exit, the waiter immediately sends SIGKILL to that captured negative PGID before awaiting pumps, persisting `_adapter/agent_exited`, marking exited/clearing PID, failing waiters, clearing timers/correlations, and closing the wakeup channel. This prevents descendants from retaining pipes or surviving the leader.

### `internal/mockagent`

```go
func Run(ctx context.Context, in io.Reader, out, errOut io.Writer) error
```

This package is private and reachable only through `AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1`. It keeps process-local initialized/session/pending-permission state and implements a strict deterministic ACP flow for tests: `initialize`; `session/new`, `session/load`, `session/resume`, `session/list`, and `session/close`; prompt updates followed by the matching prompt response; a permission reverse-call that blocks that prompt response until a matching client response arrives; and controlled delay, invalid stdout, stderr, and exit behavior. `session/load` and `session/resume` attach only sessions already known within that same subprocess. A new process has no mock sessions, so explicit load/resume after process or bridge restart may return a deterministic unknown-session JSON-RPC error. The bridge passes through and persists that response without synthesizing prompts, replaying requests, or giving the mock disk-backed state. The mock preserves raw request ID type/value in responses, validates lifecycle order/session existence, and emits JSON-RPC errors for invalid operations. It is not a public protocol compatibility promise, persisted store, CLI, or HTTP feature.

## Ordered Atomic Tasks

### Task 2.1: Add SQLite Driver and Exact Schema

**Description:** Add the sole approved dependency and create the database with the exact authoritative tables, constraints, index, and connection pragmas.

**Files:** `go.mod`, `go.sum`, `internal/acpstore/store.go`, `internal/acpstore/schema.go`, `internal/acpstore/schema_test.go`

**Symbols:** `acpstore.Store`, `acpstore.Open`; private `schema`, `configure`

**References:** Master sections **Platform and dependencies** and **ACP state endpoints and schema**; store interface above.

**Strict test-first steps:**

- [ ] Write tests opening a temporary file and querying `sqlite_master`, `PRAGMA table_info`, `PRAGMA foreign_key_list`, and `PRAGMA index_info` to assert exact table columns/primary keys, cascading foreign keys, and `(server_id,session_id,seq)` index order.
- [ ] Assert `journal_mode=wal`, `foreign_keys=1`, `busy_timeout=5000`, `synchronous=FULL`, `DB.Stats().MaxOpenConnections == 1`, and schema creation is idempotent across reopen. Force physical disposal by returning `driver.ErrBadConn` from `Conn.Raw`, close it, obtain a replacement connection, then close/reopen the store and reassert every pragma. Prove `_txlock=immediate` by beginning a transaction and showing an independent probe connection cannot `BEGIN IMMEDIATE` before the first write.
- [ ] Open database filenames containing `?`, `#`, and `%`, then assert the intended file is created/reopened and no query/fragment truncation or alternate file occurs.
- [ ] **RED evidence:** Add the smallest compiling `Open` seam, run `go test ./internal/acpstore -run TestOpenSchema`, and retain failing schema/pragma/idempotent-reopen assertions. Missing driver/symbol failure is setup evidence only.
- [ ] Use the authoritative `modernc.org/sqlite` v1.57.0 pin everywhere below and record it in the implementation change description.
- [ ] Run `go get modernc.org/sqlite@v1.57.0` once; inspect `go.mod`/`go.sum` and reject any directly added production/test module besides this driver (its transitive modules are expected).
- [ ] Implement `sql.Open("sqlite", dsn)` using the master's v1.57.0 DSN contract: build a `net/url.URL` with `Scheme: "file"` and `Path: path`, and set `RawQuery` from `url.Values` containing repeated `_pragma` values plus `_txlock=immediate`; never concatenate the path and query. `synchronous(FULL)` matches commit-before-delivery durability, and `_txlock=immediate` acquires the writer lock at `BEGIN`. Then `SetMaxOpenConns(1)`, ping, and create the schema with cleanup on every partial failure.
- [ ] **GREEN evidence:** Re-run the focused test and record all schema/pragma assertions passing for create and reopen.

**Verification:** `go test ./internal/acpstore -run TestOpenSchema -count=1 && go list -m all`

**Risk:** High because schema mistakes become persisted compatibility constraints.

**Reversibility:** Needs deletion of test databases before release; migration is required after persisted deployment.

**Deliverable:** Exact SQLite schema on a single configured connection with only the approved driver dependency.

### Task 2.2: Implement Server and Session Store Operations

**Description:** Add typed status, server lifecycle, deterministic listing, session access, cascade deletion, and transport-neutral sentinel errors.

**Files:** `internal/acpstore/models.go`, `internal/acpstore/servers.go`, `internal/acpstore/servers_test.go`

**Symbols:** `Status*`, `Server`, `Session`, `ErrNotFound`, `ErrConflict`, `ErrDeleted`, `CreateServer`, `Server`, `Servers`, `SetLive`, `SetStatus`, `MarkExited`, `DeleteServer`, `Session`, `Sessions`

**References:** Master sections **ACP lifecycle and persistence** and **ACP state endpoints and schema**.

**Strict test-first steps:**

- [ ] Test create values/timestamps, duplicate server conflict, lookup/list sorting, `creating -> idle` with PID, `idle <-> busy` status updates with correct `idle_since_ms`, exit timestamp/PID clearing, idempotent exit, and unknown-server errors.
- [ ] Seed sessions through a test-only SQL transaction or the upcoming output fixture and assert sorted session IDs plus cascade removal of sessions/events on delete.
- [ ] Use an injected test clock through a private `openWithClock` helper so exact millisecond transitions are asserted without sleeps.
- [ ] **RED evidence:** Add minimal compiling store method seams, run `go test ./internal/acpstore -run 'Test(Server|Session)'`, and retain failing lifecycle/timestamp/cascade behavior. Undefined symbols are setup evidence only.
- [ ] Implement SQL operations with explicit column lists, checked `RowsAffected`, `errors.Is(sql.ErrNoRows)`, and defensive copies of nullable values/raw data.
- [ ] **GREEN evidence:** Re-run the focused tests and record deterministic sort/timestamp/cascade cases passing.

**Verification:** `go test ./internal/acpstore -run 'Test(Server|Session)' -count=1`

**Risk:** Medium.

**Reversibility:** Needs migration after persisted use; easy before release.

**Deliverable:** Typed server/session persistence suitable for the future status/list handlers.

### Task 2.3: Atomically Persist Classified Agent Output and Session Metadata

**Description:** Implement transactional per-server sequence allocation, raw output persistence, lifecycle metadata extraction input, and event querying.

**Files:** `internal/acpstore/events.go`, `internal/acpstore/events_test.go`, `internal/acpstore/models.go`

**Symbols:** `Output`, `Event`, `EventQuery`, `AppendOutput`, `Events`; private `upsertSession`

**References:** Master sections **ACP lifecycle and persistence** and **ACP state endpoints and schema**. Classification is supplied by runtime; the store must not interpret conversation content.

**Strict test-first steps:**

- [ ] Test first sequence is 1; subsequent sequences are monotonic per server and independent across servers; server `last_event_seq` and `updated_at_ms` change in the same transaction.
- [ ] Test exact stored agent-output bytes round-trip, nullable method/session fields, all three event kinds, ascending/descending nonnegative `int64 after` behavior, limits, optional session filtering, deterministic results, unknown server/session errors, and rejection of negative query values.
- [ ] Assert stored payloads are valid UTF-8 and round-trip exactly. Runtime ingress must call `utf8.Valid` before JSON validation: invalid client JSON is rejected, and invalid agent stdout is converted to `_adapter/invalid_stdout`, so the store never receives invalid UTF-8.
- [ ] Seed a server watermark at `math.MaxInt64-1`: one append reaches `math.MaxInt64`, the next returns a typed validation/overflow error and leaves event count, watermark, timestamps, and sessions unchanged. Assert all model fields are `int64` and no SQL-to-Go conversion passes through `uint64`.
- [ ] Test lifecycle session create/update only for successful correlated `session/new`, `session/load`, and `session/resume` output, preserving `created_at_ms`, updating `updated_at_ms`, and rolling back event/watermark/session mutation together on an induced constraint/closed-DB failure. Error responses and timeout cleanup create no session; a late successful response commits event/roster/cwd atomically.
- [ ] Add a concurrent append test; despite many goroutines, resulting sequences must be gap-free and unique because the single connection serializes transactions.
- [ ] **RED evidence:** After introducing the smallest compiling API skeleton, run `go test ./internal/acpstore -run 'Test(AppendOutput|Events|SequenceOverflow|LifecycleCommit)'`; retain behavioral failures for sequence allocation, overflow rollback, byte preservation, and lifecycle atomicity rather than relying only on undefined symbols.
- [ ] Implement one `sql.Tx` that reads/checks/increments the `int64` server watermark, inserts the event, conditionally upserts successful lifecycle metadata, updates the server, and commits. Return an event only after commit succeeds.
- [ ] **GREEN evidence:** Run `go test -race ./internal/acpstore -run 'Test(AppendOutput|Events)' -count=10`; record no gaps, rollback leaks, or races.

**Verification:** `go test -race ./internal/acpstore -count=10`

**Risk:** High because commit ordering and sequence gaps directly affect future SSE replay correctness.

**Reversibility:** Needs migration/data repair after persisted use; easy before release.

**Deliverable:** Commit-before-delivery event persistence with transactionally consistent server/session metadata.

### Task 2.4: Reconcile Stale Live Rows and Checkpoint on Close

**Description:** Make process ownership safe across bridge restarts and provide explicit WAL shutdown behavior.

**Files:** `internal/acpstore/store.go`, `internal/acpstore/reconcile.go`, `internal/acpstore/reconcile_test.go`

**Symbols:** `Store.Reconcile`, `Store.Close`

**References:** Master sections **ACP lifecycle and persistence** and **ACP state endpoints and schema**. Persisted PIDs must never be signaled.

**Strict test-first steps:**

- [ ] Seed `creating`, `idle`, `busy`, and `exited` rows with PIDs; reopen/reconcile and assert only the first three become `exited`, PID and idle timestamp are cleared, `exited_at_ms`/`updated_at_ms` use the reconciliation clock, and existing exited metadata is retained.
- [ ] Use a PID belonging to a live helper process and prove reconciliation does not signal it.
- [ ] Produce WAL content, call `Close`, assert a checkpoint succeeds and reopen sees all committed data; call `Close` twice and require harmless idempotence. Assert every store method called after `Close` returns an error (driver `ErrConnDone`-shaped), never a panic.
- [ ] **RED evidence:** Add minimal compiling methods, run `go test ./internal/acpstore -run 'Test(Reconcile|Close)'`, and retain failing stale-row/no-signal/checkpoint behavior.
- [ ] Implement reconciliation as one update transaction and close as once-only checkpoint-then-DB-close, joining checkpoint/close errors where both exist. Run `PRAGMA wal_checkpoint(TRUNCATE)` through `QueryRow` and read back the result columns so a non-clean checkpoint (nonzero `busy`) is surfaced as an error rather than assumed.
- [ ] **GREEN evidence:** Re-run focused tests and retain evidence that the live helper PID remains alive until test cleanup.

**Verification:** `go test ./internal/acpstore -run 'Test(Reconcile|Close)' -count=1`

**Risk:** High because signaling persisted PIDs could kill unrelated reused processes.

**Reversibility:** Easy code rollback; reconciliation status updates are intentionally irreversible runtime history.

**Deliverable:** Safe startup reconciliation and orderly SQLite checkpoint/close.

### Task 2.5: Resolve Agent Launch Specifications with Shared Child Environment

**Description:** Resolve configured/default agent commands and test-only mock re-exec using the Phase 01 shared sanitizer rather than defining an ACP-specific environment implementation.

**Files:** `internal/acpruntime/resolver.go`, `internal/acpruntime/resolver_test.go`

**Symbols:** `LaunchSpec`, `Resolver`, `Resolver.Resolve`; shared `childenv.Sanitized`

**References:** Master section **Agent resolution**; Phase 01 `config.AgentCommand` and `childenv.Sanitized`.

**Strict test-first steps:**

- [ ] Test exact defaults for Claude, Codex, and OpenCode; binary override precedence; `LookPath` fallback; copied args; unknown agent; and errors naming both agent and binary.
- [ ] Test `mock` uses `Resolver.Executable`, no public argument/subcommand, and adds only `AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1`.
- [ ] Test inherited credentials remain while every occurrence of `AGENT_BRIDGE_TOKEN`, `AGENT_BRIDGE_PID_FILE`, `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE`, and `AGENT_BRIDGE_INTERNAL_MOCK_AGENT` is removed before the controlled mock value is added. Include duplicate keys and values containing `=`.
- [ ] **RED evidence:** Add a minimal compiling resolver, run `go test ./internal/acpruntime -run TestResolver`, and retain failing precedence/environment/error behavior.
- [ ] Implement resolution using injected `LookPath` defaulting to `exec.LookPath`; call `childenv.Sanitized(r.Environ)` and add the controlled private mock variable only for `mock`. Do not add another sanitizer or duplicate its tests in `internal/acpruntime`.
- [ ] **GREEN evidence:** Re-run focused tests and record exact program/args/environment assertions passing.

**Verification:** `go test ./internal/acpruntime -run TestResolver -count=1`

**Risk:** High because leaked bridge control variables can expose auth or recursively dispatch children.

**Reversibility:** Easy to revert before runtime integration.

**Deliverable:** Deterministic and secret-safe launch specifications for all four internal agent identifiers.

### Task 2.6: Implement the Private Mock Agent and Complete Phase 01 Dispatch

**Description:** Supply a strict, deterministic, process-local ACP JSONL child covering the full lifecycle and reverse-call flow required by later integration tests, then wire it into the existing private environment dispatch without creating a CLI/API surface.

**Files:** `internal/mockagent/mockagent.go`, `internal/mockagent/mockagent_test.go`, `internal/app/app.go`, `internal/app/app_test.go`

**Symbols:** `mockagent.Run`; Phase 01 private `runInternalMockAgent`

**References:** Master sections **ACP HTTP contract**, **ACP lifecycle and persistence**, and **Agent resolution**; target mock interface above.

**Strict test-first steps:**

- [ ] Drive one ordered protocol fixture that rejects session operations before `initialize`, initializes once, creates/lists a session, permits `session/load`/`session/resume` only for that known session in the same subprocess, closes it, and then rejects it as unknown. Assert session IDs, cwd, and lifecycle state reset in a new mock process.
- [ ] Start a fresh mock subprocess and explicitly send load/resume for a session created by the prior subprocess; assert deterministic unknown-session JSON-RPC responses and no prompt/update output. Do not add files, SQLite access, or any other persistence to `internal/mockagent`.
- [ ] Test a prompt emits deterministic ACP prompt-update notifications before its matching response and preserves string/numeric raw IDs exactly.
- [ ] Test the permission scenario emits an agent reverse-call, withholds the prompt response while waiting, accepts only the matching client response, then finishes updates and the original response. Cover mismatched response IDs and EOF while permission is pending.
- [ ] Test controlled delay enables timeout/late-response scenarios; controlled invalid stdout, stderr, and exit affect only their intended streams/process state; malformed input and invalid lifecycle operations return deterministic JSON-RPC errors; notification input produces no implicit response; EOF/cancellation exits cleanly.
- [ ] Update the Phase 01 dispatch test to expect mock execution rather than the temporary unavailable error and prove invalid host/port still cannot affect private mode.
- [ ] **RED evidence:** Add a minimal compiling `Run` loop, run `go test ./internal/mockagent ./internal/app`, and retain failing lifecycle/correlation/private-dispatch assertions. Missing package/`Run` is setup evidence only.
- [ ] Implement `initialize`, `session/new`, `session/load`, `session/resume`, `session/list`, `session/close`, prompt/update/response flow, permission reverse-call correlation, and private controlled-failure options with in-process maps/counters only. Keep control method/parameter names private to package tests and README-free. Replace the Phase 01 temporary hook with `mockagent.Run(ctx, io.Stdin, io.Stdout, io.Stderr)`.
- [ ] **GREEN evidence:** Re-run both packages and record all JSONL/dispatch tests passing.

**Verification:** `go test ./internal/mockagent ./internal/app -count=1`

**Risk:** Medium; accidental publication would create an unsupported compatibility contract.

**Reversibility:** Easy to revert; private test protocol has no public consumers.

**Deliverable:** A current-binary, strict process-local ACP mock agent reachable only through the internal environment variable.

### Task 2.7: Spawn and Kill Independent Process Groups

**Description:** Establish subprocess ownership, raw stdin framing primitive, wait/kill idempotence, and Linux process-group cleanup before adding output persistence and correlation.

**Files:** `internal/acpruntime/runtime.go`, `internal/acpruntime/runtime_process_test.go`, `internal/acpruntime/testhelper_test.go`

**Symbols:** `Runtime`, `Start`, `PID`, `Wait`, `Kill`; private `writeLine`, `killProcessGroup`, `waitProcess`

**References:** Master sections **ACP lifecycle and persistence**, **Agent resolution**, **Non-ACP endpoints** process-group rule, and **Environment, shutdown, and image**; target runtime interface above.

**Strict test-first steps:**

- [ ] Implement a test helper mode in the test binary that starts a long-lived grandchild in the same process group, prints both PIDs in one JSON line, and blocks.
- [ ] Test `Start` rejects zero/negative request timeout, sets a nonzero PID and distinct PGID, updates the existing store row to idle/PID, and its private writer emits one newline-delimited payload.
- [ ] Test `Kill` sends SIGKILL to `-pgid`, waits for the direct child and all pumps, clears PID/marks exited, and is safe concurrently/repeatedly. Poll `/proc/<pid>/stat` with a bounded deadline and parse the process state after the parenthesized command safely: the direct child must be reaped/disappear, and every descendant must disappear or be zombie (`Z`), with no non-zombie descendant remaining. Do not use `syscall.Kill(pid, 0)` as disappearance evidence because it reports zombies as present.
- [ ] Add a helper mode whose direct child exits while a grandchild remains blocked with inherited pipes. Assert the waiter immediately SIGKILLs the captured negative PGID, then joins pumps and marks exited; no descendant remains non-zombie and `Wait` does not hang.
- [ ] In non-container unit tests, require bridge-owned direct-child reaping but allow a killed zombie grandchild to persist until external test cleanup or host init reaps it. Always register bounded cleanup for reported PIDs. Phase 06's tini-backed container tests own proof that orphan zombies are reaped.
- [ ] Test failed spawn leaves the server exited with no PID and no leaked pipe/goroutine.
- [ ] **RED evidence:** Add the smallest compiling runtime/process seams, run `go test ./internal/acpruntime -run 'TestRuntime(ProcessGroup|Start|SpawnFailure)'`, and retain failing PGID/descendant/spawn-cleanup behavior.
- [ ] Create the command with `exec.CommandContext` and a runtime-owned cancellable context, set `Setpgid:true`, capture and validate the PGID after spawn, and replace `CommandContext`'s direct-child `Cancel` with guarded negative-PGID SIGKILL signaling. The sole `cmd.Wait` owner sends SIGKILL to that captured group immediately whenever the direct child exits, before waiting for pumps or publishing exit. Retain a bounded `cmd.WaitDelay` so `Wait` cannot hang on wedged pipes. Capture pipes before start, centralize wait so `Wait` and `Kill` cannot call `Cmd.Wait` twice, and track every runtime-owned writer/pump/wait goroutine with completion channels or a `sync.WaitGroup`.
- [ ] **GREEN evidence:** Run the process-group tests with `-race -count=10`; retain passing evidence that the direct child disappears, no descendant remains non-zombie, no race is reported, and every runtime-owned completion signal/WaitGroup finishes within a bounded deadline. Do not use process-wide goroutine-count deltas as leak evidence.

**Verification:** `go test -race ./internal/acpruntime -run 'TestRuntime(ProcessGroup|Start|SpawnFailure)' -count=10`

**Risk:** High because a faulty test or implementation can leak/kill processes; always use test-owned PGIDs and bounded cleanup.

**Reversibility:** Easy code rollback; leaked OS processes require explicit cleanup.

**Deliverable:** A Linux process-group-owned stdio child lifecycle with reliable descendant termination.

### Task 2.8: Pump, Classify, Persist, and Publish Agent Stdout

**Description:** Read JSONL stdout, minimally inspect routing/session fields, persist every agent envelope before publication, and synthesize invalid-output/exit notifications.

**Files:** `internal/acpruntime/output.go`, `internal/acpruntime/output_test.go`, `internal/acpruntime/runtime.go`, `internal/acpruntime/runtime_integration_test.go`

**Symbols:** `Runtime.Events`; private `readOutput`, `classifyOutput`, `sessionMutation`, `persistSynthetic`, `signalCommitted`

**References:** Master sections **ACP HTTP contract** and **ACP lifecycle and persistence**; `acpstore.AppendOutput`; private mock agent.

**Strict test-first steps:**

- [ ] Unit-test classification of agent requests (`method` plus non-null ID), responses (`id` plus exactly one result/error), notifications (`method` without ID), numeric/string IDs, invalid objects, arrays, and `id:null`.
- [ ] Test session metadata is inspected only for `session/new`, `session/load`, `session/resume`, and session-scoped messages, extracting only bounded `sessionId`/`cwd`; unrelated content with those key names must not mutate sessions. Over-limit agent metadata is omitted from classified event/session fields while the raw envelope payload persists. For a successful `session/new` response, use its matching pending lifecycle enum plus `result.sessionId` and pending cwd even though pending metadata has no prior session ID. For successful load/resume, use request session ID/cwd correlation. Error responses do not mutate sessions.
- [ ] Test valid agent output containing insignificant leading/trailing framing whitespace and deliberately non-compact internal whitespace persists and returns the exact object bytes after framing removal. It must not be decoded/re-marshaled or compacted.
- [ ] Integration-test mock output order: after each `Events()` wakeup, the committed output must already be queryable in SQLite. Fill the capacity-1 channel, commit several more outputs without consuming it, and prove sends never block and one wakeup is enough to query every sequence. Test malformed/non-object/invalid-UTF-8/oversized lines produce one persisted `_adapter/invalid_stdout` notification each and subsequent valid lines still commit.
- [ ] Test a natural mock exit persists `_adapter/agent_exited`, then marks exited and closes the wakeup channel. Assert only agent stdout/synthetic events exist; sent client payloads do not appear in the DB.
- [ ] Close/fault the store during output and assert runtime kills the entire process group, marks/fails as far as storage permits, logs the storage error, and signals no uncommitted wakeup. Typed waiter failure is added in Task 2.9; HTTP 507 mapping remains Phase 03.
- [ ] **RED evidence:** With a compiling wakeup/output skeleton, run `go test ./internal/acpruntime -run 'Test(ClassifyOutput|RuntimeOutput|WakeupCoalescing)'`; retain behavioral failures showing premature wakeup, altered payload bytes, or a blocked full wakeup channel.
- [ ] Implement a bounded `bufio.Reader` line loop that drains overlong lines, removes only JSONL framing whitespace, requires `utf8.Valid` before validating one JSON object without replacing its bytes, classifies routing fields only, calls `AppendOutput`, and attempts `select { case wake <- struct{}{}: default: }` only after commit. Create the wake channel with capacity one and ensure one owner closes it after stdout and exit persistence complete.
- [ ] **GREEN evidence:** Run focused tests under `-race -count=10`; retain commit-before-observe, invalid-line recovery, natural-exit, and store-failure assertions passing.

**Verification:** `go test -race ./internal/acpruntime -run 'Test(ClassifyOutput|RuntimeOutput)' -count=10`

**Risk:** High because output loss/reordering breaks replay and response delivery in Phase 03.

**Reversibility:** Easy before external use; persisted event semantics require migration after release.

**Deliverable:** Commit-before-event-publication ACP stdout ingestion with minimal metadata inspection and failure containment.

### Task 2.9: Correlate JSON-RPC Posts in the Stdio Runtime

**Description:** Make `Runtime.Post` the single owner of compact JSONL forwarding, bounded correlation state, lexical JSON-number ID matching, timeout/grace expiry, and committed-response delivery so Phase 03 remains an HTTP/lifecycle adapter.

**Files:** `internal/acpruntime/post.go`, `internal/acpruntime/post_test.go`, `internal/acpruntime/runtime.go`, `internal/acpruntime/output.go`, `internal/acpruntime/runtime_integration_test.go`

**Symbols:** `PostResult`, `Pending`, `Runtime.Post`; exported typed/sentinel `ErrInvalidEnvelope`, `ErrDuplicateID`, `ErrCapacity`, `ErrRequestTimeout`, `ErrExited`, `ErrWrite`, `ErrPersistence`; private `idKey`, `pendingRequest`, `reconcileStatus`, `completeResponse`, `expireLifecycle`, `failPending`

**References:** Master sections **ACP HTTP contract** and **ACP lifecycle and persistence**; target runtime interface and Task 2.8 commit ordering.

**Strict test-first steps:**

- [ ] Table-test client classification: request is `method` plus non-null string/number ID; notification is `method` without ID; client response has non-null ID, no method, and exactly one of `result`/`error`; reject arrays, invalid UTF-8 even when `json.Valid` accepts it, `id:null`, missing/invalid method, invalid ID types, and ambiguous response envelopes before reservation or stdin write.
- [ ] Prove stdin byte fidelity: every line the runtime writes to the agent equals `json.Compact(raw)` byte-for-byte for payloads containing `<`, `>`, `&`, `\uXXXX` escapes, and numeric lexemes such as `1e0`; a decode-and-re-marshal implementation must fail this assertion.
- [ ] Test `idKey` preserves exact decoded string ID values and distinguishes strings from numbers. Cover raw string and numeric ID token boundaries at 128/129 bytes. For numbers, table-test canonical behavior for `1`, `1.0`, `1e0`, invalid `01`, signed/decimal/exponent forms, normalized positive/negative zero, redundant zeros, unequal values, and exponent magnitude 1,000,000/1,000,001. Use `testing.AllocsPerRun` on boundary-sized tokens to enforce a small fixed allocation ceiling. Treat the single-pass/O(token-length), no-power-expansion, and no `float64`/arbitrary-precision restrictions as implementation review criteria rather than timing tests.
- [ ] Start the strict mock and test a request registers pending metadata before writer admission, stores only canonical ID/lifecycle enum/optional bounded session ID/cwd, writes exactly one compact JSONL record, persists the idle-to-busy transition, waits, and returns the exact committed agent response bytes after framing removal. With multiple waiting/grace/committing correlations, remain busy while any reservation exists; successful removal of the final entry persists idle/new idle timestamp, while grace expiry or process failure transitions to exited instead. Notifications/client responses never alter status.
- [ ] Post mathematically equivalent numeric IDs concurrently and assert one request proceeds while the duplicate returns `ErrDuplicateID` without writing a second line. Different numeric values and exact string IDs proceed independently; invalid/over-limit numeric IDs return `ErrInvalidEnvelope` without reserving capacity or writing.
- [ ] Test requests, notifications, and client responses all use the same capacity-256 writer admission path and configured timeout. Fill admission with a blocked writer and prove bounded state; timeout before any bytes rejects only that envelope, while timeout after a partial line kills/reaps the runtime and fails all pending work. Notifications/client responses return `PostResult{Accepted:true}` only after one complete write and create no correlation/persistence state. Use the permission mock flow to prove a forwarded client response completes the agent reverse-call and eventually the original prompt request.
- [ ] Post `session/new` and assert pending has bounded cwd but `SessionID == nil` until a successful response; pending state retains only the lifecycle enum, never an arbitrary method. Test session ID 1024/1025-byte and cwd 4096/4097-byte boundaries: over-limit client metadata returns `ErrInvalidEnvelope` before reservation/admission, while over-limit agent metadata persists the envelope without session mutation. Assert valid new/load/resume session/cwd mutation commits with output.
- [ ] After replacing the mock with a fresh subprocess, post explicit `session/load` and `session/resume` for a formerly known ID; assert `Runtime.Post` returns the mock's exact unknown-session JSON-RPC response, persists it normally, and emits no synthesized prompt/request/update.
- [ ] With injected timeout/grace timers, assert lifecycle timeout returns `ErrRequestTimeout`, detaches the waiter, retains only minimal metadata/ID and busy/capacity accounting for exactly 30 seconds, and keeps an equivalent duplicate at `ErrDuplicateID`.
- [ ] Pause `AppendOutput` after matching waiting and grace responses. Assert each entry is `committing`, its timer is canceled, advancing grace cannot kill it, duplicate ID remains `ErrDuplicateID`, the slot still contributes to the 256 cap, lifecycle metadata remains available, status remains busy, no waiter returns, and no wakeup is sent. Include successful, JSON-RPC error, and malformed lifecycle success output.
- [ ] Release a successful commit and assert strict order: remove the committing entry, update busy/idle from remaining correlations, complete the waiter if attached, then wake. A grace response has no attached waiter but follows the same release/status/wakeup order; successful lifecycle output commits session/event/cwd, while error/malformed output commits only the event.
- [ ] Deterministically pause a completion after it removes the former last entry but before status reconciliation. Insert a new Post and let its reconciliation persist busy first; then resume the older completion reconciliation. Assert it re-reads count one and writes busy, never stale idle, and the durable server remains busy. Also cover the inverse serialized ordering, where an earlier idle write is followed by the insertion's busy write.
- [ ] Under `-race`, interleave waiting-to-grace, grace-to-committing, committing removal, new insertion, non-lifecycle timeout removal, and grace expiry. Assert every nonempty map remains durably busy, idle appears only after zero, idle timestamps are not refreshed by repeated busy reconciliation, and no status write overtakes a later reconciliation.
- [ ] Inject status persistence failure during initial insertion and final removal reconciliation. Assert no request is reported successful, attached/all waiters receive `ErrPersistence`, the runtime kills/exits, timers/correlations are cleared, and no later queued reconciliation overwrites exited status.
- [ ] Fail `AppendOutput` for a committing response and assert the duplicate/capacity reservation and attached waiter remain until runtime failure handling supplies `ErrPersistence`; `Kill` then exits and clears all correlation/timer state. Race response match, grace expiry, commit completion/failure, duplicate Post, and Kill under `-race`; expiry must never win after `committing`.
- [ ] Drive repeated unique lifecycle timeouts to 256 waiting+grace+committing correlations and assert the next request returns `ErrCapacity` before writer admission. Advance grace for an unclaimed entry and prove expiry calls `Kill`, exits the runtime, releases every slot/timer/waiter, and closes every runtime-owned completion channel within a bounded deadline. Non-lifecycle timeout releases capacity immediately.
- [ ] Use a child that never reads stdin and enough payload to block the pipe. Assert every envelope timer starts before writer admission; timeout before the writer emits bytes removes only that item, while timeout/cancellation after a partial line kills and reaps the runtime, unblocks and joins the writer, releases every correlation/status reservation, and rejects later posts instead of reusing partial JSONL.
- [ ] Instrument store and event consumption to prove the matching response transaction commits before either the `Post` waiter returns or `Events()` exposes it. On commit failure, assert no waiter/event delivery, the process group dies, and every pending post receives `ErrPersistence` for future HTTP 507 mapping.
- [ ] Test a write failure before any bytes removes only that envelope and pending request, if any; a partial write follows the fatal runtime path above. Natural/controlled direct-child exit first SIGKILLs the captured negative PGID, then joins pumps and fails all pending posts with `ErrExited`. After a complete write, cancellation of one caller removes only its waiter and does not cancel output persistence, and when that request never receives a response its non-lifecycle slot and busy accounting are released when the configured timeout elapses, so capacity cannot leak.
- [ ] **RED evidence:** With compiling `Post`/error/timer/store-gate stubs, run `go test ./internal/acpruntime -run 'Test(Post|IDKey|Pending|Capacity|LifecycleGrace|Committing|StatusReconcile)'`; retain behavioral failures for sole-owner compaction, bounded lexical equivalence, cap enforcement, public busy semantics, stale-idle interleaving, grace/commit races, release ordering, and commit-failure cleanup.
- [ ] Implement one correlation mutex/map capped at 256 with explicit `waiting`, `grace`, and `committing` states, plus a distinct status-transition mutex; one capacity-256 queue and writer goroutine owned by `Runtime.Post` for every envelope type; configured timeout before admission/write; and fixed 30-second lifecycle grace with injected test timers. Notifications/client responses bypass only correlation/status, not writer admission.
- [ ] After every insertion/removal that can change zero/nonzero, invoke `reconcileStatus`: lock status transitions, reject/no-op if terminal, re-read total correlations under the correlation lock, release correlation lock, write busy/idle, then release status lock. Route initial live/idle, correlation status, and exit status writes through the same status mutex; exit records terminal state before releasing it. Never carry a previously captured count into SQL, never hold correlation lock during SQL, and on any status error fail/kill the runtime.
- [ ] On a matched response, transition the exact map entry to `committing` and stop its timer under lock, retain the entry/metadata/waiter/accounting, build the classified `Output`, and call `AppendOutput` unlocked. After success, remove only that same committing entry under lock, call serialized reconciliation, then complete the copied waiter and signal wakeup. If append/status persistence fails, preserve enough entry/waiter state, fail through `ErrPersistence`, and kill; never expose an uncommitted waiter/wakeup or let grace expiry remove a committing entry. Unmatched responses still persist and may wake consumers normally.
- [ ] **GREEN evidence:** Run the focused suite under `-race -count=20`; retain passing capacity, lexical-equivalent duplicate, timeout/grace/committing races, serialized current-count status, commit-held accounting, error/malformed release order, reverse-call, lifecycle atomicity, exit, and persistence-failure cleanup evidence.

**Verification:** `go test -race ./internal/acpruntime -run 'Test(Post|IDKey|Pending|Capacity|LifecycleGrace|Committing|StatusReconcile)' -count=20`

**Risk:** High because bounded canonicalization, duplicate/capacity handling, commit order, and grace-expiry cleanup define core ACP correctness.

**Reversibility:** Easy before HTTP integration; runtime behavior becomes externally observable through Phase 03.

**Deliverable:** Complete stdio `Runtime.Post` correlation that Phase 03 can delegate to without duplicate pending/persistence logic.

### Task 2.10: Capture, Bound, and Redact Stderr

**Description:** Retain a bounded 8 KiB stderr tail suitable for future 502 problem extensions while streaming redacted lines to structured logs.

**Files:** `internal/acpruntime/stderr.go`, `internal/acpruntime/stderr_test.go`, `internal/acpruntime/runtime.go`

**Symbols:** `Runtime.Stderr`; private `stderrTail`, `redactSensitiveLine`, `readStderr`

**References:** Master section **Authentication and errors**, agent stderr requirement.

**Strict test-first steps:**

- [ ] Test case-insensitive `token|key|secret|password` followed by `:` or `=` redacts the entire remainder, including whitespace; test benign substrings and delimiters are unchanged.
- [ ] Test multi-line/chunked input, invalid UTF-8 replacement, exact 8 KiB boundary, and overflow retaining only the latest complete byte tail without emitting invalid UTF-8.
- [ ] Through the mock agent, assert the distinctive secret value appears neither in `Runtime.Stderr()` nor captured slog output while surrounding nonsecret diagnostics remain.
- [ ] **RED evidence:** Add minimal compiling stderr seams, run `go test ./internal/acpruntime -run 'Test(Stderr|Redact)'`, and retain failing secret-absence/tail-boundary behavior.
- [ ] Implement a mutex-protected bounded byte tail, redact before both retention and logging, and normalize to valid UTF-8 using standard-library replacement. Do not persist stderr in SQLite.
- [ ] **GREEN evidence:** Re-run focused tests and retain explicit secret-absence and 8 KiB assertions passing.

**Verification:** `go test -race ./internal/acpruntime -run 'Test(Stderr|Redact)' -count=10`

**Risk:** High because a missed pattern leaks credentials into logs/HTTP errors.

**Reversibility:** Easy to revise; leaked logs cannot be recalled, so tests are release-blocking.

**Deliverable:** Future-502-ready stderr diagnostics capped and redacted at ingestion.

### Task 2.11: Wire Store Reconciliation and Close into the Application

**Description:** Open and reconcile SQLite during normal HTTP mode and checkpoint/close it during post-drain, without inventing live-runtime registration before Phase 03 owns the proxy.

**Files:** `internal/app/app.go`, `internal/app/app_test.go`

**Symbols:** existing `app.Run`, `lifecycle.Registry.Add`, `acpstore.Open`, `Reconcile`, `Close`

**References:** Master sections **ACP lifecycle and persistence**, **ACP state endpoints and schema**, and **Environment, shutdown, and image**; Phase 01 lifecycle registry.

**Strict test-first steps:**

- [ ] Add an app integration test with a temporary DB containing stale live rows; start normal mode, assert reconciliation occurred before health becomes available, cancel, and assert DB reopens cleanly after shutdown.
- [ ] Test post-drain checkpoints/closes the DB and the PID file is removed last. Runtime shutdown remains covered directly in `internal/acpruntime`; Phase 03 adds live proxy/runtime pre-drain ownership and app ordering coverage.
- [ ] Assert private mock mode still opens no DB and creates no DB/WAL files.
- [ ] **RED evidence:** After compiling the store wiring seam, run `go test ./internal/app -run 'TestRun(Database|Mock|ShutdownOrder)'`; retain behavioral failures showing missing reconciliation, checkpoint/close, or PID ordering.
- [ ] Open/reconcile after configuration but before listener readiness and register DB checkpoint/close in post-drain. Do not add a runtime registry, registration hook, or live-runtime app test in this phase.
- [ ] **GREEN evidence:** Re-run app/store tests under `-race`; retain passing reconciliation, no-private-DB, checkpoint, and PID cleanup assertions.

**Verification:** `go test -race ./internal/app ./internal/acpstore -count=1`

**Risk:** High because shutdown order protects committed output and process ownership.

**Reversibility:** Easy code rollback; reconciliation updates persisted statuses intentionally.

**Deliverable:** SQLite startup reconciliation and post-drain close integrated with no ACP HTTP or live-runtime registration surface.

## Dependencies

| Task | Depends On |
|---|---|
| 2.1 | Completed Phase 01 |
| 2.2 | 2.1 |
| 2.3 | 2.2 |
| 2.4 | 2.2, 2.3 |
| 2.5 | Completed Phase 01 configuration and `internal/childenv` |
| 2.6 | Completed Phase 01 private dispatch |
| 2.7 | 2.2, 2.5 |
| 2.8 | 2.3, 2.6, 2.7 |
| 2.9 | 2.6, 2.7, 2.8 |
| 2.10 | 2.6, 2.7 |
| 2.11 | 2.4, 2.7, 2.8, 2.9, 2.10, completed Phase 01 lifecycle |

## Phase Deliverables

- `modernc.org/sqlite` as the only direct external module and a still-static `CGO_ENABLED=0` binary.
- Exact authoritative SQLite schema, pragmas, one-connection operation, transactional event sequencing, server/session/event access, startup reconciliation, and checkpoint-on-close.
- Agent resolution for Claude, Codex, OpenCode, and private mock re-exec using Phase 01 `childenv.Sanitized`, including removal of `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE` with all bridge control variables.
- A Linux stdio runtime owning an independent process group, with one bounded writer path for every envelope, 128-byte ID tokens, bounded lifecycle-only metadata, 256 total waiting/grace/committing slots, fixed 30-second lifecycle grace, duplicate/capacity protection through commit, serialized current-count status reconciliation, request timeout, accepted forwarding, and immediate descendant cleanup on direct-child exit.
- Exact agent-output byte persistence after JSONL framing removal, commit-held correlation/busy accounting, commit-before-release/waiter/coalesced-wakeup signaling, late-response persistence, atomic successful lifecycle roster/cwd mutation, synthetic invalid-output/exit notifications, and storage-failure containment.
- An 8 KiB redacted stderr tail and secret-safe stderr logging.
- A private deterministic strict ACP mock with process-local-only initialize/session/prompt/permission state, known-session-only load/resume, and controlled failure modes, wired before normal server startup.
- No proxy manager and no ACP HTTP endpoint.

## Completion Criteria

- [ ] Every task retains a failing behavioral RED assertion against the smallest compiling seam where feasible; missing symbols alone are setup evidence. Focused GREEN output follows implementation.
- [ ] Schema introspection matches all three authoritative tables, cascading foreign keys, and the event session index exactly.
- [ ] Concurrent event insertion produces per-server gap-free monotonic `int64` sequences and signals no wakeup before commit.
- [ ] Event sequences, store queries, and future wire DTOs use nonnegative `int64`; allocation at `math.MaxInt64` rejects overflow without partial mutation.
- [ ] `Runtime.Post` alone compacts each original validated HTTP object to one JSONL record. Raw string and numeric ID tokens are at most 128 bytes; numeric IDs correlate with the bounded single-pass lexical tuple under the 1,000,000-exponent limit, without `float64`, arbitrary-precision arithmetic, or power expansion. Tests cover canonical behavior and bounded allocations; complexity/package restrictions are confirmed in review.
- [ ] Waiting plus grace-retained plus committing correlations never exceed 256. Capacity returns typed `ErrCapacity`; lifecycle timeout retains metadata/ID/busy accounting for exactly 30 seconds, and unclaimed grace expiry kills/exits and clears all timers/correlations.
- [ ] Public busy is exactly `len(waiting|grace|committing) > 0`; notifications/client responses reserve nothing. Grace and committing states remain durably busy so idle reaping cannot target their work.
- [ ] A dedicated status mutex serializes every runtime status write. Each zero/nonzero mutation reconciles by re-reading current correlation count under the correlation lock, releasing it before SQL, and writing the current busy/idle value; terminal exit suppresses queued writes.
- [ ] Deterministic completion/new-Post interleaving proves an older completion cannot write idle after the new Post reconciles busy. Race tests cover waiting/grace/committing/removal transitions, and any status persistence failure fails waiters and kills/exits the runtime.
- [ ] A matched response transitions atomically to `committing` and cancels grace, but keeps duplicate reservation, capacity/busy accounting, lifecycle metadata, and waiter state through `AppendOutput`. Grace expiry cannot win after that transition.
- [ ] Commit success removes the committing entry, updates busy/idle, completes any attached waiter, then emits the capacity-1 wakeup. JSON-RPC error and malformed lifecycle output obey the same commit-before-release order without session mutation.
- [ ] Commit failure retains enough correlation/waiter state to fail with the typed storage error, then kills the group; kill clears all state and no waiter/wakeup observes uncommitted output.
- [ ] The private mock passes initialize, same-process known-session lifecycle, prompt update/response, permission reverse-call/client-response, delay, invalid stdout, stderr, and exit tests; fresh-process load/resume returns unknown-session without synthesized traffic.
- [ ] Pending metadata contains only canonical ID, lifecycle enum, optional session ID up to 1024 UTF-8 bytes, and cwd up to 4096 UTF-8 bytes. Over-limit client metadata fails before admission; over-limit agent metadata cannot mutate sessions. Successful new/load/resume output commits event/session/cwd atomically; error/timeout creates no session, and only a response within the fixed grace may create one late.
- [ ] Reconciliation marks stale `creating|idle|busy` rows exited, clears PIDs, and never signals persisted PIDs.
- [ ] `/proc/<pid>/stat` process-group tests prove explicit kill and every direct-child exit immediately SIGKILL the captured negative PGID before pump completion/status release; the direct child is reaped and no descendant remains non-zombie. Bounded cleanup handles unit-test zombie grandchildren, while Phase 06 owns tini reaping verification.
- [ ] Store failure terminates the process group and no uncommitted wakeup reaches consumers; filling the wakeup channel never blocks stdout or loses durable events.
- [ ] Stored and returned agent responses preserve emitted JSON object bytes after framing removal; runtime-owned input compaction is separate and SSE is specified to emit those stored bytes unchanged.
- [ ] `Runtime.Kill` signals and waits process/pumps; repeated `Kill`/`Wait` are idempotent so Phase 03 pre-drain completes termination and post-drain only confirms before DB close.
- [ ] Every envelope uses the capacity-256 writer queue and configured timeout. Pre-write timeout removes only that item; partial-write timeout kills the group and joins the writer before exit.
- [ ] Stderr is at most 8 KiB, valid UTF-8, redacted in both retained output and logs, and absent from SQLite.
- [ ] `go test -race ./...` and `go vet ./...` pass.
- [ ] `CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge` succeeds as a static Linux binary.
- [ ] `go list -m -f '{{if not .Indirect}}{{.Path}}{{end}}' all` shows only the main module and `modernc.org/sqlite` as direct modules.
- [ ] `httpapi.Server` tests still expose only Phase 01 routes; requests under `/v1/acp` return the existing RFC 9457 404.
- [ ] No SSE, HTTP ACP handler, proxy/runtime registry, idle reaper, or DELETE behavior was introduced; Phase 03 is required to call `Runtime.Post` rather than add a second correlation/persistence path.

## Final Verification Commands

```sh
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge
file bin/agent-bridge
go list -m -f '{{if not .Indirect}}{{.Path}}{{end}}' all
```

## Open Questions

- None for implementation. Task 2.1 uses the authoritative `modernc.org/sqlite` v1.57.0 pin. Orchestrator token and persistence-lifetime questions remain in the authoritative specification.

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
- Before implementation starts, choose one concrete `modernc.org/sqlite` version verified compatible with Go 1.26, replace the version placeholder in Task 2.1 with that version, and record the choice in the implementation change. Pin exactly that version in `go.mod`/`go.sum`; no test-only modules are allowed.
- Database timestamps use Unix milliseconds from an injected `func() time.Time` in tests and `time.Now` in production.
- Persisted event `kind` is one of `request`, `response`, or `notification`; synthetic `_adapter/agent_exited` and `_adapter/invalid_stdout` envelopes are notifications. `request` means only an agent-originated reverse-call. Inbound client messages are never passed to `Store.AppendOutput` and therefore are never persisted.
- Runtime stdout accepts one JSON object per line. A non-object, malformed JSON, batch array, or oversized line becomes one `_adapter/invalid_stdout` synthetic notification; raw invalid content is not persisted. The pump then continues when framing permits.
- Set the stdout line ceiling to the authoritative ACP maximum, 10 MiB. Use `bufio.Reader`, not `bufio.Scanner`, so an oversized line can be drained safely and converted into one synthetic event without permanently stopping the stream.
- Process tests must verify process-group behavior on Linux with helper test subprocesses: the helper starts a grandchild, reports both PIDs, and blocks; killing negative PGID must terminate both. Tests may skip only when not running on Linux, although production itself is Linux-only.
- Runtime status transitions in this phase include `creating -> idle`, `idle <-> busy` based on at least one pending client request, and `idle|busy -> exited`. Initialize-only recreation, DELETE atomicity, idle TTL, and HTTP-driven instance lifecycle belong to Phase 03.
- `Runtime.Post` owns stdio JSON-RPC classification, exact raw ID correlation, duplicate detection, pending metadata, configured timeout, and accepted forwarding. Phase 03 delegates an already HTTP-validated raw envelope to `Runtime.Post` and only maps its typed result/errors to HTTP; it must not recreate correlation or persistence.
- The runtime completes a waiter and publishes a committed event only after the SQLite transaction commits. A timed-out request removes its waiter, but its late response is still persisted and published. It does not implement SSE subscriptions or replay buffering; Phase 03 builds those over store queries and committed-event delivery.
- SQLite checkpoint and close are registered with the Phase 01 shutdown registry before runtimes, so reverse-order cleanup kills/waits for children before checkpointing the database.

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
	LastEventSeq uint64
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
	Seq        uint64
	Kind       string
	Method     *string
	Payload    json.RawMessage
	SessionID  *string
	CreatedAtMs int64
}

type EventQuery struct {
	SessionID *string
	After     uint64
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

`Output` contains the already-validated raw envelope plus classified `Kind`, optional `Method`, optional `SessionID`, and optional lifecycle session mutation (`session/new`, `session/load`, or `session/resume` cwd/session metadata). `AppendOutput` allocates sequence, inserts the event, applies session upsert metadata, and updates the server watermark/timestamp in one transaction.

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

type CommittedEvent struct {
	Event acpstore.Event
}

type PostResult struct {
	Response json.RawMessage
	Accepted bool
}

type Pending struct {
	ID        json.RawMessage
	Method    string
	SessionID *string
}

type Runtime struct { /* private process/pumps/state */ }

func Start(ctx context.Context, store *acpstore.Store, serverID string, spec LaunchSpec, requestTimeout time.Duration, log *slog.Logger) (*Runtime, error)
func (r *Runtime) PID() int
func (r *Runtime) Post(ctx context.Context, payload json.RawMessage) (PostResult, error)
func (r *Runtime) Events() <-chan CommittedEvent
func (r *Runtime) Stderr() string
func (r *Runtime) Wait() error
func (r *Runtime) Kill(ctx context.Context) error
```

`Start` rejects a non-positive request timeout, launches with `syscall.SysProcAttr{Setpgid:true}`, pipes stdio, marks the row live/idle only after successful spawn, and starts pumps. `Post` accepts exactly the three authoritative client envelope forms. A request (`method` and non-null `id`) atomically registers `Pending` before writing, uses the exact serialized `id` bytes as its key, rejects a duplicate pending key, marks the server busy, and waits up to the configured timeout for a matching committed agent response. A notification or client response is serialized to stdin and returns `Accepted:true` immediately; a client response lets the agent complete a reverse-call but is not persisted by the bridge. Pending entries retain only ID, method, and optional session ID. Timeout removes the pending entry and updates idle state without suppressing a later response from persistence/events. Export typed/sentinel errors for invalid envelope, duplicate ID, timeout, exited process, write failure, and persistence failure so Phase 03 only maps outcomes to HTTP.

For a matching `session/new` response, no session ID exists in the pending request. Before persistence, the output classifier uses the pending method to identify the lifecycle response, extracts `result.sessionId` from that raw agent response, and assigns it to the event/session mutation. It does not invent an ID, synthesize a request, or replay a prompt. For `session/load` and `session/resume`, attribution may use the optional session ID already present in pending metadata.

The stdout pump persists an agent envelope before completing a matching `Post` waiter or sending `CommittedEvent`. SQLite failure kills the process group, marks exited where possible, closes/fails every pending waiter with the persistence error, and publishes no uncommitted event. `Kill` signals the negative process-group ID with SIGKILL, waits for pumps/process, and is idempotent. Natural exit and kill both persist `_adapter/agent_exited`, mark the server exited, clear PID, fail remaining waiters, and close the committed-event channel after all commits.

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
- [ ] Assert `journal_mode=wal`, `foreign_keys=1`, `busy_timeout=5000`, `DB.Stats().MaxOpenConnections == 1`, and schema creation is idempotent across reopen.
- [ ] **RED evidence:** Run `go test ./internal/acpstore -run TestOpenSchema`; retain compilation failure because `Open` is undefined and/or the SQLite driver is absent.
- [ ] Before writing schema/store production code, verify Go 1.26 compatibility, choose a concrete SQLite module version, replace `<PINNED_SQLITE_VERSION>` in this task with it, and record that same version in the implementation change description.
- [ ] Run `go get modernc.org/sqlite@<PINNED_SQLITE_VERSION>` once; inspect `go.mod`/`go.sum` and reject any directly added production/test module besides this driver (its transitive modules are expected).
- [ ] Implement `sql.Open("sqlite", path)`, `SetMaxOpenConns(1)`, pragma setup, ping, and schema creation with cleanup on every partial failure.
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
- [ ] **RED evidence:** Run `go test ./internal/acpstore -run 'Test(Server|Session)'`; retain undefined-symbol failures.
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
- [ ] Test exact raw JSON payload round-trip, nullable method/session fields, all three event kinds, ascending/descending `after` behavior, limits, optional session filtering, deterministic results, unknown server/session errors, and max-query validation delegated to callers rather than silently changed.
- [ ] Test lifecycle session create/update for supplied `sessionId`/`cwd`, preserving `created_at_ms`, updating `updated_at_ms`, and rolling back event/watermark/session mutation together on an induced constraint/closed-DB failure.
- [ ] Add a concurrent append test; despite many goroutines, resulting sequences must be gap-free and unique because the single connection serializes transactions.
- [ ] **RED evidence:** Run `go test ./internal/acpstore -run 'Test(AppendOutput|Events)'`; retain undefined-symbol failures.
- [ ] Implement one `sql.Tx` that reads/increments the server watermark, inserts the event, conditionally upserts session metadata, updates the server, and commits. Return an event only after commit succeeds.
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
- [ ] Produce WAL content, call `Close`, assert a checkpoint succeeds and reopen sees all committed data; call `Close` twice and require harmless idempotence.
- [ ] **RED evidence:** Run `go test ./internal/acpstore -run 'Test(Reconcile|Close)'`; retain undefined-method failures.
- [ ] Implement reconciliation as one update transaction and close as once-only checkpoint-then-DB-close, joining checkpoint/close errors where both exist.
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
- [ ] Test inherited credentials remain while every occurrence of `AGENT_BRIDGE_TOKEN`, `AGENT_BRIDGE_PID_FILE`, and `AGENT_BRIDGE_INTERNAL_MOCK_AGENT` is removed before the controlled mock value is added. Include duplicate keys and values containing `=`.
- [ ] **RED evidence:** Run `go test ./internal/acpruntime -run TestResolver`; retain undefined `Resolver` failure.
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
- [ ] **RED evidence:** Run `go test ./internal/mockagent ./internal/app`; retain missing package/`Run` failures and the old dispatch expectation failure.
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
- [ ] Test `Kill` sends SIGKILL to `-pgid`, waits, clears PID/marks exited, and is safe concurrently/repeatedly. Poll `syscall.Kill(pid, 0)` for both child and grandchild with a bounded deadline; fail if either remains. Always register test cleanup that kills both PIDs to prevent leaks after assertion failure.
- [ ] Test failed spawn leaves the server exited with no PID and no leaked pipe/goroutine.
- [ ] **RED evidence:** Run `go test ./internal/acpruntime -run 'TestRuntime(ProcessGroup|Start|SpawnFailure)'`; retain undefined `Start`/`Runtime` failures.
- [ ] Implement `exec.CommandContext` only if cancellation cannot bypass explicit group cleanup; set `Setpgid:true`, capture pipes before start, serialize stdin with a mutex, and centralize wait so `Wait` and `Kill` cannot call `Cmd.Wait` twice.
- [ ] **GREEN evidence:** Run the process-group tests with `-race -count=10`; retain passing evidence that both descendant PIDs disappear each iteration and no race/goroutine leak is reported.

**Verification:** `go test -race ./internal/acpruntime -run 'TestRuntime(ProcessGroup|Start|SpawnFailure)' -count=10`

**Risk:** High because a faulty test or implementation can leak/kill processes; always use test-owned PGIDs and bounded cleanup.

**Reversibility:** Easy code rollback; leaked OS processes require explicit cleanup.

**Deliverable:** A Linux process-group-owned stdio child lifecycle with reliable descendant termination.

### Task 2.8: Pump, Classify, Persist, and Publish Agent Stdout

**Description:** Read JSONL stdout, minimally inspect routing/session fields, persist every agent envelope before publication, and synthesize invalid-output/exit notifications.

**Files:** `internal/acpruntime/output.go`, `internal/acpruntime/output_test.go`, `internal/acpruntime/runtime.go`, `internal/acpruntime/runtime_integration_test.go`

**Symbols:** `CommittedEvent`, `Runtime.Events`; private `readOutput`, `classifyOutput`, `sessionMutation`, `persistSynthetic`

**References:** Master sections **ACP HTTP contract** and **ACP lifecycle and persistence**; `acpstore.AppendOutput`; private mock agent.

**Strict test-first steps:**

- [ ] Unit-test classification of agent requests (`method` plus non-null ID), responses (`id` plus exactly one result/error), notifications (`method` without ID), raw numeric/string IDs, invalid objects, arrays, and `id:null`.
- [ ] Test session metadata is inspected only for `session/new`, `session/load`, `session/resume`, and session-scoped messages, extracting only `sessionId`/`cwd`; unrelated content with those key names must not mutate sessions. For a `session/new` response, use its matching pending method plus `result.sessionId` to attribute the committed response even though pending metadata has no prior session ID.
- [ ] Integration-test mock output order: each value received from `Events()` must already be queryable at the same sequence/payload in SQLite. Test malformed/non-object/oversized lines produce one persisted `_adapter/invalid_stdout` notification each and subsequent valid lines still commit.
- [ ] Test a natural mock exit persists `_adapter/agent_exited`, then marks exited and closes the event channel. Assert only agent stdout/synthetic events exist; sent client payloads do not appear in the DB.
- [ ] Close/fault the store during output and assert runtime kills the entire process group, marks/fails as far as storage permits, logs the storage error, and publishes no uncommitted event. Typed waiter failure is added in Task 2.9; HTTP 507 mapping remains Phase 03.
- [ ] **RED evidence:** Run `go test ./internal/acpruntime -run 'Test(ClassifyOutput|RuntimeOutput)'`; retain undefined classifier/event-channel failures.
- [ ] Implement a bounded `bufio.Reader` line loop that drains overlong lines, validates one JSON object, classifies routing fields only, calls `AppendOutput`, and sends `CommittedEvent` only after commit. Ensure one owner closes the event channel after stdout and exit persistence complete.
- [ ] **GREEN evidence:** Run focused tests under `-race -count=10`; retain commit-before-observe, invalid-line recovery, natural-exit, and store-failure assertions passing.

**Verification:** `go test -race ./internal/acpruntime -run 'Test(ClassifyOutput|RuntimeOutput)' -count=10`

**Risk:** High because output loss/reordering breaks replay and response delivery in Phase 03.

**Reversibility:** Easy before external use; persisted event semantics require migration after release.

**Deliverable:** Commit-before-event-publication ACP stdout ingestion with minimal metadata inspection and failure containment.

### Task 2.9: Correlate JSON-RPC Posts in the Stdio Runtime

**Description:** Make `Runtime.Post` the single owner of JSON-RPC client-envelope forwarding, pending request metadata, exact raw ID matching, timeout, and committed-response delivery so Phase 03 remains an HTTP/lifecycle adapter.

**Files:** `internal/acpruntime/post.go`, `internal/acpruntime/post_test.go`, `internal/acpruntime/runtime.go`, `internal/acpruntime/output.go`, `internal/acpruntime/runtime_integration_test.go`

**Symbols:** `PostResult`, `Pending`, `Runtime.Post`; exported typed/sentinel `ErrInvalidEnvelope`, `ErrDuplicateID`, `ErrRequestTimeout`, `ErrExited`, `ErrWrite`, `ErrPersistence`; private `idKey`, `pendingRequest`, `setBusyState`, `completeResponse`, `failPending`

**References:** Master sections **ACP HTTP contract** and **ACP lifecycle and persistence**; target runtime interface and Task 2.8 commit ordering.

**Strict test-first steps:**

- [ ] Table-test client classification: request is `method` plus non-null string/number ID; notification is `method` without ID; client response has non-null ID, no method, and exactly one of `result`/`error`; reject arrays, `id:null`, missing/invalid method, invalid ID types, and ambiguous response envelopes.
- [ ] Test `idKey` uses exact serialized `json.RawMessage` bytes: string versus number and distinct valid number/string serializations remain distinct keys; the response must match the exact request key rather than a normalized Go value.
- [ ] Start the strict mock and test a request registers pending metadata before stdin write, stores only ID/method/optional session ID, persists the idle-to-busy transition, waits, and returns the exact committed raw response. With multiple requests, remain busy until the last pending entry completes, then persist idle and its new idle timestamp.
- [ ] Post the same exact ID concurrently and assert one request proceeds while the duplicate returns `ErrDuplicateID` without writing a second line. A different exact raw key proceeds independently.
- [ ] Test notifications and client responses are written once and return `PostResult{Accepted:true}` without persistence or waiter creation. Use the permission mock flow to prove a forwarded client response completes the agent reverse-call and eventually the original prompt request.
- [ ] Post `session/new` and assert its response event/session mutation is attributed to `result.sessionId` before commit despite pending `SessionID == nil`; malformed/missing result session IDs must not create or guess a session.
- [ ] After replacing the mock with a fresh subprocess, post explicit `session/load` and `session/resume` for a formerly known ID; assert `Runtime.Post` returns the mock's exact unknown-session JSON-RPC response, persists it normally, and emits no synthesized prompt/request/update.
- [ ] Configure a short timeout and controlled delayed response; assert `ErrRequestTimeout`, pending removal, correct idle transition, and later response persistence/event publication with no waiter completion attempt or process kill.
- [ ] Instrument store and event consumption to prove the matching response transaction commits before either the `Post` waiter returns or `Events()` exposes it. On commit failure, assert no waiter/event delivery, the process group dies, and every pending post receives `ErrPersistence` for future HTTP 507 mapping.
- [ ] Test stdin write failure removes only that pending request; natural/controlled exit fails all pending posts with `ErrExited`; cancellation of one caller removes only its waiter and does not cancel output persistence.
- [ ] **RED evidence:** Run `go test ./internal/acpruntime -run 'Test(Post|IDKey|Pending)'`; retain undefined `Post`/error/classifier failures.
- [ ] Implement one mutex-protected pending map keyed by copied raw ID bytes, one serialized stdin writer, configured per-runtime timeout, and `Store.SetStatus` transitions based on pending count. Treat status persistence failure like output persistence failure. In the output pump, call `AppendOutput` first, then resolve a matching waiter and publish `CommittedEvent`; unmatched/late responses still publish normally.
- [ ] **GREEN evidence:** Run the focused suite under `-race -count=20`; retain passing duplicate, exact-key, timeout/late, reverse-call, commit-order, exit, and persistence-failure evidence.

**Verification:** `go test -race ./internal/acpruntime -run 'Test(Post|IDKey|Pending)' -count=20`

**Risk:** High because duplicate handling, commit order, and timeout cleanup define core ACP correctness.

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
- [ ] **RED evidence:** Run `go test ./internal/acpruntime -run 'Test(Stderr|Redact)'`; retain undefined-symbol failures.
- [ ] Implement a mutex-protected bounded byte tail, redact before both retention and logging, and normalize to valid UTF-8 using standard-library replacement. Do not persist stderr in SQLite.
- [ ] **GREEN evidence:** Re-run focused tests and retain explicit secret-absence and 8 KiB assertions passing.

**Verification:** `go test -race ./internal/acpruntime -run 'Test(Stderr|Redact)' -count=10`

**Risk:** High because a missed pattern leaks credentials into logs/HTTP errors.

**Reversibility:** Easy to revise; leaked logs cannot be recalled, so tests are release-blocking.

**Deliverable:** Future-502-ready stderr diagnostics capped and redacted at ingestion.

### Task 2.11: Wire Store/Reconciliation into Application Shutdown

**Description:** Open and reconcile SQLite during normal HTTP mode and guarantee runtime cleanup precedes checkpoint/close, without adding ACP routes or a proxy manager.

**Files:** `internal/app/app.go`, `internal/app/app_test.go`, `internal/acpruntime/runtime_integration_test.go`

**Symbols:** existing `app.Run`, `lifecycle.Registry.Add`, `acpstore.Open`, `Reconcile`, `Close`

**References:** Master sections **ACP lifecycle and persistence**, **ACP state endpoints and schema**, and **Environment, shutdown, and image**; Phase 01 lifecycle registry.

**Strict test-first steps:**

- [ ] Add an app integration test with a temporary DB containing stale live rows; start normal mode, assert reconciliation occurred before health becomes available, cancel, and assert DB reopens cleanly after shutdown.
- [ ] Add a registry-order integration test with one live runtime proving its process group is gone before the DB close callback executes and the PID file is removed last according to explicit registration order.
- [ ] Assert private mock mode still opens no DB and creates no DB/WAL files.
- [ ] **RED evidence:** Run `go test ./internal/app -run 'TestRun(Database|Mock)'`; retain failures showing DB is not opened/reconciled.
- [ ] Open/reconcile after configuration but before listener readiness; register DB close before runtime cleanup registrations so reverse order kills runtimes first. Keep runtime registration as a narrow internal hook for Phase 03 rather than constructing agents in the app now.
- [ ] **GREEN evidence:** Re-run app/runtime integration tests under `-race`; retain passing reconciliation, process-order, no-private-DB, checkpoint, and PID cleanup assertions.

**Verification:** `go test -race ./internal/app ./internal/acpruntime ./internal/acpstore -count=1`

**Risk:** High because shutdown order protects committed output and process ownership.

**Reversibility:** Easy code rollback; reconciliation updates persisted statuses intentionally.

**Deliverable:** SQLite startup/shutdown integrated into the server lifecycle with no ACP HTTP surface.

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
- Agent resolution for Claude, Codex, OpenCode, and private mock re-exec using Phase 01 `childenv.Sanitized`.
- A Linux stdio runtime owning an independent process group, with serialized writes, exact raw ID correlation, duplicate protection, request timeout, pending metadata, accepted forwarding, and process-tree cleanup.
- Commit-before-waiter/event publication stdout processing, late-response persistence, minimal routing/session inspection, synthetic invalid-output/exit notifications, and storage-failure containment.
- An 8 KiB redacted stderr tail and secret-safe stderr logging.
- A private deterministic strict ACP mock with process-local-only initialize/session/prompt/permission state, known-session-only load/resume, and controlled failure modes, wired before normal server startup.
- No proxy manager and no ACP HTTP endpoint.

## Completion Criteria

- [ ] Every task retains intentional RED output before implementation and focused GREEN output afterward.
- [ ] Schema introspection matches all three authoritative tables, cascading foreign keys, and the event session index exactly.
- [ ] Concurrent event insertion produces per-server gap-free monotonic sequences and publishes nothing before commit.
- [ ] `Runtime.Post` preserves exact raw ID keys, rejects duplicate pending IDs, forwards notifications/client responses as accepted, enforces configured timeout, and persists/publishes late responses.
- [ ] Matching output commits before waiter completion and event publication; persistence failure kills the group and fails all pending posts with the typed storage error.
- [ ] The private mock passes initialize, same-process known-session lifecycle, prompt update/response, permission reverse-call/client-response, delay, invalid stdout, stderr, and exit tests; fresh-process load/resume returns unknown-session without synthesized traffic.
- [ ] A committed `session/new` response is attributed from `result.sessionId` using its pending method even though no session ID existed before the response.
- [ ] Reconciliation marks stale `creating|idle|busy` rows exited, clears PIDs, and never signals persisted PIDs.
- [ ] Process-group tests prove both helper child and grandchild terminate on runtime kill and shutdown, with bounded cleanup on test failure.
- [ ] Store failure terminates the process group and no uncommitted event reaches consumers.
- [ ] Stderr is at most 8 KiB, valid UTF-8, redacted in both retained output and logs, and absent from SQLite.
- [ ] `go test -race ./...` and `go vet ./...` pass.
- [ ] `CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge` succeeds as a static Linux binary.
- [ ] `go list -m -f '{{if not .Indirect}}{{.Path}}{{end}}' all` shows only the main module and `modernc.org/sqlite` as direct modules.
- [ ] `httpapi.Server` tests still expose only Phase 01 routes; requests under `/v1/acp` return the existing RFC 9457 404.
- [ ] No SSE, HTTP ACP handler, proxy registry, idle reaper, or DELETE behavior was introduced; Phase 03 is required to call `Runtime.Post` rather than add a second correlation/persistence path.

## Final Verification Commands

```sh
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge
file bin/agent-bridge
go list -m -f '{{if not .Indirect}}{{.Path}}{{end}}' all
```

## Open Questions

- None for implementation. Selecting, pinning, and recording one concrete Go 1.26-compatible `modernc.org/sqlite` version is a mandatory pre-implementation step in Task 2.1, not an unresolved version policy. Orchestrator token and persistence-lifetime questions remain in the authoritative specification.

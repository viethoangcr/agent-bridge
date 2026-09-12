# Reference: Go Coding Standards

**Date:** 2026-09-12
**Status:** CURRENT
**Purpose:** Behavior-level coding rules for `agent-bridge`, distilled from official Go guidance and "100 Go Mistakes and How to Avoid Them" (Teiva Harsanyi), and tuned for Go 1.26.8, the standard library, and this project's concurrency/subprocess/filesystem surface.

**Precedence:** specification (`20260815-agent-bridge.md`) > phase plans > this document. If a rule conflicts with a plan requirement, the plan wins; flag the conflict rather than deviating silently.

**Top rules (if you read nothing else):**

1. `internal/` + `cmd/` layout; services never construct their own dependencies (`internal/app` and the `acpproxy` runtime factory do).
2. Interfaces belong to the consumer; prefer concrete returns; no premature abstraction.
3. `context.Context` first for blocking/cancellable work; propagate and cancel properly; use `WithCancelCause`/`Cause`.
4. Every goroutine has a documented exit; bound concurrency with a buffered-channel semaphore (`golang.org/x/sync` is not available).
5. Channels transfer ownership or signal; mutexes guard shared state. Never leak internal maps/slices.
6. Wrap with `%w` only when callers must inspect; use `errors.Is`/`errors.As`; never string-match errors; handle an error once (log or return).
7. No panics for expected failures; recover only at an explicit boundary.
8. `defer` sits next to acquisition; no `defer` in loops; join deferred `Close` errors.
9. Bound every request body with `http.MaxBytesReader`; configure server timeouts; never inherit secrets into child processes.
10. Persisted and delivered bytes are sacred: agent output is stored/returned/streamed exactly after framing removal; `Runtime.Post` alone compacts validated client input; never decode/re-marshal ACP payloads.
11. Table-driven tests, injected clocks, `-race` in CI; no sleeps except the single bounded real-time heartbeat test; no third-party test libraries.
12. Optimize only with evidence (`pprof`, `-benchmem`); do not add `sync.Pool` unless a benchmark requires it and the memory budgets account for it.

## 1. Go 1.26 toolkit

Use (available in the pinned toolchain):

| Feature | Since | Use for |
|---|---|---|
| `net/http` method/path patterns, `{$}` | 1.22 | all routing; exact-match root |
| per-iteration loop variables | 1.22 | delete `tc := tc` copies; still copy values you retain deliberately |
| `for range` over int; `min`, `max`, `clear` | 1.22 / 1.21 | concise loops and bounds |
| `errors.Join`; `errors.AsType` | 1.20 / 1.26 | multi-error cleanup; typed error extraction |
| `log/slog` | 1.21 | the only logging API |
| `context.WithCancelCause`, `context.Cause` | 1.20 | actionable cancellation reasons |
| `slices`, `maps`, `cmp` | 1.21 | generic helpers instead of hand-rolled loops |
| `sync.OnceFunc/OnceValue/OnceValues`; `sync.WaitGroup.Go` | 1.21 / 1.25 | idempotent init; structured goroutine accounting |
| `http.MaxBytesReader` / `http.MaxBytesError` | existing / 1.19 | bounded request bodies; map over-limit to 413 |
| `crypto/rand.Text` | 1.24 | generating tokens/secrets |
| `database/sql.Null[T]` | 1.22 | nullable columns without manual `Valid` plumbing |
| `os.Root` (expanded method set) | 1.24+ (check pinned patch advisories) | path confinement for one relative tree; not a substitute for the Phase 05 contract (see Section 9) |
| `runtime.AddCleanup` | 1.24 | leak backstop only; explicit `Close` stays authoritative (nondeterministic timing) |
| `json` `omitzero` tag | 1.24 | omit zero structs; keep `omitempty` only for empty containers |
| `testing/synctest`, `T.Context`, `T.Chdir`, `B.Loop` | 1.25 / 1.24 | time-virtualized concurrency tests; subprocess lifetimes; non-parallel cwd tests; benchmarks |

Do not rely on:

- `golang.org/x/sync` (`errgroup`, `semaphore`) or any other external module. stdlib substitutes: buffered channel semaphore, `sync.WaitGroup`, first-error capture + `errors.Join`.
- `encoding/json/v2` or `GOEXPERIMENT=jsonv2` (experiment-gated in 1.26). The pinned toolchain is the defense; golden JSON tests protect required wire behavior.
- Toolchain auto-upgrade or floating `go` directives.
- `time.After` in loops, `time.Sleep` in tests, unbounded `sync.Pool`, or generics that abstract a single concrete use.

## 2. Style and readability

- Prefer early returns and flat control flow; avoid nested `else` chains.
- Skip getters/setters; export fields only when the type is a plain data holder, otherwise keep state private and methods meaningful.
- Avoid `init()`; do setup explicitly in `app` or tests. Startup validation returns errors.
- Error strings: lowercase, no trailing punctuation, include the offending value/identifier (never secrets).
- Acronyms are consistently cased: `ID`, `URL`, `HTTP`, `ACP`, `DB`, `PID`. Exported names read naturally (`ParseACPEnvelope`, `acpstore.Server`).
- Comment intent and invariants, not mechanics. Every exported symbol has a doc comment starting with its name.
- Package naming follows `docs/references/go-project-layout.md`; no `util`/`common` packages.
- Avoid confusing shadowing of `err`/`ctx` in nested scopes; name deliberately (100 Go Mistakes #1).

## 3. Errors and panics

- Wrap with `%w` when a caller must inspect (`errors.Is`/`errors.As`/`errors.AsType`); use `%v` at transport boundaries where only a message is needed (#49).
- Compare errors with `errors.Is`/`errors.As`, never `==` on wrapped errors or string matching (#50-51).
- Sentinel errors are package-level `ErrX` values for conditions the HTTP layer maps; typed errors carry structured detail.
- Do not duplicate logging of the same failure across layers: log at the boundary that owns the recovery or response, otherwise return the wrapped error (#52). Do not discard errors; annotate intentional ignores (`_ = f.Close() // best effort`).
- Do not panic for expected failures (#48). Recover only at an explicit boundary; none exists in this project today, so no stray `recover()`.
- `defer` close/release errors are captured and joined: `defer func() { err = errors.Join(err, f.Close()) }()` (#54).
- HTTP handlers never expose internal error detail: centrally map owner-package typed errors to the specification's problem statuses and stable `detail` text, log the underlying error server-side, and keep ACP JSON-RPC error envelopes as HTTP 200.
- Startup configuration errors fail fast with the variable name and never print secret values.

## 4. Interfaces, types, and generics

- Define interfaces on the consumer side, sized to actual use (e.g., `acpproxy`'s private runtime interface matching only the methods it calls) (#5, #6).
- Prefer concrete returns. Return a consumer-facing interface when it hides an implementation or supplies a required seam, such as `Subscription` (#7).
- Use `any` only where the contract genuinely accepts arbitrary JSON, such as problem extensions; elsewhere prefer concrete types or generics for real duplication (#8).
- Generics: only to remove proven duplication (e.g., `database/sql.Null[T]`); never for single-use abstraction or to look clever (#9).
- Embedding is for behavior reuse, not for promoting fields accidentally; prefer explicit delegation (#10).
- Prefer typed string enums (`type Lifecycle string`) with constants over untyped strings or ints.
- Keep types honest: `acpstore.Server` (durable row) and `httpapi.Server` (HTTP surface) are intentionally different concepts in different packages.
- Constructors validate and return an error only when construction can fail; value types are immutable by convention, and types containing mutexes are used by pointer only (#74).

## 5. Slices, maps, and strings

- Preallocate only when the size is known and measured: `make([]T, 0, n)`, `make(map[K]V, n)` (#21, #27).
- Never expose an internal slice/map that callers could mutate; copy under the lock (#26, #70).
- `append` may share backing arrays: do not append to a slice you do not own; be explicit about copies (#25).
- Map iteration order is random; sort keys for deterministic output; never insert/delete during range and expect a consistent view (#33).
- Range copies elements; mutate via index when needed (#30).
- `break` exits the innermost `for`/`switch`/`select`; label loops when breaking outward (#34).
- `defer` in unbounded loops accumulates; scope each iteration in a helper so cleanup runs per iteration (#35).
- Use `strings.Cut`/`TrimSuffix`/`TrimPrefix`, never `TrimRight` with a string cutset (#38).
- Build strings with `strings.Builder` (+`Grow`) in loops; use `bytes` for byte assembly (#39).
- `strings.Clone` long-lived small substrings extracted from large buffers (#41).
- Avoid gratuitous `[]byte(s)`/`string(b)` round-trips; keep data in its native form (#40).

## 6. Concurrency and context

- `context.Context` is the first parameter for blocking, cancellable, SQL, HTTP, subprocess, and lifecycle operations, derived from the request/root; every derived cancel function is called. Use `WithCancelCause`/`Cause` to record why (#60, #61).
- Every goroutine has a documented exit condition: context cancellation, channel close, or a sentinel result (#62). Prefer synchronous functions; when async, return a stop/wait handle.
- Channels transfer ownership or signal; mutexes guard shared state (#57). The process/runtime registries use mutexes; event fan-out uses capacity-one notification channels.
- A buffered channel is the project's semaphore: `sem := make(chan struct{}, n)`. `golang.org/x/sync/semaphore` is not available.
- `sync.WaitGroup`: `Add` before `go`, `Done` in the goroutine, or use `WaitGroup.Go` (whose function must not panic) (#71).
- Never send on a channel without a guaranteed receiver; guard with `select { case ch <- v: case <-ctx.Done(): }`. Close to broadcast; nil channels disable a `select` case on purpose (#65, #66).
- Channel buffer sizes: 0 for synchronization, 1 for a one-slot handoff/wakeup, larger only with evidence (#67).
- `select` with multiple ready cases is random; handle every case and never rely on ordering (#64).
- Use typed atomics (`atomic.Int64`, `atomic.Pointer[T]`); they do not replace mutexes for compound invariants.
- The race detector finds data races on executed paths only; it does not prove correctness (#58). CI runs `-race`; tests run `-race -count=N` where the plans specify.
- Project lock rules: never hold global/live-map, lifecycle, status, or correlation locks during store I/O, `Post`, signals, waits, or pump joins. Status reconciliation may briefly read correlation state while holding the status lock, never in the inverse order. Reaping gates activity, waits, then rechecks generation and durable status before killing; DELETE/shutdown gate and kill before waiting for leases; the injected filesystem mutation mutex serializes bridge-originated mutations.
- SQLite: the store uses `SetMaxOpenConns(1)` with the exact escaped DSN pragmas from the specification, including `_txlock=immediate`; let `database/sql` serialize callers. Consume and `Close` `Rows` before another operation on the single connection, check every `rows.Err()`, use context-aware SQL methods, and keep transactional sequence allocation `int64`.

## 7. Subprocesses and OS resources

- `exec.CommandContext` with an explicit `Cmd.Env` (from `childenv.Sanitized`) and an explicit `Cmd.Dir`; resolve agent binaries to absolute paths (#81 applies to timeouts, see below).
- Set `Setpgid: true`; after successful start, capture and validate the owned PGID (`>1`) before `syscall.Kill(-pgid, sig)`. Never signal a bare child PID and never derive signaling targets from persisted state.
- Set `Cmd.WaitDelay` so `Wait` cannot hang on wedged pipes; replace default cancellation with guarded negative-PGID SIGKILL when the plans require it.
- One owner calls `Cmd.Wait` exactly once; project wrappers (`Runtime.Kill/Wait`, manager operations) provide idempotent ownership around it.
- On every direct-child exit, SIGKILL the captured negative PGID before awaiting pumps or releasing capacity, so descendants cannot survive the group leader.
- Close pipes immediately after use; drains run to EOF so children cannot block on a full pipe.
- Tests: accept only missing `/proc/<pid>/stat` or state `Z` as "stopped"; register bounded cleanup for every spawned PID; never use `kill(pid, 0)` as evidence.
- Do not kill PIDs read from persistence at startup; only live, runtime-owned PIDs are signaled.

## 8. HTTP and JSON

- Routing uses Go 1.22+ patterns; root is `GET /{$}`; the single `/` fallback derives `Allow` by probing `mux.Handler` per method and returns problem+json 405/404.
- Configure `http.Server` timeouts: `ReadHeaderTimeout`, `IdleTimeout`, and no global write timeout that would break SSE. Bound bodies with `http.MaxBytesReader`, map `*http.MaxBytesError` to 413, and apply each route's specified limit (ACP/JSON 10 MiB, FS PUT/upload 512 MiB, process input's active decoded limit).
- Required Content-Type, no trailing JSON, `DisallowUnknownFields`, require EOF; reject repeated scalar query keys. Error responses are RFC 9457 `application/problem+json` (#80: `return` after writing a response).
- ACP payloads are raw bytes: validate without decoding/re-marshaling; `Runtime.Post` alone compacts validated client input with `json.Compact`; never store a mutated client payload. Agent output is persisted and emitted exactly after JSONL framing removal; ordinary enclosing DTOs promise raw JSON embedding, not lexical whitespace fidelity.
- SSE: `event: message`, monotonic `int64` ids, stored bytes as `data`, heartbeat comment, flush per frame, `Cache-Control: no-cache`.
- Response bodies from outbound clients are drained and closed, or reused; never leak body descriptors (#79).
- Customize the HTTP client (`Timeout`, transport) wherever the project calls out (#81); no naked `http.Get`.
- Secrets never reach logs, errors, or problem details; Authorization is never logged.

## 9. Filesystem and security

- Validate at trust boundaries: strict JSON, bounded sizes, explicit path checks. Treat subprocess output as untrusted.
- Compare tokens with `sha256.Sum256` + `crypto/subtle.ConstantTimeCompare`; generate secrets with `crypto/rand.Text`; never `==` on secrets.
- Filesystem behavior is spec-defined: lexical component checks before cleaning, `Lstat` symlink rechecks before mutation, and the sandbox (not pathname checks) is the security boundary. Lexical checks alone are not confinement.
- Where a single confined tree is the actual surface (e.g., staging directories), prefer `os.Root` (Linux, Go 1.24+, full method set in 1.26): the kernel enforces the boundary, including symlink escapes, without TOCTOU reviews. Do not retrofit `os.Root` into the Phase 05 contract without a spec update.
- Uploads: reject absolute/escaping names, links/devices, symlink components, duplicate paths, trailing bytes; validate and extract into staging before merge.
- Config writes are atomic (same-directory temp, `0600`, `fsync`, rename) under the shared mutation mutex.
- Error messages at the API boundary do not leak host paths, SQL text, or stack traces.

## 10. Time and scheduling

- Always name units: `500 * time.Millisecond`, never `time.Sleep(10)` (#75).
- Prefer `time.NewTimer` + `Stop` or `context.WithTimeout` over `time.After` in loops (#76).
- Inject clocks/tickers for tests; production uses `time.Now` behind the same seam.
- Use `testing/synctest` when concurrency and time interact; otherwise injected clocks are the default.
- TTL/timer semantics follow the plans exactly (idle TTL, 30s lifecycle grace, fixed server-owned input deadline).

## 11. Logging

- `log/slog` only, structured key/value attributes; no `fmt.Println`, no `log.Printf` outside `main`'s startup errors.
- Log once per event: request logs (method, URI, status, latency, never Authorization), lifecycle transitions, and failures with enough context to debug.
- Redact before logging or storage: agent stderr lines containing case-insensitive `token|key|secret|password` followed by `:` or `=` have their remainder replaced with `[REDACTED]`; never log ACP payloads or Authorization.
- Child processes receive only `childenv.Sanitized` environments: the four bridge-only variables are removed exactly; never pass raw `os.Environ()`.
- Debug logging is opt-in via `AGENT_BRIDGE_LOG_LEVEL`; errors include the operation and stable identifiers (server ID, process ID), not payload contents.

## 12. Testing

- Table-driven tests with `t.Run`, `got` before `want`, and failure messages that name the input (#85; Go test comments).
- Use `t.Helper` in helpers, `t.Cleanup` for teardown, `t.TempDir` for files, and `t.Context` for subprocess/HTTP lifetimes. `t.Chdir` changes process-wide cwd and is only valid in non-parallel tests with no parallel ancestor.
- No sleeps: synchronize on channels/events, inject clocks, or use `testing/synctest`; the only real-time test is the single `TestRealHeartbeat15Seconds` (#86, #87).
- Use stdlib utilities: `httptest`, `testing/fstest`, `os/exec` re-exec helpers (#88).
- Build tags: `//go:build e2e` for Docker tests; the plans define the only tag and skip rules (#82). Fuzzing is out of scope unless a plan adds its corpus location and CI budget.
- CI runs `-race ./...`; focused concurrency tests run `-race -count=N` per the plans (#83).
- Benchmarks: `b.Loop()`, `-benchmem`, `benchstat` for comparisons; `testing.AllocsPerRun` for allocation ceilings (not under `t.Parallel`) (#89).
- Prefer behavioral RED evidence per the plans: a failing assertion against a seam, not a missing symbol.

## 13. Performance and allocations

- Profile first (`pprof` CPU/heap/block/mutex, `runtime/trace`); do not guess (#98).
- Preallocate with evidence; measure with `-benchmem` before and after (#21, #27, #96).
- Stream large data with `io.Copy` (32 KiB buffer, fast paths); use `io.ReadAll` only for small bounded payloads.
- Size `bufio` readers/writers for known frames; flush SSE per event.
- Do not introduce `sync.Pool`: it can retain buffers outside the project's charged log/run budgets; revisit only with a benchmark plus budget-accounting update (#96).
- This project's guardrail budgets (log memory, one-shot peak reservation) are admission controls, not heap guarantees; do not replace them with wishful reasoning.

## 14. "100 Go Mistakes" quick index

| # | Rule applied here |
|---|---|
| 1 | Never shadow `err`/`ctx`; be aware of variable shadowing in nested blocks |
| 2 | Prefer early returns and flat code over nested `else` |
| 3 | Avoid `init()`; explicit construction |
| 4 | No getter/setter boilerplate; expose meaningful methods |
| 5-7 | Consumer-side interfaces, no interface pollution, return concrete types |
| 8 | `any` says nothing; use concrete types or generics |
| 9 | Generics only for proven duplication |
| 10 | Embedding: intentional behavior reuse only |
| 12-13 | No misorganized projects, no `util` packages |
| 15-16 | Document exported symbols; run linters (`staticcheck`) |
| 21, 27 | Preallocate slices/maps with evidence |
| 25-26 | Slice aliasing and internal-state leaks |
| 28 | Maps never shrink; bound key growth |
| 30, 33-35 | Range copies, map order, `break` labels, `defer` in loops |
| 38-41 | Trim/cut helpers, builder concatenation, substring retention, string/byte conversions |
| 42, 44, 47 | Receiver consistency, named-result side effects, `defer` argument evaluation |
| 48-54 | No panics for failures, correct wrapping, `errors.Is/As`, handle once, join close errors |
| 55-58 | Concurrency only when measured, channel vs mutex, race detector |
| 60-62 | Context scope, correct parent, goroutine exit conditions |
| 64-67 | `select` randomness, close-to-broadcast, nil channels, buffer sizing |
| 69-71, 74 | Concurrent append, reference leaks under mutexes, WaitGroup discipline, no sync-type copies |
| 75-76 | Duration units, timer leaks |
| 77-81 | JSON tags, SQL rows/pooling, resource closing, returning after writes, client/server timeouts |
| 82-90 | Test categorization, race flag, table-driven tests, no sleeps, time injection, stdlib utilities, sound benchmarks |
| 95-99 | Heap and allocation measurement, profiling, budget-aware tuning |

## 15. Related documents

- `docs/references/go-project-layout.md` - package layout, ownership, and hygiene gates.
- `docs/references/acp-v1-protocol.md` - ACP v1 factual baseline.
- `docs/plans/20260815-agent-bridge.md` - authoritative specification and task conventions.

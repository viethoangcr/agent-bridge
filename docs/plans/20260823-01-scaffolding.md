# Plan: Phase 01 - Scaffolding and Server Foundation

**Date:** 2026-08-23
**Status:** DRAFT
**Risk Level:** Medium

---

## Phase Goal

Create the dependency-free Go 1.26 repository and production server foundation on which every later phase builds: validated environment configuration, private mock-mode dispatch, RFC 9457 HTTP behavior, authentication, route body-limit helpers, structured request logging, and bounded graceful shutdown with PID-file and cleanup-hook support. This phase does not implement ACP persistence, subprocesses, or any domain API beyond `/` and `/v1/health`.

## Assumptions and Specification References

- `docs/plans/20260815-agent-bridge.md` is authoritative. This plan elaborates its contract but does not replace it.
- Apply the master specification sections **Platform and dependencies**, **Authentication and errors**, **Agent resolution**, **Non-ACP endpoints**, and **Environment, shutdown, and image**.
- The workspace initially contains only documentation; all code and project conventions are new.
- The module path is `github.com/viethoangcr/agent-bridge`, matching the repository location.
- Phase 01 uses only the Go standard library. `modernc.org/sqlite` is deliberately added in Phase 02, not here.
- `AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1` selects an internal execution path before HTTP configuration, listener creation, or PID-file creation. Phase 01 establishes and tests this dispatch seam; Phase 02 replaces the temporary `internal mock agent is not implemented` result with the private JSONL loop.
- Durations parsed from `*_MS` variables are base-10 integers interpreted as milliseconds. `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` must be positive; `AGENT_BRIDGE_IDLE_TTL_MS` is non-negative and `0` disables reaping. Invalid values fail startup; no silent fallback is permitted.
- `AGENT_BRIDGE_PORT` is an integer in `1..65535`. `AGENT_BRIDGE_LOG_LEVEL` accepts `debug`, `info`, `warn`, or `error` case-insensitively and stores the normalized lowercase value.
- PID-file content is the decimal PID followed by `\n`; creation uses mode `0600` and fails rather than replacing an existing file. Removal ignores only `os.ErrNotExist`.
- Later phases register ACP, managed-process, and database cleanup functions in the same shutdown registry. This phase must not predeclare those implementations.

## Scope Boundaries

**Included:** module/repository files, environment parsing, shared child-environment sanitization, private dispatch seam, extensible HTTP server/router, root/health handlers, custom 404/405 problems, auth middleware, JSON/body-limit utilities, request logging, PID-file lifecycle, signal handling, cleanup registry, Makefile, README skeleton, and project `AGENTS.md`.

**Excluded:** SQLite and `modernc.org/sqlite`, ACP schema/store/runtime/proxy/handlers/SSE, agent resolution or launch, process API, filesystem/config APIs, image packaging, telemetry, daemon mode, and public CLI subcommands.

## Target Files and Interfaces

### `internal/config`

```go
type AgentCommand struct {
	Binary string
	Args   []string
}

type Config struct {
	Host              string
	Port              int
	LogLevel          slog.Level
	DBPath            string
	Token             string
	PIDFile           string
	ACPRequestTimeout time.Duration
	IdleTTL           time.Duration
	Agents            map[string]AgentCommand
}

func Load(getenv func(string) string) (Config, error)
func (c Config) Address() string
```

`Load` owns defaults and validation for all startup variables already defined by the master contract, including agent binary/JSON-array argument overrides. It must copy argument slices and return errors naming the invalid environment variable without exposing secret values.

### `internal/httpapi`

```go
type Problem struct {
	Type   string         `json:"type"`
	Title  string         `json:"title"`
	Status int            `json:"status"`
	Detail string         `json:"detail"`
	Ext    map[string]any `json:"-"`
}

type Dependencies struct {
	Token string
	Log   *slog.Logger
}

type Server struct { /* private mux and composed handler */ }

func NewServer(deps Dependencies) *Server
func (s *Server) Handler() http.Handler
func WriteProblem(w http.ResponseWriter, p Problem)
func DecodeJSON(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool
```

`Server` owns the `http.ServeMux`, route registration, and composed auth/logging handler so later phases extend `server.go` instead of replacing a phase-local router constructor. `Handler` returns the stable fully wrapped handler. `Problem.Ext` is flattened into top-level JSON members, but extension keys may not replace `type`, `title`, `status`, or `detail`. `DecodeJSON` is the sole foundation for bounded JSON routes: install `http.MaxBytesReader`, reject malformed/trailing JSON, map `*http.MaxBytesError` to 413, and emit problems itself.

### `internal/childenv`

```go
func Sanitized(environ []string) []string
```

`Sanitized` returns a fresh environment slice with every occurrence of `AGENT_BRIDGE_TOKEN`, `AGENT_BRIDGE_PID_FILE`, and `AGENT_BRIDGE_INTERNAL_MOCK_AGENT` removed. It splits entries only at the first `=`, preserves order and all unrelated values, and is the only child-environment sanitizer used by ACP and managed-process phases.

### `internal/lifecycle`

```go
type Cleanup func(context.Context) error

type Registry struct { /* private state */ }

func (r *Registry) Add(name string, cleanup Cleanup) error
func (r *Registry) Shutdown(ctx context.Context) error
```

Registration is rejected after shutdown starts. `Shutdown` invokes every registered cleanup once in reverse registration order, continues after errors, and returns `errors.Join` output. The implementation must be race-safe and idempotent.

### `internal/app`

```go
type IO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func Run(ctx context.Context, getenv func(string) string, io IO) error
```

`Run` checks private mock dispatch first, then loads configuration, starts the listener, writes the PID file, serves HTTP, and coordinates a maximum ten-second graceful shutdown. Private helpers include `runInternalMockAgent(context.Context, IO) error`, `writePIDFile(string) error`, and `removePIDFile(string) error`.

## Ordered Atomic Tasks

### Task 1.1: Establish Module, Entrypoint, and Baseline Checks

**Description:** Create the Go module, minimal command entrypoint, package layout, and reproducible developer commands without adding dependencies.

**Files:** `go.mod`, `cmd/agent-bridge/main.go`, `Makefile`

**Symbols:** `main`, initial `internal/app.Run` call site (allowed not to compile during RED only)

**References:** Master platform constraints; target `internal/app.Run` interface above.

**Strict test-first steps:**

- [ ] Add `cmd/agent-bridge/main_test.go` asserting the command package is buildable and that cancellation is translated to a clean return through an injected/context-driven app path once implemented.
- [ ] **RED evidence:** Run `go test ./cmd/agent-bridge`; retain output showing compilation fails because `internal/app`/`Run` does not exist. A test that passes before production code is not acceptable RED evidence.
- [ ] Create `go.mod` with `go 1.26`, the module path above, and no `require` entries; add `main.go` using `signal.NotifyContext` for `os.Interrupt` and `syscall.SIGTERM`, invoking `app.Run` once and exiting nonzero only for a returned startup/runtime error.
- [ ] Add `Makefile` targets `test`, `lint`, `build`, and `check`; use `CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge`, `go vet ./...`, and `go test ./...` without external tools.
- [ ] **GREEN evidence:** Run `go test ./cmd/agent-bridge` and record `ok`; run `CGO_ENABLED=0 go build ./cmd/agent-bridge` and record exit status 0.

**Verification:** `make check`

**Risk:** Low

**Reversibility:** Easy to revert; all files are new.

**Deliverable:** A standard-library-only Go 1.26 module with one command and deterministic check/build commands.

### Task 1.2: Parse and Validate Environment Configuration

**Description:** Implement all master-contract startup defaults and overrides now so later phases consume one validated `Config` rather than reading environment variables ad hoc.

**Files:** `internal/config/config.go`, `internal/config/config_test.go`

**Symbols:** `config.Config`, `config.AgentCommand`, `config.Load`, `Config.Address`; private `parseMilliseconds`, `parseAgentArgs`

**References:** Master sections **ACP HTTP contract**, **ACP lifecycle and persistence**, **Agent resolution**, and **Environment, shutdown, and image**; the interface above.

**Strict test-first steps:**

- [ ] Write table-driven tests for empty-environment defaults: host `127.0.0.1`, port `2468`, level `info`, DB `./agent-bridge.db`, request timeout `120000ms`, idle TTL `900000ms`, empty token/PID file, and the exact three default agent commands.
- [ ] Add tests for every override, JSON argument arrays (including spaces and empty strings), defensive argument-slice ownership, idle TTL `0`, and normalized log levels.
- [ ] Add rejection tests for malformed/out-of-range ports, zero/negative/non-integer request timeout, negative/non-integer idle TTL, unsupported log levels, non-array args, non-string args, and malformed JSON; assert errors name the responsible variable.
- [ ] **RED evidence:** Run `go test ./internal/config`; retain the undefined-symbol/build failure for `Load` and `Config`.
- [ ] Implement parsing with `strconv`, `encoding/json`, `time.Duration`, `net.JoinHostPort`, and `log/slog`; do not read `os.Getenv` inside this package.
- [ ] **GREEN evidence:** Run `go test ./internal/config -run TestLoad -count=1`; record all cases passing.

**Verification:** `go test ./internal/config -count=1`

**Risk:** Medium because all later phases depend on these defaults.

**Reversibility:** Easy to revert before later phases consume the type.

**Deliverable:** One validated immutable-by-convention configuration value covering the authoritative environment contract.

### Task 1.3: Add Shared Child Environment Sanitization

**Description:** Establish the common sanitizer required independently by ACP subprocesses in Phase 02 and managed processes in Phase 04.

**Files:** `internal/childenv/env.go`, `internal/childenv/env_test.go`

**Symbols:** `childenv.Sanitized`

**References:** Master sections **Agent resolution** and **Non-ACP endpoints**; target `internal/childenv` interface above.

**Strict test-first steps:**

- [ ] Test removal of all occurrences of the three bridge-only variables, including duplicate keys, empty values, entries without `=`, and values containing additional `=` characters.
- [ ] Test that credential-shaped and unrelated variables remain in original order and that mutating either input or output after the call cannot mutate the other slice.
- [ ] **RED evidence:** Run `go test ./internal/childenv`; retain compilation failure because `Sanitized` is undefined.
- [ ] Implement one linear pass using `strings.Cut`, copying retained strings into a newly allocated slice; do not read `os.Environ` inside the helper.
- [ ] **GREEN evidence:** Re-run `go test ./internal/childenv -count=1` and record all removal/order/ownership cases passing.

**Verification:** `go test ./internal/childenv -count=1`

**Risk:** High because incorrect filtering leaks bridge credentials or private dispatch into child processes.

**Reversibility:** Easy to revert before child runtimes consume it.

**Deliverable:** One tested sanitizer shared by Phases 02 and 04.

### Task 1.4: Implement RFC 9457 Problem Responses and Bounded JSON Decoding

**Description:** Build reusable error and request-body primitives before handlers so every route can satisfy content type and body-limit rules consistently.

**Files:** `internal/httpapi/problem.go`, `internal/httpapi/problem_test.go`, `internal/httpapi/body.go`, `internal/httpapi/body_test.go`

**Symbols:** `httpapi.Problem`, `httpapi.WriteProblem`, `httpapi.DecodeJSON`; private `problemMap`

**References:** Master sections **Authentication and errors** and **Non-ACP endpoints** body-limit requirements; interface above.

**Strict test-first steps:**

- [ ] Test exact `application/problem+json` content type and required fields for representative 400, 404, 405, 413, and 500 responses.
- [ ] Test flattened extension members and reserved-key protection.
- [ ] Test `DecodeJSON` for one valid object, malformed JSON, an empty body, a second JSON value/trailing bytes, exact-limit success, and over-limit 413. Decode failures must return `false` and produce a problem response.
- [ ] **RED evidence:** Run `go test ./internal/httpapi -run 'Test(WriteProblem|DecodeJSON)'`; retain undefined-symbol failures.
- [ ] Implement with `encoding/json`, `http.MaxBytesReader`, `errors.As` for `*http.MaxBytesError`, and one EOF check after the first decoded value. Do not buffer an unbounded body.
- [ ] **GREEN evidence:** Re-run the focused command and record `ok`, including the over-limit case reporting status 413 and RFC 9457 JSON.

**Verification:** `go test ./internal/httpapi -run 'Test(WriteProblem|DecodeJSON)' -count=1`

**Risk:** Medium because inconsistent use would violate a global HTTP guarantee.

**Reversibility:** Easy to revert; isolated package.

**Deliverable:** Tested RFC 9457 and body-limit foundations for all later route implementations.

### Task 1.5: Add Authentication Middleware

**Description:** Protect every `/v1/*` request only when a token is configured while leaving `GET /` public, using constant-time credential comparison.

**Files:** `internal/httpapi/auth.go`, `internal/httpapi/auth_test.go`

**Symbols:** private `authenticate(token string, next http.Handler) http.Handler`, `bearerToken`

**References:** Master section **Authentication and errors**.

**Strict test-first steps:**

- [ ] Test unset-token pass-through; configured-token success; missing, wrong, malformed, empty, duplicate, and wrong-scheme Authorization values; and public root bypass.
- [ ] Assert failures are 401 RFC 9457 responses and do not reflect either supplied or configured token. Assert ACP-looking JSON-RPC bodies do not alter auth failure semantics.
- [ ] **RED evidence:** Run `go test ./internal/httpapi -run TestAuthenticate`; retain the undefined `authenticate` failure.
- [ ] Implement exact `Bearer <token>` parsing and compare expected/supplied byte strings with `crypto/subtle.ConstantTimeCompare`; avoid logging or placing Authorization in context.
- [ ] **GREEN evidence:** Re-run the focused test and record all auth cases passing.

**Verification:** `go test ./internal/httpapi -run TestAuthenticate -count=1`

**Risk:** High within this phase because a bypass exposes every future API.

**Reversibility:** Easy to revert mechanically; security behavior must not be removed after release.

**Deliverable:** A route-prefix-aware, constant-time authentication layer.

### Task 1.6: Build Extensible HTTP Server, Root/Health Routes, and Custom 404/405 Handling

**Description:** Construct `httpapi.Server` around the Go 1.26 `ServeMux`, explicit method/path patterns, composed middleware, and custom problem fallbacks. Later phases add routes through the same server implementation rather than introducing parallel routers.

**Files:** `internal/httpapi/server.go`, `internal/httpapi/server_test.go`

**Symbols:** `httpapi.Dependencies`, `httpapi.Server`, `httpapi.NewServer`, `Server.Handler`; private `registerRoutes`, `root`, `health`, `notFound`, `methodNotAllowed`

**References:** Master sections **Authentication and errors** and **Non-ACP endpoints**. The docs URL value may be a stable repository README URL until Phase 06 finalizes public documentation.

**Strict test-first steps:**

- [ ] Add black-box table tests for `GET /`, `GET /v1/health`, an unknown path, and wrong methods on both known paths.
- [ ] Assert exact success JSON shapes, root's public behavior with auth enabled, health's required auth, `Allow: GET` on 405, and RFC 9457 bodies/content types for 404/405.
- [ ] Add path tests proving `/v1/health/extra` and `/unknown` are 404 rather than accidentally matched by a subtree handler.
- [ ] Test `NewServer(...).Handler()` as the only public HTTP assembly path and assert repeated `Handler` calls return the same composed handler.
- [ ] **RED evidence:** Run `go test ./internal/httpapi -run TestServer`; retain failure because `Server`/`NewServer` is undefined.
- [ ] Register `GET /` and `GET /v1/health` method/path patterns on the server-owned mux. Register methodless known-path fallbacks for 405 and a final `/` fallback that returns 405 only for the exact root path and 404 otherwise. Compose `/v1/*` auth once without buffering responses, preserving future SSE compatibility.
- [ ] **GREEN evidence:** Re-run the focused test; record every status/content-type/Allow assertion passing.

**Verification:** `go test ./internal/httpapi -run TestServer -count=1`

**Risk:** Medium because mux precedence mistakes can leak auth or return non-problem errors.

**Reversibility:** Easy to revert while routes are limited to two.

**Deliverable:** An extensible server whose successful foundation endpoints and all current routing failures satisfy the master contract.

### Task 1.7: Add Structured Request Logging

**Description:** Log method, request URI, status, and latency for every request without reading or logging Authorization.

**Files:** `internal/httpapi/logging.go`, `internal/httpapi/logging_test.go`, `internal/httpapi/server.go`

**Symbols:** private `requestLogger(*slog.Logger, http.Handler) http.Handler`, `statusWriter`

**References:** Master section **Environment, shutdown, and image**, request logging requirement.

**Strict test-first steps:**

- [ ] Capture JSON slog output and test a success plus a problem response for `method`, full request URI, final status, and non-negative `latency_ms`.
- [ ] Send a distinctive Authorization value and assert it is absent from the entire captured log payload. Test an implicit handler status is recorded as 200 and optional interfaces needed by streaming are not blocked.
- [ ] **RED evidence:** Run `go test ./internal/httpapi -run TestRequestLogger`; retain undefined-symbol failure.
- [ ] Implement a minimal status-capturing writer and preserve `http.Flusher`; do not log request or response headers/bodies. Install logging once as the outer `Server.Handler` middleware.
- [ ] **GREEN evidence:** Re-run the focused test and retain passing output plus the explicit secret-absence assertion.

**Verification:** `go test ./internal/httpapi -run TestRequestLogger -count=1`

**Risk:** Medium due to credential leakage and future SSE compatibility.

**Reversibility:** Easy to revert; isolated middleware.

**Deliverable:** Structured, secret-safe request logs with correct response statuses.

### Task 1.8: Implement the Shutdown Registry

**Description:** Provide a concurrency-safe cleanup registry that later phases can use for processes, streams, and SQLite.

**Files:** `internal/lifecycle/registry.go`, `internal/lifecycle/registry_test.go`

**Symbols:** `lifecycle.Cleanup`, `lifecycle.Registry`, `Registry.Add`, `Registry.Shutdown`

**References:** Master section **Environment, shutdown, and image**, shutdown requirement; interface above.

**Strict test-first steps:**

- [ ] Test reverse-order execution, exactly-once behavior under concurrent `Shutdown` calls, continued cleanup after an error, joined error identity, context propagation, and rejection of late registration.
- [ ] Run race-sensitive test iterations to expose duplicate callbacks or map/slice races.
- [ ] **RED evidence:** Run `go test -race ./internal/lifecycle`; retain undefined-type failures.
- [ ] Implement with `sync.Mutex` and a completion channel or equivalent standard-library synchronization; invoke callbacks outside the lock and publish one shared result.
- [ ] **GREEN evidence:** Run `go test -race ./internal/lifecycle -count=20`; record all iterations passing without race reports.

**Verification:** `go test -race ./internal/lifecycle -count=20`

**Risk:** Medium because shutdown correctness later prevents process/database corruption.

**Reversibility:** Easy to revert before consumers exist.

**Deliverable:** Idempotent reverse-order shutdown orchestration.

### Task 1.9: Integrate Private Dispatch, Listener, PID File, and Ten-Second Shutdown

**Description:** Implement the application lifecycle from mode selection through clean server termination. Listener creation must precede PID-file creation so a failed bind never leaves a PID file.

**Files:** `internal/app/app.go`, `internal/app/app_test.go`, `cmd/agent-bridge/main.go`

**Symbols:** `app.IO`, `app.Run`; private `runInternalMockAgent`, `writePIDFile`, `removePIDFile`, `serve`

**References:** Master sections **Agent resolution** and **Environment, shutdown, and image**; configuration, `httpapi.Server`, and lifecycle interfaces above.

**Strict test-first steps:**

- [ ] Test that `AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1` bypasses invalid HTTP environment configuration, does not bind a port or create a PID file, and returns the temporary phase-boundary error from the private hook.
- [ ] Test PID mode/content, refusal to replace an existing file, cleanup on context cancellation, no file after bind failure, and no failure when cleanup finds an already-removed file.
- [ ] Start `Run` against `127.0.0.1` with a test-selected free port; assert health serves, cancel context, assert return within ten seconds, and assert a subsequent bind to the same address succeeds.
- [ ] Test a registered cleanup error is logged/returned only after all callbacks run, while context cancellation and `http.ErrServerClosed` alone are clean.
- [ ] **RED evidence:** Run `go test ./internal/app`; retain undefined `Run`/PID helper failures.
- [ ] Implement mode check before `config.Load`; create `slog.Logger`, `net.Listen`, `httpapi.NewServer(...).Handler()`, `http.Server`, and PID file; register PID removal; call `http.Server.Shutdown` under a ten-second timeout when context ends; then execute the registry. Configure finite `ReadHeaderTimeout` and no global write timeout that would break future SSE.
- [ ] Update `main` to treat cancellation-driven clean shutdown as exit 0 and print startup/runtime errors to stderr without dumping configuration or token values.
- [ ] **GREEN evidence:** Run `go test -race ./internal/app -count=10`; retain passing lifecycle/PID assertions and no race output.

**Verification:** `go test -race ./internal/app -count=10 && go test ./...`

**Risk:** High because ordering errors can leave PID files or prevent bounded shutdown.

**Reversibility:** Easy to revert before deployment; PID behavior itself is externally observable once used.

**Deliverable:** A working HTTP binary with private pre-server dispatch and bounded graceful lifecycle.

### Task 1.10: Add Project Guidance and README Skeleton

**Description:** Document only the behavior delivered in Phase 01 and establish concise repository rules for later builders.

**Files:** `README.md`, `AGENTS.md`

**Symbols:** None.

**References:** Master sections **Overview**, **Platform and dependencies**, **Authentication and errors**, and **Environment, shutdown, and image**, plus its explicit out-of-scope list.

**Strict test-first steps:**

- [ ] Add `internal/projectdocs/projectdocs_test.go` that reads repository-root `README.md`, `AGENTS.md`, and `Makefile`, asserting the documented build/test commands and required Go/static/no-extra-dependency rules exist. Use only standard-library testing.
- [ ] **RED evidence:** Run `go test ./internal/projectdocs`; retain failure reporting missing `README.md`/`AGENTS.md`.
- [ ] Write a README skeleton with purpose, current phase status, prerequisites, `make check`, `make build`, environment table, auth/root/health examples, and a pointer to the authoritative specification. Do not advertise unfinished APIs or the private mock agent.
- [ ] Write project `AGENTS.md` requiring master-contract precedence, numeric phase order, stdlib plus only `modernc.org/sqlite`, strict test-first evidence, `CGO_ENABLED=0`, RFC 9457 errors, no public mock interface, no runtime installs, and no expansion into excluded features.
- [ ] **GREEN evidence:** Re-run the docs test and record `ok`.

**Verification:** `go test ./internal/projectdocs -count=1 && make check`

**Risk:** Low.

**Reversibility:** Easy to revise.

**Deliverable:** Accurate phase-scoped onboarding and enforceable local implementation guidance.

## Dependencies

| Task | Depends On |
|---|---|
| 1.1 | None |
| 1.2 | 1.1 |
| 1.3 | 1.1 |
| 1.4 | 1.1 |
| 1.5 | 1.4 |
| 1.6 | 1.4, 1.5 |
| 1.7 | 1.6 |
| 1.8 | 1.1 |
| 1.9 | 1.2, 1.6, 1.7, 1.8 |
| 1.10 | 1.1, 1.9 |

## Phase Deliverables

- A Go 1.26 module with no external dependencies and a `CGO_ENABLED=0` build.
- A validated environment configuration contract ready for later phases.
- A shared `internal/childenv.Sanitized` helper for ACP and managed process children.
- A private mock dispatch seam that executes before server startup, with mock behavior explicitly deferred to Phase 02.
- An extensible `internal/httpapi.Server` with public root and authenticated health endpoints using `net/http` method/path patterns.
- RFC 9457 responses for handler and router errors, including custom 404/405 and bounded-body failures.
- Secret-safe structured request logging.
- PID-file and cleanup-registry support with SIGINT/SIGTERM shutdown bounded to ten seconds.
- Minimal repository commands and documentation.

## Completion Criteria

- [ ] Every task has retained RED output demonstrating the intended test failed for the intended missing behavior before implementation.
- [ ] Every task has retained GREEN output demonstrating its focused verification passed after implementation.
- [ ] `go mod edit -json` shows Go 1.26 and no dependencies.
- [ ] `go test -race ./...` passes.
- [ ] `go vet ./...` passes.
- [ ] `CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge` succeeds and `file bin/agent-bridge` reports no dynamic interpreter/shared-library dependency on Linux.
- [ ] `internal/childenv` tests prove all bridge-only variables are removed while unrelated credentials and ordering are preserved.
- [ ] All HTTP construction goes through `httpapi.NewServer(...).Handler()`; no standalone router constructor exists.
- [ ] With no token, `GET /v1/health` returns 200; with a token, it returns 401 without a valid bearer token and 200 with one; `GET /` remains public.
- [ ] Unknown routes and wrong methods return `application/problem+json`, with wrong methods carrying the correct `Allow` header.
- [ ] SIGINT and SIGTERM each stop acceptance, execute cleanup once, remove the PID file, and exit 0 within ten seconds.
- [ ] No ACP endpoint, SQLite code, subprocess runtime, or non-foundation domain API was introduced.

## Final Verification Commands

```sh
go test -race ./...
go vet ./...
make build
file bin/agent-bridge
```

## Open Questions

- None for implementation. The orchestrator/token and persistence-lifetime questions remain owned by the authoritative specification and do not block this phase.

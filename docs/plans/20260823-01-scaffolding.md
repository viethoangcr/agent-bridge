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
- Durations parsed from `*_MS` variables are base-10 integers interpreted as milliseconds. Parsing must reject values whose checked millisecond-to-`time.Duration` conversion would overflow. `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` is in `1ms..1h`; `AGENT_BRIDGE_IDLE_TTL_MS` is in `0..30d`, where `0` disables reaping. Invalid or out-of-range values fail startup; no silent fallback is permitted. Phase 04 owns its process-specific timeout and byte-count bounds, but must use this phase's checked parsing conventions rather than unchecked multiplication.
- `AGENT_BRIDGE_PORT` is an integer in `1..65535`. `AGENT_BRIDGE_LOG_LEVEL` accepts `debug`, `info`, `warn`, or `error` case-insensitively and stores the normalized lowercase value.
- Remote unauthenticated listening is forbidden by default. Startup fails when `AGENT_BRIDGE_HOST` is not provably loopback and `AGENT_BRIDGE_TOKEN` is empty unless `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1`. Loopback means an IP for which `net.IP.IsLoopback` is true or the exact case-insensitive hostname `localhost`; unknown hostnames are treated as remote. The override accepts only empty/`0` or `1`, is never inherited by children, and must be documented as unsafe.
- PID-file content is the decimal PID followed by `\n`; creation uses mode `0600` and fails rather than replacing an existing file. Removal ignores only `os.ErrNotExist`.
- Later phases register ACP, managed-process, and database cleanup functions in the same shutdown registry. This phase must not predeclare those implementations.

## Scope Boundaries

**Included:** module/repository files, environment parsing, shared child-environment sanitization, private dispatch seam, extensible HTTP server/router, root/health handlers, custom 404/405 problems, auth middleware, JSON/body-limit utilities, request logging, PID-file lifecycle, signal handling, staged lifecycle registries, Makefile, README skeleton, and project `AGENTS.md`.

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
	AllowInsecureRemote bool
	PIDFile           string
	ACPRequestTimeout time.Duration
	IdleTTL           time.Duration
	Agents            map[string]AgentCommand
}

func Load(getenv func(string) string) (Config, error)
func (c Config) Address() string
```

`Load` owns defaults and validation for all startup variables already defined by the master contract, including agent binary/JSON-array argument overrides and the remote-auth guard. It must copy argument slices and return errors naming the invalid environment variable without exposing secret values.

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

`Server` owns the Phase 01 `http.ServeMux` and consumes already-constructed dependencies; it must not construct stores, runtimes, reapers, or other services. `Handler` returns the stable fully wrapped handler. Phase 06 owns the final `Dependencies` shape and route composition after parallel Phases 02-05. `Problem.Ext` is flattened into top-level JSON members, but extension keys may not replace `type`, `title`, `status`, or `detail`. `DecodeJSON` is the sole foundation for bounded JSON routes: install `http.MaxBytesReader`, reject malformed/trailing JSON, map `*http.MaxBytesError` to 413, and emit problems itself. The `application/problem+json` guarantee applies to requests that reach bridge handlers or middleware; malformed request lines, invalid headers, header-size failures, and other `net/http` transport/parser rejections are explicit exceptions.

### `internal/childenv`

```go
func Sanitized(environ []string) []string
```

`Sanitized` returns a fresh environment slice with every occurrence of `AGENT_BRIDGE_TOKEN`, `AGENT_BRIDGE_PID_FILE`, `AGENT_BRIDGE_INTERNAL_MOCK_AGENT`, and `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE` removed. It splits entries only at the first `=`, preserves order and all unrelated values, and is the only child-environment sanitizer used by ACP and managed-process phases.

### `internal/lifecycle`

```go
type Cleanup func(context.Context) error

type Registry struct { /* private state */ }

func (r *Registry) Add(name string, cleanup Cleanup) error
func (r *Registry) Shutdown(ctx context.Context) error
```

Registration is rejected after shutdown starts. `Shutdown` invokes every registered hook once in reverse registration order, continues after errors, and returns `errors.Join` output. The implementation must be race-safe and idempotent. `internal/app` uses separate instances for pre-drain hooks and post-drain cleanup; a single registry must not obscure the stage boundary.

### `internal/app`

```go
type IO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func Run(ctx context.Context, getenv func(string) string, io IO) error
```

`Run` checks private mock dispatch first, then loads configuration and constructs all services in `internal/app`, starts the listener, writes the PID file, serves HTTP, and coordinates a maximum ten-second graceful shutdown. On cancellation it uses one absolute deadline, independent of the already-canceled run context: close the listener to stop acceptance; invoke pre-drain hooks that close SSE streams, stop reapers, and signal-and-wait termination of ACP/managed process groups, including process reap and pump completion; call `http.Server.Shutdown` so now-unblocked active handlers can finish; invoke post-drain hooks that idempotently confirm process/pump completion, wait any non-process commit work, and checkpoint/close the database; finally remove the PID file. Every stage receives a fresh context with the same remaining absolute deadline, never a new ten-second budget. Cleanup errors are logged, but cancellation-driven shutdown returns nil after bounded best effort so SIGINT/SIGTERM exits 0. Phase 01 implements and tests the staging seam; Phase 06 finalizes hook ownership for integrated services. Private helpers include `runInternalMockAgent(context.Context, IO) error`, `writePIDFile(string) error`, and `removePIDFile(string) error`.

## Test-First Evidence Policy

- RED must be a failing behavioral assertion against the smallest compilable seam, fake, injected clock, or injected limit feasible for the task. A missing file, package, or symbol is useful setup evidence but is not sufficient RED evidence by itself.
- Time and size behavior must be tested with injected clocks/deadlines and small configurable limits. Test an actual maximum boundary once where conversion or protocol compatibility requires it; do not repeatedly allocate huge payloads or wait for production-duration timers.
- GREEN retains the focused behavioral command and assertion output, not merely successful compilation.

## Ordered Atomic Tasks

### Task 1.1: Establish Module, Entrypoint, and Baseline Checks

**Description:** Create the Go module, minimal command entrypoint, package layout, and reproducible developer commands without adding dependencies.

**Files:** `go.mod`, `.tool-versions`, `cmd/agent-bridge/main.go`, `Makefile`

**Symbols:** `main`, initial `internal/app.Run` call site (allowed not to compile during RED only)

**References:** Master platform constraints; target `internal/app.Run` interface above.

**Strict test-first steps:**

- [ ] Add `cmd/agent-bridge/main_test.go` asserting the command package is buildable and that cancellation is translated to a clean return through an injected/context-driven app path once implemented.
- [ ] Add the smallest compilable injected app-run seam, then **RED evidence:** run `go test ./cmd/agent-bridge` and retain the failing cancellation/exit assertion. Any earlier missing-`internal/app` compilation failure is setup evidence only.
- [ ] Create `go.mod` with `go 1.26.7` (exact latest patch) and a matching `toolchain go1.26.7` directive, the module path above, and no `require` entries; add a repository-root `.tool-versions` pinning `golang 1.26.7`; add `main.go` using `signal.NotifyContext` for `os.Interrupt` and `syscall.SIGTERM`, invoking `app.Run` once and exiting nonzero only for a returned startup/runtime error. Signal-triggered cleanup errors are logged by `app.Run` but return cleanly so signals exit 0. The patch is pinned deliberately because Go 1.26.1-1.26.7 carry security fixes; CI resolves the toolchain via `go-version-file: go.mod`. The `go` directive is a floor, not a selector, and toolchain switching never downgrades: Go 1.27 (released alongside 1.26.7) would silently change `encoding/json` internals, so `.tool-versions` + the README must document that local builds use exactly 1.26.7.
- [ ] Add `Makefile` targets `test`, `lint`, `build`, `vuln`, and `check`; use `CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge`, `go test ./...`, `go vet ./...`, and pinned dev-time tools invoked only through `go run ...@version` so no module dependency is added: `go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 ./...` for `lint` and `go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...` for `vuln`. `check` starts with the exact-version guard `test "$$(go env GOVERSION)" = go1.26.7`, then composes `test`, `lint`, `build`, a `gofmt -l .` emptiness assertion, and a `go mod tidy` cleanliness assertion (`go mod tidy && git diff --exit-code -- go.mod go.sum`), without other external tools. Once the e2e suite exists (Phase 06), `lint` and `vuln` add `-tags=e2e`; every Go file in a tag-gated package, including tests, must carry that package tag so plain `go test ./...` remains valid.
- [ ] **GREEN evidence:** Run `go test ./cmd/agent-bridge` and record `ok`; run `CGO_ENABLED=0 go build ./cmd/agent-bridge` and record exit status 0.

**Verification:** `make check`

**Risk:** Low

**Reversibility:** Easy to revert; all files are new.

**Deliverable:** A standard-library-only Go 1.26.7-pinned module with one command and deterministic hygiene/check/build commands.

### Task 1.2: Parse and Validate Environment Configuration

**Description:** Implement all master-contract startup defaults and overrides now so later phases consume one validated `Config` rather than reading environment variables ad hoc.

**Files:** `internal/config/config.go`, `internal/config/config_test.go`

**Symbols:** `config.Config`, `config.AgentCommand`, `config.Load`, `Config.Address`; private `parseMilliseconds`, `parseAgentArgs`

**References:** Master sections **ACP HTTP contract**, **ACP lifecycle and persistence**, **Agent resolution**, and **Environment, shutdown, and image**; the interface above.

**Strict test-first steps:**

- [ ] Write table-driven tests for empty-environment defaults: host `127.0.0.1`, port `2468`, level `info`, DB `./agent-bridge.db`, request timeout `120000ms`, idle TTL `900000ms`, empty token/PID file, disabled insecure-remote override, and the exact three default agent commands.
- [ ] Add tests for every override, JSON argument arrays (including spaces and empty strings), defensive argument-slice ownership, idle TTL `0`, normalized log levels, loopback hosts without a token, and authenticated non-loopback hosts.
- [ ] Add rejection tests for malformed/out-of-range ports; request timeout `0`, negative, over `1h`, non-integer, or conversion overflow; idle TTL negative, over `30d`, non-integer, or conversion overflow; unsupported log levels; non-array/non-string/malformed agent args; invalid insecure-remote flags; and empty-token non-loopback hosts without the override. Assert errors name the responsible variable without containing token values.
- [ ] Add explicit safety tests that `0.0.0.0`, `::`, and unknown hostnames require a token, `127.0.0.1`, `::1`, and case-insensitive `localhost` do not, and only `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1` permits the unsafe case.
- [ ] Add a minimal `Load` seam returning unchecked/default values, then **RED evidence:** run `go test ./internal/config -run TestLoad` and retain behavioral failures for bounds and remote-auth policy. Undefined symbols are setup evidence only.
- [ ] Implement parsing with `strconv`, `encoding/json`, checked integer bounds before `time.Duration(ms)*time.Millisecond`, `net.IP.IsLoopback`, `net.JoinHostPort`, and `log/slog`; do not read `os.Getenv` inside this package.
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

- [ ] Test removal of all occurrences of the four bridge-only variables, including duplicate keys, empty values, entries without `=`, and values containing additional `=` characters.
- [ ] Test that credential-shaped and unrelated variables remain in original order and that mutating either input or output after the call cannot mutate the other slice.
- [ ] Add a minimal pass-through `Sanitized` seam, then **RED evidence:** run `go test ./internal/childenv` and retain failures showing bridge-only variables remain or slice ownership is violated. Missing-symbol failure is setup evidence only.
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
- [ ] Add minimal compilable response/decoder seams, then **RED evidence:** run `go test ./internal/httpapi -run 'Test(WriteProblem|DecodeJSON)'` and retain failing status/content-type/body-limit assertions. Missing-symbol failures are setup evidence only.
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

- [ ] Test unset-token pass-through; configured-token success; missing, wrong, malformed, empty, duplicate, wrong-scheme, shorter, and longer Authorization values; and public root bypass. Assert authentication hashes both values with SHA-256 before constant-time comparison rather than comparing variable-length tokens directly.
- [ ] Assert failures are 401 RFC 9457 responses and do not reflect either supplied or configured token. Assert ACP-looking JSON-RPC bodies do not alter auth failure semantics.
- [ ] Add a minimal pass-through middleware seam, then **RED evidence:** run `go test ./internal/httpapi -run TestAuthenticate` and retain failing unauthorized-request assertions. Missing-symbol failure is setup evidence only.
- [ ] Implement exact `Bearer <token>` parsing, hash expected and supplied values with `sha256.Sum256`, and pass the equal-length digests to `crypto/subtle.ConstantTimeCompare`; avoid logging or placing Authorization in context.
- [ ] **GREEN evidence:** Re-run the focused test and record all auth cases passing.

**Verification:** `go test ./internal/httpapi -run TestAuthenticate -count=1`

**Risk:** High within this phase because a bypass exposes every future API.

**Reversibility:** Easy to revert mechanically; security behavior must not be removed after release.

**Deliverable:** A route-prefix-aware, constant-time authentication layer.

### Task 1.6: Build Extensible HTTP Server, Root/Health Routes, and Custom 404/405 Handling

**Description:** Construct the foundation `httpapi.Server` around the Go 1.26 `ServeMux`, explicit method/path patterns, composed middleware, and custom problem fallbacks. It consumes dependencies supplied by `internal/app`; Phase 06 composes the final routes from Phases 02-05 rather than allowing `httpapi` to construct services.

**Files:** `internal/httpapi/server.go`, `internal/httpapi/server_test.go`

**Symbols:** `httpapi.Dependencies`, `httpapi.Server`, `httpapi.NewServer`, `Server.Handler`; private `registerRoutes`, `root`, `health`, `notFound`, `methodNotAllowed`

**References:** Master sections **Authentication and errors** and **Non-ACP endpoints**. The docs URL value may be a stable repository README URL until Phase 06 finalizes public documentation.

**Strict test-first steps:**

- [ ] Add black-box table tests for `GET /`, `GET /v1/health`, an unknown path, and wrong methods on both known paths.
- [ ] Assert exact success JSON shapes, root's public behavior with auth enabled, health's required auth, `Allow: GET` on 405, and RFC 9457 bodies/content types for 404/405.
- [ ] Add path tests proving `/v1/health/extra` and `/unknown` are 404 rather than accidentally matched by a subtree handler.
- [ ] Test `NewServer(...).Handler()` as the only public HTTP assembly path and assert repeated `Handler` calls return the same composed handler.
- [ ] Add the smallest compilable server returning an empty handler, then **RED evidence:** run `go test ./internal/httpapi -run TestServer` and retain failing route/auth/problem assertions. Missing-symbol failure is setup evidence only.
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
- [ ] Add a minimal pass-through logger seam, then **RED evidence:** run `go test ./internal/httpapi -run TestRequestLogger` and retain failures for status/latency fields or secret absence. Missing-symbol failure is setup evidence only.
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

- [ ] Test reverse-order execution, exactly-once behavior under concurrent `Shutdown` calls, continued cleanup after an error, joined error identity, context propagation, and rejection of late registration. Use channels/fakes rather than real elapsed-time waits.
- [ ] Run race-sensitive test iterations to expose duplicate callbacks or map/slice races.
- [ ] Add a minimal compilable registry that does not yet enforce ordering/idempotence, then **RED evidence:** run `go test -race ./internal/lifecycle` and retain the behavioral ordering or duplicate-callback failure. Undefined-type failure is setup evidence only.
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
- [ ] Test a registered hook error is logged only after every hook in that stage runs; once cancellation starts shutdown, cleanup errors or deadline expiry do not change the clean return/exit-0 signal contract.
- [ ] With fakes/channels and an injected shutdown budget/deadline source, assert exact ordering: listener close; pre-drain closes streams/stops reapers and signal-and-wait termination completes process reap and pumps; `http.Server.Shutdown`; post-drain idempotently confirms completion, waits non-process commits, and checkpoints/closes DB; PID removal. Assert an open streaming handler is explicitly closed by pre-drain before `Shutdown` waits, and each stage sees the same absolute deadline with decreasing remaining time rather than a reset budget. Assert that when the injected budget expires during `Shutdown`, the `http.Server.Close` fallback force-closes remaining connections and post-drain still runs.
- [ ] Add a minimal compilable `Run` lifecycle seam, then **RED evidence:** run `go test ./internal/app` and retain failures for PID lifecycle, remote-auth startup rejection, or staged shutdown ordering. Undefined helpers are setup evidence only.
- [ ] Implement mode check before `config.Load`; create `slog.Logger`, `net.Listen`, app-owned services, `httpapi.NewServer(...).Handler()`, `http.Server`, and PID file. On cancellation create one ten-second absolute deadline from a non-canceled parent, close the listener, run pre-drain signal-and-wait process termination through pump completion, call `http.Server.Shutdown`, run post-drain idempotent confirmation/non-process commit/DB cleanup, and remove the PID file last. Pass contexts carrying the same deadline to every stage, continue best-effort after bounded errors, and normalize expected listener closure. Configure finite `ReadHeaderTimeout`, `IdleTimeout` (keep-alive connections otherwise linger with no read deadline), and no global write timeout that would break future SSE. If the absolute deadline expires while `Shutdown` is still draining, call `http.Server.Close` to force-close remaining connections, then proceed to post-drain.
- [ ] Update `main` to treat every cancellation-driven bounded shutdown as exit 0; log cleanup failures, and print only pre-shutdown startup/runtime errors to stderr without dumping configuration or token values.
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
- [ ] Create minimal placeholder documents if absent, then **RED evidence:** run `go test ./internal/projectdocs` and retain failing assertions for missing required commands, environment safety, or repository rules. Missing files alone are setup evidence only.
- [ ] Write a README skeleton with purpose, current phase status, prerequisites (exactly Go 1.26.7, enforced by `make check`), `make check`, `make build`, environment table, auth/root/health examples, the non-loopback token requirement and unsafe override warning, and a pointer to the authoritative specification. Do not advertise unfinished APIs or the private mock agent.
- [ ] Write project `AGENTS.md` requiring master-contract precedence, numeric phase order, stdlib plus only `modernc.org/sqlite`, behavioral test-first evidence, injected clocks/limits, `CGO_ENABLED=0`, handler-level RFC 9457 errors, no public mock interface, no runtime installs, and no expansion into excluded features. Also require the pinned `go 1.26.7` toolchain, `gofmt`/`go mod tidy` cleanliness before every commit, and that `make lint` (staticcheck 2026.2.1) and `make vuln` (govulncheck v1.7.0) are green in CI, with dev-time tools run only through pinned `go run ...@version` so `go.mod`/`go.sum` stay dependency-free.
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

- An exactly Go 1.26.7 module with no external dependencies and a `CGO_ENABLED=0` build.
- A validated environment configuration contract ready for later phases.
- Refusal to expose an unauthenticated non-loopback listener unless the explicit unsafe override is set.
- A shared `internal/childenv.Sanitized` helper for ACP and managed process children.
- A private mock dispatch seam that executes before server startup, with mock behavior explicitly deferred to Phase 02.
- An extensible `internal/httpapi.Server` with public root and authenticated health endpoints using `net/http` method/path patterns.
- RFC 9457 responses for requests reaching bridge handlers and middleware, including custom 404/405 and bounded-body failures; transport parser/header errors remain owned by `net/http`.
- Secret-safe structured request logging.
- PID-file and staged pre-drain/post-drain registry support with SIGINT/SIGTERM shutdown sharing one ten-second budget.
- Minimal repository commands and documentation.

## Completion Criteria

- [ ] Every task has retained RED output demonstrating a behavioral assertion failed against a minimal compilable seam; missing-file/missing-symbol output is not the sole RED evidence.
- [ ] Every task has retained GREEN output demonstrating its focused verification passed after implementation.
- [ ] `go mod edit -json` shows Go 1.26.7 and no `require` entries; `go.mod` carries `toolchain go1.26.7`, `.tool-versions` pins `golang 1.26.7`, `make check` rejects any other `go env GOVERSION`, and README requires exactly Go 1.26.7.
- [ ] `gofmt -l .` is empty and `go mod tidy` produces no `go.mod`/`go.sum` diff.
- [ ] `go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 ./...` reports no findings.
- [ ] `go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...` reports no findings (network required; owned by CI).
- [ ] `go test -race ./...` passes.
- [ ] `go vet ./...` passes.
- [ ] `CGO_ENABLED=0 go build -trimpath -o bin/agent-bridge ./cmd/agent-bridge` succeeds and `file bin/agent-bridge` reports no dynamic interpreter/shared-library dependency on Linux.
- [ ] `internal/childenv` tests prove all bridge-only variables are removed while unrelated credentials and ordering are preserved.
- [ ] All HTTP construction goes through `httpapi.NewServer(...).Handler()`; no standalone router constructor exists.
- [ ] With no token, `GET /v1/health` returns 200; with a token, it returns 401 without a valid bearer token and 200 with one; `GET /` remains public.
- [ ] Empty-token startup succeeds only for loopback hosts unless `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1`; non-loopback authenticated startup succeeds, and the unsafe override is removed from child environments.
- [ ] Unknown routes and wrong methods return `application/problem+json`, with wrong methods carrying the correct `Allow` header.
- [ ] SIGINT and SIGTERM each stop acceptance; complete pre-drain signal-and-wait process/pump termination before waiting on handlers; use post-drain only for idempotent completion confirmation, non-process commits, and DB cleanup; remove the PID file last; and exit 0 within one shared ten-second deadline.
- [ ] No ACP endpoint, SQLite code, subprocess runtime, or non-foundation domain API was introduced.

## Final Verification Commands

```sh
test "$(go env GOVERSION)" = go1.26.7
gofmt -l .
go mod tidy && git diff --exit-code -- go.mod go.sum
go vet ./...
go test -race ./...
go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 ./...
make build
file bin/agent-bridge
```

## Open Questions

- None for implementation. The orchestrator/token and persistence-lifetime questions remain owned by the authoritative specification and do not block this phase.

# Plan: agent-bridge (Go port of sandbox-agent server core)

**Date:** 2026-08-15
**Status:** DRAFT
**Risk Level:** Medium

---

## Overview

Re-implement the sandbox-agent Rust server core in Go: an HTTP server that runs inside a sandbox and bridges ACP (Agent Client Protocol) JSON-RPC between a remote client (HTTP/SSE) and coding-agent subprocesses (stdio). Plus run-command, filesystem, and config APIs. Agents are **pre-provisioned in Docker images**, never installed at runtime.

## Goal

A single static Go binary (`agent-bridge`) that:
1. Tunnels raw ACP JSON-RPC envelopes between `POST/GET/DELETE /v1/acp/{serverId}` and agent subprocesses (Claude Code, Codex, OpenCode via their ACP adapters) — pure passthrough, same contract as the Rust server.
2. Provides `/v1/processes/*` (run/start/stop commands), `/v1/fs/*` (files, upload-batch seeding), `/v1/config/{mcp,skills}` (per-project config).
3. Resolves agent binaries from PATH + env overrides. **No download/install code exists in the server.**
4. Ships pinned-agent Docker images (agent + ACP adapter versions pinned at build time).

## Requirements (constraints + acceptance criteria)

- **Dependencies: Go stdlib only.** No external Go modules. Justification: ACP is a raw JSON-RPC passthrough (no SDK needed — mirrors the Rust server which uses zero ACP crates), SSE is ~30 lines with `net/http`, no PTY/WebSocket in scope (client chose pipes-only processes). If PTY/terminal WS is needed later, add `creack/pty` + `coder/websocket` then — extension point documented, not built now.
- **Go 1.26** (toolchain floor = what's installed locally). `net/http` ServeMux with method+path patterns (Go 1.22+).
- **No runtime agent installation.** Any attempt to auto-download/npm-install an agent is out of scope and must not exist in code.
- **Auth:** optional global bearer token via `AGENT_BRIDGE_TOKEN`. When set, ALL `/v1/*` routes (including health) require `Authorization: Bearer <token>`, else 401 problem+json. When unset, no auth.
- **Error format:** `application/problem+json` (RFC 7807): `{type, title, status, detail, extensions?}` for all non-2xx errors.
- **ACP contract parity with sandbox-agent** (client-side compatibility is the point):
  - `POST /v1/acp/{serverId}`: `Content-Type: application/json` required (415 otherwise); `Accept: application/json` required (406 otherwise). Query param `agent` required on FIRST POST (400 if missing, 409 if conflicting with existing server's agent). Body is a raw JSON-RPC envelope: request (method+id) → 200 with the response envelope; notification (no id) → 202 empty. 404 unknown serverId. 504 on response timeout.
  - `GET /v1/acp/{serverId}`: `Accept: text/event-stream` required (406 otherwise). SSE: `event: message`, `id: <monotonic seq>`, `data: <JSON-RPC envelope>`. Replays buffered envelopes after `Last-Event-ID` (if present), then live stream. Keepalive `: heartbeat` every 15s.
  - `DELETE /v1/acp/{serverId}`: 204, kills the agent subprocess.
  - Synthetic notifications (adapter-level, mirror Rust): `_adapter/agent_exited` on subprocess exit, `_adapter/invalid_stdout` on unparseable stdout line. These are the client's turn-end/diagnostic signals.
  - Per-agent Pi-style payload normalization is OUT of scope (Pi not supported).
- **Agents supported:** `claude`, `codex`, `opencode`. Resolution (PATH lookup via `exec.LookPath`, env override wins):
  - claude → bin `claude-agent-acp` (override `AGENT_BRIDGE_CLAUDE_BIN`, `AGENT_BRIDGE_CLAUDE_ARGS`)
  - codex → bin `codex-acp` (override `AGENT_BRIDGE_CODEX_BIN`, `AGENT_BRIDGE_CODEX_ARGS`)
  - opencode → bin `opencode` args `["acp"]` (override `AGENT_BRIDGE_OPENCODE_BIN`, `AGENT_BRIDGE_OPENCODE_ARGS`)
  - `mock` → built-in Go mock agent (test support only, mirrors Rust's mock; enabled when `agent=mock`). Hidden from docs.
- **One subprocess per serverId.** Sessions inside an agent are multiplexed by the adapter itself (ACP `session/new`); the server never inspects session semantics.
- **Request timeout:** default 120s, env `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` (mirrors `SANDBOX_AGENT_ACP_REQUEST_TIMEOUT_MS`).
- **Ring buffer:** last 1024 envelopes per server, monotonic sequence.
- **Server env:** `AGENT_BRIDGE_HOST` (default 127.0.0.1), `AGENT_BRIDGE_PORT` (default 2468), `AGENT_BRIDGE_LOG_LEVEL` (default info, `log/slog`).
- **Graceful shutdown:** SIGINT/SIGTERM → kill all agent subprocesses, drain and close.
- **Out of scope (explicit):** PTY/terminal WS, desktop API, agent install/list endpoints, OpenCode-compat `/opencode` surface, Inspector UI, telemetry, daemon mode, CLI subcommands.

### Endpoint contract (non-ACP, mirrors sandbox-agent)

| Endpoint | Behavior |
|---|---|
| `GET /v1/health` | 200 `{"status":"ok"}` |
| `GET /` | 200 `{"name":"agent-bridge","docs":"..."}` |
| `POST /v1/processes` | `{command, args[], cwd?, env{}}` → 200 snapshot `{id, command, args, cwd, status:"running", pid, createdAtMs}` |
| `GET /v1/processes` | 200 `{processes:[snapshot]}` sorted by id |
| `GET /v1/processes/{id}` | 200 snapshot / 404 |
| `POST /v1/processes/{id}/stop` | SIGTERM, wait ≤2s → snapshot |
| `POST /v1/processes/{id}/kill` | SIGKILL, wait ≤1s → snapshot |
| `DELETE /v1/processes/{id}` | 204; 409 if still running |
| `GET /v1/processes/{id}/logs?stream=stdout\|stderr\|combined&tail=N&since=seq` | 200 `{entries:[{sequence, stream, timestampMs, data(base64), encoding:"base64"}]}` |
| `POST /v1/processes/{id}/input` | `{data, encoding: base64\|utf8}` → 200 `{bytesWritten}`; 409 if exited |
| `POST /v1/processes/run` | `{command, args, cwd?, env{}, timeoutMs?, maxOutputBytes?}` → 200 `{exitCode, timedOut, stdout, stderr, stdoutTruncated, stderrTruncated, durationMs}` |
| `GET/POST /v1/processes/config` | runtime limits: maxConcurrentProcesses=64, defaultRunTimeoutMs=30000, maxRunTimeoutMs=300000, maxOutputBytes=1MiB, maxLogBytesPerProcess=10MiB, maxInputBytesPerRequest=64KiB |
| `GET /v1/fs/entries?directory=&type=all\|file\|dir` | 200 `{entries:[{name, path, type, size, modifiedMs}]}` sorted |
| `GET /v1/fs/file?path=` | 200 raw bytes (Content-Type octet-stream) / 404 |
| `PUT /v1/fs/file?path=` | raw body → 200 `{path, size}` |
| `DELETE /v1/fs/entry?path=` | 204 (recursive for dirs) / 404 |
| `POST /v1/fs/mkdir` | `{directory, name}` → 200 `{path}` |
| `POST /v1/fs/move` | `{source, destination}` → 200 `{path}` |
| `GET /v1/fs/stat?path=` | 200 `{path, type, size, modifiedMs, mode}` / 404 |
| `POST /v1/fs/upload-batch?directory=` | body = tar.gz, extract into directory → 200 `{files:[{path, size}]}` (safe extraction: no absolute paths, no `..` escapes) |
| `GET/PUT/DELETE /v1/config/mcp?directory=` | JSON map `{name: {command, args, env}}` stored at `{directory}/.agent-bridge/config/mcp.json` |
| `GET/PUT/DELETE /v1/config/skills?directory=` | JSON map stored at `{directory}/.agent-bridge/config/skills.json` |

Processes run with piped stdio (no TTY). Logs are base64 lines with monotonic sequence, capped by `maxLogBytesPerProcess`.

### Pinned versions (Phase 5 image defaults, build ARGs — bump by changing ARG, no code change)

| Artifact | Source | Pin (verified 2026-08-15) |
|---|---|---|
| claude ACP adapter | npm `@agentclientprotocol/claude-agent-acp` | 0.68.0 |
| codex ACP adapter | npm `@agentclientprotocol/codex-acp` | 1.3.0 |
| opencode | npm `opencode-ai` | 1.18.18 |
| node (runtime for npm adapters) | node:24-slim (Debian glibc) | — |

### Verified facts (2026-08-15, checked against npm registry, GitHub sources, ACP spec, agentclientprotocol registry)

**Adapter/agent bundling (build image implications):**
- `claude-agent-acp` bundles the native Claude CLI via its dependency `@anthropic-ai/claude-agent-sdk` platform optionalDependencies (~320MB). **No `claude` binary install needed.** `engines.node: >=22` (node 24 OK). Override: `CLAUDE_CODE_EXECUTABLE`.
- `codex-acp` bundles the native codex binary via `@openai/codex` optionalDependencies (~315MB, musl-static, runs on glibc+Alpine). **No `codex` binary install needed.** No engines field. Override: `CODEX_PATH`.
- `opencode-ai` ships a stub bin replaced at **postinstall** by the native platform binary via optionalDependencies (linux-x64/arm64, glibc or musl; ~184MB). `opencode acp` is a native stdio JSON-RPC subcommand; no HTTP server needed. No engines field.
- **Install rules: never use `--omit=optional` or `--ignore-scripts`** — either breaks the native binaries. None of the packages have install/postinstall build-tool needs (beyond opencode's stub replacement, which needs node+npm on PATH — present in node:24-slim).
- No extra apt packages required by any adapter. Expected image size ~1GB+ (node base + ~640MB adapter binaries + ~184MB opencode) — acceptable for a sandbox provisioning image, noted here so it's not mistaken for bloat later.
- Runtime hardening envs (headless sandbox): `DISABLE_AUTOUPDATER=1` (claude), `NO_BROWSER=1` (codex).

**Keyless behavior (drives e2e strictness):**

| Agent | `initialize` without keys | `session/new` without keys |
|---|---|---|
| claude (via claude-agent-acp) | Always succeeds (pure handshake, offline) | Fails (auth required) |
| codex (via codex-acp) | Always succeeds | Fails with `-32000` auth-required envelope (unless `DEFAULT_AUTH_REQUEST` set) |
| opencode (`opencode acp`) | Always succeeds (static response, zero credential checks) | Succeeds; auth surfaces at prompt time |

- Codex api-key envs: `CODEX_API_KEY` (precedence) or `OPENAI_API_KEY`. Claude: `ANTHROPIC_API_KEY` (or terminal/gateway auth). OpenCode: provider keys or `opencode auth login`.

**ACP handshake essentials (stable v1, for tests + mock agent correctness):**
- `initialize` request MUST carry `protocolVersion` as an **integer** (`1`); agent MUST echo a supported version. `clientCapabilities` all optional; omitted = unsupported.
- `initialize` response schema-required fields: `protocolVersion`, `agentCapabilities`, `authMethods` (`[]` = no auth surface; client MUST NOT call `authenticate` then). **There is no `instructions` field** (MCP concept).
- `session/new` minimal params: `{cwd: "<absolute path>", mcpServers: []}`.
- `session/prompt` minimal: single `{"type":"text","text":...}` block; baseline all agents support Text.
- **Turn completion = the prompt RESPONSE envelope** with `result.stopReason` ∈ {`end_turn`, `max_tokens`, `max_turn_requests`, `refusal`, `cancelled`} — NOT a notification. It arrives after all `session/update` notifications for the turn.
- Stable v1 `sessionUpdate` subtypes: `user_message_chunk`, `agent_message_chunk`, `agent_thought_chunk`, `tool_call`, `tool_call_update`, `plan`, `available_commands_update`, `current_mode_update`, `session_info_update`, `usage_update`.
- stdio framing: newline-delimited JSON, one message per line, no embedded newlines; stderr = logs; stdout = ACP messages only.
- Registry confirms our launch specs: claude-acp/codex-acp need no args; opencode needs arg `acp`.

### Reference implementation (Rust sandbox-agent) — source of truth for wire contract parity

The client-facing **wire contract** must match the Rust server. Repo: `/home/viethoangcr/Workspace/github/rivet/sandbox-agent` (package `sandbox-agent`). The Rust server keeps all state **in memory only** (ring buffer + instance map) and has **no sqlite / no persisted session state** — this plan deliberately diverges internally (see `#DB-EVENTS`, `#STATE`) while preserving the wire contract.

| Plan task | Rust file (sandbox-agent) | Notes |
|---|---|---|
| 1.2 Adapter runtime | `server/packages/acp-http-adapter/src/process.rs` | `AdapterRuntime` (spawn + stdio JSON-RPC loop); **in-memory ring buffer `RING_BUFFER_SIZE=1024` (line 18) — replaced by sqlite `#DB-EVENTS`**; broadcast channel 512 (line 118); pending-request matched by id (lines 400–437); SSE stream + `Last-Event-ID` replay (`subscribe()`/`sse_stream()`, lines 257–304); stderr tail `STDERR_TAIL_SIZE=16` (line 19); `_adapter/invalid_stdout` (lines 376–398); `_adapter/agent_exited` exit-watcher (lines 507–565); `id_key` (line 623) |
| 1.3 Proxy manager | `server/packages/sandbox-agent/src/acp_proxy_runtime.rs` | `AcpProxyRuntime` (**in-memory instance map** `HashMap<String, Arc<ProxyInstance>>` line 29 — replaced by persisted state `#STATE`); get-or-create + per-server mutex (lines 220–276); 409 agent conflict (lines 225–237); missing-agent 400 (line 262); `delete` kill+remove (lines 186–192); `shutdown_all` (line 194); `list_instances` (line 86); agent resolution (line 300); error mapping + `agentStderr` (lines 478–603) |
| 1.4 ACP handlers + SSE | `server/packages/sandbox-agent/src/router.rs` | `get_v1_acp_servers` (line 3115); `post_v1_acp` (line 3153, content-type/accept 3160–3171, 200/202); `get_v1_acp` SSE (line 3213, keep-alive 15s 3228–3232); `delete_v1_acp` (line 3246) |
| 1.4 / 3.1 validation | `server/packages/sandbox-agent/src/router/support.rs` | `parse_last_event_id` (line 582), `content_type_is` (line 529), `accept_allows` (line 539), `problem_from_sandbox_error` (line 601); FS path safety `resolve_fs_path`/`sanitize_relative_path` (lines 483/500) |
| 2.1 Process runtime | `server/packages/sandbox-agent/src/process_runtime.rs` | `ProcessConfig` maxConcurrentProcesses=64 (lines 110/121), `ManagedProcess` map (line 140); limits/validation |

---

## Phase 0: Scaffolding

### Task 0.1: Repo init

**Description:** Create project skeleton: `go.mod` (module `github.com/viethoangcr/agent-bridge`, `go 1.26`), `.gitignore`, `README.md` (scope + build), `Makefile` (build/test/vet), `AGENTS.md` (project conventions: stdlib-only rule, no runtime install rule, contract parity rule).
**Files:** `go.mod`, `.gitignore`, `README.md`, `Makefile`, `AGENTS.md`
**Test:** none (infra).
**Verify:** `go build ./...` succeeds; `make vet` passes.

- [ ] `go mod init github.com/viethoangcr/agent-bridge`
- [ ] Write Makefile: `build`, `test`, `vet` (go vet + gofmt check), `image` (Phase 5)
- [ ] Write AGENTS.md with the three project rules above

**Risk:** Low. **Reversibility:** Easy.

### Task 0.2: Server bootstrap + auth + problem+json

**Description:** `cmd/agent-bridge/main.go`: parse env config, build router, SIGINT/SIGTERM graceful shutdown (shutdown hook registry called on signal). `internal/server/server.go`: router assembly, auth middleware (constant-time compare), `internal/server/problem.go`: `ProblemDetails` type + `writeProblem(w, status, type, title, detail)`. `GET /v1/health`, `GET /`.
**Files:** `cmd/agent-bridge/main.go`, `internal/server/server.go`, `internal/server/problem.go`, `internal/server/server_test.go`
**Test:** write first: health returns 200 ok; token set → no header = 401 problem+json, wrong token = 401, right token = 200; problem+json body shape matches contract.
**Verify:** `go test ./internal/server/ -v`

- [ ] Implement `internal/server/problem.go` (RFC 7807 struct, helper)
- [ ] Implement auth middleware + config struct (Host, Port, Token, LogLevel, AcpTimeout)
- [ ] Implement health + root handlers
- [ ] Implement graceful shutdown (signal → hook list, 10s force-exit)

**Risk:** Low. **Reversibility:** Easy.

---

## Phase 1: ACP bridge core

### Task 1.1: Mock ACP agent (test support)

**Description:** `internal/mockagent/mockagent.go`: reads newline-delimited JSON-RPC from stdin, replies on stdout. Behavior: `initialize` → `{"protocolVersion":1, "agentCapabilities":{"loadSession":false,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":false,"sse":false},"sessionCapabilities":{},"auth":{}}, "authMethods":[], "agentInfo":{"name":"mock-agent","version":"0.1.0"}}` (schema-required fields present, no `instructions` key); `session/new` → `{sessionId:"mock-1"}`; `session/prompt` → emits 2 `session/update` notifications (`agent_message_chunk`) then response `{stopReason:"end_turn"}` (turn completion is the response, mirroring real agents); `session/request_permission` reverse-call emitted when prompt params contain `"triggerPermission":true`, waits for client response on stdin, then completes prompt. Unknown request → error `-32601`. Exposed as agent id `mock` in the bridge (test-only path, same trick as Rust server).
**Files:** `internal/mockagent/mockagent.go`, `internal/mockagent/mockagent_test.go`
**Test:** write first: feed JSONL, assert response id-matching, notification ordering, permission round-trip.
**Verify:** `go test ./internal/mockagent/ -v`

- [ ] JSON-RPC loop: parse line, request vs notification dispatch
- [ ] initialize / session/new / session/prompt handlers
- [ ] permission reverse-call with pending response table

**Risk:** Low. **Reversibility:** Easy.

### Task 1.2: Adapter runtime (stdio JSON-RPC bridge)

**Description:** `internal/acp/runtime.go`: spawn agent subprocess with piped stdin/stdout/stderr. Loop reads stdout lines → parse JSON → if response (`id` set, no `method`) match pending request by id (stringified id as key) and deliver; broadcast ALL envelopes (responses included, in order) to ring buffer (1024) + channel fan-out. stderr tail (last 16 lines). Exit watcher → broadcast `_adapter/agent_exited`. Unparseable line → `_adapter/invalid_stdout`. `Post(ctx, envelope)`: request → write line, await matching response or timeout; notification → write, return accepted. `Stream(lastSeq)` → replay + live channel. `Shutdown()` → kill child, wait.
**Files:** `internal/acp/runtime.go`, `internal/acp/runtime_test.go`
**Test:** write first using mock agent subprocess: request/response match; notification broadcast order (notification before response); replay from lastSeq; timeout (5ms timeout, mock sleeps); agent_exited emitted on exit; stderr tail captured; shutdown kills child.
**Verify:** `go test ./internal/acp/ -v`

- [ ] Spawn + pipe setup, env passthrough
- [ ] stdout loop: parse, id-match, ring + broadcast
- [ ] stderr tail + exit watcher
- [ ] Post (request/notification paths), Stream, Shutdown

**Risk:** Medium (core concurrency). **Reversibility:** Needs backup (foundational).

### Task 1.3: Proxy manager (instances + agent resolution)

**Description:** `internal/acp/proxy.go`: `Proxy` holds `map[serverId]*Instance` (RWMutex) + per-server create locks. `Post(serverId, bootstrapAgent, envelope)`: get-or-create instance (requires agent on first POST; 409 on mismatch); resolve launch spec (env override → LookPath → error if missing: "agent process not found for {agent}: binary '{bin}' not on PATH"); spawn runtime; forward. `Stream`, `Delete` (kill + remove), `ShutdownAll`. Env var parsing for overrides.
**Files:** `internal/acp/proxy.go`, `internal/acp/resolve.go`, `internal/acp/proxy_test.go`
**Test:** write first with mock agent (env override pointing at mock binary): create-on-first-post, reuse, 409 conflict, missing-binary error message contains agent name, delete kills, list not needed (no agents endpoint).
**Verify:** `go test ./internal/acp/ -v`

- [ ] `resolve.go`: LaunchSpec per agent id (defaults + env overrides + LookPath)
- [ ] `proxy.go`: instance map, per-server mutex, get-or-create, delete, shutdown-all

**Risk:** Medium. **Reversibility:** Needs backup.

### Task 1.4: HTTP handlers (POST/GET/DELETE /v1/acp/{serverId})

**Description:** `internal/server/acp_handlers.go`: content-type/accept validation (415/406), body read (cap 10MiB), parse JSON (400 on bad), forward via Proxy, map outcomes: response → 200 JSON; accepted → 202; timeout → 504 problem+json; exited → 502 problem+json with stderr tail in detail. GET → SSE writer (`text/event-stream`, flush after each event, `event: message` + `id:` + `data:`, heartbeat every 15s, close on client disconnect / server shutdown). DELETE → 204. Wire into router at `/v1/acp/{serverId}` + `/v1/acp` list (server ids — cheap, mirror Rust `GET /v1/acp`).
**Files:** `internal/server/acp_handlers.go`, `internal/server/acp_handlers_test.go`
**Test:** write first with mock agent + `httptest.Server`: full initialize→session/new→prompt flow over HTTP; notification via SSE received in order with correct ids; replay with `Last-Event-ID: N` returns only newer; 415/406/400/404/409/504 cases; SSE keeps connection open ≥ heartbeat interval.
**Verify:** `go test ./internal/server/ -v -run Acp`

- [ ] POST handler + validation
- [ ] SSE handler (replay + live + heartbeat + cancellation)
- [ ] DELETE handler + `GET /v1/acp` list

**Risk:** Medium. **Reversibility:** Needs backup.

---

## Phase 2: Processes API

### Task 2.1: Process runtime core

**Description:** `internal/process/runtime.go`: managed processes (`proc_N` ids, atomic counter): start (piped stdio), status tracking (running/exited, exit code, pid, timestamps), stop (SIGTERM on pid via `os.FindProcess`+`Signal`, wait ≤2s), kill (SIGKILL ≤1s), delete (only exited), log ring (sequence, stream, base64 data, byte cap 10MiB, broadcast channel), input (stdin pipe, byte cap), config limits with validation. Output pump goroutines per stream (8KiB reads). One-shot `Run` (timeout via context, output caps, truncation flags, kill on timeout).
**Files:** `internal/process/runtime.go`, `internal/process/runtime_test.go`
**Test:** write first: `sh -c "echo hi; sleep 5"` → stop exits; `sh -c "cat"` input round-trip; logs sequence order stdout/stderr; log byte cap evicts oldest; kill works; delete blocked while running; one-shot run: exit code, timeout flag, output truncation, stdout/stderr split.
**Verify:** `go test ./internal/process/ -v`

- [ ] ManagedProcess: start/status/stop/kill/delete
- [ ] log ring + broadcast + caps
- [ ] input writer with cap
- [ ] RunSpec one-shot with timeout + truncation
- [ ] Config struct + validation

**Risk:** Medium. **Reversibility:** Needs backup.

### Task 2.2: Processes HTTP handlers

**Description:** `internal/server/process_handlers.go`: all `/v1/processes*` endpoints per contract table, JSON decode validation (400 on bad), problem+json on 404/409. Config GET/POST with validation errors.
**Files:** `internal/server/process_handlers.go`, `internal/server/process_handlers_test.go`
**Test:** write first: full CRUD over httptest, logs with tail/since filters, run with timeout, input base64, config validation (0 values rejected).
**Verify:** `go test ./internal/server/ -v -run Process`

- [ ] Handlers for create/list/get/stop/kill/delete/logs/input/run/config
- [ ] Decode + validate + problem mapping

**Risk:** Low. **Reversibility:** Easy.

---

## Phase 3: Filesystem API

### Task 3.1: FS service + handlers

**Description:** `internal/fs/service.go`: entries (sorted, type/size/modifiedMs), read (raw bytes), write (create parents), delete (recursive dirs), mkdir, move (rename, cross-device copy+delete fallback not needed — same FS only), stat. `internal/fs/upload_batch.go`: tar.gz extract with safety (reject absolute names, `..` traversal, symlink/hardlink entries → error; strip leading `./`; size cap 512MiB). Handlers wire to `/v1/fs/*`.
**Files:** `internal/fs/service.go`, `internal/fs/upload_batch.go`, `internal/fs/service_test.go`, `internal/fs/upload_batch_test.go`, `internal/server/fs_handlers.go`, `internal/server/fs_handlers_test.go`
**Test:** write first (t.TempDir): entries sort/type; read/write round-trip incl. nested dirs; delete dir recursive; move; stat fields; upload-batch: valid tar.gz extracts preserving structure, malicious tar (absolute path, `../`, symlink) rejected with error and no file outside target; handlers: 404 unknown path, GET returns exact bytes.
**Verify:** `go test ./internal/fs/ ./internal/server/ -v -run 'Fs|Upload'`

- [ ] `service.go` ops
- [ ] `upload_batch.go` safe extractor
- [ ] `fs_handlers.go` wiring

**Risk:** Medium (path traversal = security boundary). **Reversibility:** Needs backup.

---

## Phase 4: Config API (mcp/skills)

### Task 4.1: Config service + handlers

**Description:** `internal/config/service.go`: read/write/delete named JSON maps at `{directory}/.agent-bridge/config/{mcp,skills}.json` (create dirs; empty→404 on GET; invalid JSON→400). Handlers: `GET/PUT/DELETE /v1/config/mcp?directory=`, same for skills. No schema beyond JSON object.
**Files:** `internal/config/service.go`, `internal/config/service_test.go`, `internal/server/config_handlers.go`, `internal/server/config_handlers_test.go`
**Test:** write first: round-trip map, PUT creates file at expected path (assert `.agent-bridge/config/mcp.json` location), DELETE removes, GET missing → 404, invalid JSON → 400, directory traversal in `directory` param rejected.
**Verify:** `go test ./internal/config/ ./internal/server/ -v -run Config`

- [ ] `service.go` with path validation
- [ ] handlers

**Risk:** Low. **Reversibility:** Easy.

---

## Phase 5: Pinned-agent images

### Task 5.1: Runtime image

**Description:** `docker/runtime/Dockerfile`: base `node:24-slim`; ARGs for all pins (table above); `npm install -g` the two adapters + opencode at exact versions; COPY built `agent-bridge` binary; ENTRYPOINT `["agent-bridge"]`. `Makefile image` target: `go build` + `docker build --build-arg ...`. Default ARG values live in ONE file (`docker/runtime/versions.env` + `--build-arg` passthrough), so bumping versions = editing one file, zero Go changes. Verified install commands (do NOT add `--omit=optional`/`--ignore-scripts` — they strip the bundled native binaries; no native `claude`/`codex` CLI installs needed; no extra apt packages):
```dockerfile
RUN npm install -g @agentclientprotocol/claude-agent-acp@${CLAUDE_ADAPTER_VERSION} \
                 @agentclientprotocol/codex-acp@${CODEX_ADAPTER_VERSION} \
                 opencode-ai@${OPENCODE_VERSION}
ENV DISABLE_AUTOUPDATER=1 NO_BROWSER=1
```
**Files:** `docker/runtime/Dockerfile`, `docker/runtime/versions.env`, `Makefile`
**Test:** build the image; run `docker run --rm <img> sh -c 'which claude-agent-acp codex-acp opencode && claude-agent-acp --version && codex-acp --version && opencode --version'` → all three resolve + print versions.
**Verify:** `make image && docker run --rm agent-bridge:latest sh -c 'which claude-agent-acp codex-acp opencode && claude-agent-acp --version && codex-acp --version && opencode --version'`

- [ ] Dockerfile with ARG pins (base node:24-slim, exact verified install commands)
- [ ] versions.env + Makefile wiring
- [ ] ENTRYPOINT + image size documented in README (expect ~1GB)

**Risk:** Low. **Reversibility:** Easy.

### Task 5.2: Agent boot verification recipe

**Description:** `scripts/verify-agents.sh`: for each agent, spawn the adapter, send `initialize` envelope on stdin, assert a valid response with `protocolVersion` (numeric 1) comes back within 30s; print pass/fail. Run inside the image. Catches broken adapter installs / version skew at build time.
**Files:** `scripts/verify-agents.sh`
**Test:** run in the built image for all 3 agents.
**Verify:** `docker run --rm -i agent-bridge:latest sh scripts/verify-agents.sh` (all 3 PASS)

- [ ] initialize-handshake script (printf JSONL, read reply, jq-less assertion via grep)

**Risk:** Low. **Reversibility:** Easy.

---

## Phase 6: E2E + hardening

### Task 6.1: Docker e2e tests

**Description:** `tests/e2e/e2e_test.go` (build tag `e2e`): builds image (docker CLI via `os/exec`, tag cached in package-level var — no cross-process cache, mirrors Rust in-memory pattern), runs container with server (port map 2468, token env), waits for health, then per agent runs the verified keyless matrix:

| Agent | `initialize` | `session/new` | `session/prompt` |
|---|---|---|---|
| claude | strict: 200, `protocolVersion:1`, schema-required fields | accept auth-required error envelope as pass | lenient (accept error) |
| codex | strict (same) | accept `-32000`-style auth error as pass | lenient |
| opencode | strict | strict: `sessionId` returned | lenient (auth may surface here) |
| mock | strict | strict | strict: ≥1 `agent_message_chunk` notification BEFORE the response; response `stopReason:"end_turn"` |

Assertions: `initialize` request uses integer `protocolVersion:1`; prompt response envelope carries `result.stopReason` (turn completion is the response, not a notification); no `authenticate` call when `authMethods` is empty. Plus fs write/read + process run through the live server.
**Files:** `tests/e2e/e2e_test.go`, `tests/e2e/docker.go`
**Test:** n/a (this IS the test layer).
**Verify:** `make e2e` (requires docker daemon)

- [ ] docker helper: build (cached tag), run, wait-health, teardown
- [ ] ACP e2e per agent per keyless matrix
- [ ] mock agent full strict flow (notifications-before-response ordering)
- [ ] fs + process e2e against live server

**Risk:** Medium (external docker dependency). **Reversibility:** Easy.

### Task 6.2: Hardening

**Description:** request body cap (10MiB) with 413; per-request timeouts (server-level `http.Server` ReadHeaderTimeout); slog structured logs (method/uri/status/latency, redact Authorization); pid file for ops tooling (AGENT_BRIDGE_PID_FILE, written at start, removed on shutdown); agent stderr tail attached to 502 problem detail (mirror Rust `agentStderr`); integration test: SIGTERM → agent subprocess dead (verify via kill -0), server exits 0.
**Files:** `internal/server/middleware.go`, `internal/server/logging.go`, `internal/server/shutdown_test.go`, `cmd/agent-bridge/main.go`
**Test:** write first: 413 on oversize POST; shutdown test spawns server + mock agent, SIGTERM, asserts child gone.
**Verify:** `go test ./internal/server/ -v -run 'BodyCap|Shutdown'`

- [ ] body cap middleware
- [ ] request logging (slog, auth redaction)
- [ ] stderr-tail in 502 detail
- [ ] pid file + shutdown integration test

**Risk:** Low. **Reversibility:** Easy.

### Task 6.3: Docs

**Description:** README: build, run, env reference table, agent resolution rules, image pinning (how to bump versions), API summary, sandbox deployment sketch. Keep aligned with implemented endpoints only.
**Files:** `README.md`
**Test:** none.
**Verify:** `make vet && go test ./...` green; README env table matches `cmd/agent-bridge/main.go` env parsing (manual check).

- [ ] Full env var table + agent mapping
- [ ] Version-bump procedure for images
- [ ] API summary with curl examples

**Risk:** Low. **Reversibility:** Easy.

---

## Dependencies

| Task | Depends On |
|------|------------|
| 0.2 | 0.1 |
| 1.1 | 0.2 |
| 1.2 | 1.1 |
| 1.3 | 1.2 |
| 1.4 | 1.3 |
| 2.1 | 0.2 |
| 2.2 | 2.1 |
| 3.1 | 0.2 |
| 4.1 | 0.2 |
| 5.1 | 1.4 (needs working binary) |
| 5.2 | 5.1 |
| 6.1 | 5.1 |
| 6.2 | 1.4, 2.1 |
| 6.3 | 6.2 |

Phases 2/3/4 are independent of Phase 1 and of each other; can be built in parallel by different builders.

## Open Questions

- ~~Keyless behavior of real adapters in e2e~~ — RESOLVED (verified 2026-08-15): all three complete `initialize` keyless; opencode completes `session/new` keyless; claude/codex fail `session/new` with auth-required — encoded in Task 6.1 matrix.
- ~~Whether codex-acp requires the native codex CLI in the image~~ — RESOLVED: no, both adapters bundle native binaries via npm optionalDependencies (~640MB combined); install must keep optional deps.
- Terminal/PTY later: plan is pipes-only by client decision; extension point documented in README (add `creack/pty` + WS then).
- Full authenticated prompt e2e (real keys): needs sandbox credentials; deferred until CI has a test key vault — strict prompt flow currently covered by `mock` agent only.

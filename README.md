# agent-bridge

`agent-bridge` is a static Linux Go binary that bridges ACP JSON-RPC between
remote HTTP/SSE clients and pre-provisioned coding-agent subprocesses, and
exposes process, filesystem, and per-project config APIs.

> **Status: Phases 01-06 are complete.** The binary provides validated
> environment configuration, a public `GET /` root, an authenticated
> `GET /v1/health`, bearer-token authentication, structured request logging,
> PID-file plus graceful-shutdown lifecycle, internal SQLite persistence, the
> private stdio ACP runtime, the ACP HTTP/SSE API, the managed process/one-shot
> API, the filesystem API, atomic per-project MCP/skills config, and a
> digest-pinned multi-architecture runtime image with in-container E2E
> coverage.

The authoritative contract is
[`docs/plans/20260815-agent-bridge.md`](docs/plans/20260815-agent-bridge.md).
Where this README conflicts with that specification, the specification wins.

## Build

The host binary is built with exactly **Go 1.26.8**, `CGO_ENABLED=0`, and
`-trimpath` as one static Linux binary.

| Command | Purpose |
|---|---|
| `make check` | Exact-Go guard, tests, lint, static build, `gofmt` check, `go mod tidy` cleanliness |
| `make build` | `CGO_ENABLED=0` static binary at `bin/agent-bridge` |
| `make test` | `go test ./...` |
| `make lint` | `go vet ./...` plus staticcheck 2026.2.1 |
| `make vuln` | govulncheck v1.7.0 |

`make lint` and `make vuln` run their tools through pinned
`go run ...@version` invocations, so `go.mod`/`go.sum` stay dependency-free.

The runtime image builds from `docker/runtime/Dockerfile` with Docker Buildx
and supports `linux/amd64` and `linux/arm64`:

```sh
# Multi-architecture build (no local load).
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/runtime/Dockerfile \
  -t agent-bridge:local .

# Single-architecture build loaded into the local Docker daemon.
docker buildx build --platform linux/amd64 \
  -f docker/runtime/Dockerfile -t agent-bridge:local --load .
```

The image is intentionally large: expect **at least 1 GB** because it preserves
the pinned agents' optional dependencies and lifecycle scripts. It runs as the
fixed non-root user `agentbridge` at numeric **UID 10001** (HOME
`/home/agentbridge`, workdir `/workspace`), ships checksum-pinned
**Tini 0.19.0** as **PID 1** with an exec-form entrypoint, and uses Tini as a
**subreaper** so orphaned descendants are reaped while the container keeps
running. `EXPOSE 2468` is declared, and the agents under `/opt/agents` are
root-owned and **immutable**: the bridge never downloads, installs, or updates
agents at runtime.

## Run

The image sets `AGENT_BRIDGE_HOST=0.0.0.0`, so normal image startup requires
`AGENT_BRIDGE_TOKEN`:

```sh
docker run --rm -p 2468:2468 \
  -e AGENT_BRIDGE_TOKEN="$AGENT_BRIDGE_TOKEN" \
  -v agent-bridge-db:/data \
  -e AGENT_BRIDGE_DB=/data/agent-bridge.db \
  agent-bridge:local
```

On a host loopback bind no token is required, but any non-loopback bind is
rejected unless a token is configured:

```sh
AGENT_BRIDGE_HOST=127.0.0.1 ./bin/agent-bridge
AGENT_BRIDGE_HOST=127.0.0.1 AGENT_BRIDGE_TOKEN=... ./bin/agent-bridge
```

**Never** use `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1` in a deployment example;
it is **unsafe** and exposes an unauthenticated remote API. It is only for
isolated local testing.

## Configuration

`internal/config` reads and validates the startup environment. Invalid or
out-of-range values fail startup; there is no silent fallback.

| Variable | Default | Notes |
|---|---|---|
| `AGENT_BRIDGE_HOST` | `127.0.0.1` | Listen host. Non-loopback requires a token (see below). |
| `AGENT_BRIDGE_PORT` | `2468` | Integer `1..65535`. |
| `AGENT_BRIDGE_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`, case-insensitive. |
| `AGENT_BRIDGE_DB` | `./agent-bridge.db` | SQLite path; the parent must be writable and persistent for resume. |
| `AGENT_BRIDGE_TOKEN` | unset | When set, every `/v1/*` route, including `GET /v1/health`, requires `Authorization: Bearer <token>`. |
| `AGENT_BRIDGE_PID_FILE` | unset | Created after startup and removed last on clean shutdown. |
| `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` | `600000` | `1ms..1h` per ACP request. |
| `AGENT_BRIDGE_IDLE_TTL_MS` | `900000` | `0..30d`; `0` disables idle reaping. |
| `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE` | `0` | `0` or `1`; unsafe override, never use in production. |
| `AGENT_BRIDGE_CLAUDE_BIN` / `AGENT_BRIDGE_CLAUDE_ARGS` | `claude-agent-acp` / `[]` | Agent overrides. |
| `AGENT_BRIDGE_CODEX_BIN` / `AGENT_BRIDGE_CODEX_ARGS` | `codex-acp` / `[]` | Agent overrides. |
| `AGENT_BRIDGE_OPENCODE_BIN` / `AGENT_BRIDGE_OPENCODE_ARGS` | `opencode` / `["acp"]` | Agent overrides. |

`*_ARGS` values are **JSON arrays** of strings; malformed values fail startup.
The token is never logged. Health probes are authenticated exactly like every
other `/v1/*` route:

```sh
curl -s -H "Authorization: Bearer $AGENT_BRIDGE_TOKEN" \
  http://127.0.0.1:2468/v1/health
```

## Agents

Three agents are preinstalled in the runtime image on **Node 24**. The bridge
never installs or updates them, and there is no agent install/list API.

| Agent | Pinned version | Default command | Overrides |
|---|---|---|---|
| Claude | `0.68.0` | `claude-agent-acp` | `AGENT_BRIDGE_CLAUDE_BIN`, `AGENT_BRIDGE_CLAUDE_ARGS` |
| Codex | `1.3.0` | `codex-acp` | `AGENT_BRIDGE_CODEX_BIN`, `AGENT_BRIDGE_CODEX_ARGS` |
| OpenCode | `OpenCode 1.18.18` | `opencode acp` | `AGENT_BRIDGE_OPENCODE_BIN`, `AGENT_BRIDGE_OPENCODE_ARGS` |

A binary override wins, then `exec.LookPath`. Agent children **inherits** the
bridge environment except `AGENT_BRIDGE_TOKEN`, `AGENT_BRIDGE_PID_FILE`,
`AGENT_BRIDGE_ALLOW_INSECURE_REMOTE`, and the private internal flag, so agent
**credentials** remain available through the environment. No **credentials**
are included in the image.

## ACP

The public root returns
`{"name":"agent-bridge","docs":"https://github.com/viethoangcr/agent-bridge#readme"}`.

ACP content is **raw passthrough**: the bridge may compact JSON whitespace for
JSONL framing and inspect routing/session metadata, but never normalizes or
interprets conversation content. `POST /v1/acp/{serverId}?agent=claude|codex|opencode`
creates the server on first use; the body is one JSON-RPC 2.0 object of at most
`10MiB`, and a client **initialize** must precede any `session/load` or
`session/resume`. A conflicting agent later is `409`; more than 256 pending
correlations is `429`; a request that exceeds
`AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` is `504`. Exactly **one process per server ID** is kept (at most 64 live); it multiplexes ACP sessions itself.

- `GET /v1/acp` lists servers, `GET /v1/acp/{serverId}/status` reports status,
  and `GET /v1/acp/{serverId}/events` pages durable events.
- `GET /v1/acp/{serverId}` is the **SSE** stream: `event: message`, a monotonic
  `id`, raw JSON `data`, and a periodic heartbeat. `Last-Event-ID` replays from
  the stored sequence with no gaps or duplicates.
- `DELETE /v1/acp/{serverId}` kills the process group and prunes durable state,
  returning `204`.
- At bridge startup, stale live servers are marked **exited**. An **exited**
  server is recreated only by `initialize`; any other request is `409`.

Event retention is intentionally **unbounded** until `DELETE`. The bridge
adds no retention or pruning subsystem; operators who need a hard bound should
apply an OS/container volume **quota**.

The bridge does not advertise `clientCapabilities.auth.terminal`. Agents may
advertise no auth methods, and unauthenticated sessions fail in band with
**-32000**. Environment-based **credentials** are the supported auth path.

Minimal authenticated examples (`TOKEN` is your bearer token):

```sh
BASE=http://127.0.0.1:2468

# Health; GET / stays public.
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/v1/health"

# Create the server and initialize the agent.
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -X POST "$BASE/v1/acp/my-server?agent=opencode" \
  -d '{"jsonrpc":"2.0","method":"initialize","id":"init-1","params":{}}'

# Subscribe to the SSE stream.
curl -N -H "Authorization: Bearer $TOKEN" -H 'Accept: text/event-stream' \
  "$BASE/v1/acp/my-server"

# Start a managed process.
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -X POST "$BASE/v1/processes" \
  -d '{"command":"/bin/echo","args":["hello"]}'

# List a directory.
curl -s -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/fs/entries?directory=/workspace"

# Upload a tar.gz archive.
curl -s -H "Authorization: Bearer $TOKEN" \
  -X POST "$BASE/v1/fs/upload-batch?directory=/workspace" \
  --data-binary @archive.tar.gz

# Replace the whole MCP config object.
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -X PUT "$BASE/v1/config/mcp?directory=/workspace" \
  -d '{"alpha":{"command":"run","args":["--x"],"env":{"K":"v"}}}'

# Replace the whole skills config object.
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -X PUT "$BASE/v1/config/skills?directory=/workspace" \
  -d '{"skills":{}}'
```

## Processes

Managed processes and one-shot runs share one concurrency budget. The relevant
endpoints are `POST /v1/processes`, `GET /v1/processes`,
`GET /v1/processes/{id}`, `POST /v1/processes/{id}/stop`,
`POST /v1/processes/{id}/kill`, `DELETE /v1/processes/{id}`,
`GET /v1/processes/{id}/logs`, `POST /v1/processes/{id}/input`,
`POST /v1/processes/run`, and `GET/POST /v1/processes/config`.

Defaults are `64` concurrent processes, a `30s` default and `300s` maximum run
timeout, `1MiB` output, `10MiB` retained logs per process, and `64KiB` decoded
input per request. Processes use pipes with no TTY and independent process
groups; stop, kill, timeout, reaper, and shutdown signal the group. Conflicts
such as a full budget return `409`, and over-limit bodies return `413`.

## Filesystem And Project Config

The filesystem API lives under `/v1/fs/`: `/v1/fs/entries`, `/v1/fs/file`
(GET/PUT), `DELETE /v1/fs/entry`, `/v1/fs/mkdir`, `/v1/fs/move`,
`/v1/fs/stat`, and `/v1/fs/upload-batch`, which validates and stages a safe
**tar.gz** extraction before merging. Absolute paths are direct; safe relative
paths resolve under `$HOME`. All bridge filesystem and config mutations share
one process-local mutation lock, but the sandbox remains the security boundary.

Per-project config is a whole JSON object under `/v1/config/mcp` and
`/v1/config/skills`, stored atomically at
`{directory}/.agent-bridge/config/{mcp,skills}.json` with mode `0600`. `PUT`
and `DELETE` return `204`; a missing `GET`/`DELETE` is `404`.

Body limits are `10MiB` for JSON, and `512MiB` for filesystem `PUT` and for
both compressed and extracted uploads; excess is `413`.

## Persistence

State lives in one SQLite database at `AGENT_BRIDGE_DB`, using **WAL** mode,
foreign keys, a 5s busy timeout, and a **checkpoint** on shutdown. One bridge
owns one DB. Only agent stdout envelopes are persisted; sessions and events
survive restarts, and a `DELETE` is the only pruning path (retention is
**unbounded** until then).

For resume across restarts, a container must mount the agent home directories
and the `AGENT_BRIDGE_DB` parent:

```sh
docker run --rm -p 2468:2468 \
  -e AGENT_BRIDGE_TOKEN="$AGENT_BRIDGE_TOKEN" \
  -v agent-bridge-db:/data -e AGENT_BRIDGE_DB=/data/agent-bridge.db \
  -v claude-home:/home/agentbridge/.claude \
  -v codex-home:/home/agentbridge/.codex \
  -v opencode-state:/home/agentbridge/.local/share/opencode \
  agent-bridge:local
```

The required paths are `~/.claude`, `~/.codex`, **OpenCode state** (by default
`~/.local/share/opencode`), and the `AGENT_BRIDGE_DB` parent. A container
without them loses resume state.

## Shutdown

`SIGINT` and `SIGTERM` use one **10-second** staged budget. The listener stops
accepting; pre-drain hooks close SSE, stop reapers, and signal-and-wait process
groups and pumps; `http.Server.Shutdown` drains the now-unblocked handlers;
post-drain idempotently confirms pumps/commits, checkpoints and closes SQLite,
and removes the **PID file** last. Signal-triggered shutdown **exit 0** after
this bounded best-effort sequence even when cleanup fails or the budget
expires; failures are logged, and SSE closure never waits behind the HTTP
drain.

## Testing

```sh
make check                       # exact-Go guard, tests, lint, build, hygiene
go test ./...                    # unit and integration tests
go test -race ./... -count=1     # race detector
go test -tags=e2e ./tests/e2e -count=1 -v   # Docker end-to-end suite
```

The Docker suite builds the image and runs the **keyless** real-agent matrix,
raw-byte protocol checks, persistence/restart recovery, reaper/process-group
checks, and the bounded shutdown gate. Live authenticated **prompt/resume**
end-to-end coverage is explicitly **deferred** until isolated CI credentials
exist.

## Limitations

- **Keyless** agents: all three initialize. Claude `session/new` succeeds and
  auth fails at `session/prompt` with `-32000`; Codex auth failures surface in
  band (its `session/new` may return auth-required); OpenCode session creation
  succeeds and auth may fail at prompt. OpenCode 1.18.18 may omit `sessionId`
  from `session/load`.
- The bridge does not advertise `clientCapabilities.auth.terminal`; agents may
  advertise no auth methods, and unauthenticated sessions fail in band with
  `-32000`. Environment **credentials** are the supported path.
- Authenticated **prompt/resume** E2E is **deferred**. No **credentials** are
  ever included in the image.
- No agent install/update, PTY/terminal WebSocket, desktop API, `/opencode`
  compatibility, Inspector UI, telemetry, daemon mode, or public CLI
  subcommands.

## Contributing

All changes follow the project ground rules:

- [`docs/references/go-project-layout.md`](docs/references/go-project-layout.md)
  defines layout, package ownership, and hygiene gates.
- [`docs/references/go-coding-standards.md`](docs/references/go-coding-standards.md)
  defines behavior-level coding rules.

The specification and phase plans remain authoritative for behavior.

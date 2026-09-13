# agent-bridge

agent-bridge exposes pre-provisioned coding agents (Claude, Codex, OpenCode)
as headless ACP JSON-RPC servers behind one authenticated HTTP/SSE API, with
process, filesystem, and per-project config endpoints on the same listener.

It exists so remote clients can drive agents inside a sandbox without embedding
agent SDKs. It is one static Linux binary plus one SQLite file, installs or
updates nothing at runtime, and treats conversation content as opaque bytes:
the sandbox, not the bridge, is the security boundary.

## Build

The host binary is exactly **Go 1.26.8**, `CGO_ENABLED=0`, and `-trimpath`, as
one static Linux binary.

| Command | Purpose |
|---|---|
| `make check` | Exact-Go guard, tests, lint, static build, `gofmt` and `go mod tidy` cleanliness |
| `make build` | `CGO_ENABLED=0` static binary at `bin/agent-bridge` |
| `make test` | `go test ./...` |
| `make lint` | `go vet ./...` plus staticcheck 2026.2.1 |
| `make vuln` | govulncheck v1.7.0 |

`make lint` and `make vuln` run pinned `go run ...@version` tools, so
`go.mod`/`go.sum` stay dependency-free.

The runtime image builds from `docker/runtime/Dockerfile` and supports
`linux/amd64` and `linux/arm64`:

```sh
docker buildx build --platform linux/amd64,linux/arm64 \
  -f docker/runtime/Dockerfile -t agent-bridge:local .

# Single architecture, loaded into the local daemon.
docker buildx build --platform linux/amd64 \
  -f docker/runtime/Dockerfile -t agent-bridge:local --load .
```

The image is intentionally large (at least **1 GB**) because it preserves the
pinned agents' optional dependencies and lifecycle scripts. It runs as
non-root `agentbridge` (**UID 10001**, HOME `/home/agentbridge`, workdir
`/workspace`), ships checksum-pinned **Tini 0.19.0** as **PID 1** and
subreaper, declares `EXPOSE 2468`, and keeps `/opt/agents` root-owned and
**immutable**: the bridge never downloads, installs, or updates agents at
runtime.

### Published Image

Every merge to `main` that passes CI publishes a multi-architecture image to
the GitHub Container Registry at `ghcr.io/viethoangcr/agent-bridge`:

```sh
docker pull ghcr.io/viethoangcr/agent-bridge:latest
```

`:latest` is the stable release channel and moves only on a stable `vX.Y.Z`
tag. `:main` tracks the latest green merge and `:sha-<commit>` pins one exact
commit; both are unreleased edge channels for early testing.

### Releases

Tagging `main` with an annotated `vX.Y.Z` (or `-rc.N`) publishes a GitHub
Release with `agent-bridge_<X.Y.Z>_linux_amd64.tar.gz`,
`agent-bridge_<X.Y.Z>_linux_arm64.tar.gz`, and `SHA256SUMS`. Verify a download
before running it:

```sh
sha256sum -c SHA256SUMS
```

The tag, trigger, image-tag, and rollback contract is owned by
[`docs/references/releasing.md`](docs/references/releasing.md).

## Run

The image sets `AGENT_BRIDGE_HOST=0.0.0.0`, so startup requires
`AGENT_BRIDGE_TOKEN`:

```sh
docker run --rm -p 2468:2468 \
  -e AGENT_BRIDGE_TOKEN="$AGENT_BRIDGE_TOKEN" \
  -v agent-bridge-db:/data -e AGENT_BRIDGE_DB=/data/agent-bridge.db \
  agent-bridge:local
```

A loopback bind needs no token; any non-loopback bind is rejected unless one
is configured:

```sh
AGENT_BRIDGE_HOST=127.0.0.1 ./bin/agent-bridge
```

`AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1` disables that check and is **unsafe**;
use it only for isolated local testing.

## Configuration

`internal/config` reads and validates the startup environment. Invalid values
fail startup; there is no silent fallback.

| Variable | Default | Notes |
|---|---|---|
| `AGENT_BRIDGE_HOST` | `127.0.0.1` | Non-loopback requires a token. |
| `AGENT_BRIDGE_PORT` | `2468` | Integer `1..65535`. |
| `AGENT_BRIDGE_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. |
| `AGENT_BRIDGE_DB` | `./agent-bridge.db` | SQLite path; the parent must be writable and persistent for resume. |
| `AGENT_BRIDGE_TOKEN` | unset | When set, every `/v1/*` route, including `GET /v1/health`, requires `Authorization: Bearer <token>`. |
| `AGENT_BRIDGE_PID_FILE` | unset | PID file created after startup and removed last on clean shutdown. |
| `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` | `600000` | `1ms..1h` per ACP request. |
| `AGENT_BRIDGE_IDLE_TTL_MS` | `900000` | `0..30d`; `0` disables idle reaping. |
| `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE` | `0` | `0` or `1`; unsafe override. |
| `AGENT_BRIDGE_CLAUDE_BIN` / `AGENT_BRIDGE_CLAUDE_ARGS` | `claude-agent-acp` / `[]` | Agent overrides. |
| `AGENT_BRIDGE_CODEX_BIN` / `AGENT_BRIDGE_CODEX_ARGS` | `codex-acp` / `[]` | Agent overrides. |
| `AGENT_BRIDGE_OPENCODE_BIN` / `AGENT_BRIDGE_OPENCODE_ARGS` | `opencode` / `["acp"]` | Agent overrides. |

Agent args are `JSON arrays` of strings; malformed values fail startup. The
token is never logged.

## Agents

Three agents are preinstalled in the runtime image on **Node 24**. The bridge
never installs or updates them, and there is no agent install/list API.

| Agent | Pinned version | Default command |
|---|---|---|
| Claude | `0.68.0` | `claude-agent-acp` |
| Codex | `1.3.0` | `codex-acp` |
| OpenCode | `OpenCode 1.18.18` | `opencode acp` |

A binary override wins, then `exec.LookPath`. A child agent inherits the bridge
environment except the token, the PID file, the insecure-remote flag, and the
private internal flag, so agent credentials stay available through the
environment. No credentials are included in the image.

## ACP

The public root returns
`{"name":"agent-bridge","docs":"https://github.com/viethoangcr/agent-bridge#readme"}`.

`POST /v1/acp/{serverId}?agent=claude|codex|opencode` creates the server on
first use. The body is one JSON-RPC 2.0 object of at most `10MiB`; a client
`initialize` must precede `session/load` or `session/resume`. Content is **raw passthrough**:
only JSONL framing whitespace may be compacted, never normalized or
interpreted. A conflicting agent is `409`, more than 256 pending correlations
is `429`, a request over `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` is `504`, and
exactly one process per server ID is kept (at most 64 live), multiplexing ACP
sessions itself.

- `GET /v1/acp` lists servers, `GET /v1/acp/{serverId}/status` reports status,
  and `GET /v1/acp/{serverId}/events` pages durable events.
- `GET /v1/acp/{serverId}` is the **SSE** stream: `event: message`, a monotonic
  `id`, raw JSON `data`, and a periodic heartbeat. `Last-Event-ID` replays with
  no gaps or duplicates.
- `DELETE /v1/acp/{serverId}` kills the process group, prunes durable state,
  and returns `204`. Startup marks stale live servers **exited**; only
  `initialize` recreates an **exited** server, everything else is `409`.

Event retention is unbounded until `DELETE`; operators who need a hard bound
should apply a container/volume **quota**. The bridge does not advertise
`clientCapabilities.auth.terminal`; unauthenticated sessions fail in band with
`-32000`, and environment credentials are the supported auth path.

Authenticated examples (`$TOKEN` is the bearer token):

```sh
BASE=http://127.0.0.1:2468

curl -s -H "Authorization: Bearer $TOKEN" "$BASE/v1/health"

curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -X POST "$BASE/v1/acp/my-server?agent=opencode" \
  -d '{"jsonrpc":"2.0","method":"initialize","id":"init-1","params":{}}'

curl -N -H "Authorization: Bearer $TOKEN" -H 'Accept: text/event-stream' \
  "$BASE/v1/acp/my-server"

curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -X POST "$BASE/v1/processes/run" -d '{"command":"/bin/echo","args":["hello"]}'

curl -s -H "Authorization: Bearer $TOKEN" "$BASE/v1/fs/entries?directory=/workspace"

curl -s -H "Authorization: Bearer $TOKEN" \
  -X POST "$BASE/v1/fs/upload-batch?directory=/workspace" --data-binary @archive.tar.gz

curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -X PUT "$BASE/v1/config/mcp?directory=/workspace" -d '{"alpha":{"command":"run"}}'
```

## Processes

Managed processes and one-shot runs share one budget. Endpoints:
`POST /v1/processes`, `GET /v1/processes`, `GET /v1/processes/{id}`,
`POST /v1/processes/{id}/stop`, `POST /v1/processes/{id}/kill`,
`DELETE /v1/processes/{id}`, `GET /v1/processes/{id}/logs`,
`POST /v1/processes/{id}/input`, `POST /v1/processes/run`,
`GET/POST /v1/processes/config`.

Defaults: 64 concurrent processes, a `30s` default and `300s` maximum run
timeout, `1MiB` output, `10MiB` retained logs per process, and `64KiB` decoded
input per request. Processes use pipes with no TTY and independent process
groups; stop, kill, timeout, reaper, and shutdown signal the group. A full
budget is `409`; over-limit bodies are `413`.

## Filesystem And Project Config

The filesystem API under `/v1/fs/`: `/v1/fs/entries`, `/v1/fs/file` (GET/PUT),
`DELETE /v1/fs/entry`, `/v1/fs/mkdir`, `/v1/fs/move`, `/v1/fs/stat`, and
`/v1/fs/upload-batch`, which validates and stages a safe **tar.gz** extraction
before merging. Absolute paths are direct; safe relative paths resolve under
`$HOME`. Bridge filesystem and config mutations share one process-local lock;
the sandbox remains the security boundary.

`/v1/config/mcp` and `/v1/config/skills` store a whole JSON object atomically at
`{directory}/.agent-bridge/config/{mcp,skills}.json` with mode `0600`; `PUT`
and `DELETE` return `204`, and a missing `GET`/`DELETE` is `404`.

Body limits are `10MiB` for JSON and `512MiB` for filesystem `PUT` and for both
compressed and extracted uploads; excess is `413`.

## Persistence

State lives in one SQLite database at `AGENT_BRIDGE_DB` with **WAL** mode,
foreign keys, a 5s busy timeout, and a **checkpoint** on shutdown. One bridge
owns one DB. Only agent stdout envelopes are persisted; sessions and events
survive restarts, and `DELETE` is the only pruning path.

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

The required paths are `~/.claude`, `~/.codex`, OpenCode state (by default
`~/.local/share/opencode`), and the `AGENT_BRIDGE_DB` parent. A container
without them loses resume state.

## Shutdown

`SIGINT` and `SIGTERM` use one **10-second** staged budget: stop accepting,
close SSE, stop reapers, signal-and-wait process groups and pumps, drain HTTP,
confirm commits, checkpoint and close SQLite, and remove the PID file last.
Signal-triggered shutdown returns **exit 0** after this bounded best-effort
sequence even when cleanup fails or the budget expires; failures are logged,
and SSE closure never waits behind the HTTP drain.

## Testing

```sh
make check                                  # exact-Go guard, tests, lint, build, hygiene
go test ./...                               # unit and integration tests
go test -race ./... -count=1                # race detector
go test -tags=e2e ./tests/e2e -count=1 -v   # Docker end-to-end suite
```

The Docker suite covers the **keyless** real-agent matrix, raw-byte protocol
checks, persistence/restart recovery, process-group checks, and the bounded
shutdown gate. Live authenticated **prompt/resume** end-to-end coverage is
explicitly **deferred** until isolated CI credentials exist.

## Limitations

- **Keyless** agents: all three initialize. Claude `session/new` succeeds and
  auth fails at `session/prompt` with `-32000`; Codex auth failures surface in
  band (its `session/new` may return auth-required); OpenCode session creation
  succeeds and auth may fail at prompt, and OpenCode 1.18.18 may omit
  `sessionId` from `session/load`.
- Environment credentials are the supported auth path; no credentials are ever
  included in the image.
- No agent install/update, PTY/terminal WebSocket, desktop API, `/opencode`
  compatibility, Inspector UI, telemetry, daemon mode, or public CLI
  subcommands.

## License

Released under the [MIT License](LICENSE).

## Contributing

- [`docs/references/go-project-layout.md`](docs/references/go-project-layout.md)
  defines layout, package ownership, and hygiene gates.
- [`docs/references/go-coding-standards.md`](docs/references/go-coding-standards.md)
  defines behavior-level coding rules.
- [`docs/references/releasing.md`](docs/references/releasing.md)
  defines versioning, publishing triggers, image tags, artifacts, and rollback.

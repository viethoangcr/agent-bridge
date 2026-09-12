# agent-bridge

`agent-bridge` is a static Linux Go binary that bridges ACP JSON-RPC between
remote HTTP/SSE clients and pre-provisioned coding-agent subprocesses, and
exposes process, filesystem, and per-project config APIs.

> **Status: Phases 01-05 are complete.** The binary provides validated
> environment configuration, a public `GET /` root, an authenticated
> `GET /v1/health`, bearer-token authentication, structured request logging,
> PID-file plus graceful-shutdown lifecycle, internal SQLite persistence with
> the private stdio ACP runtime, the ACP HTTP/SSE API (`POST /v1/acp/{serverId}`,
> `GET /v1/acp`, `GET /v1/acp/{serverId}/status`, `GET /v1/acp/{serverId}/events`,
> SSE at `GET /v1/acp/{serverId}`, `DELETE /v1/acp/{serverId}`), and the managed
> process/one-shot API under `/v1/processes` (start/list/get/stop/kill/delete,
> bounded logs, stdin input, `/run`, and runtime config), the filesystem API
> (`/v1/fs/*`: entries, file GET/PUT, recursive delete, mkdir, move, stat,
> validated tar.gz upload), and atomic per-project MCP/skills config under
> `/v1/config/{mcp,skills}`. The runtime image and in-container E2E phase
> remain.

The authoritative contract is
[`docs/plans/20260815-agent-bridge.md`](docs/plans/20260815-agent-bridge.md).
Where this README conflicts with that specification, the specification wins.

## Prerequisites

- Linux.
- Exactly **Go 1.26.8**. `make check` rejects any other toolchain.
- Builds use `CGO_ENABLED=0` (already set by `make build`).

## Commands

| Command | Purpose |
|---|---|
| `make check` | Exact-Go guard, tests, lint, static build, `gofmt` check, `go mod tidy` cleanliness |
| `make build` | `CGO_ENABLED=0` static binary at `bin/agent-bridge` |
| `make test` | `go test ./...` |
| `make lint` | `go vet ./...` plus staticcheck 2026.2.1 |
| `make vuln` | govulncheck v1.7.0 |

`make lint` and `make vuln` run their tools through pinned
`go run ...@version` invocations, so `go.mod`/`go.sum` stay dependency-free.

## Configuration

`internal/config` reads and validates the startup environment. Invalid or
out-of-range values fail startup; there is no silent fallback.

| Variable | Default | Notes |
|---|---|---|
| `AGENT_BRIDGE_HOST` | `127.0.0.1` | Listen host. Non-loopback requires a token (see below). |
| `AGENT_BRIDGE_PORT` | `2468` | Integer `1..65535`. |
| `AGENT_BRIDGE_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error`, case-insensitive. |
| `AGENT_BRIDGE_DB` | `./agent-bridge.db` | SQLite path, reserved for a later phase. |
| `AGENT_BRIDGE_TOKEN` | unset | When set, every `/v1/*` route requires bearer auth. |
| `AGENT_BRIDGE_PID_FILE` | unset | Created on startup, removed on clean shutdown. |
| `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS` | `600000` | `1ms..1h`; used by a later phase. |
| `AGENT_BRIDGE_IDLE_TTL_MS` | `900000` | `0..30d`; `0` disables reaping; later phase. |
| `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE` | `0` | `0` or `1`; unsafe override (see below). |
| `AGENT_BRIDGE_CLAUDE_BIN` / `AGENT_BRIDGE_CLAUDE_ARGS` | `claude-agent-acp` / `[]` | Overrides parsed now; agent launch is a later phase. |
| `AGENT_BRIDGE_CODEX_BIN` / `AGENT_BRIDGE_CODEX_ARGS` | `codex-acp` / `[]` | Overrides parsed now; agent launch is a later phase. |
| `AGENT_BRIDGE_OPENCODE_BIN` / `AGENT_BRIDGE_OPENCODE_ARGS` | `opencode` / `["acp"]` | Overrides parsed now; agent launch is a later phase. |

`*_ARGS` values are JSON arrays of strings; malformed values fail startup.

## Usage

```sh
# GET / is always public.
curl -s http://127.0.0.1:2468/

# GET /v1/health needs no token only when no token is configured.
curl -s http://127.0.0.1:2468/v1/health

# With AGENT_BRIDGE_TOKEN set, every /v1/* route requires the bearer token.
curl -s -H 'Authorization: Bearer <token>' http://127.0.0.1:2468/v1/health
```

`GET /` returns `{"name":"agent-bridge","docs":"..."}` and `GET /v1/health`
returns `{"status":"ok"}`. Authentication failures and routing errors are RFC
9457 `application/problem+json` responses.

## Non-loopback hosts require a token

When `AGENT_BRIDGE_HOST` is a non-loopback value (loopback means any IP for
which `net.IP.IsLoopback` is true, or the case-insensitive hostname
`localhost`), startup fails unless `AGENT_BRIDGE_TOKEN` is set.
Setting `AGENT_BRIDGE_ALLOW_INSECURE_REMOTE=1` disables that guard and exposes
an unauthenticated remote API. **This override is unsafe** and is only for
isolated local testing; never enable it in production. The runtime image binds
`0.0.0.0` and therefore requires a token.

## Contributing

All changes follow the project ground rules:

- [`docs/references/go-project-layout.md`](docs/references/go-project-layout.md)
  defines layout, package ownership, and hygiene gates.
- [`docs/references/go-coding-standards.md`](docs/references/go-coding-standards.md)
  defines behavior-level coding rules.

The specification and phase plans remain authoritative for behavior.

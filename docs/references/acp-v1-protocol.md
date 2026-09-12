# Reference: ACP v1 Protocol and Supported Coding Agents

**Date:** 2026-09-02
**Revised:** 2026-09-12
**Status:** CURRENT
**Scope:** ACP v1 only. The bridge implements and passes through ACP protocol version 1 (`"protocolVersion": 1`); the ACP v2 draft (published 2026-07-20) is out of scope until it is stable and the specification is revised.
**Purpose:** Factual baseline for the agent-bridge plans (`docs/plans/`). All bridge behavior claims about ACP must be checked against this document and the sources below.

## Sources

- Spec: https://agentclientprotocol.com/protocol/v1/{overview,initialization,session-setup,prompt-turn,authentication,transports,schema}
- Schema: https://github.com/agentclientprotocol/agent-client-protocol/releases/latest/download/schema.json (latest stable release: schema-v1.21.0, 2026-08-20)
- TypeScript SDK: https://www.npmjs.com/package/@agentclientprotocol/sdk (latest stable 1.4.0)
- v2 draft migration: https://agentclientprotocol.com/protocol/v2/migration (awareness only; out of scope)
- Agents page: https://agentclientprotocol.com/get-started/agents
- GitHub releases and npm registry: stable `latest` dist-tags only (verified 2026-09-12)

## 1. Transport and framing

- **stdio is the only official transport.** HTTP/SSE exists only as an RFD draft (`/rfds/streamable-http-websocket-transport`); agent-bridge's HTTP/SSE layer is its own extension, not ACP.
- Newline-delimited JSON-RPC 2.0, UTF-8. One JSON object per line; **no embedded newlines**; agent MUST NOT write non-ACP data to stdout; client MUST NOT write non-ACP data to stdin. Logging goes to stderr (optional).
- v1 docs do **not** define JSON-RPC batching. v2 draft formally allows batch arrays (with `-32600` per invalid entry) and forbids batching `initialize`, `auth/login`, `session/new`, `session/resume`, `session/prompt`.

## 2. JSON-RPC conventions and error codes

- camelCase keys; snake_case discriminator values (`sessionUpdate`, `stopReason`, `kind`, `status`).
- Doc examples use integer IDs; no restriction on string IDs. Notifications never get responses.
- Error codes: standard JSON-RPC codes plus ACP-specific:
  - `-32800` request cancelled
  - `-32000` **auth_required**
  - `-32002` resource not found
  - `-32600`..`-32603` standard.

## 3. Versioning and negotiation

- `protocolVersion` is a single integer (major version only). `"protocolVersion": 1` is stable and is the only version this project supports; `2` is draft (published 2026-07-20) and out of scope.
- Negotiation: client sends its latest; agent echoes if supported, else returns its own latest; client disconnects if it cannot support the response. Non-breaking features ship as capabilities, not version bumps. Stabilized in the v1 line during 2026: config options (Feb), session list/info (Mar), session resume + close (Apr), logout (May), `additionalDirectories` (Jun 1), session delete + message IDs + usage updates (Jun 5), model config category (Jun 24), `$/cancel_request` (Jun 29), boolean config options (Jul 6), elicitation (Jul 22).

## 4. Method inventory (v1)

### Agent-implemented (client → agent)

| Method | Params (required) | Response | Gate |
|---|---|---|---|
| `initialize` | `protocolVersion`, `clientCapabilities` | `protocolVersion`, `agentCapabilities`, `authMethods[]` | — |
| `authenticate` | `methodId` | `{}` | `type:"agent"` auth methods only |
| `logout` | `{}` | `{}` | `agentCapabilities.auth.logout` |
| `session/new` | `cwd` (absolute), `mcpServers`, optional `additionalDirectories` | `sessionId`, optional `modes` | — |
| `session/load` | `sessionId`, `cwd`, `mcpServers`, optional `additionalDirectories` | `null` (or `modes`) | `agentCapabilities.loadSession`. **Agent MUST replay the full conversation as `session/update` notifications before responding.** Removed in v2. |
| `session/resume` | `sessionId`, `cwd`, optional `additionalDirectories`, optional `mcpServers` | `{}` (optional `modes`) | `sessionCapabilities.resume`. **MUST NOT replay history.** |
| `session/prompt` | `sessionId`, `prompt: ContentBlock[]` | `stopReason` (`end_turn`,`max_tokens`,`max_turn_requests`,`refusal`,`cancelled`) | **Response stays pending for the entire turn.** |
| `session/set_mode` | `sessionId`, `modeId` | `{}` | modes capability |
| `session/set_config_option` | `sessionId`, `configId`, value | `configOptions[]` | config options |
| `session/close` | `sessionId` | `{}` | `sessionCapabilities.close` |
| `session/list` | optional `cursor`, optional `cwd` filter | `sessions[]`, `nextCursor?` | `sessionCapabilities.list` |
| `session/delete` | `sessionId` | `{}` | `sessionCapabilities.delete` |
| `session/cancel` (notification) | `sessionId` | none | pending prompt should end with `stopReason:"cancelled"` |

Session lifecycle requests also accept optional absolute `additionalDirectories` (stabilized 2026-06-01); the bridge treats it as opaque passthrough and does not persist or enforce it.

### Client-implemented (agent → client reverse-calls)

| Method | Params | Response |
|---|---|---|
| `session/request_permission` | `sessionId`, `options[]`, `toolCall` | `outcome` (`selected`/`cancelled`) |
| `fs/read_text_file` | `sessionId`, `path`, optional `line`, `limit` | `content` |
| `fs/write_text_file` | `sessionId`, `path`, `content` | `{}` |
| `terminal/create` | `sessionId`, `command`, optional `args`,`cwd`,`env`,`outputByteLimit` | `terminalId` |
| `terminal/output` / `wait_for_exit` / `kill` / `release` | `sessionId`, `terminalId` | varies |
| `elicitation/create` | `message` + form/url variant (**no sessionId**) | `accept`/`decline`/`cancel` |

### Notifications (agent → client)

- `session/update` — `sessionId` + `update` discriminated by `sessionUpdate`: `user_message_chunk`, `agent_message_chunk`, `agent_thought_chunk` (optional `messageId`), `tool_call`, `tool_call_update`, `plan`, `available_commands_update`, `current_mode_update`, `config_option_update`, `session_info_update`, `usage_update`.
- `elicitation/complete` — `elicitationId`.

### Protocol-level notifications (either direction)

- `$/cancel_request` — cancels an outstanding request by `requestId` (stabilized 2026-06-29); the cancelled request normally completes with `-32800 request cancelled`.

**Does not exist in any ACP version:** `session/unload`.

## 5. cwd / sessionId field map (authoritative for bridge inspection)

- **`cwd` appears only on `session/new`, `session/load`, `session/resume`** (required param), plus optional `cwd` filter on `session/list` and optional per-command cwd on `terminal/create` (agent→client). There is **no per-message cwd** on `session/prompt`, `session/update`, `fs/*`, or permission requests.
- **`sessionId` is required on every session-scoped message**: `session/prompt`, `session/cancel`, `session/update`, `session/request_permission`, `fs/read_text_file`, `fs/write_text_file`, all `terminal/*`, `session/set_mode`, `session/set_config_option`, `session/close`, `session/delete`, and in the params of `session/new` (response), `session/load`, `session/resume`. `elicitation/create` has no sessionId.
- `additionalDirectories` (optional, absolute) may accompany `cwd` on `session/new|load|resume` only; `$/cancel_request` carries `requestId`, not `sessionId`.
- ACP defines no length limits on `sessionId`/`cwd`; any byte caps are bridge policy.

## 6. Authentication flow

- `initialize` response `authMethods[]`: `{id, name, description?, type?}` — `type` defaults to `"agent"`; `"terminal"` requires `clientCapabilities.auth.terminal`.
- Auth failure surfaces in-band as JSON-RPC error **`-32000 auth_required`** (typically from `session/new` or `session/prompt`); the agent does not exit.
- `authenticate {methodId}` retries agent-type auth. **Terminal-type auth requires the client to relaunch the agent interactively** (e.g. `ACP_INTERACTIVE_LOGIN=1`, exit 0 = success, then reconnect and re-initialize) — impossible through a headless HTTP bridge that owns spawning.

## 7. Coding agents (stable versions verified 2026-09-12)

| Agent | Package (current) | Pinned in plans | Binary | Launch | Node | Keyless behavior |
|---|---|---|---|---|---|---|
| Claude | `@agentclientprotocol/claude-agent-acp` — **latest 0.76.0 (2026-09-09)** | 0.68.0 (valid, stale) | `claude-agent-acp` | `claude-agent-acp` (no args) | >=22 | `initialize` succeeds; **auth failure surfaces at `session/prompt`** as `-32000` (`RequestError.authRequired()`); expired creds may surface as `-32603`. Bundles Claude Agent SDK (includes Claude Code executable, large). Auth via `ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`, or `~/.claude`. |
| Codex | `@agentclientprotocol/codex-acp` — **latest 1.11.0 (2026-09-09)** | 1.3.0 (valid, stale) | `codex-acp` | `codex-acp` (no args) | Node (TS adapter) | Advertises ChatGPT-login + API-key auth methods; **`NO_BROWSER=1` hides browser login (official headless var)**. Auth failure in-band; bundles `@openai/codex` Rust binary (npm dep, `CODEX_PATH` override). |
| OpenCode | `opencode-ai` — **latest 1.18.30 (2026-09-09)** | 1.18.18 (valid, stale) | `opencode` | **`opencode acp`** (native, no adapter) | none at runtime (Bun-compiled static binary via optionalDependencies + postinstall) | `initialize` and `session/new` succeed without auth; failure at prompt time. `--ignore-scripts` breaks install (postinstall copies the platform binary). |

- Deprecated predecessors: `@zed-industries/claude-code-acp` (→ 0.16.2), `@zed-industries/codex-acp` (→ 0.16.0). Do not use.
- Headless env conventions: `NO_BROWSER=1` (official, codex-acp README); `DISABLE_AUTOUPDATER=1`, `CI=true` are Claude Code community conventions, not ACP-official. Writable `HOME` required (`~/.claude`, `CLAUDE_CONFIG_DIR`).
- Image ≥1GB is expected: Claude SDK bundles the Claude Code executable; Codex bundles the Rust binary; only the matching OpenCode platform optionalDependency downloads.
- Latest-version values drift and are date-stamped; the pinned plan versions remain deliberate (behavioral stability), and bumping any pin requires re-running the keyless matrix. When bumping, use the stable `latest` dist-tag; `preview`, `beta`, `next`, `dev`, and snapshot releases are never used.

### 7.1 ACP tooling versions (stable, verified 2026-09-12)

| Component | Stable version | Notes |
|---|---|---|
| ACP schema release | schema-v1.21.0 (2026-08-20) | v1 line; the v2 schema exists only as a draft |
| `@agentclientprotocol/sdk` (TypeScript) | 1.4.0 (2026-08-20) | used by the Claude/Codex adapters; the Rust SDK also reached 1.0 in June 2026 |
| `claude-agent-acp` dependencies | `@agentclientprotocol/sdk` 1.4.0, `@anthropic-ai/claude-agent-sdk` 0.3.257 | bundles the Claude Code executable |
| `codex-acp` dependencies | `@agentclientprotocol/sdk` ^1.4.0, `@openai/codex` ^0.153.4 | bundles the Codex binary |

## 8. v2 draft deltas (out of scope; awareness only)

`authenticate`/`logout` → `auth/login`/`auth/logout`; **`session/load` removed** (replaced by `session/resume` + `replayFrom`); `session/list`/`close`/`resume` become required baseline; `fs/*` and `terminal/*` client methods removed; `session/set_mode` removed; `session/prompt` responds immediately (stop reason moves to `state_update` notification); `tool_call` variant removed; modes → config options; batching formally allowed. If v2 support is ever added, the bridge's lifecycle enum (`none|new|load|resume`) and its HTTP contract change shape. v2 is excluded from this project: while it remains draft, no v2 code paths, feature flags, or conditional branches are implemented.

## 9. Implications for the agent-bridge plans

1. The bridge's HTTP/SSE layer is a custom transport extension — permissible ("custom transports must preserve JSON-RPC framing"), but it is not part of ACP; document it as such.
2. "Session lifecycle/scoped messages" in the plans should be replaced with the explicit field map in §5: `cwd` on `session/new|load|resume` only (matches the plans' "lifecycle cwd"); `sessionId` on the enumerated session-scoped methods. Response envelopes carry no `sessionId`; the bridge attributes them from the retained request `sessionId` for the same method list.
3. `session/prompt` stays pending for the whole turn — coding-agent turns routinely exceed 120s, so the original `AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS=120000` default guaranteed frequent 504s. The plans adopted a 600000 ms default (2026-09-12 revision); beyond that, non-lifecycle correlation is released, the late response is persisted, and updates continue via SSE — recoverable, but documented.
4. Terminal-type auth methods cannot be fulfilled through the bridge (it owns spawning; no TTY). Document as a limitation; env-based auth remains the supported path.
5. `session/load` obligates the agent to replay the full conversation as `session/update` before responding — a load through the bridge produces a large persisted-event burst (retention is unbounded until DELETE). The private mock does not replay (acceptable; it makes no conformance promise — E2E must not assert replay from the mock).
6. Keyless E2E outcomes must match real adapter behavior: Claude errors at prompt (so `session/new` likely succeeds — plain success is the observable keyless outcome), Codex errors in-band with `-32000`; OpenCode succeeds at `session/new` and fails at prompt. Structural auth matching should assert JSON-RPC code `-32000` explicitly.
7. Rejecting JSON-RPC batch arrays at the HTTP layer is v1-conformant (v1 defines no batching). Record this as a deliberate v1 decision; v2 would require rework.
8. `$/cancel_request` is forwarded like any other notification (HTTP 202) and is never synthesized by the bridge; the agent normally completes the cancelled request with `-32800`.
9. Optional `additionalDirectories` on lifecycle requests is preserved by passthrough; the bridge persists only `cwd` and `sessionId` per §5.

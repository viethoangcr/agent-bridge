# Agent Guide

These rules are binding for every change in this repository. They are
enforceable by review and by `internal/projectdocs`.

## Precedence and order

- `docs/plans/20260815-agent-bridge.md` is the authoritative contract. Phase
  plans elaborate it; where a phase plan or a reference doc conflicts with the
  specification, the specification has precedence.
- Execute implementation plans in numeric order, except where the
  specification explicitly allows phases 04 and 05 to run in parallel.
- Read the task's plan entry and every file it references before writing code.

## Dependencies and platform

- Use the Go standard library plus exactly one external module:
  `modernc.org/sqlite`, and only from Phase 02 onward. No other production or
  test modules; dev tools run only through pinned `go run ...@version` so
  `go.mod`/`go.sum` stay dependency-free.
- Build with `CGO_ENABLED=0`, `-trimpath`, as one static Linux binary.
- No runtime installs: the bridge never downloads, installs, or updates agents.
- Do not expand into the specification's out of scope features. PTY/terminal
  WebSocket, desktop APIs, agent install/list APIs, `/opencode` compatibility,
  Inspector UI, telemetry, daemon mode, and public CLI subcommands stay out of
  scope.
- There is no public mock interface. The private mock is reachable only through
  `AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1` and is never documented publicly.

## Testing

- Follow test-first TDD: write a failing behavioral assertion first, record RED,
  then implement the minimum, then record GREEN. A missing file or symbol is
  setup evidence, not RED evidence.
- Use injected clocks, tickers, and size limits; do not sleep and do not wait
  for production-duration timers. Keep the single bounded real-time heartbeat
  test as the only exception.
- No third-party test frameworks; use `testing` and the standard library.
- Tests must stay green under `-race` where the plan requires it.

## HTTP and errors

- Every non-2xx response for a request that reaches routing or handlers uses
  RFC 9457 `application/problem+json` with `{type,title,status,detail}` plus
  top-level extension members. Decide and write these errors at the handler
  level; never surface raw internal errors.
- Bound every request body with `http.MaxBytesReader` and map over-limit bodies
  to 413. Keep ACP JSON-RPC error envelopes as HTTP 200.
- Never log or leak `Authorization`, tokens, or other secrets.

## Hygiene gates

Before every commit and in CI:

- `gofmt -l .` must be empty and `go mod tidy` must leave `go.mod`/`go.sum`
  unchanged.
- `make lint` must be green; it runs `go vet ./...` and
  `staticcheck@2026.2.1` through pinned `go run`.
- `make vuln` must be green; it runs `govulncheck@v1.7.0` through pinned
  `go run`.
- `make check` composes the exact-Go 1.26.8 guard, tests, lint, a static build,
  and the formatting/tidy checks.

## Ground-rules documents

- `docs/references/go-project-layout.md` — layout, package ownership, hygiene.
- `docs/references/go-coding-standards.md` — behavior-level coding standards.

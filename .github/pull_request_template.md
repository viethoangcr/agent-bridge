<!--
Title: use Conventional Commits, e.g. "fix(acp): reject over-limit bodies".
Types: build, chore, ci, docs, feat, fix, perf, refactor, revert, style, test.
Scope and "!" (breaking change) are optional.
-->

## Summary

<!-- What changed and why, in a few direct sentences. -->

## Plan / phase

<!-- Link the phase plan or the authoritative spec entry this implements. -->

## Testing

<!--
Record the TDD evidence: the failing assertion (RED) then the passing run
(GREEN). Paste the exact commands. Note any check skipped and why.
-->

- RED:
- GREEN:

## Hygiene

- [ ] `make check` is green
- [ ] `gofmt -l .` is empty and `go mod tidy` leaves `go.mod`/`go.sum` unchanged
- [ ] No secrets, tokens, or `Authorization` values added to code, logs, or fixtures
- [ ] Out-of-scope features are not introduced

## Risk / rollback

<!--
Behavioral or contract risk, and how to revert safely (flag, revert commit,
data migration). Write "none" when genuinely none.
-->

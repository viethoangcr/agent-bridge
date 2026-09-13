# Reference: Releasing and Publishing

**Date:** 2026-09-13
**Status:** CURRENT
**Purpose:** The durable release contract for `agent-bridge`: version and tag
policy, publishing triggers, image and artifact naming, the release checklist,
rollback, and the one-time repository settings. `README.md` is the
operator-facing contract, `docs/references/go-project-layout.md` owns layout and
CI structure, and `AGENTS.md` owns contributor rules; update the owning document
before the change.

## 1. Versioning policy

- Releases follow [Semantic Versioning](https://semver.org/) (`SemVer`):
  `MAJOR.MINOR.PATCH`. Incompatible HTTP API changes bump `MAJOR`, backward
  compatible additions bump `MINOR`, and backward compatible fixes bump
  `PATCH`.
- The first public version is `v0.1.0`. Everything before `v1.0.0` is pre-1.0:
  the HTTP API is not frozen, `MINOR` may break it, and operators must read the
  release notes before upgrading. `v1.0.0` is the first stability commitment.
- A release candidate is the same version with an `-rc.N` suffix (for example
  `v0.1.0-rc.1`). Candidates are published as GitHub prereleases and never move
  `:latest` or the repository's "Latest release".
- Releases are cut only from commits already merged to `main` and already
  published as an edge image; the release job retags that exact image instead
  of rebuilding it.
- GitHub Releases are the changelog; there is no `CHANGELOG.md`. Notes are
  generated from merged pull-request titles and labels.

## 2. Tag format and ruleset

- Release tags are annotated Git tags named `vMAJOR.MINOR.PATCH`, optionally
  with `-rc.N` for candidates, for example
  `git tag -a v0.1.0 -m "v0.1.0"`.
- Create the tag on `main` after the merge commit is green and published, then
  push it with `git push origin v0.1.0`.
- A repository tag ruleset for `v*` is required: restrict updates and
  deletions, and keep creation available to maintainers. This keeps published
  versions immutable.
- Artifact filenames drop the leading `v` (`0.1.0`), while image tags and the
  GitHub Release keep it (`v0.1.0`).

## 3. Workflow triggers

- Merge to `main`: `.github/workflows/publish.yml` runs on `workflow_run` of
  the `ci` workflow with `types: [completed]` and `branches: [main]`, and its
  job runs only when
  `github.event.workflow_run.event == 'push'` and
  `github.event.workflow_run.conclusion == 'success'`. A failed or cancelled
  `ci` run publishes nothing, and `workflow_run` never executes pull-request or
  fork code. A manual `workflow_dispatch` publishes only an immutable
  `:sha-<commit>` tag.
- Release: `.github/workflows/release.yml` runs on `push` for tags matching
  `v*`. It builds and checksums the Linux binaries, creates the GitHub Release,
  and then retags the already-pushed image index for the tagged commit.
- `ci.yml` stays non-publishing; all registry pushes live only in `publish.yml`
  and `release.yml`.

## 4. Image tag scheme

The canonical image reference is `ghcr.io/viethoangcr/agent-bridge`.

Edge channel, published on every green merge to `main`:

- `:main` - moving convenience tag for the latest merged commit.
- `:sha-<12-lowercase-hex>` - immutable tag for the exact commit
  (`git rev-parse --short=12 HEAD`).

Release channel, created by retagging the commit's existing `:sha-` index with
`docker buildx imagetools create` (no rebuild):

- `:vX.Y.Z` - the exact release version.
- `:X.Y` - latest patch of that minor line.
- `:X` - latest minor of that major line.
- `:latest` - latest stable release only. Prerelease tags (`-rc.N`) never move
  `:latest`.

Both edge and release images are multi-architecture
(`linux/amd64,linux/arm64`).

## 5. Release artifacts and checksum verification

Each GitHub Release attaches:

- `agent-bridge_<X.Y.Z>_linux_amd64.tar.gz`
- `agent-bridge_<X.Y.Z>_linux_arm64.tar.gz`
- `SHA256SUMS`

Binaries are built with `CGO_ENABLED=0 -trimpath`, inspected with `readelf` for
the correct machine type, `EXEC` type, and static linkage, and the archives and
`SHA256SUMS` are produced in the runner workspace, never committed.

Verify a download before running it:

```sh
sha256sum -c SHA256SUMS
tar -xzf agent-bridge_<X.Y.Z>_linux_amd64.tar.gz
```

## 6. Release checklist

1. Confirm `main` is green: the latest `ci` run passed and `publish.yml`
   pushed the `:sha-<commit>` image for the target commit.
2. Confirm every merged PR since the last release carries one of the labels
   `feature`, `fix`, `docs`, `ci`, or `build` so the generated notes group
   correctly; unlabeled entries still appear under "Other changes".
3. Create and push the annotated tag `vMAJOR.MINOR.PATCH` (or `-rc.N`).
4. Watch the `release` workflow to completion and confirm both jobs succeed.
5. Verify the GitHub Release page lists both tarballs and `SHA256SUMS`, and
   that `docker buildx imagetools inspect` shows the expected release tags and
   platforms.
6. Announce the release in the usual channel and link the release page.

## 7. Rollback procedure

- Bad edge image (the `:main` code is broken but no release was cut): fix
  forward on `main` and let `publish.yml` republish. To point `:main` back at a
  known-good commit immediately, run the manual backfill for that commit
  (`gh workflow run publish.yml -f ref=<good-commit>`), then retag `:main` from
  its `:sha-` index with `docker buildx imagetools create`. The old `:sha-`
  tags are immutable and remain pullable throughout.
- Bad release image or binaries: delete the GitHub Release and the Git tag,
  then cut a new patch version. Do not retag an existing version; published
  versions are immutable. If the image must be withdrawn, make the GHCR package
  version private or delete that package version in the registry settings.
- A release that only moved image tags can be repaired by retagging the
  intended `:sha-` index with `docker buildx imagetools create`; record the
  reason in the release notes.

## 8. One-time repository and package settings

Performed once by a maintainer before the first release; not workflow steps.

- Actions -> General -> Workflow permissions: allow read and write so
  `packages: write` works through `GITHUB_TOKEN`; confirm no organization
  policy caps it.
- After the first successful `publish` run: open the `agent-bridge` package
  settings, connect it to the repository, and set visibility to Public. Verify
  anonymously with
  `docker pull ghcr.io/viethoangcr/agent-bridge:sha-<commit>`.
- Rulesets -> tag ruleset for `v*`: restrict updates and deletions, allow
  maintainer creation.
- Rulesets -> `main`: require the `checks` and `docker-e2e` status checks,
  require pull requests with squash merges, and enable auto-delete of head
  branches.
- Create labels `feature`, `fix`, `docs`, `ci`, and `build` exactly as named
  so `.github/release.yml` categories resolve.
- Confirm GitHub detects the MIT `LICENSE` in the repository sidebar and that
  release source archives contain `LICENSE`.
- Dry run: `gh workflow run publish.yml`, then confirm the `:sha-` image
  contains both platforms. Cut `v0.1.0-rc.1` to exercise the release path, then
  cut `v0.1.0`.

## 9. Related documents

- `README.md` - operator-facing contract: build, run, configuration, APIs, and
  limits.
- `docs/references/go-project-layout.md` - repository layout, CI structure, and
  hygiene gates.
- `AGENTS.md` - contributor rules and documentation ownership.

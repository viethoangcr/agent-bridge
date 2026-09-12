# syntax=docker/dockerfile:1
#
# Digest-pinned, target-aware runtime image for agent-bridge.
#
# The build context is the repository root so the builder stage can see the Go
# source and the locked npm manifest. All base images are pinned by the
# multi-platform manifest digest resolved with `docker buildx imagetools`.

# ---------------------------------------------------------------------------
# Stage: bridge-build
# Cross-compiles the static bridge for TARGETOS/TARGETARCH on the native build
# platform, caching Go modules and build output in BuildKit cache mounts.
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.26.8-bookworm@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81 AS bridge-build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /out/agent-bridge ./cmd/agent-bridge

# ---------------------------------------------------------------------------
# Stage: binary-verify
# Disposable, target-aware inspection. readelf ships in the pinned
# buildpack-deps builder base, so no extra tooling is installed. Only the
# verified binary is copied forward; the tools never reach the runtime image.
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.26.8-bookworm@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81 AS binary-verify

ARG TARGETARCH

COPY --from=bridge-build /out/agent-bridge /out/agent-bridge

RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) want_machine="Advanced Micro Devices X86-64" ;; \
      arm64) want_machine="AArch64" ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    machine="$(readelf -h /out/agent-bridge | sed -n 's/^ *Machine: *//p')"; \
    test "$machine" = "$want_machine"; \
    test "$(readelf -h /out/agent-bridge | sed -n 's/^ *Type: *//p')" = "EXEC (Executable file)"; \
    ! readelf -l /out/agent-bridge | grep -q 'INTERP'; \
    ! readelf -d /out/agent-bridge 2>/dev/null | grep -q 'NEEDED'

# ---------------------------------------------------------------------------
# Stage: tini
# Downloads the official Tini 0.19.0 static release asset for TARGETARCH,
# verifies its pinned SHA-256, and exposes only /usr/bin/tini. Unknown or
# wrong architectures fail the build. Tini is never installed from apt.
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.26.8-bookworm@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81 AS tini

ARG TARGETARCH

RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) url="https://github.com/krallin/tini/releases/download/v0.19.0/tini-static-amd64"; \
             sha256="c5b0666b4cb676901f90dfcb37106783c5fe2077b04590973b885950611b30ee" ;; \
      arm64) url="https://github.com/krallin/tini/releases/download/v0.19.0/tini-static-arm64"; \
             sha256="eae1d3aa50c48fb23b8cbdf4e369d0910dfc538566bfd09df89a774aa84a48b9" ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "$url" -o /usr/bin/tini; \
    echo "$sha256  /usr/bin/tini" | sha256sum -c -; \
    chmod 0755 /usr/bin/tini

# ---------------------------------------------------------------------------
# Stage: agent-deps
# Installs the locked agents with their lifecycle scripts and optional
# platform dependencies. This stage intentionally runs on the *target*
# platform (no --platform=$BUILDPLATFORM) so npm resolves TARGETARCH optional
# binaries; building it natively would bake host-arch native packages into a
# cross-built image.
# ---------------------------------------------------------------------------
FROM node:24-bookworm-slim@sha256:2fe369e969550cde8e867afc3fe370b260140cab4a23d467074295b42163d553 AS agent-deps

WORKDIR /opt/agents

COPY docker/runtime/package.json docker/runtime/package-lock.json ./
RUN npm ci --include=optional

# ---------------------------------------------------------------------------
# Stage: runtime
# Minimal Node 24 runtime: CA certificates for outbound TLS, a fixed non-root
# UID/GID 10001, root-owned non-writable agents and bridge binary, Tini as
# PID 1, and a writable HOME and workdir. No token, no insecure override, no
# healthcheck.
# ---------------------------------------------------------------------------
FROM node:24-bookworm-slim@sha256:2fe369e969550cde8e867afc3fe370b260140cab4a23d467074295b42163d553 AS runtime

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates=20250419~deb12u1; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    groupadd --gid 10001 agentbridge; \
    useradd --uid 10001 --gid 10001 --create-home --home-dir /home/agentbridge --shell /usr/sbin/nologin agentbridge; \
    install -d -o 10001 -g 10001 -m 0755 /workspace

COPY --from=agent-deps --chown=root:root /opt/agents /opt/agents
COPY --from=binary-verify /out/agent-bridge /usr/local/bin/agent-bridge
COPY --from=tini /usr/bin/tini /usr/bin/tini
COPY scripts/verify-agents.sh /usr/local/bin/verify-agents.sh

RUN set -eux; \
    chmod 0755 /usr/local/bin/agent-bridge /usr/bin/tini /usr/local/bin/verify-agents.sh; \
    chown -R root:root /opt/agents /usr/local/bin/agent-bridge /usr/bin/tini /usr/local/bin/verify-agents.sh

ENV HOME=/home/agentbridge \
    PATH=/opt/agents/node_modules/.bin:$PATH \
    AGENT_BRIDGE_HOST=0.0.0.0 \
    DISABLE_AUTOUPDATER=1 \
    NO_BROWSER=1

# Fail the build if the installed agents, commands, Tini, environment, or
# permissions violate the runtime contract. The script stays in the final
# image so `verify-agents.sh --image` can rerun it as the runtime user.
RUN /usr/local/bin/verify-agents.sh

EXPOSE 2468

USER 10001:10001
WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/agent-bridge"]

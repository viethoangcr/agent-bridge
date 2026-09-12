#!/bin/sh
# verify-agents.sh - verify the agent-bridge runtime image contract.
#
# Usage:
#   scripts/verify-agents.sh              verify the current filesystem
#   scripts/verify-agents.sh --image IMG  inspect IMG on the host, then verify
#                                         its runtime filesystem as the image's
#                                         configured non-root user.
#
# verify_local uses only commands present in the final runtime image: sh,
# coreutils, node, and the npm-installed files. ELF architecture and static
# linkage belong to the disposable Dockerfile binary-verify stage and host CI,
# so this script never calls file, readelf, ldd, or npm.
set -eu

AGENTS_ROOT=/opt/agents
AGENTS_BIN=$AGENTS_ROOT/node_modules/.bin
BRIDGE=/usr/local/bin/agent-bridge
TINI=/usr/bin/tini
EXPECTED_TINI_VERSION=0.19.0
OPENCODE_PROBE_SECONDS=10
PROBE_OUT=${TMPDIR:-/tmp}/verify-agents-probe.$$
PROBE_HOME=${TMPDIR:-/tmp}/verify-agents-probe-home.$$
TOKEN_OUT=${TMPDIR:-/tmp}/verify-agents-token.$$

cleanup() {
  rm -rf "$PROBE_OUT" "$PROBE_HOME" "$TOKEN_OUT"
}
trap 'cleanup' 0 1 2 3 15

pass() {
  printf 'PASS: %s\n' "$*"
}

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

# package_version prints the exact version recorded in the installed package
# manifest, read by Node. Human-oriented npm output is never parsed.
package_version() {
  pkg=$1
  manifest=$AGENTS_ROOT/node_modules/$pkg/package.json
  [ -f "$manifest" ] || fail "package $pkg: missing $manifest"
  version=$(node -e '
    var fs = require("fs");
    var pkg = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
    if (!pkg.version) { process.exit(2); }
    process.stdout.write(pkg.version);
  ' "$manifest" 2>/dev/null) || fail "package $pkg: cannot read version from $manifest"
  printf '%s\n' "$version"
}

# require_command prints the resolved path for cmd and fails when it is missing
# or not executable. When expected_dir is given, the PATH entry must live
# directly under it and its readlink target must stay inside
# /opt/agents/node_modules.
require_command() {
  cmd=$1
  expected_dir=${2:-}
  path=$(command -v "$cmd" 2>/dev/null || true)
  [ -n "$path" ] || fail "command $cmd: not found on PATH"
  [ -x "$path" ] || fail "command $cmd: $path is not executable"
  if [ -n "$expected_dir" ]; then
    case "$path" in
      $expected_dir/*) ;;
      *) fail "command $cmd: resolves to $path, expected under $expected_dir" ;;
    esac
    target=$(readlink -f "$path" 2>/dev/null || true)
    [ -n "$target" ] || fail "command $cmd: cannot resolve $path"
    case "$target" in
      $AGENTS_ROOT/node_modules/*) ;;
      *) fail "command $cmd: target $target escapes $AGENTS_ROOT/node_modules" ;;
    esac
  fi
  printf '%s\n' "$path"
}

env_value() {
  printenv "$1" 2>/dev/null || true
}

# probe_opencode runs the bounded, non-interactive ACP startup probe. A clean
# exit or an explicit timeout both prove the command started; browser-launch
# output and any surviving agent process are rejected.
probe_opencode() {
  # opencode writes logs, locks, and a DB under HOME/XDG. Point it at a
  # scratch HOME so the build's root invocation never bakes root-owned state
  # into the runtime user's real HOME.
  rm -rf "$PROBE_HOME"
  mkdir -p "$PROBE_HOME" || fail "probe opencode: cannot create $PROBE_HOME"
  set +e
  HOME="$PROBE_HOME" XDG_DATA_HOME="$PROBE_HOME/data" \
    XDG_CACHE_HOME="$PROBE_HOME/cache" XDG_CONFIG_HOME="$PROBE_HOME/config" \
    timeout -k 2 "$OPENCODE_PROBE_SECONDS" "$AGENTS_BIN/opencode" acp </dev/null >"$PROBE_OUT" 2>&1
  probe_rc=$?
  set -e

  case "$probe_rc" in
    0 | 124 | 137) ;;
    *) fail "probe opencode: 'opencode acp' exited $probe_rc: $(cat "$PROBE_OUT")" ;;
  esac

  if grep -qiE 'xdg-open|www-browser|failed to open browser|open[^ ]* browser|browser[^ ]* open' "$PROBE_OUT"; then
    fail "probe opencode: browser launch output: $(cat "$PROBE_OUT")"
  fi

  # Resolve each process executable instead of scanning command lines, so a
  # shell whose arguments mention an agent command is not mistaken for a
  # surviving agent process.
  leftover=''
  for exe in /proc/[0-9]*/exe; do
    pid=${exe#/proc/}
    pid=${pid%/exe}
    [ "$pid" = "$$" ] && continue
    target=$(readlink "$exe" 2>/dev/null || true)
    case "$target" in
      $AGENTS_ROOT/* | "$BRIDGE") leftover="$leftover $pid" ;;
    esac
  done
  if [ -n "$leftover" ]; then
    fail "probe opencode: leftover agent/bridge process (pid$leftover)"
  fi

  pass "probe opencode: 'opencode acp' viable (rc=$probe_rc, no browser launch, no leftover process)"
}

# verify_local validates the runtime filesystem using only final-image tools.
verify_local() {
  version=$(package_version '@agentclientprotocol/claude-agent-acp')
  [ "$version" = 0.68.0 ] || fail "package @agentclientprotocol/claude-agent-acp: version $version, want 0.68.0"
  pass "package @agentclientprotocol/claude-agent-acp=$version"

  version=$(package_version '@agentclientprotocol/codex-acp')
  [ "$version" = 1.3.0 ] || fail "package @agentclientprotocol/codex-acp: version $version, want 1.3.0"
  pass "package @agentclientprotocol/codex-acp=$version"

  version=$(package_version 'opencode-ai')
  [ "$version" = 1.18.18 ] || fail "package opencode-ai: version $version, want 1.18.18"
  pass "package opencode-ai=$version"

  path=$(require_command claude-agent-acp "$AGENTS_BIN")
  pass "command claude-agent-acp -> $path"
  path=$(require_command codex-acp "$AGENTS_BIN")
  pass "command codex-acp -> $path"
  path=$(require_command opencode "$AGENTS_BIN")
  pass "command opencode -> $path"
  path=$(require_command "$BRIDGE")
  pass "command agent-bridge -> $path"

  tini_out=$("$TINI" --version 2>&1 || true)
  case "$tini_out" in
    *"$EXPECTED_TINI_VERSION"*) pass "tini $EXPECTED_TINI_VERSION" ;;
    *) fail "tini: want $EXPECTED_TINI_VERSION, got '$tini_out'" ;;
  esac

  for spec in HOME=/home/agentbridge AGENT_BRIDGE_HOST=0.0.0.0 DISABLE_AUTOUPDATER=1 NO_BROWSER=1; do
    name=${spec%%=*}
    want=${spec#*=}
    got=$(env_value "$name")
    [ "$got" = "$want" ] || fail "env $name: got '$got', want '$want'"
    pass "env $name=$got"
  done

  if [ "$(id -u)" -ne 0 ]; then
    if [ -w "$AGENTS_ROOT" ]; then
      fail "permissions: $AGENTS_ROOT is writable by uid $(id -u)"
    fi
    if [ -w "$BRIDGE" ]; then
      fail "permissions: $BRIDGE is writable by uid $(id -u)"
    fi
    pass "permissions: $AGENTS_ROOT and $BRIDGE are not writable by uid $(id -u)"
  fi

  probe_opencode
}

# verify_image inspects the image configuration on the host, then runs
# verify_local inside the image as its configured non-root user.
verify_image() {
  image=$1
  [ -n "$image" ] || fail "usage: verify-agents.sh --image IMAGE"
  command -v docker >/dev/null 2>&1 || fail "docker: not found on PATH"
  docker image inspect "$image" >/dev/null 2>&1 || fail "image $image: not found"

  user=$(docker image inspect --format '{{.Config.User}}' "$image")
  case "$user" in
    '' | 0 | 0:* | root | root:*)
      fail "image $image: runtime user '$user' is root or unset" ;;
  esac
  uid_part=${user%%:*}
  case "$uid_part" in
    '' | *[!0-9]*)
      fail "image $image: runtime user '$user' is not a numeric non-root UID" ;;
  esac
  [ "$uid_part" -ne 0 ] || fail "image $image: runtime uid is 0"
  pass "image $image: non-root user $user"

  home=$(docker image inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$image" | sed -n 's/^HOME=//p')
  [ "$home" = /home/agentbridge ] || fail "image $image: HOME='$home', want /home/agentbridge"
  pass "image $image: HOME=$home"

  ep_len=$(docker image inspect --format '{{len .Config.Entrypoint}}' "$image")
  [ "$ep_len" = 3 ] || fail "image $image: entrypoint has $ep_len elements, want 3 (tini -- agent-bridge)"
  ep0=$(docker image inspect --format '{{index .Config.Entrypoint 0}}' "$image")
  ep1=$(docker image inspect --format '{{index .Config.Entrypoint 1}}' "$image")
  ep2=$(docker image inspect --format '{{index .Config.Entrypoint 2}}' "$image")
  if [ "$ep0" != /usr/bin/tini ] || [ "$ep1" != -- ] || [ "$ep2" != /usr/local/bin/agent-bridge ]; then
    fail "image $image: entrypoint '$ep0 $ep1 $ep2', want '/usr/bin/tini -- /usr/local/bin/agent-bridge'"
  fi
  pass "image $image: entrypoint /usr/bin/tini -- /usr/local/bin/agent-bridge"

  docker run --rm --entrypoint /usr/local/bin/verify-agents.sh "$image" \
    || fail "image $image: in-image verify_local failed"
  pass "image $image: in-image verify_local passed"

  runtime_uid=$(docker run --rm --entrypoint id "$image" -u)
  [ "$runtime_uid" -ne 0 ] || fail "image $image: runtime uid is 0"
  pass "image $image: runtime uid=$runtime_uid"

  set +e
  timeout -k 2 30 docker run --rm "$image" >"$TOKEN_OUT" 2>&1
  token_rc=$?
  set -e
  if [ "$token_rc" -eq 0 ]; then
    fail "image $image: image started without AGENT_BRIDGE_TOKEN"
  fi
  grep -q 'non-loopback' "$TOKEN_OUT" \
    || fail "image $image: missing non-loopback token refusal: $(cat "$TOKEN_OUT")"
  grep -q 'AGENT_BRIDGE_TOKEN' "$TOKEN_OUT" \
    || fail "image $image: token refusal does not mention AGENT_BRIDGE_TOKEN: $(cat "$TOKEN_OUT")"
  pass "image $image: refuses to start without AGENT_BRIDGE_TOKEN"

  running=$(docker ps --filter "ancestor=$image" --format '{{.ID}}')
  [ -z "$running" ] || fail "image $image: leftover running container(s): $(printf '%s' "$running" | tr '\n' ' ')"
  pass "image $image: no leftover running containers"
}

main() {
  case "${1:-}" in
    '') verify_local ;;
    --image)
      [ "$#" -eq 2 ] || fail "usage: verify-agents.sh --image IMAGE"
      verify_image "$2" ;;
    *) fail "usage: verify-agents.sh [--image IMAGE]" ;;
  esac
}

main "$@"

//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// dockerRunImage runs one command in a disposable container with the image
// entrypoint overridden. It fails the test on a non-zero exit so a probe can
// assert success by plain output matching.
func dockerRunImage(t *testing.T, image string, args ...string) string {
	t.Helper()
	base := append([]string{"run", "--rm", "--entrypoint", args[0], image}, args[1:]...)
	out, err := docker(base...)
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(base, " "), err, out)
	}
	return string(out)
}

// imageEnv parses the image's configured environment into key/value pairs.
func imageEnv(t *testing.T, image string) map[string]string {
	t.Helper()
	raw := dockerOrFail(t, "image", "inspect", "--format", "{{json .Config.Env}}", image)
	var entries []string
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode image env: %v (%s)", err, raw)
	}
	env := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, _ := strings.Cut(entry, "=")
		env[key] = value
	}
	return env
}

// assertImageContract checks the immutable image configuration and runtime
// filesystem contract: fixed non-root UID/GID, writable HOME, root-owned
// read-only agent files and bridge binary, required environment, exact
// versions, and the absence of npm caches, build tooling, and ELF inspection
// tools in the final layers.
func assertImageContract(t *testing.T, image string) {
	t.Helper()

	if user := strings.TrimSpace(string(dockerOrFail(t, "image", "inspect", "--format", "{{.Config.User}}", image))); user != "10001:10001" {
		t.Fatalf("image user = %q, want 10001:10001", user)
	}
	entryJSON := strings.TrimSpace(string(dockerOrFail(t, "image", "inspect", "--format", "{{json .Config.Entrypoint}}", image)))
	var entrypoint []string
	if err := json.Unmarshal([]byte(entryJSON), &entrypoint); err != nil {
		t.Fatalf("decode entrypoint: %v (%s)", err, entryJSON)
	}
	wantEntry := []string{"/usr/bin/tini", "--", "/usr/local/bin/agent-bridge"}
	if len(entrypoint) != len(wantEntry) {
		t.Fatalf("entrypoint = %v, want %v", entrypoint, wantEntry)
	}
	for i := range wantEntry {
		if entrypoint[i] != wantEntry[i] {
			t.Fatalf("entrypoint = %v, want %v", entrypoint, wantEntry)
		}
	}

	env := imageEnv(t, image)
	for key, want := range map[string]string{
		"HOME":                "/home/agentbridge",
		"AGENT_BRIDGE_HOST":   "0.0.0.0",
		"DISABLE_AUTOUPDATER": "1",
		"NO_BROWSER":          "1",
	} {
		if got := env[key]; got != want {
			t.Fatalf("image env %s = %q, want %q", key, got, want)
		}
	}
	if !strings.Contains(env["PATH"], "/opt/agents/node_modules/.bin") {
		t.Fatalf("image PATH = %q, want /opt/agents/node_modules/.bin on it", env["PATH"])
	}

	probe := `set -eu
echo "identity=$(id -u):$(id -g) home=$HOME"
test "$(id -u)" = 10001
test "$(id -g)" = 10001
test -w "$HOME"
touch "$HOME/.hardening-write" && rm "$HOME/.hardening-write"
test ! -w /opt/agents
test ! -w /usr/local/bin/agent-bridge
stat -c 'owner=%u:%g' /opt/agents /usr/local/bin/agent-bridge
for tool in gcc cc g++ make python3 node-gyp readelf file objdump nm strip strings curl wget; do
  if command -v "$tool" >/dev/null 2>&1; then echo "TOOL=$tool"; fi
done
for cache in /root/.npm "$HOME/.npm" /opt/agents/.npm /usr/local/share/.cache; do
  if [ -e "$cache" ]; then echo "CACHE=$cache"; fi
done
find /opt/agents \( -name '*.tgz' -o -name '*.tar.gz' \) 2>/dev/null | sed 's/^/TARBALL=/'
echo PROBE_OK
`
	out := dockerRunImage(t, image, "sh", "-c", probe)
	if !strings.Contains(out, "PROBE_OK") {
		t.Fatalf("image filesystem probe did not complete:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(line, "TOOL="):
			t.Errorf("final image contains build/inspection tool %q", strings.TrimPrefix(line, "TOOL="))
		case strings.HasPrefix(line, "CACHE="):
			t.Errorf("final image contains npm cache %q", strings.TrimPrefix(line, "CACHE="))
		case strings.HasPrefix(line, "TARBALL="):
			t.Errorf("final image contains package tarball %q", strings.TrimPrefix(line, "TARBALL="))
		}
	}
	if n := strings.Count(out, "owner=0:0"); n != 2 {
		t.Fatalf("expected /opt/agents and the bridge binary root-owned (owner=0:0); got %d matches:\n%s", n, out)
	}

	versionScript := `const fs = require("fs");
const want = {
  "@agentclientprotocol/claude-agent-acp": "0.68.0",
  "@agentclientprotocol/codex-acp": "1.3.0",
  "opencode-ai": "1.18.18",
};
for (const [pkg, version] of Object.entries(want)) {
  const got = JSON.parse(fs.readFileSync("/opt/agents/node_modules/" + pkg + "/package.json", "utf8")).version;
  if (got !== version) { process.stdout.write("MISMATCH " + pkg + "=" + got + "\n"); process.exit(1); }
  process.stdout.write("OK " + pkg + "=" + got + "\n");
}
`
	versions := dockerRunImage(t, image, "node", "-e", versionScript)
	for _, want := range []string{
		"OK @agentclientprotocol/claude-agent-acp=0.68.0",
		"OK @agentclientprotocol/codex-acp=1.3.0",
		"OK opencode-ai=1.18.18",
	} {
		if !strings.Contains(versions, want) {
			t.Fatalf("missing version check %q in:\n%s", want, versions)
		}
	}

	assertStaticTargetBinary(t, image)
}

// assertStaticTargetBinary extracts the bridge binary with docker cp and runs
// the host readelf against it. The final image must never be required to carry
// inspection tooling.
func assertStaticTargetBinary(t *testing.T, image string) {
	t.Helper()
	readelf, err := exec.LookPath("readelf")
	if err != nil {
		// Docker is available here, so only a genuinely non-Linux host may skip
		// ELF inspection; a Linux host without readelf is a real failure.
		if runtime.GOOS != "linux" {
			t.Skipf("host readelf unavailable on %s: %v", runtime.GOOS, err)
		}
		t.Fatalf("host readelf unavailable on Linux: %v", err)
	}

	dir := t.TempDir()
	name := "agent-bridge-extract-" + uniqueSuffix()
	dockerOrFail(t, "create", "--name", name, image)
	t.Cleanup(func() {
		if !keep() {
			_, _ = docker("rm", "-f", name)
		}
	})
	if out, err := docker("cp", name+":/usr/local/bin/agent-bridge", filepath.Join(dir, "agent-bridge")); err != nil {
		t.Fatalf("docker cp bridge binary: %v\n%s", err, out)
	}
	binary := filepath.Join(dir, "agent-bridge")

	var wantMachine string
	switch runtime.GOARCH {
	case "amd64":
		wantMachine = "Advanced Micro Devices X86-64"
	case "arm64":
		wantMachine = "AArch64"
	default:
		if runtime.GOOS != "linux" {
			t.Skipf("no target machine mapping for host arch %s on %s", runtime.GOARCH, runtime.GOOS)
		}
		t.Fatalf("no target machine mapping for host arch %s", runtime.GOARCH)
	}

	header := runHostTool(t, readelf, "-h", binary)
	if !strings.Contains(header, wantMachine) {
		t.Fatalf("bridge ELF machine does not match host %s: want %q in\n%s", runtime.GOARCH, wantMachine, header)
	}
	if !strings.Contains(header, "EXEC") && !strings.Contains(header, "DYN") {
		t.Fatalf("bridge ELF type is neither EXEC nor DYN:\n%s", header)
	}
	if segments := runHostTool(t, readelf, "-l", binary); strings.Contains(segments, "INTERP") {
		t.Fatalf("bridge has a dynamic interpreter:\n%s", segments)
	}
	if dynamic, err := exec.Command(readelf, "-d", binary).CombinedOutput(); err == nil && bytes.Contains(dynamic, []byte("NEEDED")) {
		t.Fatalf("bridge has dynamic NEEDED entries:\n%s", dynamic)
	}
}

// runHostTool runs a host inspection tool and fails on error.
func runHostTool(t *testing.T, tool string, args ...string) string {
	t.Helper()
	out, err := exec.Command(tool, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", tool, strings.Join(args, " "), err, out)
	}
	return string(out)
}

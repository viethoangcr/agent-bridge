//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestDockerHardeningContract is the cross-cutting release gate for the
// authoritative security, error, logging, child-environment, and limit
// boundaries. Image-level structural checks that need no live server run
// first; the live checks each own the container they create. Expensive real
// boundaries (10MiB ACP, 512MiB filesystem, 8KiB stderr) stay in their existing
// owning tests and are deliberately not repeated here.
func TestDockerHardeningContract(t *testing.T) {
	image := buildImage(t)

	t.Run("image", func(t *testing.T) { assertImageContract(t, image) })
	t.Run("http", func(t *testing.T) { assertHTTPContract(t, image) })
	t.Run("insecure-remote", func(t *testing.T) { assertInsecureRemoteContract(t, image) })
	t.Run("child-env", func(t *testing.T) { assertChildEnvContract(t, image) })
	t.Run("limits", func(t *testing.T) { assertInjectedLimits(t, image) })
}

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
		t.Skipf("host readelf unavailable: %v", err)
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
		t.Skipf("no target machine mapping for host arch %s", runtime.GOARCH)
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

// startContainerAllowEmpty launches a detached bridge container that may omit
// AGENT_BRIDGE_TOKEN. It exists only for the explicit insecure-remote startup
// test; every other hardening container uses the token-requiring harness.
func startContainerAllowEmpty(t *testing.T, image, token string, env map[string]string) *container {
	t.Helper()
	c := &container{
		t:              t,
		name:           "agent-bridge-e2e-harden-" + uniqueSuffix(),
		image:          image,
		token:          token,
		requestTimeout: requestTimeout,
	}
	args := []string{"run", "-d", "--name", c.name, "-p", "127.0.0.1::" + containerPort}
	if token != "" {
		args = append(args, "-e", "AGENT_BRIDGE_TOKEN="+token)
	}
	for key, value := range env {
		args = append(args, "-e", key+"="+value)
	}
	args = append(args, image)
	if out, err := docker(args...); err != nil {
		t.Fatalf("docker run: %v\n%s", err, out)
	}
	t.Cleanup(c.cleanup)

	port, err := c.discoverPort()
	if err != nil {
		t.Fatalf("%v\n%s", err, c.diagnostics())
	}
	c.port = port
	c.base = "http://127.0.0.1:" + port
	return c
}

// rawRequest performs one authenticated request with fully controlled headers.
func rawRequest(t *testing.T, c *container, method, path string, headers map[string]string, body []byte) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.base+path, reader)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := (&http.Client{Timeout: c.requestTimeout}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", method, path, err, c.diagnostics())
	}
	data, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		t.Fatalf("read %s %s: %v", method, path, readErr)
	}
	return resp, data
}

// assertProblemJSON asserts one response is the bridge's RFC 9457 document.
func assertProblemJSON(t *testing.T, resp *http.Response, data []byte, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, want, data)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var p problem
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode problem %s: %v", data, err)
	}
	if p.Type != "about:blank" || p.Status != want || p.Title == "" || p.Detail == "" {
		t.Fatalf("problem = %+v, want about:blank %d with title and detail", p, want)
	}
}

// v1Route is one representative method/path pair for a registered route family.
type v1Route struct {
	method string
	target string
}

// everyV1Route enumerates at least one method/path per registered /v1 pattern
// so the auth boundary is proven for the whole subtree rather than a sample.
func everyV1Route() []v1Route {
	return []v1Route{
		{http.MethodGet, "/v1/health"},
		{http.MethodGet, "/v1/acp"},
		{http.MethodPost, "/v1/acp/demo?agent=mock"},
		{http.MethodPost, "/v1/acp/"},
		{http.MethodGet, "/v1/acp/demo"},
		{http.MethodDelete, "/v1/acp/demo"},
		{http.MethodGet, "/v1/acp/demo/status"},
		{http.MethodGet, "/v1/acp/demo/events"},
		{http.MethodGet, "/v1/processes/config"},
		{http.MethodPost, "/v1/processes/config"},
		{http.MethodGet, "/v1/processes/run"},
		{http.MethodDelete, "/v1/processes/run"},
		{http.MethodPost, "/v1/processes/run"},
		{http.MethodPost, "/v1/processes"},
		{http.MethodGet, "/v1/processes"},
		{http.MethodGet, "/v1/processes/demo"},
		{http.MethodPost, "/v1/processes/demo/stop"},
		{http.MethodPost, "/v1/processes/demo/kill"},
		{http.MethodDelete, "/v1/processes/demo"},
		{http.MethodGet, "/v1/processes/demo/logs"},
		{http.MethodPost, "/v1/processes/demo/input"},
		{http.MethodGet, "/v1/fs/entries"},
		{http.MethodGet, "/v1/fs/file"},
		{http.MethodPut, "/v1/fs/file"},
		{http.MethodDelete, "/v1/fs/entry"},
		{http.MethodPost, "/v1/fs/mkdir"},
		{http.MethodPost, "/v1/fs/move"},
		{http.MethodGet, "/v1/fs/stat"},
		{http.MethodPost, "/v1/fs/upload-batch"},
		{http.MethodGet, "/v1/config/mcp"},
		{http.MethodPut, "/v1/config/mcp"},
		{http.MethodDelete, "/v1/config/mcp"},
		{http.MethodGet, "/v1/config/skills"},
		{http.MethodPut, "/v1/config/skills"},
		{http.MethodDelete, "/v1/config/skills"},
		{http.MethodGet, "/v1/unknown"},
	}
}

// assertHTTPContract proves the public root, the complete /v1 auth boundary,
// handler-level RFC 9457 errors, request-log redaction/shape, and that
// transport-level parser/header errors are never claimed as bridge problems.
func assertHTTPContract(t *testing.T, image string) {
	token := uniqueToken("harden")
	c := startMock(t, image, token, nil)

	rootResp, rootBody, err := c.doNoAuth(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatalf("public root: %v", err)
	}
	if rootResp.StatusCode != http.StatusOK || !bytes.Contains(rootBody, []byte(`"name":"agent-bridge"`)) {
		t.Fatalf("public root = %d %s, want 200 agent-bridge document", rootResp.StatusCode, rootBody)
	}

	for _, route := range everyV1Route() {
		t.Run("auth "+route.method+" "+route.target, func(t *testing.T) {
			resp, data, err := c.doNoAuth(route.method, route.target, nil)
			if err != nil {
				t.Fatalf("unauthenticated %s %s: %v", route.method, route.target, err)
			}
			assertProblemJSON(t, resp, data, http.StatusUnauthorized)
		})
	}

	t.Run("handler-404", func(t *testing.T) {
		resp, data := c.request(http.MethodGet, "/v1/unknown", nil)
		assertProblemJSON(t, resp, data, http.StatusNotFound)
	})
	t.Run("handler-405", func(t *testing.T) {
		resp, data := c.request(http.MethodPost, "/v1/health", nil)
		assertProblemJSON(t, resp, data, http.StatusMethodNotAllowed)
		if allow := resp.Header.Get("Allow"); allow != "GET, HEAD" {
			t.Fatalf("Allow = %q, want GET, HEAD", allow)
		}
	})

	t.Run("media-negotiation", func(t *testing.T) {
		body := rpc("ct", "initialize", map[string]any{"protocolVersion": 1})
		resp, data := rawRequest(t, c, http.MethodPost, acpPath("media", "mock"), map[string]string{"Content-Type": "text/plain"}, body)
		assertProblemJSON(t, resp, data, http.StatusUnsupportedMediaType)

		resp, data = rawRequest(t, c, http.MethodPost, acpPath("media", "mock"), map[string]string{
			"Content-Type": "application/json",
			"Accept":       "text/plain",
		}, body)
		assertProblemJSON(t, resp, data, http.StatusNotAcceptable)

		resp, data = rawRequest(t, c, http.MethodGet, "/v1/acp/media", map[string]string{"Accept": "application/json"}, nil)
		assertProblemJSON(t, resp, data, http.StatusNotAcceptable)
	})

	t.Run("invalid-server-id", func(t *testing.T) {
		body := rpc("bad", "initialize", map[string]any{"protocolVersion": 1})
		resp, data := rawRequest(t, c, http.MethodPost, "/v1/acp/"+strings.Repeat("a", 129), map[string]string{"Content-Type": "application/json"}, body)
		assertProblemJSON(t, resp, data, http.StatusBadRequest)
	})

	t.Run("filesystem-errors", func(t *testing.T) {
		resp, data := c.request(http.MethodGet, "/v1/fs/stat?path=/workspace", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("stat /workspace = %d: %s", resp.StatusCode, data)
		}
		resp, data = c.request(http.MethodGet, "/v1/fs/stat?path=/workspace/does-not-exist-missing", nil)
		assertProblemJSON(t, resp, data, http.StatusNotFound)
		resp, data = c.request(http.MethodGet, "/v1/fs/entries?directory=..%2Fescape", nil)
		assertProblemJSON(t, resp, data, http.StatusBadRequest)
	})

	// A distinctive body must never appear in request logs. The marker is
	// written through the filesystem API so the request is otherwise harmless.
	marker := "distinctive-hardening-body-" + uniqueSuffix()
	if resp, data := c.request(http.MethodPut, "/v1/fs/file?path=/workspace/hardening-marker", []byte(marker)); resp.StatusCode != http.StatusOK {
		t.Fatalf("marker write = %d: %s", resp.StatusCode, data)
	}

	t.Run("request-log", func(t *testing.T) {
		logs := string(dockerOrFail(t, "logs", c.name))
		if strings.Contains(logs, token) {
			t.Fatal("container logs leaked the bearer token")
		}
		if strings.Contains(logs, "Authorization") {
			t.Fatal("container logs leaked the Authorization header")
		}
		if strings.Contains(logs, marker) {
			t.Fatal("container logs leaked a request body")
		}
		var requests int
		for _, line := range strings.Split(logs, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || line[0] != '{' {
				continue
			}
			// The bridge logs one JSON record per request; Docker may prefix
			// agent stderr or other lines, so decode leniently.
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				continue
			}
			if rec["msg"] != "request" {
				continue
			}
			requests++
			if _, ok := rec["method"].(string); !ok {
				t.Errorf("request log missing method: %v", rec)
			}
			if _, ok := rec["uri"].(string); !ok {
				t.Errorf("request log missing uri: %v", rec)
			}
			if _, ok := rec["status"].(float64); !ok {
				t.Errorf("request log missing status: %v", rec)
			}
			if _, ok := rec["latency_ms"].(float64); !ok {
				t.Errorf("request log missing latency_ms: %v", rec)
			}
		}
		if requests == 0 {
			t.Fatalf("no request log records found:\n%s", logs)
		}
	})
}

// assertInsecureRemoteContract proves the non-loopback empty-token default
// refuses to start and the explicit unsafe override is the only escape hatch.
func assertInsecureRemoteContract(t *testing.T, image string) {
	t.Run("empty-token-refused", func(t *testing.T) {
		name := "agent-bridge-e2e-emptytoken-" + uniqueSuffix()
		t.Cleanup(func() {
			if !keep() {
				_, _ = docker("rm", "-f", name)
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", "run", "--name", name, image).CombinedOutput()
		if err == nil {
			t.Fatalf("image started on the non-loopback bind without a token: %s", out)
		}
		if !bytes.Contains(out, []byte("non-loopback")) || !bytes.Contains(out, []byte("AGENT_BRIDGE_TOKEN")) {
			t.Fatalf("empty-token refusal does not name the rule and variable: %s", out)
		}
	})

	t.Run("override-allows-startup", func(t *testing.T) {
		c := startContainerAllowEmpty(t, image, "", map[string]string{"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE": "1"})
		c.mustHealthy()
		resp, data, err := c.doNoAuth(http.MethodGet, "/v1/health", nil)
		if err != nil {
			t.Fatalf("unauthenticated health under override: %v\n%s", err, c.diagnostics())
		}
		if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(data)) != `{"status":"ok"}` {
			t.Fatalf("override health = %d %s, want 200 {\"status\":\"ok\"}", resp.StatusCode, data)
		}
	})
}

// assertChildEnvContract spawns a real agent child through the ACP surface and
// proves the four bridge-only variables are stripped while a benign
// credential-shaped variable survives. The child writes only variable names so
// no value, including the benign one, can ever be printed.
func assertChildEnvContract(t *testing.T, image string) {
	const (
		benignName  = "AGENTBRIDGE_BENIGN_CREDENTIAL_TOKEN"
		benignValue = "hardening-benign-credential-value-7f3a"
	)
	script := `env | cut -d= -f1 | sort > /workspace/agent-env-keys.txt; sleep 300`
	args, err := json.Marshal([]string{"-c", script})
	if err != nil {
		t.Fatalf("marshal agent args: %v", err)
	}
	env := map[string]string{
		"AGENT_BRIDGE_PID_FILE":               "/workspace/bridge.pid",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE":  "1",
		"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "1500",
		"AGENT_BRIDGE_CLAUDE_BIN":             "/bin/sh",
		"AGENT_BRIDGE_CLAUDE_ARGS":            string(args),
		benignName:                            benignValue,
	}
	c := startMock(t, image, uniqueToken("childenv"), env)

	// The shell agent never answers ACP, so the request times out while the
	// runtime stays live; only the file it wrote before sleeping matters.
	body := rpc("child", "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
	if resp, data := c.request(http.MethodPost, acpPath("childenv", "claude"), body); resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("shell agent initialize = %d: %s, want the bounded 504", resp.StatusCode, data)
	}

	keys := waitFileKeys(t, c, "/workspace/agent-env-keys.txt")
	for _, name := range []string{
		"AGENT_BRIDGE_TOKEN",
		"AGENT_BRIDGE_PID_FILE",
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE",
	} {
		if keys[name] {
			t.Fatalf("agent child environment leaked %s; child keys = %v", name, sortedKeys(keys))
		}
	}
	if !keys[benignName] {
		t.Fatalf("benign credential-shaped variable %s did not survive; child keys = %v", benignName, sortedKeys(keys))
	}
}

// waitFileKeys polls for a container file that holds one variable name per
// line and returns the set. Only names are read and reported.
func waitFileKeys(t *testing.T, c *container, path string) map[string]bool {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out, err := docker("exec", c.name, "cat", path)
		if err == nil {
			keys := map[string]bool{}
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if line = strings.TrimSpace(line); line != "" {
					keys[line] = true
				}
			}
			if len(keys) > 0 {
				return keys
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("agent environment keys file %s never appeared\n%s", path, c.diagnostics())
	return nil
}

// sortedKeys returns the key names in stable order for diagnostics. Only names
// are ever printed; values are never observed.
func sortedKeys(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// assertInjectedLimits exercises ACP, process input, process output, and
// process log boundaries through small runtime-injected limits. The real
// 10MiB/512MiB/8KiB production boundaries stay in their owning unit and mock
// tests and are intentionally not repeated here.
func assertInjectedLimits(t *testing.T, image string) {
	c := startMock(t, image, uniqueToken("limits"), nil)

	const smallConfig = `{"maxConcurrentProcesses":4,"defaultRunTimeoutMs":1000,"maxRunTimeoutMs":5000,"maxOutputBytes":64,"maxLogBytesPerProcess":16384,"maxInputBytesPerRequest":16}`
	resp, data := c.request(http.MethodPost, "/v1/processes/config", []byte(smallConfig))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("install small process config = %d: %s", resp.StatusCode, data)
	}

	t.Run("process-input-decoded", func(t *testing.T) {
		startBody := []byte(`{"command":"/bin/sleep","args":["300"]}`)
		resp, data := c.request(http.MethodPost, "/v1/processes", startBody)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("managed start = %d: %s", resp.StatusCode, data)
		}
		var view processView
		if err := json.Unmarshal(data, &view); err != nil || view.ID == "" {
			t.Fatalf("managed snapshot = %s (%v)", data, err)
		}
		inputBody := []byte(`{"data":"` + strings.Repeat("A", 24) + `","encoding":"utf8"}`)
		resp, data = c.request(http.MethodPost, "/v1/processes/"+view.ID+"/input", inputBody)
		assertProblemJSON(t, resp, data, http.StatusRequestEntityTooLarge)
	})

	t.Run("process-output-truncated", func(t *testing.T) {
		payload := strings.Repeat("B", 256)
		body, err := json.Marshal(map[string]any{
			"command":   "/bin/sh",
			"args":      []string{"-c", "printf '" + payload + "'"},
			"timeoutMs": 4000,
		})
		if err != nil {
			t.Fatalf("marshal run: %v", err)
		}
		resp, data := c.request(http.MethodPost, "/v1/processes/run", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("run = %d: %s", resp.StatusCode, data)
		}
		var result struct {
			Stdout          string `json:"stdout"`
			StdoutTruncated bool   `json:"stdoutTruncated"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("decode run result %s: %v", data, err)
		}
		if !result.StdoutTruncated {
			t.Fatalf("stdout not truncated at the injected 64-byte cap: %s", data)
		}
		if len(result.Stdout) > 64 {
			t.Fatalf("stdout length = %d, want <= 64", len(result.Stdout))
		}
	})

	t.Run("process-log-cap", func(t *testing.T) {
		body, err := json.Marshal(map[string]any{
			"command": "/bin/sh",
			"args":    []string{"-c", "dd if=/dev/zero bs=1000 count=40 2>/dev/null | tr '\\0' C; sleep 0.2"},
		})
		if err != nil {
			t.Fatalf("marshal managed start: %v", err)
		}
		resp, data := c.request(http.MethodPost, "/v1/processes", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("managed start = %d: %s", resp.StatusCode, data)
		}
		var view processView
		if err := json.Unmarshal(data, &view); err != nil || view.ID == "" {
			t.Fatalf("managed snapshot = %s (%v)", data, err)
		}
		waitFor(t, c, 10*time.Second, "managed log writer exit", func() bool {
			resp, data := c.request(http.MethodGet, "/v1/processes/"+view.ID, nil)
			if resp.StatusCode != http.StatusOK {
				return false
			}
			var got processView
			return json.Unmarshal(data, &got) == nil && got.Status == "exited"
		})
		resp, data = c.request(http.MethodGet, "/v1/processes/"+view.ID+"/logs", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("logs = %d: %s", resp.StatusCode, data)
		}
		var logs struct {
			Entries []struct {
				Data string `json:"data"`
			} `json:"entries"`
		}
		if err := json.Unmarshal(data, &logs); err != nil {
			t.Fatalf("decode logs %s: %v", data, err)
		}
		if len(logs.Entries) == 0 {
			t.Fatalf("expected retained log entries under the 16KiB cap: %s", data)
		}
		total := 0
		for _, entry := range logs.Entries {
			raw, err := base64.StdEncoding.DecodeString(entry.Data)
			if err != nil {
				t.Fatalf("decode log entry: %v", err)
			}
			total += len(raw)
		}
		if total > 16384 {
			t.Fatalf("retained log bytes = %d, want <= 16384", total)
		}
	})

	t.Run("acp-timeout-late-response", func(t *testing.T) {
		slow := startMock(t, image, uniqueToken("late"), map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "300"})
		initialize(t, slow, "late", "mock")

		go func() {
			_, _, _ = slow.do(http.MethodPost, acpPath("late", ""),
				[]byte(`{"jsonrpc":"2.0","id":"delay","method":"_mock/delay","params":{"ms":900}}`))
		}()
		waitFor(t, slow, 5*time.Second, "mock busy before late response", func() bool {
			view, code := status(t, slow, "late")
			return code == http.StatusOK && view.Status == "busy"
		})
		resp, body := slow.request(http.MethodPost, acpPath("late", ""), rpc("new", "session/new", map[string]any{"cwd": "/late"}))
		assertProblemJSON(t, resp, body, http.StatusGatewayTimeout)
		waitFor(t, slow, 10*time.Second, "late persisted response", func() bool {
			list, code := events(t, slow, "late", "limit=1000")
			if code != http.StatusOK {
				return false
			}
			for _, event := range list {
				if event.Kind == "response" && event.SessionID != nil && *event.SessionID != "" {
					return true
				}
			}
			return false
		})
	})

	t.Run("redacted-stderr", func(t *testing.T) {
		script := `printf 'secret=hunter2\n' 1>&2; sleep 0.3; exit 1`
		red := startMock(t, image, uniqueToken("redacth"), claudeShellEnv(script))
		body := rpc("r", "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
		resp, data := red.request(http.MethodPost, acpPath("redacth", "claude"), body)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("redacted stderr = %d: %s", resp.StatusCode, data)
		}
		var p problem
		if err := json.Unmarshal(data, &p); err != nil {
			t.Fatalf("decode 502 problem %s: %v", data, err)
		}
		if strings.Contains(p.AgentStderr, "hunter2") {
			t.Fatal("stderr problem leaked the secret value")
		}
		if !strings.Contains(p.AgentStderr, "[REDACTED]") {
			t.Fatalf("stderr problem was not redacted: %q", p.AgentStderr)
		}
	})
}

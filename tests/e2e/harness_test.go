//go:build e2e

// Package e2e contains Docker end-to-end tests for the built runtime image.
// Every file in this package carries the e2e build tag so ordinary
// `go test ./...` neither builds nor runs it.
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	containerPort = "2468"
	// requestTimeout bounds every harness HTTP request. It is deliberately
	// longer than any server-side 504 the tests assert.
	requestTimeout = 30 * time.Second
)

var (
	imageOnce sync.Once
	imageTag  string
	imageErr  error

	uniqueCounter atomic.Int64
)

// TestMain removes the per-process image after the suite unless the operator
// asked to keep it for debugging. Containers and volumes are removed by their
// own t.Cleanup handlers.
func TestMain(m *testing.M) {
	code := m.Run()
	if imageTag != "" && os.Getenv("AGENT_BRIDGE_E2E_KEEP") != "1" {
		_ = exec.Command("docker", "image", "rm", "-f", imageTag).Run()
	}
	os.Exit(code)
}

// repoRoot returns the repository root from this file's location
// (<root>/tests/e2e/harness_test.go).
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

// uniqueSuffix returns a process-unique, test-unique token used for container,
// volume, and image names.
func uniqueSuffix() string {
	return fmt.Sprintf("%d-%d-%d", os.Getpid(), time.Now().UnixNano(), uniqueCounter.Add(1))
}

// uniqueToken returns a fresh non-empty bearer token for one container.
func uniqueToken(prefix string) string {
	return prefix + "-" + uniqueSuffix()
}

// keep reports whether the operator asked to retain Docker state.
func keep() bool { return os.Getenv("AGENT_BRIDGE_E2E_KEEP") == "1" }

// buildImage builds the runtime image exactly once per test process with a
// unique tag. The image is removed at process exit unless
// AGENT_BRIDGE_E2E_KEEP=1.
func buildImage(t *testing.T) string {
	t.Helper()
	imageOnce.Do(func() {
		tag := "agent-bridge-e2e:" + uniqueSuffix()
		t.Logf("building e2e image %s", tag)
		cmd := exec.Command("docker", "buildx", "build", "--load",
			"-f", "docker/runtime/Dockerfile", "-t", tag, ".")
		cmd.Dir = repoRoot()
		out, err := cmd.CombinedOutput()
		if err != nil {
			imageErr = fmt.Errorf("docker buildx build failed: %w\n%s", err, out)
			return
		}
		imageTag = tag
	})
	if imageErr != nil {
		t.Fatalf("%v", imageErr)
	}
	return imageTag
}

// docker runs docker and returns its combined output.
func docker(args ...string) ([]byte, error) {
	return exec.Command("docker", args...).CombinedOutput()
}

// dockerOrFail runs docker and fails the test on error.
func dockerOrFail(t *testing.T, args ...string) []byte {
	t.Helper()
	out, err := docker(args...)
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// container is one running bridge container reachable over its mapped
// localhost port.
type container struct {
	t              *testing.T
	name           string
	image          string
	token          string
	volume         string
	ownsVolume     bool
	port           string
	base           string
	requestTimeout time.Duration

	mu         sync.Mutex
	logs       string
	inspect    string
	removed    bool
	lastStatus int
	lastBody   []byte
	lastSSE    int64
}

// containerOptions configures the single private container-launch seam.
type containerOptions struct {
	namePrefix      string
	token           string
	env             map[string]string
	volume          string
	newVolume       bool
	entrypoint      []string
	allowEmptyToken bool
}

// startContainerOptions launches the image detached with a unique name and a
// Docker-assigned localhost port. The token may be empty only when the caller
// sets allowEmptyToken. newVolume creates and owns a unique named volume at the
// image workdir unless an existing volume is supplied, and entrypoint overrides
// the image entrypoint for startup hooks. Cleanup is registered immediately
// after creation.
func startContainerOptions(t *testing.T, image string, opts containerOptions) *container {
	t.Helper()
	if opts.token == "" && !opts.allowEmptyToken {
		t.Fatal("startContainerOptions requires a non-empty AGENT_BRIDGE_TOKEN")
	}
	if _, ok := opts.env["AGENT_BRIDGE_TOKEN"]; ok {
		t.Fatal("startContainerOptions env must not override AGENT_BRIDGE_TOKEN")
	}
	if _, ok := opts.env["AGENT_BRIDGE_HOST"]; ok {
		t.Fatal("startContainerOptions must leave AGENT_BRIDGE_HOST unset so the image default 0.0.0.0 applies")
	}

	prefix := opts.namePrefix
	if prefix == "" {
		prefix = "agent-bridge-e2e-"
	}
	c := &container{
		t:              t,
		name:           prefix + uniqueSuffix(),
		image:          image,
		token:          opts.token,
		requestTimeout: requestTimeout,
	}

	args := []string{"run", "-d", "--name", c.name, "-p", "127.0.0.1::" + containerPort}
	if opts.token != "" {
		args = append(args, "-e", "AGENT_BRIDGE_TOKEN="+opts.token)
	}
	switch {
	case opts.newVolume:
		volume := c.name + "-vol"
		dockerOrFail(t, "volume", "create", volume)
		c.ownsVolume = true
		c.volume = volume
		vol := volume
		t.Cleanup(func() {
			if !keep() {
				_, _ = docker("volume", "rm", "-f", vol)
			}
		})
		args = append(args, "-v", volume+":/workspace")
	case opts.volume != "":
		c.volume = opts.volume
		args = append(args, "-v", opts.volume+":/workspace")
	}
	for key, value := range opts.env {
		args = append(args, "-e", key+"="+value)
	}
	if len(opts.entrypoint) > 0 {
		args = append(args, "--entrypoint", opts.entrypoint[0])
	}
	args = append(args, image)
	if len(opts.entrypoint) > 1 {
		args = append(args, opts.entrypoint[1:]...)
	}
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

// startContainer launches the image with a mandatory unique token and, when no
// volume is supplied, a harness-owned unique named volume.
func startContainer(t *testing.T, image, token string, env map[string]string, volume string) *container {
	t.Helper()
	return startContainerOptions(t, image, containerOptions{
		token:     token,
		env:       env,
		volume:    volume,
		newVolume: volume == "",
	})
}

// restartContainer starts one bridge container on an existing named volume so
// two generations can share durable state. A non-nil entrypoint overrides the
// image entrypoint, which the sentinel negative check uses to occupy a PID
// before the bridge starts.
func restartContainer(t *testing.T, image, token, volume string, env map[string]string, entrypoint []string) *container {
	t.Helper()
	return startContainerOptions(t, image, containerOptions{
		namePrefix: "agent-bridge-e2e-state-",
		token:      token,
		env:        env,
		volume:     volume,
		entrypoint: entrypoint,
	})
}

// startContainerAllowEmpty launches a detached bridge container that may omit
// AGENT_BRIDGE_TOKEN and mounts no volume. It exists only for the explicit
// insecure-remote startup test; every other hardening container uses the
// token-requiring harness.
func startContainerAllowEmpty(t *testing.T, image, token string, env map[string]string) *container {
	t.Helper()
	return startContainerOptions(t, image, containerOptions{
		namePrefix:      "agent-bridge-e2e-harden-",
		token:           token,
		env:             env,
		allowEmptyToken: true,
	})
}

// discoverPort reads the Docker-assigned host port for the container's 2468
// binding.
func (c *container) discoverPort() (string, error) {
	out, err := docker("port", c.name, containerPort+"/tcp")
	if err != nil {
		return "", fmt.Errorf("docker port %s: %w\n%s", c.name, err, out)
	}
	for _, line := range strings.Fields(strings.TrimSpace(string(out))) {
		if i := strings.LastIndex(line, ":"); i >= 0 && i+1 < len(line) {
			return line[i+1:], nil
		}
	}
	return "", fmt.Errorf("docker port %s returned no mapping: %q", c.name, out)
}

// cleanup captures diagnostics and removes the container (and a harness-owned
// volume) unless the operator asked to keep Docker state.
func (c *container) cleanup() {
	c.mu.Lock()
	if c.removed {
		c.mu.Unlock()
		return
	}
	c.removed = true
	c.mu.Unlock()

	logs, _ := docker("logs", c.name)
	inspect, _ := docker("inspect", c.name)
	c.mu.Lock()
	c.logs = string(logs)
	c.inspect = string(inspect)
	c.mu.Unlock()

	if keep() {
		return
	}
	_, _ = docker("rm", "-f", c.name)
}

// diagnostics reports the container's current logs, inspect state, HTTP status,
// and last observed SSE sequence for a failing assertion.
func (c *container) diagnostics() string {
	c.mu.Lock()
	logs, inspect := c.logs, c.inspect
	status, body, lastSSE := c.lastStatus, c.lastBody, c.lastSSE
	c.mu.Unlock()
	if logs == "" {
		out, _ := docker("logs", "--tail", "200", c.name)
		logs = string(out)
	}
	if inspect == "" {
		out, _ := docker("inspect", "--format", "{{json .State}}", c.name)
		inspect = string(out)
	}
	return fmt.Sprintf("container=%s status=%d body=%s lastSSE=%d\ninspect: %s\nlogs:\n%s",
		c.name, status, body, lastSSE, inspect, logs)
}

// doRequest is the single request primitive. It attaches the bearer token when
// auth is set, applies controlled headers, and defaults a JSON Content-Type for
// a request body unless the caller set one. The status and body are recorded
// for failure diagnostics.
func (c *container) doRequest(method, path string, headers map[string]string, auth bool, body []byte) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return nil, nil, err
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, nil, err
	}
	data, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	c.mu.Lock()
	c.lastStatus, c.lastBody = resp.StatusCode, data
	c.mu.Unlock()
	if readErr != nil {
		return resp, data, readErr
	}
	return resp, data, nil
}

// do performs one authenticated request and returns the response and body
// without failing on a non-2xx status.
func (c *container) do(method, path string, body []byte) (*http.Response, []byte, error) {
	return c.doRequest(method, path, nil, strings.HasPrefix(path, "/v1/"), body)
}

// request performs one authenticated request and fails the test on a transport
// error. It returns the response and body for status assertions.
func (c *container) request(method, path string, body []byte) (*http.Response, []byte) {
	c.t.Helper()
	resp, data, err := c.do(method, path, body)
	if err != nil {
		c.t.Fatalf("%s %s: %v\n%s", method, path, err, c.diagnostics())
	}
	return resp, data
}

// getNoAuth performs a GET without a bearer token so tests can prove that
// /v1/* routes, including health, are authenticated.
func (c *container) getNoAuth(path string) (*http.Response, []byte) {
	c.t.Helper()
	resp, data, err := c.doNoAuth(http.MethodGet, path, nil)
	if err != nil {
		c.t.Fatalf("GET %s without auth: %v\n%s", path, err, c.diagnostics())
	}
	return resp, data
}

// doNoAuth performs one unauthenticated request.
func (c *container) doNoAuth(method, path string, body []byte) (*http.Response, []byte, error) {
	return c.doRequest(method, path, nil, false, body)
}

// rawRequest performs one authenticated request with fully controlled headers.
func rawRequest(t *testing.T, c *container, method, path string, headers map[string]string, body []byte) (*http.Response, []byte) {
	t.Helper()
	resp, data, err := c.doRequest(method, path, headers, true, body)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", method, path, err, c.diagnostics())
	}
	return resp, data
}

// waitHealthy polls the authenticated health endpoint until it returns 200 or
// ctx expires. A non-200 status or transport error is retried; the terminal
// failure reports the last status/body and full diagnostics.
func (c *container) waitHealthy(ctx context.Context) error {
	c.t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastStatus int
	var lastBody []byte
	var lastErr error
	for {
		resp, body, err := c.do(http.MethodGet, "/v1/health", nil)
		if err != nil {
			lastErr = err
		} else if resp.StatusCode == http.StatusOK {
			return nil
		} else {
			lastStatus, lastBody, lastErr = resp.StatusCode, body, nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("container never became healthy: status=%d body=%s err=%v\n%s",
				lastStatus, lastBody, lastErr, c.diagnostics())
		case <-ticker.C:
		}
	}
}

// mustHealthy starts polling and fails the test if the container never becomes
// healthy within the bounded context.
func (c *container) mustHealthy() {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.waitHealthy(ctx); err != nil {
		c.t.Fatal(err)
	}
}

// noteSSE records the last sequence delivered over SSE for failure diagnostics.
func (c *container) noteSSE(seq int64) {
	c.mu.Lock()
	if seq > c.lastSSE {
		c.lastSSE = seq
	}
	c.mu.Unlock()
}

// copyWorkspace copies the container's /workspace volume into a host temp dir.
func copyWorkspace(t *testing.T, c *container) string {
	t.Helper()
	dir := t.TempDir()
	if out, err := docker("cp", c.name+":/workspace/.", dir); err != nil {
		t.Fatalf("docker cp workspace: %v\n%s", err, out)
	}
	return dir
}

// readProcStat returns the raw /proc/<pid>/stat line inside the container and
// whether the process still exists.
func readProcStat(name string, pid int) (string, bool) {
	out, err := docker("exec", name, "cat", "/proc/"+strconv.Itoa(pid)+"/stat")
	if err != nil {
		return "", false
	}
	return string(out), true
}

// parseProcState extracts the state byte from a /proc/<pid>/stat line. The
// command name may contain spaces or parentheses, so the state is the first
// field after the final ')'.
func parseProcState(data string) (byte, bool) {
	end := strings.LastIndex(data, ")")
	if end < 0 || end+2 >= len(data) {
		return 0, false
	}
	return data[end+2], true
}

// processState returns the /proc state of pid inside the container and whether
// the process still exists. A missing process reports ok=false.
func processState(t *testing.T, name string, pid int) (byte, bool) {
	t.Helper()
	stat, ok := readProcStat(name, pid)
	if !ok {
		return 0, false
	}
	return parseProcState(stat)
}

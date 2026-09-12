//go:build e2e

// Package e2e contains Docker end-to-end tests for the built runtime image.
// Every file in this package carries the e2e build tag so ordinary
// `go test ./...` neither builds nor runs it.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// startContainer launches the image detached with a mandatory unique token, a
// unique named volume mounted at the image workdir, and an unset
// AGENT_BRIDGE_HOST so the image default 0.0.0.0 applies. Cleanup is registered
// immediately after creation.
func startContainer(t *testing.T, image, token string, env map[string]string, volume string) *container {
	t.Helper()
	if token == "" {
		t.Fatal("startContainer requires a non-empty AGENT_BRIDGE_TOKEN")
	}
	if _, ok := env["AGENT_BRIDGE_TOKEN"]; ok {
		t.Fatal("startContainer env must not override AGENT_BRIDGE_TOKEN")
	}
	if _, ok := env["AGENT_BRIDGE_HOST"]; ok {
		t.Fatal("startContainer must leave AGENT_BRIDGE_HOST unset so the image default 0.0.0.0 applies")
	}

	c := &container{
		t:              t,
		name:           "agent-bridge-e2e-" + uniqueSuffix(),
		image:          image,
		token:          token,
		requestTimeout: requestTimeout,
	}
	if volume == "" {
		volume = c.name + "-vol"
		dockerOrFail(t, "volume", "create", volume)
		c.ownsVolume = true
		vol := volume
		t.Cleanup(func() {
			if os.Getenv("AGENT_BRIDGE_E2E_KEEP") != "1" {
				_, _ = docker("volume", "rm", "-f", vol)
			}
		})
	}
	c.volume = volume

	args := []string{
		"run", "-d", "--name", c.name,
		"-p", "127.0.0.1::" + containerPort,
		"-e", "AGENT_BRIDGE_TOKEN=" + token,
		"-v", volume + ":/workspace",
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

	if os.Getenv("AGENT_BRIDGE_E2E_KEEP") == "1" {
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

// do performs one authenticated request and returns the response and body
// without failing on a non-2xx status.
func (c *container) do(method, path string, body []byte) (*http.Response, []byte, error) {
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
	if strings.HasPrefix(path, "/v1/") {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	data, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		return resp, data, readErr
	}
	c.mu.Lock()
	c.lastStatus, c.lastBody = resp.StatusCode, data
	c.mu.Unlock()
	return resp, data, nil
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
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, nil, err
	}
	data, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, data, readErr
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

// rpcBody marshal one JSON-RPC 2.0 request, preserving the Go type of id.
func rpc(id any, method string, params any) []byte {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		panic("marshal rpc: " + err.Error())
	}
	return body
}

// rpcError is one JSON-RPC error object.
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// rpcEnvelope is the decoded JSON-RPC envelope used by client and agent
// messages.
type rpcEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// decodeEnvelope decodes one JSON-RPC envelope, failing the test on bad JSON.
func decodeEnvelope(t *testing.T, data []byte) rpcEnvelope {
	t.Helper()
	var env rpcEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("decode envelope %q: %v", data, err)
	}
	return env
}

// acpPath builds the ACP POST path with an optional agent query.
func acpPath(serverID, agent string) string {
	path := "/v1/acp/" + serverID
	if agent != "" {
		path += "?agent=" + agent
	}
	return path
}

// postACP posts one ACP envelope and returns the raw status and body.
func postACP(t *testing.T, c *container, serverID, agent string, body []byte) (int, []byte) {
	t.Helper()
	resp, data := c.request(http.MethodPost, acpPath(serverID, agent), body)
	return resp.StatusCode, data
}

// initialize performs the ACP initialize handshake and returns the response
// envelope.
func initialize(t *testing.T, c *container, serverID, agent string) rpcEnvelope {
	t.Helper()
	body := rpc("initialize", "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{},
	})
	code, data := postACP(t, c, serverID, agent, body)
	if code != http.StatusOK {
		t.Fatalf("initialize %s = %d: %s\n%s", serverID, code, data, c.diagnostics())
	}
	return decodeEnvelope(t, data)
}

// eventView mirrors the persisted event DTO returned by the events endpoint.
type eventView struct {
	Seq         int64           `json:"seq"`
	Kind        string          `json:"kind"`
	Method      *string         `json:"method"`
	Payload     json.RawMessage `json:"payload"`
	SessionID   *string         `json:"sessionId"`
	CreatedAtMs int64           `json:"createdAtMs"`
}

// statusView mirrors the status endpoint DTO.
type statusView struct {
	ServerID     string   `json:"serverId"`
	Agent        string   `json:"agent"`
	Status       string   `json:"status"`
	CreatedAtMs  int64    `json:"createdAtMs"`
	LastEventSeq int64    `json:"lastEventSeq"`
	SessionIDs   []string `json:"sessionIds"`
	PID          *int     `json:"pid"`
	UpdatedAtMs  int64    `json:"updatedAtMs"`
}

// serverListView mirrors the server-list endpoint DTO.
type serverListView struct {
	Servers []struct {
		ServerID    string `json:"serverId"`
		Agent       string `json:"agent"`
		Status      string `json:"status"`
		CreatedAtMs int64  `json:"createdAtMs"`
		UpdatedAtMs int64  `json:"updatedAtMs"`
	} `json:"servers"`
}

// getJSON performs an authenticated GET and decodes a 200 body into out.
func getJSON(t *testing.T, c *container, path string, out any) (int, []byte) {
	t.Helper()
	resp, data := c.request(http.MethodGet, path, nil)
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, out); err != nil {
			t.Fatalf("decode %s: %v (%s)\n%s", path, err, data, c.diagnostics())
		}
	}
	return resp.StatusCode, data
}

// status fetches one server's status view.
func status(t *testing.T, c *container, serverID string) (statusView, int) {
	t.Helper()
	var view statusView
	code, _ := getJSON(t, c, "/v1/acp/"+serverID+"/status", &view)
	return view, code
}

// events fetches one server's events with an optional query string.
func events(t *testing.T, c *container, serverID, query string) ([]eventView, int) {
	t.Helper()
	var out struct {
		Events []eventView `json:"events"`
	}
	path := "/v1/acp/" + serverID + "/events"
	if query != "" {
		path += "?" + query
	}
	code, _ := getJSON(t, c, path, &out)
	return out.Events, code
}

// waitFor polls cond every 25ms until it is true or the deadline passes.
func waitFor(t *testing.T, c *container, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s\n%s", desc, c.diagnostics())
}

// waitEventMethod polls durable events for the first event carrying method.
func waitEventMethod(t *testing.T, c *container, serverID, method string, timeout time.Duration) eventView {
	t.Helper()
	var found eventView
	waitFor(t, c, timeout, "event "+method, func() bool {
		list, code := events(t, c, serverID, "limit=1000")
		if code != http.StatusOK {
			return false
		}
		for _, event := range list {
			if event.Method != nil && *event.Method == method {
				found = event
				return true
			}
		}
		return false
	})
	return found
}

// sseEvent is one decoded Server-Sent Events frame.
type sseEvent struct {
	Event   string
	ID      int64
	Data    []byte
	Comment bool
	Text    string
}

// sseStream decodes one SSE response with support for comments, event, id, and
// multi-line data fields.
type sseStream struct {
	resp    *http.Response
	scanner *bufio.Scanner

	cur      sseEvent
	hasField bool
}

// subscribe opens an authenticated SSE stream, optionally resuming after
// lastEventID.
func (c *container) subscribe(ctx context.Context, serverID, lastEventID string) (*sseStream, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/acp/"+serverID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	// A dedicated client without a Timeout so long-lived streams are bounded
	// only by the request context.
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("sse %s status %d: %s", serverID, resp.StatusCode, data)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	return &sseStream{resp: resp, scanner: scanner}, nil
}

// close releases the underlying connection.
func (s *sseStream) close() { _ = s.resp.Body.Close() }

// next returns the next frame. Comments are returned immediately with
// Comment=true. It returns io.EOF when the stream ends normally.
func (s *sseStream) next() (sseEvent, error) {
	for s.scanner.Scan() {
		line := s.scanner.Text()
		switch {
		case line == "":
			if s.hasField {
				event := s.cur
				s.cur = sseEvent{}
				s.hasField = false
				return event, nil
			}
		case strings.HasPrefix(line, ":"):
			return sseEvent{Comment: true, Text: strings.TrimSpace(line[1:])}, nil
		case strings.HasPrefix(line, "event:"):
			s.cur.Event = strings.TrimSpace(line[len("event:"):])
			s.hasField = true
		case strings.HasPrefix(line, "id:"):
			if n, err := strconv.ParseInt(strings.TrimSpace(line[len("id:"):]), 10, 64); err == nil {
				s.cur.ID = n
			}
			s.hasField = true
		case strings.HasPrefix(line, "data:"):
			value := strings.TrimPrefix(line[len("data:"):], " ")
			if s.cur.Data != nil {
				s.cur.Data = append(s.cur.Data, '\n')
			}
			s.cur.Data = append(s.cur.Data, value...)
			s.hasField = true
		default:
			// Unknown SSE field: ignore its content but keep the frame open.
			s.hasField = true
		}
	}
	if err := s.scanner.Err(); err != nil {
		return sseEvent{}, err
	}
	return sseEvent{}, io.EOF
}

// nextMessage returns the next non-comment message frame, recording its
// sequence for diagnostics.
func (s *sseStream) nextMessage(c *container) (sseEvent, error) {
	for {
		event, err := s.next()
		if err != nil {
			return sseEvent{}, err
		}
		if event.Comment {
			continue
		}
		c.noteSSE(event.ID)
		return event, nil
	}
}

// waitComment waits for one comment (heartbeat) frame or the deadline.
func (s *sseStream) waitComment(timeout time.Duration) (sseEvent, error) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return sseEvent{}, errors.New("no SSE comment within deadline")
		}
		event, err := s.next()
		if err != nil {
			return sseEvent{}, err
		}
		if event.Comment {
			return event, nil
		}
	}
}

// problem is the RFC 9457 problem document plus the agentStderr extension.
type problem struct {
	Type        string `json:"type"`
	Title       string `json:"title"`
	Status      int    `json:"status"`
	Detail      string `json:"detail"`
	AgentStderr string `json:"agentStderr"`
}

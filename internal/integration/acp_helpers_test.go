package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/app"
	"github.com/viethoangcr/agent-bridge/internal/mockagent"
)

// TestMain is the only place this package re-execs itself as the private mock
// agent: the bridge resolves ?agent=mock to os.Executable(), so the mock
// subprocess is this test binary with AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1.
func TestMain(m *testing.M) {
	if os.Getenv("AGENT_BRIDGE_INTERNAL_MOCK_AGENT") == "1" {
		if err := mockagent.Run(context.Background(), os.Stdin, os.Stdout, os.Stderr); err != nil {
			fmt.Fprintln(os.Stderr, "mock agent:", err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// bridge is a running app.Run instance reachable over real HTTP.
type bridge struct {
	t      *testing.T
	base   string
	dbPath string
	dir    string
	token  string
	client *http.Client
	cancel context.CancelFunc
	done   chan error
}

// bridgeOptions are the shared harness inputs: extra environment overrides and,
// when set, the bearer token that also authenticates health polling.
type bridgeOptions struct {
	env   map[string]string
	token string
}

// startBridge starts the full app against real HTTP and waits for a healthy
// health endpoint. A non-empty token enables auth and authenticated polling.
func startBridge(t *testing.T, opts bridgeOptions) *bridge {
	t.Helper()
	addr := freeAddr(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	dir := t.TempDir()
	b := &bridge{
		t:      t,
		base:   "http://" + addr,
		dbPath: filepath.Join(dir, "bridge.db"),
		dir:    dir,
		token:  opts.token,
		client: &http.Client{},
	}
	env := map[string]string{
		"AGENT_BRIDGE_HOST":        host,
		"AGENT_BRIDGE_PORT":        port,
		"AGENT_BRIDGE_DB":          b.dbPath,
		"AGENT_BRIDGE_PID_FILE":    filepath.Join(dir, "bridge.pid"),
		"AGENT_BRIDGE_LOG_LEVEL":   "error",
		"AGENT_BRIDGE_IDLE_TTL_MS": "0",
	}
	if opts.token != "" {
		env["AGENT_BRIDGE_TOKEN"] = opts.token
	}
	for key, value := range opts.env {
		env[key] = value
	}
	getenv := func(key string) string { return env[key] }

	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.done = make(chan error, 1)
	go func() {
		b.done <- app.Run(ctx, getenv, app.IO{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard})
	}()
	b.waitHealthy()
	return b
}

func (b *bridge) waitHealthy() {
	b.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, b.base+"/v1/health", nil)
		if err != nil {
			b.t.Fatalf("new health request: %v", err)
		}
		if b.token != "" {
			req.Header.Set("Authorization", "Bearer "+b.token)
		}
		resp, err := b.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.stop()
	b.t.Fatal("bridge never served health")
}

func (b *bridge) stop() {
	if b.cancel == nil {
		return
	}
	b.cancel()
	select {
	case <-b.done:
	case <-time.After(20 * time.Second):
		b.t.Error("bridge did not stop within 20s")
	}
	b.cancel = nil
}

func (b *bridge) do(method, path, body string, headers map[string]string) *http.Response {
	b.t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, b.base+path, reader)
	if err != nil {
		b.t.Fatalf("new request %s %s: %v", method, path, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		b.t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func (b *bridge) getJSON(path string, out any) int {
	b.t.Helper()
	resp := b.do(http.MethodGet, path, "", nil)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, out); err != nil {
			b.t.Fatalf("decode %s: %v (%s)", path, err, body)
		}
	}
	return resp.StatusCode
}

func (b *bridge) postEnvelope(serverID, agent, body string) (int, []byte) {
	b.t.Helper()
	path := "/v1/acp/" + serverID
	if agent != "" {
		path += "?agent=" + url.QueryEscape(agent)
	}
	resp := b.do(http.MethodPost, path, body, nil)
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

type eventView struct {
	Seq       int64           `json:"seq"`
	Kind      string          `json:"kind"`
	Method    *string         `json:"method"`
	Payload   json.RawMessage `json:"payload"`
	SessionID *string         `json:"sessionId"`
}

type statusView struct {
	ServerID     string   `json:"serverId"`
	Agent        string   `json:"agent"`
	Status       string   `json:"status"`
	LastEventSeq int64    `json:"lastEventSeq"`
	SessionIDs   []string `json:"sessionIds"`
	PID          *int     `json:"pid"`
}

type listView struct {
	Servers []struct {
		ServerID string `json:"serverId"`
		Agent    string `json:"agent"`
		Status   string `json:"status"`
	} `json:"servers"`
}

func (b *bridge) status(serverID string) (statusView, int) {
	var view statusView
	code := b.getJSON("/v1/acp/"+serverID+"/status", &view)
	return view, code
}

func (b *bridge) events(serverID, query string) ([]eventView, int) {
	var out struct {
		Events []eventView `json:"events"`
	}
	path := "/v1/acp/" + serverID + "/events"
	if query != "" {
		path += "?" + query
	}
	code := b.getJSON(path, &out)
	return out.Events, code
}

func (b *bridge) initialize(serverID, agent string) {
	b.t.Helper()
	code, body := b.postEnvelope(serverID, agent, `{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
	if code != http.StatusOK {
		b.t.Fatalf("initialize %s = %d: %s", serverID, code, body)
	}
}

func (b *bridge) sessionNew(serverID, cwd string) string {
	b.t.Helper()
	body := `{"jsonrpc":"2.0","id":"new","method":"session/new","params":{"cwd":"` + cwd + `"}}`
	code, data := b.postEnvelope(serverID, "", body)
	if code != http.StatusOK {
		b.t.Fatalf("session/new %s = %d: %s", serverID, code, data)
	}
	var out struct {
		Result struct {
			SessionID string `json:"sessionId"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &out); err != nil || out.Result.SessionID == "" {
		b.t.Fatalf("session/new response = %s (%v)", data, err)
	}
	return out.Result.SessionID
}

func (b *bridge) prompt(serverID, sessionID, text string) (int, []byte) {
	b.t.Helper()
	body := `{"jsonrpc":"2.0","id":"prompt","method":"session/prompt","params":{"sessionId":"` + sessionID + `","prompt":[{"type":"text","text":"` + text + `"}]}}`
	return b.postEnvelope(serverID, "", body)
}

func (b *bridge) waitFor(timeout time.Duration, desc string, cond func() bool) {
	b.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.t.Fatalf("timed out waiting for %s", desc)
}

// waitEventMethod polls the durable events until one carries method.
func (b *bridge) waitEventMethod(serverID, method string, timeout time.Duration) eventView {
	b.t.Helper()
	var found eventView
	b.waitFor(timeout, "event "+method, func() bool {
		events, _ := b.events(serverID, "limit=1000")
		for _, event := range events {
			if event.Method != nil && *event.Method == method {
				found = event
				return true
			}
		}
		return false
	})
	return found
}

type sseEvent struct {
	ID   int64
	Data []byte
}

// sse connects to the server-sent events endpoint and reads up to want message
// events. The caller supplies the bounding context.
func (b *bridge) sse(ctx context.Context, serverID, lastEventID string, want int) ([]sseEvent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.base+"/v1/acp/"+serverID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sse status %d: %s", resp.StatusCode, data)
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var events []sseEvent
	var current sseEvent
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if current.Data != nil {
				events = append(events, current)
				current = sseEvent{}
			}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "id: "):
			current.ID, _ = strconv.ParseInt(strings.TrimPrefix(line, "id: "), 10, 64)
		case strings.HasPrefix(line, "data: "):
			current.Data = append(current.Data, []byte(strings.TrimPrefix(line, "data: "))...)
		}
		if len(events) >= want {
			return events, nil
		}
	}
	return events, scanner.Err()
}

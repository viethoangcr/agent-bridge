// Package integration exercises the full Phase 03 composition against the real
// Phase 02 store, runtime, resolver, and private mock agent over real HTTP.
//
// TestMain is the only place this package re-execs itself as the private mock
// agent: the bridge resolves ?agent=mock to os.Executable(), so the mock
// subprocess is this test binary with AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1.
package integration

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/app"
	"github.com/viethoangcr/agent-bridge/internal/mockagent"

	_ "modernc.org/sqlite"
)

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
	client *http.Client
	cancel context.CancelFunc
	done   chan error
}

func startBridge(t *testing.T, extra map[string]string) *bridge {
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
	for key, value := range extra {
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
		resp, err := b.client.Get(b.base + "/v1/health")
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

// TestACPLifecycle verifies the end-to-end ACP lifecycle through the app.
func TestACPLifecycle(t *testing.T) {
	t.Run("session-prompt-attribution-and-events", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()

		b.initialize("life", "mock")
		sessionID := b.sessionNew("life", "/work")
		code, body := b.prompt("life", sessionID, "hello")
		if code != http.StatusOK {
			t.Fatalf("session/prompt = %d: %s", code, body)
		}

		events, code := b.events("life", "sessionId="+url.QueryEscape(sessionID))
		if code != http.StatusOK {
			t.Fatalf("events = %d", code)
		}
		if len(events) < 3 {
			t.Fatalf("session events = %d, want at least the two updates and the response", len(events))
		}
		var sawResponse bool
		var sawUpdate int
		for _, event := range events {
			if event.SessionID == nil || *event.SessionID != sessionID {
				t.Errorf("event seq %d sessionId = %v, want %q", event.Seq, event.SessionID, sessionID)
			}
			switch event.Kind {
			case "response":
				sawResponse = true
				var envelope struct {
					ID json.RawMessage `json:"id"`
				}
				if err := json.Unmarshal(event.Payload, &envelope); err == nil && string(envelope.ID) == `"prompt"` {
					if !bytes.Equal(event.Payload, body) {
						t.Errorf("response event payload = %s, want exact HTTP response %s", event.Payload, body)
					}
				}
			case "notification":
				if event.Method != nil && *event.Method == "session/update" {
					sawUpdate++
				}
			}
		}
		if !sawResponse {
			t.Error("no response event carried the retained request sessionId")
		}
		if sawUpdate != 2 {
			t.Errorf("session/update notifications = %d, want 2", sawUpdate)
		}

		status, code := b.status("life")
		if code != http.StatusOK || status.Status != string(acpstore.StatusIdle) {
			t.Fatalf("status = %d %+v, want idle", code, status)
		}
		if status.PID == nil {
			t.Error("live status omitted the current PID")
		}
		if len(status.SessionIDs) != 1 || status.SessionIDs[0] != sessionID {
			t.Errorf("status sessionIds = %v, want [%s]", status.SessionIDs, sessionID)
		}
	})

	t.Run("raw-compaction-and-exact-response", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()

		// The multi-line object is not one JSONL record; only Runtime.Post's
		// whitespace-only compaction lets the mock answer it.
		pretty := "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": \"init\",\n  \"method\": \"initialize\",\n  \"params\": {\"protocolVersion\": 1, \"clientCapabilities\": {}}\n}"
		code, body := b.postEnvelope("compact", "mock", pretty)
		want := `{"jsonrpc":"2.0","id":"init","result":{"agentCapabilities":{},"authMethods":[],"protocolVersion":1}}`
		if code != http.StatusOK || string(body) != want {
			t.Fatalf("compacted response = %d %q, want 200 %q", code, body, want)
		}
	})

	t.Run("reverse-call-and-notification", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()

		b.initialize("call", "mock")
		sessionID := b.sessionNew("call", "/call")

		type result struct {
			code int
			body []byte
		}
		done := make(chan result, 1)
		go func() {
			code, body := b.prompt("call", sessionID, "[mock:request_permission]")
			done <- result{code: code, body: body}
		}()

		request := b.waitEventMethod("call", "session/request_permission", 10*time.Second)
		var envelope struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(request.Payload, &envelope); err != nil || len(envelope.ID) == 0 {
			t.Fatalf("permission request payload = %s (%v)", request.Payload, err)
		}
		clientResponse := `{"jsonrpc":"2.0","id":` + string(envelope.ID) + `,"result":{"outcome":{"outcome":"selected","optionId":"allow"}}}`
		if code, body := b.postEnvelope("call", "", clientResponse); code != http.StatusAccepted {
			t.Fatalf("client response = %d: %s", code, body)
		}
		if code, body := b.postEnvelope("call", "", `{"jsonrpc":"2.0","method":"notifications/initialized"}`); code != http.StatusAccepted {
			t.Fatalf("notification = %d: %s", code, body)
		}

		select {
		case got := <-done:
			if got.code != http.StatusOK {
				t.Fatalf("reverse-call prompt = %d: %s", got.code, got.body)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("prompt did not complete after the client response")
		}
	})

	t.Run("duplicate-numeric-ids", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()
		b.initialize("dup", "mock")

		go func() {
			_, _ = b.postEnvelope("dup", "", `{"jsonrpc":"2.0","id":1,"method":"_mock/delay","params":{"ms":3000}}`)
		}()
		b.waitFor(5*time.Second, "busy after numeric request", func() bool {
			status, code := b.status("dup")
			return code == http.StatusOK && status.Status == string(acpstore.StatusBusy)
		})
		code, body := b.postEnvelope("dup", "", `{"jsonrpc":"2.0","id":1.0,"method":"_mock/delay","params":{"ms":3000}}`)
		if code != http.StatusConflict {
			t.Fatalf("lexically duplicate numeric ID = %d: %s", code, body)
		}
	})

	t.Run("invalid-metadata", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()

		longID := strings.Repeat("a", 129)
		longSession := strings.Repeat("s", 1025)
		cases := []struct {
			name string
			body string
		}{
			{"null-id", `{"jsonrpc":"2.0","id":null,"method":"initialize","params":{"protocolVersion":1}}`},
			{"over-limit-id", `{"jsonrpc":"2.0","id":"` + longID + `","method":"initialize","params":{"protocolVersion":1}}`},
			{"over-limit-session", `{"jsonrpc":"2.0","id":"p","method":"session/prompt","params":{"sessionId":"` + longSession + `","prompt":[]}}`},
		}
		for _, tc := range cases {
			code, body := b.postEnvelope("meta", "", tc.body)
			if code != http.StatusBadRequest {
				t.Errorf("%s = %d: %s, want 400", tc.name, code, body)
			}
		}
	})

	t.Run("correlation-capacity", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()
		b.initialize("corr", "mock")

		const total = 300
		results := make(chan int, total)
		for i := 0; i < total; i++ {
			go func(i int) {
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"_mock/delay","params":{"ms":30000}}`, i)
				code, _ := b.postEnvelope("corr", "", body)
				results <- code
			}(i)
		}
		overflow, rejected := 0, 0
		deadline := time.After(20 * time.Second)
		for overflow < total-256 && rejected < total-256 {
			select {
			case code := <-results:
				switch code {
				case http.StatusTooManyRequests:
					overflow++
				case http.StatusOK:
					// Still blocked; the response only arrives after cleanup.
				default:
					t.Errorf("unexpected correlation result %d", code)
				}
			case <-deadline:
				t.Fatalf("saw %d capacity rejections, want %d", overflow, total-256)
			}
		}
	})

	t.Run("runtime-capacity", func(t *testing.T) {
		if testing.Short() {
			t.Skip("spawns 64 real mock runtimes")
		}
		b := startBridge(t, nil)
		defer b.stop()

		for i := 0; i < 64; i++ {
			serverID := fmt.Sprintf("cap-%02d", i)
			code, body := b.postEnvelope(serverID, "mock", `{"jsonrpc":"2.0","id":"d","method":"_mock/delay","params":{"ms":0}}`)
			if code != http.StatusOK {
				t.Fatalf("runtime %d = %d: %s", i, code, body)
			}
		}
		code, body := b.postEnvelope("cap-over", "mock", `{"jsonrpc":"2.0","id":"d","method":"_mock/delay","params":{"ms":0}}`)
		if code != http.StatusTooManyRequests {
			t.Fatalf("65th runtime = %d: %s, want 429", code, body)
		}
	})

	t.Run("timeout-grace-busy-late-event", func(t *testing.T) {
		b := startBridge(t, map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "150"})
		defer b.stop()
		b.initialize("slow", "mock")

		// Block the single mock loop so the lifecycle request cannot be read
		// until its request deadline has already moved it into grace.
		go func() {
			_, _ = b.postEnvelope("slow", "", `{"jsonrpc":"2.0","id":"delay","method":"_mock/delay","params":{"ms":900}}`)
		}()
		b.waitFor(5*time.Second, "mock loop busy", func() bool {
			status, code := b.status("slow")
			return code == http.StatusOK && status.Status == string(acpstore.StatusBusy)
		})

		code, body := b.postEnvelope("slow", "", `{"jsonrpc":"2.0","id":"new","method":"session/new","params":{"cwd":"/late"}}`)
		if code != http.StatusGatewayTimeout {
			t.Fatalf("lifecycle timeout = %d: %s, want 504", code, body)
		}
		status, code := b.status("slow")
		if code != http.StatusOK || status.Status != string(acpstore.StatusBusy) {
			t.Fatalf("status during grace = %d %+v, want durable busy", code, status)
		}

		// The mock's late response commits the request and becomes observable.
		var sessionID string
		b.waitFor(10*time.Second, "late session/new response", func() bool {
			events, _ := b.events("slow", "limit=1000")
			for _, event := range events {
				if event.Kind == "response" && event.SessionID != nil && *event.SessionID != "" {
					sessionID = *event.SessionID
					return true
				}
			}
			return false
		})
		if sessionID == "" {
			t.Fatal("late response did not carry a committed session")
		}
	})

	t.Run("invalid-stdout-synthetic", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()
		b.initialize("badout", "mock")

		code, body := b.postEnvelope("badout", "", `{"jsonrpc":"2.0","id":"bad","method":"_mock/invalid_stdout","params":{"line":"{not json"}}`)
		if code != http.StatusOK {
			t.Fatalf("invalid_stdout = %d: %s", code, body)
		}
		b.waitEventMethod("badout", "_adapter/invalid_stdout", 5*time.Second)
	})

	t.Run("process-exit-reinitialize", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()
		b.initialize("exit", "mock")

		if code, body := b.postEnvelope("exit", "", `{"jsonrpc":"2.0","id":"bye","method":"_mock/exit","params":{}}`); code != http.StatusOK {
			t.Fatalf("_mock/exit = %d: %s", code, body)
		}
		// Wait until the exited runtime is no longer the live generation.
		b.waitFor(10*time.Second, "exited server without a live PID", func() bool {
			status, code := b.status("exit")
			return code == http.StatusOK && status.Status == string(acpstore.StatusExited) && status.PID == nil
		})
		code, body := b.postEnvelope("exit", "", `{"jsonrpc":"2.0","id":"again","method":"_mock/delay","params":{"ms":0}}`)
		if code != http.StatusConflict {
			t.Fatalf("non-initialize after exit = %d: %s, want 409", code, body)
		}
		b.initialize("exit", "mock")
	})

	t.Run("list-status-events-sorted", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()
		b.initialize("zeta", "mock")
		b.initialize("alpha", "mock")

		var list listView
		if code := b.getJSON("/v1/acp", &list); code != http.StatusOK {
			t.Fatalf("list = %d", code)
		}
		if len(list.Servers) != 2 || list.Servers[0].ServerID != "alpha" || list.Servers[1].ServerID != "zeta" {
			t.Fatalf("list order = %+v, want alpha then zeta", list.Servers)
		}

		events, _ := b.events("alpha", "order=desc")
		for i := 1; i < len(events); i++ {
			if events[i].Seq >= events[i-1].Seq {
				t.Fatalf("desc events not strictly descending: %v", events)
			}
		}
	})

	t.Run("stderr-safe-502", func(t *testing.T) {
		b := startBridge(t, map[string]string{
			"AGENT_BRIDGE_CLAUDE_BIN":  "/bin/sh",
			"AGENT_BRIDGE_CLAUDE_ARGS": `["-c","printf 'password=hunter2\n' 1>&2; sleep 0.3; exit 1"]`,
		})
		defer b.stop()

		code, body := b.postEnvelope("secret", "claude", `{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
		if code != http.StatusBadGateway {
			t.Fatalf("failing agent = %d: %s, want 502", code, body)
		}
		if bytes.Contains(body, []byte("hunter2")) {
			t.Fatalf("502 leaked the secret: %s", body)
		}
		var problem struct {
			AgentStderr string `json:"agentStderr"`
		}
		if err := json.Unmarshal(body, &problem); err != nil {
			t.Fatalf("502 body = %s (%v)", body, err)
		}
		if problem.AgentStderr == "" {
			t.Fatalf("agentStderr omitted after a process had started: %s", body)
		}
		if !strings.Contains(problem.AgentStderr, "[REDACTED]") {
			t.Fatalf("agentStderr not redacted: %q", problem.AgentStderr)
		}
		if strings.Contains(problem.AgentStderr, "hunter2") {
			t.Fatalf("agentStderr leaked the secret: %q", problem.AgentStderr)
		}
	})
}

// TestACPReplayAfterRestart covers SSE replay, bridge restart, and startup
// reconciliation of stale live rows.
func TestACPReplayAfterRestart(t *testing.T) {
	b := startBridge(t, nil)
	b.initialize("replay", "mock")
	sessionID := b.sessionNew("replay", "/replay")
	if code, body := b.prompt("replay", sessionID, "hello"); code != http.StatusOK {
		t.Fatalf("prompt = %d: %s", code, body)
	}
	events, _ := b.events("replay", "limit=1000")
	if len(events) < 3 {
		t.Fatalf("events = %d, want at least 3", len(events))
	}
	firstSeq := events[0].Seq
	dbPath := b.dbPath

	// Replay live through SSE from an exact int64 sequence.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	replayed, err := b.sse(ctx, "replay", strconv.FormatInt(firstSeq, 10), len(events)-1)
	cancel()
	if err != nil {
		t.Fatalf("live sse reconnect: %v", err)
	}
	if len(replayed) != len(events)-1 || replayed[0].ID != events[1].Seq {
		t.Fatalf("live replay IDs = %+v, want starting at %d", replayed, events[1].Seq)
	}

	b.stop()

	// Restart the bridge on the same database; persisted events replay again.
	restarted := startBridge(t, map[string]string{"AGENT_BRIDGE_DB": dbPath})
	defer restarted.stop()
	replayed, err = restarted.sse(ctx2(t), "replay", "0", len(events))
	if err != nil {
		t.Fatalf("restart sse replay: %v", err)
	}
	if len(replayed) != len(events) {
		t.Fatalf("restart replay = %d events, want %d", len(replayed), len(events))
	}
	for i := 1; i < len(replayed); i++ {
		if replayed[i].ID <= replayed[i-1].ID {
			t.Fatalf("restart replay not strictly ascending: %+v", replayed)
		}
	}
	status, code := restarted.status("replay")
	if code != http.StatusOK || status.Status != string(acpstore.StatusExited) {
		t.Fatalf("restarted status = %d %+v, want exited", code, status)
	}
}

func ctx2(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestACPDeleteRace races DELETE with an active Post lease and a failed prune.
func TestACPDeleteRace(t *testing.T) {
	t.Run("delete-releases-active-post", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()
		b.initialize("del", "mock")
		b.sessionNew("del", "/del")
		if code, body := b.prompt("del", "mock-session-1", "hello"); code != http.StatusOK {
			t.Fatalf("prompt = %d: %s", code, body)
		}

		// Hold an activity lease with a request whose timeout is far away.
		blocked := make(chan int, 1)
		go func() {
			code, _ := b.postEnvelope("del", "", `{"jsonrpc":"2.0","id":"hold","method":"_mock/delay","params":{"ms":60000}}`)
			blocked <- code
		}()
		b.waitFor(5*time.Second, "active lease", func() bool {
			status, code := b.status("del")
			return code == http.StatusOK && status.Status == string(acpstore.StatusBusy)
		})

		start := time.Now()
		resp := b.do(http.MethodDelete, "/v1/acp/del", "", nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE = %d, want 204", resp.StatusCode)
		}
		if elapsed := time.Since(start); elapsed > 10*time.Second {
			t.Fatalf("DELETE waited %s, want immediate signal-and-wait kill", elapsed)
		}
		select {
		case code := <-blocked:
			if code == http.StatusOK {
				t.Fatalf("blocked post succeeded after DELETE")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("active post did not release after DELETE")
		}

		if code := b.getJSON("/v1/acp/del/status", nil); code != http.StatusNotFound {
			t.Fatalf("status after DELETE = %d, want 404", code)
		}
	})

	t.Run("delete-prune-failure-retry", func(t *testing.T) {
		b := startBridge(t, nil)
		defer b.stop()
		b.initialize("prune", "mock")

		side, err := sql.Open("sqlite", "file:"+b.dbPath+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatalf("open side db: %v", err)
		}
		defer side.Close()
		if _, err := side.Exec(`CREATE TRIGGER block_prune BEFORE DELETE ON servers BEGIN SELECT RAISE(ABORT, 'prune blocked'); END`); err != nil {
			t.Fatalf("create prune trigger: %v", err)
		}

		resp := b.do(http.MethodDelete, "/v1/acp/prune", "", nil)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("DELETE with failed prune = %d: %s, want 500", resp.StatusCode, body)
		}
		status, code := b.status("prune")
		if code != http.StatusOK || status.Status != string(acpstore.StatusExited) {
			t.Fatalf("status after failed prune = %d %+v, want exited", code, status)
		}

		if _, err := side.Exec(`DROP TRIGGER block_prune`); err != nil {
			t.Fatalf("drop prune trigger: %v", err)
		}
		resp = b.do(http.MethodDelete, "/v1/acp/prune", "", nil)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("DELETE retry = %d, want 204", resp.StatusCode)
		}
		if code := b.getJSON("/v1/acp/prune/status", nil); code != http.StatusNotFound {
			t.Fatalf("status after retry DELETE = %d, want 404", code)
		}
	})
}

// TestACPReconcileStaleLiveRows asserts startup reconciliation rewrites stale
// live rows without ever signaling a persisted PID.
func TestACPReconcileStaleLiveRows(t *testing.T) {
	sleep := exec.Command("sleep", "60")
	if err := sleep.Start(); err != nil {
		t.Fatalf("start sentinel process: %v", err)
	}
	defer func() { _ = sleep.Process.Kill(); _, _ = sleep.Process.Wait() }()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bridge.db")
	store, err := acpstore.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open seed store: %v", err)
	}
	if _, err := store.CreateServer(context.Background(), "stale", "mock"); err != nil {
		t.Fatalf("create seed server: %v", err)
	}
	if err := store.SetLive(context.Background(), "stale", sleep.Process.Pid); err != nil {
		t.Fatalf("set seed live: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("close seed store: %v", err)
	}

	b := startBridge(t, map[string]string{"AGENT_BRIDGE_DB": dbPath})
	defer b.stop()

	status, code := b.status("stale")
	if code != http.StatusOK || status.Status != string(acpstore.StatusExited) || status.PID != nil {
		t.Fatalf("reconciled status = %d %+v, want exited without a PID", code, status)
	}
	if err := syscall.Kill(sleep.Process.Pid, 0); err != nil {
		t.Fatalf("startup reconciliation signaled the persisted PID: %v", err)
	}
}

// TestRealHeartbeat15Seconds is the sole real-time test: it waits for exactly
// one production 15-second SSE heartbeat under a 20-second outer deadline.
// It is deliberately excluded from the focused `-race` run by its name and is
// run once, without -race, via the full `go test ./...` sweep.
func TestRealHeartbeat15Seconds(t *testing.T) {
	if testing.Short() {
		t.Skip("real-time heartbeat test skipped under -short")
	}
	b := startBridge(t, nil)
	defer b.stop()
	b.initialize("beat", "mock")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.base+"/v1/acp/beat", nil)
	if err != nil {
		t.Fatalf("new sse request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := b.client.Do(req)
	if err != nil {
		t.Fatalf("sse connect: %v", err)
	}
	defer resp.Body.Close()

	start := time.Now()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if scanner.Text() == ": heartbeat" {
			elapsed := time.Since(start)
			if elapsed < 10*time.Second || elapsed > 20*time.Second {
				t.Fatalf("heartbeat after %s, want one production 15s interval inside 20s", elapsed)
			}
			return
		}
	}
	t.Fatalf("no heartbeat received within the 20s deadline: %v", scanner.Err())
}

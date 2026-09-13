//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// mockCWD is a path that is writable by the image's non-root runtime user.
const mockCWD = "/workspace"

// startMock starts and health-checks a mock-capable bridge container.
func startMock(t *testing.T, image, token string, env map[string]string) *container {
	t.Helper()
	c := startContainer(t, image, token, env, "")
	c.mustHealthy()
	return c
}

// sessionNewResult posts session/new and returns its sessionId and echoed cwd.
func sessionNewResult(t *testing.T, c *container, serverID, cwd string) (string, string) {
	t.Helper()
	code, data := postACP(t, c, serverID, "", rpc("session-new", "session/new", map[string]any{"cwd": cwd}))
	if code != http.StatusOK {
		t.Fatalf("session/new %s = %d: %s\n%s", serverID, code, data, c.diagnostics())
	}
	env := decodeEnvelope(t, data)
	if env.Error != nil {
		t.Fatalf("session/new returned error: %+v", env.Error)
	}
	var result struct {
		SessionID string `json:"sessionId"`
		CWD       string `json:"cwd"`
	}
	if err := json.Unmarshal(env.Result, &result); err != nil || result.SessionID == "" {
		t.Fatalf("session/new result = %s (%v)", env.Result, err)
	}
	return result.SessionID, result.CWD
}

// prompt posts session/prompt synchronously and returns the status and body.
func prompt(t *testing.T, c *container, serverID, sessionID, text string) (int, []byte) {
	t.Helper()
	return postACP(t, c, serverID, "", rpc("prompt", "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []map[string]any{{"type": "text", "text": text}},
	}))
}

// claudeShellEnv points the claude agent at a shell script for deterministic
// process-failure and stderr assertions.
func claudeShellEnv(script string) map[string]string {
	args, err := json.Marshal([]string{"-c", script})
	if err != nil {
		panic(err)
	}
	return map[string]string{
		"AGENT_BRIDGE_CLAUDE_BIN":  "/bin/sh",
		"AGENT_BRIDGE_CLAUDE_ARGS": string(args),
	}
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

// processView is the subset of a managed-process snapshot these tests need.
type processView struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	PID    *int   `json:"pid"`
}

// problem is the RFC 9457 problem document plus the agentStderr extension.
type problem struct {
	Type        string `json:"type"`
	Title       string `json:"title"`
	Status      int    `json:"status"`
	Detail      string `json:"detail"`
	AgentStderr string `json:"agentStderr"`
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

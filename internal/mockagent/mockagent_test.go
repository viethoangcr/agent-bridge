package mockagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// rpcMessage is the subset of a JSONL record the tests inspect.
type rpcMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpcErrorBody   `json:"error"`
}

type rpcErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// requestLine builds a request or, when id is empty, a notification.
func requestLine(t *testing.T, id, method string, params any) string {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params for %s: %v", method, err)
	}
	methodJSON, err := json.Marshal(method)
	if err != nil {
		t.Fatalf("marshal method %s: %v", method, err)
	}
	if id == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","method":%s,"params":%s}`, methodJSON, raw)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":%s,"params":%s}`, id, methodJSON, raw)
}

// responseLine builds a client response to an agent reverse-call.
func responseLine(t *testing.T, id string, result any) string {
	t.Helper()
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, id, raw)
}

// run executes the mock synchronously over an in-memory input script.
func run(t *testing.T, input string) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := Run(context.Background(), strings.NewReader(input), &out, &errOut)
	return out.String(), errOut.String(), err
}

func parseOutput(t *testing.T, out string) []rpcMessage {
	t.Helper()
	var msgs []rpcMessage
	for _, l := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if l == "" {
			continue
		}
		var m rpcMessage
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("output line %q is not a JSON object: %v", l, err)
		}
		msgs = append(msgs, m)
	}
	return msgs
}

func initLine(t *testing.T, id string) string {
	t.Helper()
	return requestLine(t, id, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{},
	})
}

func newSessionLine(t *testing.T, id, cwd string) string {
	t.Helper()
	return requestLine(t, id, "session/new", map[string]any{"cwd": cwd})
}

// TestProtocolFixture drives the ordered lifecycle: session ops are rejected
// before initialize, initialize runs once, a session is created and listed,
// load/resume attach to that same-process session, close removes it, and a
// later load is rejected as unknown. Raw request IDs are preserved.
func TestProtocolFixture(t *testing.T) {
	input := strings.Join([]string{
		newSessionLine(t, "1", "/work"),
		initLine(t, "2"),
		initLine(t, "3"),
		newSessionLine(t, "4", "/work"),
		requestLine(t, "5", "session/list", map[string]any{}),
		requestLine(t, "6", "session/load", map[string]any{"sessionId": "mock-session-1", "cwd": "/work", "mcpServers": []any{}}),
		requestLine(t, "7", "session/resume", map[string]any{"sessionId": "mock-session-1", "cwd": "/work"}),
		requestLine(t, "8", "session/close", map[string]any{"sessionId": "mock-session-1"}),
		requestLine(t, "9", "session/load", map[string]any{"sessionId": "mock-session-1", "cwd": "/work"}),
		requestLine(t, "10", "session/list", map[string]any{}),
	}, "\n") + "\n"

	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 10 {
		t.Fatalf("got %d responses, want 10:\n%s", len(msgs), out)
	}

	if msgs[0].Error == nil || msgs[0].Error.Code != -32600 {
		t.Fatalf("session/new before initialize error = %+v, want -32600", msgs[0].Error)
	}
	if msgs[1].Error != nil {
		t.Fatalf("initialize error = %+v, want success", msgs[1].Error)
	}
	var initResult struct {
		ProtocolVersion int `json:"protocolVersion"`
	}
	if err := json.Unmarshal(msgs[1].Result, &initResult); err != nil {
		t.Fatalf("initialize result %s: %v", msgs[1].Result, err)
	}
	if initResult.ProtocolVersion != 1 {
		t.Fatalf("protocolVersion = %d, want 1", initResult.ProtocolVersion)
	}
	if msgs[2].Error == nil || msgs[2].Error.Code != -32600 {
		t.Fatalf("second initialize error = %+v, want -32600", msgs[2].Error)
	}

	var newResult struct {
		SessionID string `json:"sessionId"`
		CWD       string `json:"cwd"`
	}
	if err := json.Unmarshal(msgs[3].Result, &newResult); err != nil {
		t.Fatalf("session/new result %s: %v", msgs[3].Result, err)
	}
	if newResult.SessionID != "mock-session-1" {
		t.Fatalf("sessionId = %q, want deterministic mock-session-1", newResult.SessionID)
	}
	if newResult.CWD != "/work" {
		t.Fatalf("session/new cwd = %q, want /work", newResult.CWD)
	}

	type listResult struct {
		Sessions []struct {
			SessionID string `json:"sessionId"`
			CWD       string `json:"cwd"`
		} `json:"sessions"`
	}
	var listed listResult
	if err := json.Unmarshal(msgs[4].Result, &listed); err != nil {
		t.Fatalf("session/list result %s: %v", msgs[4].Result, err)
	}
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "mock-session-1" || listed.Sessions[0].CWD != "/work" {
		t.Fatalf("session/list = %+v, want one session with cwd /work", listed.Sessions)
	}

	if string(msgs[5].Result) != "null" {
		t.Fatalf("session/load known result = %s, want null", msgs[5].Result)
	}
	if string(msgs[6].Result) != "{}" {
		t.Fatalf("session/resume known result = %s, want {}", msgs[6].Result)
	}
	if string(msgs[7].Result) != "{}" {
		t.Fatalf("session/close result = %s, want {}", msgs[7].Result)
	}
	if msgs[8].Error == nil || msgs[8].Error.Code != -32002 {
		t.Fatalf("load after close error = %+v, want -32002", msgs[8].Error)
	}
	var closedList listResult
	if err := json.Unmarshal(msgs[9].Result, &closedList); err != nil {
		t.Fatalf("final session/list result %s: %v", msgs[9].Result, err)
	}
	if len(closedList.Sessions) != 0 {
		t.Fatalf("session/list after close = %+v, want empty", closedList.Sessions)
	}

	wantIDs := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	for i, m := range msgs {
		if string(m.ID) != wantIDs[i] {
			t.Fatalf("response %d id = %s, want %s", i, m.ID, wantIDs[i])
		}
	}
}

// TestFreshRunRejectsKnownSession proves mock state is process-local: a session
// created by one Run is unknown to a fresh Run, which emits no synthesized
// prompt/update traffic.
func TestFreshRunRejectsKnownSession(t *testing.T) {
	first := strings.Join([]string{
		initLine(t, "1"),
		newSessionLine(t, "2", "/work"),
	}, "\n") + "\n"
	firstOut, _, err := run(t, first)
	if err != nil {
		t.Fatalf("first Run() = %v, want nil", err)
	}
	firstMsgs := parseOutput(t, firstOut)
	if len(firstMsgs) != 2 {
		t.Fatalf("first run responses = %d, want 2:\n%s", len(firstMsgs), firstOut)
	}
	var created struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(firstMsgs[1].Result, &created); err != nil || created.SessionID == "" {
		t.Fatalf("first session result %s, err %v", firstMsgs[1].Result, err)
	}

	fresh := strings.Join([]string{
		initLine(t, "1"),
		requestLine(t, "2", "session/load", map[string]any{"sessionId": created.SessionID, "cwd": "/work"}),
		requestLine(t, "3", "session/resume", map[string]any{"sessionId": created.SessionID, "cwd": "/work"}),
	}, "\n") + "\n"
	freshOut, _, err := run(t, fresh)
	if err != nil {
		t.Fatalf("fresh Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, freshOut)
	if len(msgs) != 3 {
		t.Fatalf("fresh responses = %d, want 3:\n%s", len(msgs), freshOut)
	}
	if msgs[1].Error == nil || msgs[1].Error.Code != -32002 {
		t.Fatalf("fresh session/load error = %+v, want -32002", msgs[1].Error)
	}
	if msgs[2].Error == nil || msgs[2].Error.Code != -32002 {
		t.Fatalf("fresh session/resume error = %+v, want -32002", msgs[2].Error)
	}
	if strings.Contains(freshOut, "session/update") {
		t.Fatalf("fresh process synthesized update traffic:\n%s", freshOut)
	}
}

// TestMalformedAndUnknownInput asserts deterministic JSON-RPC errors for
// malformed JSON, non-objects, id:null, missing params, and unknown methods.
func TestMalformedAndUnknownInput(t *testing.T) {
	input := strings.Join([]string{
		`{"jsonrpc":`,
		`[1,2]`,
		`{"jsonrpc":"2.0","id":null,"method":"initialize","params":{}}`,
		initLine(t, "1"),
		newSessionLine(t, "2", ""),
		requestLine(t, "3", "does/not-exist", map[string]any{}),
	}, "\n") + "\n"

	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 6 {
		t.Fatalf("got %d responses, want 6:\n%s", len(msgs), out)
	}
	cases := []struct {
		index int
		code  int
	}{
		{0, -32700},
		{1, -32600},
		{2, -32600},
		{4, -32602},
		{5, -32601},
	}
	for _, tc := range cases {
		if msgs[tc.index].Error == nil || msgs[tc.index].Error.Code != tc.code {
			t.Fatalf("response %d error = %+v, want code %d", tc.index, msgs[tc.index].Error, tc.code)
		}
	}
	if msgs[3].Error != nil {
		t.Fatalf("valid initialize error = %+v, want success", msgs[3].Error)
	}
}

// TestRejectsMissingOrWrongJSONRPC asserts every input form must carry exactly
// "jsonrpc":"2.0"; missing, wrong, or non-string versions return the
// deterministic invalid-request error, including for notification-shaped
// input that would otherwise be unanswered.
func TestRejectsMissingOrWrongJSONRPC(t *testing.T) {
	input := strings.Join([]string{
		`{"id":1,"method":"initialize","params":{"protocolVersion":1}}`,
		`{"jsonrpc":"1.0","id":2,"method":"initialize","params":{"protocolVersion":1}}`,
		`{"jsonrpc":2,"id":3,"method":"initialize","params":{"protocolVersion":1}}`,
		`{"jsonrpc":null,"method":"session/cancel"}`,
		`{"method":"session/cancel"}`,
		`{"jsonrpc":"2.0","id":6,"method":"initialize","params":{"protocolVersion":1}}`,
	}, "\n") + "\n"

	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 6 {
		t.Fatalf("got %d responses, want 6:\n%s", len(msgs), out)
	}
	for i := 0; i < 5; i++ {
		if msgs[i].Error == nil || msgs[i].Error.Code != -32600 {
			t.Fatalf("response %d error = %+v, want code -32600", i, msgs[i].Error)
		}
	}
	if msgs[5].Error != nil {
		t.Fatalf("valid initialize error = %+v, want success", msgs[5].Error)
	}
}

// TestInitializeRequiresProtocolVersion asserts initialize params are strict.
func TestInitializeRequiresProtocolVersion(t *testing.T) {
	out, _, err := run(t, requestLine(t, "1", "initialize", map[string]any{})+"\n")
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 1 || msgs[0].Error == nil || msgs[0].Error.Code != -32602 {
		t.Fatalf("initialize without protocolVersion = %+v, want -32602", msgs)
	}
}

// TestNotificationProducesNoResponse asserts notifications are never answered.
func TestNotificationProducesNoResponse(t *testing.T) {
	input := strings.Join([]string{
		initLine(t, "1"),
		requestLine(t, "", "session/cancel", map[string]any{"sessionId": "mock-session-1"}),
	}, "\n") + "\n"
	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 1 {
		t.Fatalf("got %d responses, want 1 (initialize only):\n%s", len(msgs), out)
	}
}

// TestCleanEOFAndCancellation asserts empty input and a canceled context both
// exit cleanly with nil.
func TestCleanEOFAndCancellation(t *testing.T) {
	out, _, err := run(t, "")
	if err != nil {
		t.Fatalf("Run(empty) = %v, want nil", err)
	}
	if out != "" {
		t.Fatalf("Run(empty) wrote %q, want nothing", out)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	if err := Run(ctx, pr, io.Discard, io.Discard); err != nil {
		t.Fatalf("Run(canceled) = %v, want nil", err)
	}
}

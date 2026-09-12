// Package mockagent implements the private, deterministic ACP JSONL mock
// agent. It is reachable only through the private
// AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1 dispatch in internal/app and keeps all
// state in process memory. It is not a public protocol compatibility promise,
// persisted store, CLI, or HTTP feature.
//
// Private wire-level control hooks (undocumented, not a public contract, shared
// with later-phase integration tests):
//
//	_mock/delay           request {"ms":<int>}       sleeps, then replies {}
//	_mock/invalid_stdout  request {"line":"<raw>"}   writes one malformed stdout line, then replies {}
//	_mock/stderr          request {"line":"<raw>"}   writes one stderr line, then replies {}
//	_mock/exit            request                    replies {} then stops the loop cleanly
//
// A session/prompt whose prompt content contains the literal
// "[mock:request_permission]" emits a session/request_permission reverse-call
// and withholds the prompt response until a client response echoes the exact
// reverse-call id.
package mockagent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

// JSON-RPC and ACP error codes emitted deterministically.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeNotFound       = -32002
)

// ACP v1 method names handled by the mock.
const (
	methodInitialize    = "initialize"
	methodSessionNew    = "session/new"
	methodSessionLoad   = "session/load"
	methodSessionResume = "session/resume"
	methodSessionList   = "session/list"
	methodSessionClose  = "session/close"
	methodSessionPrompt = "session/prompt"

	methodRequestPermission = "session/request_permission"
	methodSessionUpdate     = "session/update"
)

// protocolVersion is the only ACP version the mock negotiates.
const protocolVersion = 1

// message is the routing subset of one JSONL record.
type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Run serves the strict mock ACP JSONL loop until EOF or ctx cancellation,
// returning nil on either clean exit and a write error if the output stream
// fails.
func Run(ctx context.Context, in io.Reader, out, errOut io.Writer) error {
	lines, readErr, cleanup := readLines(in)
	defer cleanup()

	s := &mockServer{
		ctx:      ctx,
		lines:    lines,
		out:      out,
		errOut:   errOut,
		sessions: make(map[string]string),
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-lines:
			if !ok {
				return lastReadError(readErr)
			}
			if err := s.handle(line); err != nil {
				return err
			}
			if s.stop {
				return nil
			}
		}
	}
}

// readLines streams newline-delimited records from in on an unbuffered channel
// so permission waits can consume the next client line. cleanup closes done and
// the reader (when closable) so the reader goroutine always exits.
func readLines(in io.Reader) (lines <-chan []byte, readErr <-chan error, cleanup func()) {
	linesCh := make(chan []byte)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(linesCh)
		r := bufio.NewReader(in)
		for {
			line, err := r.ReadBytes('\n')
			if len(line) > 0 {
				select {
				case linesCh <- line:
				case <-done:
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				return
			}
		}
	}()
	return linesCh, errCh, func() {
		close(done)
		if c, ok := in.(io.Closer); ok {
			_ = c.Close()
		}
	}
}

func lastReadError(readErr <-chan error) error {
	select {
	case err := <-readErr:
		return err
	default:
		return nil
	}
}

type mockServer struct {
	ctx           context.Context
	lines         <-chan []byte
	out           io.Writer
	errOut        io.Writer
	stop          bool
	initialized   bool
	sessions      map[string]string
	sessionSeq    int
	permissionSeq int
}

// handle classifies and routes one line. Notifications are never answered;
// malformed or otherwise invalid input produces one deterministic JSON-RPC
// error.
func (s *mockServer) handle(line []byte) error {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return nil
	}
	if !json.Valid(trimmed) {
		return s.writeError(nil, codeParseError, "parse error")
	}
	var msg message
	if err := json.Unmarshal(trimmed, &msg); err != nil {
		return s.writeError(nil, codeInvalidRequest, "invalid request")
	}
	// Every input form, including notifications, must be JSON-RPC 2.0. A
	// missing or wrong version is a deterministic invalid request.
	if msg.JSONRPC != "2.0" {
		return s.writeError(msg.ID, codeInvalidRequest, "invalid request")
	}
	switch {
	case msg.Method != "" && msg.ID == nil:
		return nil
	case msg.Method != "" && isNull(msg.ID):
		return s.writeError(nil, codeInvalidRequest, "invalid request")
	case msg.Method != "":
		return s.dispatch(msg)
	case msg.ID != nil:
		// Unsolicited client response: ignored.
		return nil
	default:
		return s.writeError(nil, codeInvalidRequest, "invalid request")
	}
}

func (s *mockServer) dispatch(msg message) error {
	switch msg.Method {
	case methodInitialize,
		methodSessionNew,
		methodSessionLoad,
		methodSessionResume,
		methodSessionList,
		methodSessionClose,
		methodSessionPrompt:
		if !s.initialized && msg.Method != methodInitialize {
			return s.writeError(msg.ID, codeInvalidRequest, "not initialized")
		}
	case methodDelay, methodInvalidStdout, methodStderr, methodExit:
		// Control hooks work before initialize.
	default:
		return s.writeError(msg.ID, codeMethodNotFound, "method not found")
	}

	switch msg.Method {
	case methodInitialize:
		return s.initialize(msg)
	case methodSessionNew:
		return s.sessionNew(msg)
	case methodSessionLoad:
		return s.sessionLoad(msg)
	case methodSessionResume:
		return s.sessionResume(msg)
	case methodSessionList:
		return s.sessionList(msg)
	case methodSessionClose:
		return s.sessionClose(msg)
	case methodSessionPrompt:
		return s.sessionPrompt(msg)
	case methodDelay:
		return s.delay(msg)
	case methodInvalidStdout:
		return s.invalidStdout(msg)
	case methodStderr:
		return s.stderr(msg)
	case methodExit:
		return s.exit(msg)
	default:
		return s.writeError(msg.ID, codeMethodNotFound, "method not found")
	}
}

func (s *mockServer) writeUpdate(sessionID string, update map[string]any) error {
	return s.writeNotification(methodSessionUpdate, map[string]any{
		"sessionId": sessionID,
		"update":    update,
	})
}

func (s *mockServer) writeResult(id json.RawMessage, result any) error {
	body, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return s.writeObject(func(buf *bytes.Buffer) {
		buf.WriteString(`{"jsonrpc":"2.0","id":`)
		buf.Write(rawID(id))
		buf.WriteString(`,"result":`)
		buf.Write(body)
		buf.WriteByte('}')
	})
}

func (s *mockServer) writeError(id json.RawMessage, code int, text string) error {
	body, err := json.Marshal(rpcError{Code: code, Message: text})
	if err != nil {
		return err
	}
	return s.writeObject(func(buf *bytes.Buffer) {
		buf.WriteString(`{"jsonrpc":"2.0","id":`)
		buf.Write(rawID(id))
		buf.WriteString(`,"error":`)
		buf.Write(body)
		buf.WriteByte('}')
	})
}

func (s *mockServer) writeRequest(id json.RawMessage, method string, params any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	methodJSON, err := json.Marshal(method)
	if err != nil {
		return err
	}
	return s.writeObject(func(buf *bytes.Buffer) {
		buf.WriteString(`{"jsonrpc":"2.0","id":`)
		buf.Write(rawID(id))
		buf.WriteString(`,"method":`)
		buf.Write(methodJSON)
		buf.WriteString(`,"params":`)
		buf.Write(body)
		buf.WriteByte('}')
	})
}

func (s *mockServer) writeNotification(method string, params any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	methodJSON, err := json.Marshal(method)
	if err != nil {
		return err
	}
	return s.writeObject(func(buf *bytes.Buffer) {
		buf.WriteString(`{"jsonrpc":"2.0","method":`)
		buf.Write(methodJSON)
		buf.WriteString(`,"params":`)
		buf.Write(body)
		buf.WriteByte('}')
	})
}

// writeObject serializes a composed object and emits one newline-terminated
// record, preserving raw ID tokens verbatim.
func (s *mockServer) writeObject(build func(*bytes.Buffer)) error {
	var buf bytes.Buffer
	build(&buf)
	return s.writeLine(buf.Bytes())
}

func (s *mockServer) writeLine(line []byte) error {
	if _, err := s.out.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

func rawID(id json.RawMessage) []byte {
	if len(id) == 0 || isNull(id) {
		return []byte("null")
	}
	return id
}

func isNull(raw json.RawMessage) bool {
	return string(bytes.TrimSpace(raw)) == "null"
}

func decodeParams(raw json.RawMessage, dst any) error {
	if len(raw) == 0 || isNull(raw) {
		return errors.New("missing params")
	}
	return json.Unmarshal(raw, dst)
}

func toolCallUpdate() map[string]any {
	return map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "mock-tool-1",
		"title":         "Mock tool call",
		"kind":          "other",
		"status":        "completed",
	}
}

func messageChunkUpdate() map[string]any {
	return map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]any{"type": "text", "text": "mock reply"},
	}
}

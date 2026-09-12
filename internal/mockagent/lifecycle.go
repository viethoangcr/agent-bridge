package mockagent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
)

func (s *mockServer) initialize(msg message) error {
	if s.initialized {
		return s.writeError(msg.ID, codeInvalidRequest, "already initialized")
	}
	var params struct {
		ProtocolVersion *int `json:"protocolVersion"`
	}
	if decodeParams(msg.Params, &params) != nil || params.ProtocolVersion == nil {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	s.initialized = true
	return s.writeResult(msg.ID, map[string]any{
		"protocolVersion":   protocolVersion,
		"agentCapabilities": map[string]any{},
		"authMethods":       []any{},
	})
}

func (s *mockServer) sessionNew(msg message) error {
	var params struct {
		CWD string `json:"cwd"`
	}
	if decodeParams(msg.Params, &params) != nil || params.CWD == "" {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	s.sessionSeq++
	id := fmt.Sprintf("mock-session-%d", s.sessionSeq)
	s.sessions[id] = params.CWD
	return s.writeResult(msg.ID, map[string]any{"sessionId": id, "cwd": params.CWD})
}

func (s *mockServer) sessionList(msg message) error {
	sessions := make([]map[string]any, 0, len(s.sessions))
	for _, id := range slices.Sorted(maps.Keys(s.sessions)) {
		sessions = append(sessions, map[string]any{"sessionId": id, "cwd": s.sessions[id]})
	}
	return s.writeResult(msg.ID, map[string]any{"sessions": sessions})
}

func (s *mockServer) sessionLoad(msg message) error {
	var params struct {
		SessionID string `json:"sessionId"`
		CWD       string `json:"cwd"`
	}
	if decodeParams(msg.Params, &params) != nil || params.SessionID == "" {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	if _, ok := s.sessions[params.SessionID]; !ok {
		return s.writeError(msg.ID, codeNotFound, "unknown session")
	}
	if params.CWD != "" {
		s.sessions[params.SessionID] = params.CWD
	}
	// ACP session/load replies null; the mock deliberately does not replay.
	return s.writeResult(msg.ID, nil)
}

func (s *mockServer) sessionResume(msg message) error {
	var params struct {
		SessionID string `json:"sessionId"`
		CWD       string `json:"cwd"`
	}
	if decodeParams(msg.Params, &params) != nil || params.SessionID == "" {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	if _, ok := s.sessions[params.SessionID]; !ok {
		return s.writeError(msg.ID, codeNotFound, "unknown session")
	}
	if params.CWD != "" {
		s.sessions[params.SessionID] = params.CWD
	}
	return s.writeResult(msg.ID, map[string]any{})
}

func (s *mockServer) sessionClose(msg message) error {
	var params struct {
		SessionID string `json:"sessionId"`
	}
	if decodeParams(msg.Params, &params) != nil || params.SessionID == "" {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	if _, ok := s.sessions[params.SessionID]; !ok {
		return s.writeError(msg.ID, codeNotFound, "unknown session")
	}
	delete(s.sessions, params.SessionID)
	return s.writeResult(msg.ID, map[string]any{})
}

func (s *mockServer) sessionPrompt(msg message) error {
	var params struct {
		SessionID string          `json:"sessionId"`
		Prompt    json.RawMessage `json:"prompt"`
	}
	if decodeParams(msg.Params, &params) != nil || params.SessionID == "" || len(params.Prompt) == 0 {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	if _, ok := s.sessions[params.SessionID]; !ok {
		return s.writeError(msg.ID, codeNotFound, "unknown session")
	}
	if bytes.Contains(params.Prompt, []byte(permissionTrigger)) {
		return s.permissionPrompt(msg, params.SessionID)
	}
	if err := s.writeUpdate(params.SessionID, toolCallUpdate()); err != nil {
		return err
	}
	if err := s.writeUpdate(params.SessionID, messageChunkUpdate()); err != nil {
		return err
	}
	return s.writeResult(msg.ID, map[string]any{"stopReason": "end_turn"})
}

// permissionPrompt emits the reverse-call, waits for the matching client
// response, then finishes updates and the withheld prompt response. EOF or
// cancellation while pending ends the loop without answering the prompt.
func (s *mockServer) permissionPrompt(msg message, sessionID string) error {
	if err := s.writeUpdate(sessionID, toolCallUpdate()); err != nil {
		return err
	}
	s.permissionSeq++
	permID := json.RawMessage(strconv.Quote(fmt.Sprintf("mock-permission-%d", s.permissionSeq)))
	if err := s.writeRequest(permID, methodRequestPermission, map[string]any{
		"sessionId": sessionID,
		"options": []map[string]any{
			{"optionId": "allow", "name": "Allow", "kind": "allow_once"},
			{"optionId": "reject", "name": "Reject", "kind": "reject_once"},
		},
		"toolCall": map[string]any{"toolCallId": "mock-tool-1", "title": "Mock permission"},
	}); err != nil {
		return err
	}
	if !s.awaitPermission(permID) {
		s.stop = true
		return nil
	}
	if err := s.writeUpdate(sessionID, messageChunkUpdate()); err != nil {
		return err
	}
	return s.writeResult(msg.ID, map[string]any{"stopReason": "end_turn"})
}

// awaitPermission reads lines until a client response with the exact raw
// reverse-call id arrives. Mismatched responses are ignored.
func (s *mockServer) awaitPermission(permID json.RawMessage) bool {
	for {
		select {
		case <-s.ctx.Done():
			return false
		case line, ok := <-s.lines:
			if !ok {
				return false
			}
			trimmed := bytes.TrimSpace(line)
			if !json.Valid(trimmed) {
				continue
			}
			var msg message
			if err := json.Unmarshal(trimmed, &msg); err != nil {
				continue
			}
			// Only a valid JSON-RPC 2.0 client response can complete the
			// reverse-call; wrong-version input is ignored.
			if msg.JSONRPC != "2.0" {
				continue
			}
			if msg.Method != "" || msg.ID == nil {
				continue
			}
			if bytes.Equal(bytes.TrimSpace(msg.ID), permID) {
				return true
			}
		}
	}
}

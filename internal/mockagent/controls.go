package mockagent

import (
	"encoding/json"
	"io"
	"time"
)

// Private wire-level control hooks used by later-phase integration tests.
// They are not a public contract.
const (
	methodDelay         = "_mock/delay"
	methodInvalidStdout = "_mock/invalid_stdout"
	methodStderr        = "_mock/stderr"
	methodExit          = "_mock/exit"

	permissionTrigger = "[mock:request_permission]"
)

// sleepFn is the delay seam so tests observe delay handling without waiting.
var sleepFn = time.Sleep

func (s *mockServer) delay(msg message) error {
	var params struct {
		MS int `json:"ms"`
	}
	if decodeParams(msg.Params, &params) != nil || params.MS < 0 {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	sleepFn(time.Duration(params.MS) * time.Millisecond)
	return s.writeResult(msg.ID, map[string]any{})
}

func (s *mockServer) invalidStdout(msg message) error {
	var params struct {
		Line string `json:"line"`
	}
	_ = json.Unmarshal(msg.Params, &params)
	if params.Line == "" {
		params.Line = "mock invalid stdout"
	}
	if err := s.writeLine([]byte(params.Line)); err != nil {
		return err
	}
	return s.writeResult(msg.ID, map[string]any{})
}

func (s *mockServer) stderr(msg message) error {
	var params struct {
		Line string `json:"line"`
	}
	if decodeParams(msg.Params, &params) != nil {
		return s.writeError(msg.ID, codeInvalidParams, "invalid params")
	}
	if _, err := io.WriteString(s.errOut, params.Line+"\n"); err != nil {
		return err
	}
	return s.writeResult(msg.ID, map[string]any{})
}

func (s *mockServer) exit(msg message) error {
	if err := s.writeResult(msg.ID, map[string]any{}); err != nil {
		return err
	}
	s.stop = true
	return nil
}

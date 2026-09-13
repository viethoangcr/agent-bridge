package acpruntime

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// maxSessionIDBytes and maxCWDBytes bound agent-supplied session metadata.
const (
	maxSessionIDBytes = 1024
	maxCWDBytes       = 4096
)

// Lifecycle is the bounded lifecycle kind retained for a client request.
type Lifecycle string

const (
	// LifecycleNone marks a message that is not a lifecycle request.
	LifecycleNone Lifecycle = "none"
	// LifecycleNew marks a session/new request.
	LifecycleNew Lifecycle = "new"
	// LifecycleLoad marks a session/load request.
	LifecycleLoad Lifecycle = "load"
	// LifecycleResume marks a session/resume request.
	LifecycleResume Lifecycle = "resume"
)

// pendingMeta is the bounded request metadata the output classifier reads for a
// matching response.
type pendingMeta struct {
	Lifecycle Lifecycle
	SessionID *string
	CWD       *string
}

// pendingLookup resolves a response ID to its reserved request metadata.
type pendingLookup func(id json.RawMessage) (pendingMeta, bool)

// classifiedOutput is one classified agent-output envelope ready to persist.
type classifiedOutput struct {
	Kind      string
	Method    *string
	Payload   json.RawMessage
	SessionID *string
	Mutation  *acpstore.SessionMutation
}

func (c classifiedOutput) storeOutput() acpstore.Output {
	return acpstore.Output{
		Kind:      c.Kind,
		Method:    c.Method,
		Payload:   c.Payload,
		SessionID: c.SessionID,
		Mutation:  c.Mutation,
	}
}

// sessionScopedMethods is the authoritative §5 field map: sessionId is
// inspected only on these agent-originated methods. elicitation/create is
// deliberately absent because it carries no sessionId.
var sessionScopedMethods = map[string]bool{
	"session/prompt":             true,
	"session/cancel":             true,
	"session/update":             true,
	"session/request_permission": true,
	"fs/read_text_file":          true,
	"fs/write_text_file":         true,
	"terminal/create":            true,
	"terminal/output":            true,
	"terminal/wait_for_exit":     true,
	"terminal/kill":              true,
	"terminal/release":           true,
	"session/set_mode":           true,
	"session/set_config_option":  true,
	"session/close":              true,
	"session/delete":             true,
}

// classifyOutput inspects one agent-output object without decoding or
// re-marshaling its bytes. It classifies kind and method, validates the ID
// shape, and inspects only bounded session metadata for session-scoped
// methods. Unrelated objects carrying sessionId/cwd keys are left untouched.
func classifyOutput(raw []byte, lookup pendingLookup) (classifiedOutput, bool) {
	if !utf8.Valid(raw) {
		return classifiedOutput{}, false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return classifiedOutput{}, false
	}

	methodRaw, hasMethod := obj["method"]
	idRaw, hasID := obj["id"]
	_, hasResult := obj["result"]
	_, hasError := obj["error"]

	var method string
	if hasMethod {
		if isJSONNull(methodRaw) || json.Unmarshal(methodRaw, &method) != nil || method == "" {
			return classifiedOutput{}, false
		}
	}
	if hasID && !validOutputID(idRaw) {
		return classifiedOutput{}, false
	}

	out := classifiedOutput{Payload: raw}
	switch {
	case hasMethod && hasID:
		out.Kind = "request"
	case hasMethod:
		out.Kind = "notification"
	case hasID:
		if hasResult == hasError {
			return classifiedOutput{}, false
		}
		out.Kind = "response"
	default:
		return classifiedOutput{}, false
	}
	if hasMethod {
		out.Method = &method
	}

	if out.Kind == "response" {
		out.attachResponse(obj, lookup)
		return out, true
	}
	if sessionScopedMethods[method] {
		if sessionID, ok := paramsString(obj["params"], "sessionId", maxSessionIDBytes); ok {
			out.SessionID = &sessionID
		}
	}
	return out, true
}

// attachResponse attributes a response event and, for a successful lifecycle
// response, derives its session roster/cwd mutation from the matching pending
// request. A session/new response has no prior session ID, so its result
// sessionId is used with the pending cwd.
func (o *classifiedOutput) attachResponse(obj map[string]json.RawMessage, lookup pendingLookup) {
	if lookup == nil {
		return
	}
	_, hasError := obj["error"]
	result := obj["result"]

	pending, ok := lookup(obj["id"])
	if !ok {
		return
	}
	if pending.SessionID != nil {
		o.SessionID = pending.SessionID
	}
	o.Mutation = sessionMutation(pending, result, hasError)
	if pending.Lifecycle == LifecycleNew && !hasError {
		if sessionID, ok := resultSessionID(result); ok {
			o.SessionID = &sessionID
		}
	}
}

// sessionMutation derives the roster/cwd change of a successful lifecycle
// response. Error responses and malformed or over-limit success payloads never
// mutate; session/load and session/resume use the retained request identity.
func sessionMutation(pending pendingMeta, result json.RawMessage, hasError bool) *acpstore.SessionMutation {
	if hasError {
		return nil
	}
	switch pending.Lifecycle {
	case LifecycleNew:
		sessionID, ok := resultSessionID(result)
		if !ok {
			return nil
		}
		return &acpstore.SessionMutation{SessionID: sessionID, CWD: boundedCWD(pending.CWD)}
	case LifecycleLoad, LifecycleResume:
		if pending.SessionID == nil {
			return nil
		}
		return &acpstore.SessionMutation{SessionID: *pending.SessionID, CWD: boundedCWD(pending.CWD)}
	default:
		return nil
	}
}

func resultSessionID(result json.RawMessage) (string, bool) {
	if len(result) == 0 {
		return "", false
	}
	var body struct {
		SessionID json.RawMessage `json:"sessionId"`
	}
	if json.Unmarshal(result, &body) != nil {
		return "", false
	}
	return boundedString(body.SessionID, maxSessionIDBytes)
}

func paramsString(params json.RawMessage, key string, limit int) (string, bool) {
	if len(params) == 0 || isJSONNull(params) {
		return "", false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(params, &fields) != nil {
		return "", false
	}
	return boundedString(fields[key], limit)
}

func boundedString(raw json.RawMessage, limit int) (string, bool) {
	if len(raw) == 0 || isJSONNull(raw) {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	if len(s) > limit {
		return "", false
	}
	return s, true
}

func validOutputID(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false
	}
	switch raw[0] {
	case '"':
		return json.Valid(raw)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return json.Valid(raw)
	default:
		return false
	}
}

func isJSONNull(raw json.RawMessage) bool {
	return string(bytes.TrimSpace(raw)) == "null"
}

func boundedCWD(p *string) string {
	if p == nil || len(*p) > maxCWDBytes {
		return ""
	}
	return *p
}

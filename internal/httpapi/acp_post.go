package httpapi

import (
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strings"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// handleACPPost negotiates, validates, and dispatches one client envelope. All
// validation happens before any runtime is created or admitted.
func (s *Server) handleACPPost(w http.ResponseWriter, r *http.Request) {
	serverID, ok := s.acpServerID(w, r)
	if !ok {
		return
	}
	if !requireJSONContentType(w, r) {
		return
	}
	if !acceptsJSONResponse(w, r) {
		return
	}
	payload, method, ok := s.decodeACPEnvelope(w, r)
	if !ok {
		return
	}
	agent, ok := parseAgentQuery(r)
	if !ok {
		writeProblem(w, http.StatusBadRequest, "invalid agent")
		return
	}
	if s.deps.ACP == nil {
		writeProblem(w, http.StatusServiceUnavailable, "ACP proxy is unavailable")
		return
	}

	result, err := s.deps.ACP.Post(r.Context(), serverID, agent, method, payload)
	if err != nil {
		s.writeACPError(w, serverID, err)
		return
	}
	if result.Accepted {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(result.Response)
}

// requireJSONContentType enforces a parseable application/json Content-Type,
// case-insensitively and with parameters allowed.
func requireJSONContentType(w http.ResponseWriter, r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		writeProblem(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	return true
}

// acceptsJSONResponse accepts a missing/empty Accept header or any value whose
// media range allows application/json.
func acceptsJSONResponse(w http.ResponseWriter, r *http.Request) bool {
	values := r.Header.Values("Accept")
	sawRange := false
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			sawRange = true
			mediaType, acceptable := parseAcceptRange(part)
			if !acceptable {
				continue
			}
			switch mediaType {
			case "application/json", "application/*", "*/*":
				return true
			}
		}
	}
	if !sawRange {
		return true
	}
	writeProblem(w, http.StatusNotAcceptable, "Accept must allow application/json")
	return false
}

// decodeACPEnvelope bounds and reads exactly one JSON object, then validates it
// through the envelope classifier. It returns the original raw object and the
// method (empty for a client response) used only for initialize-only recreation
// policy. The raw object is never compacted or re-marshalled here.
func (s *Server) decodeACPEnvelope(w http.ResponseWriter, r *http.Request) (json.RawMessage, string, bool) {
	var payload json.RawMessage
	if !decodeJSONRequest(w, r, maxJSONBodyBytes, &payload) {
		return nil, "", false
	}
	if _, _, err := acpruntime.ClassifyClientEnvelope(payload); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid ACP envelope")
		return nil, "", false
	}
	var envelope struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal(payload, &envelope)
	return payload, envelope.Method, true
}

// parseAgentQuery reads the optional agent query value. Absence returns a nil
// agent; a malformed query, or an empty, repeated, or unknown value is
// rejected.
func parseAgentQuery(r *http.Request) (*string, bool) {
	values, err := parseQuery(r, "agent")
	if err != nil {
		return nil, false
	}
	if !values.Has("agent") {
		return nil, true
	}
	agent, ok := singleQuery(values, "agent")
	if !ok {
		return nil, false
	}
	if _, known := knownACPAgents[agent]; !known {
		return nil, false
	}
	return &agent, true
}

// writeACPError maps a typed acpproxy error to an RFC 9457 problem.
// Only known sentinels are interpreted; anything else is a 502 process failure.
// A 502 carries the already capped and redacted agent stderr tail, when
// available, as an extension member.
func (s *Server) writeACPError(w http.ResponseWriter, serverID string, err error) {
	status, detail := http.StatusBadGateway, "ACP agent process failure"
	switch {
	case errors.Is(err, acpstore.ErrNotFound):
		status, detail = http.StatusNotFound, detailNotFound
	case errors.Is(err, acpruntime.ErrInvalidEnvelope):
		status, detail = http.StatusBadRequest, "invalid ACP envelope"
	case errors.Is(err, acpproxy.ErrMissingAgent):
		status, detail = http.StatusBadRequest, "agent is required for a new server"
	case errors.Is(err, acpproxy.ErrReinitialize):
		status, detail = http.StatusConflict, "exited server requires initialize"
	case errors.Is(err, acpruntime.ErrDuplicateID),
		errors.Is(err, acpproxy.ErrAgentConflict),
		errors.Is(err, acpproxy.ErrDeleting),
		errors.Is(err, acpstore.ErrDeleted),
		errors.Is(err, acpstore.ErrConflict):
		status, detail = http.StatusConflict, "ACP request conflict"
	case errors.Is(err, acpproxy.ErrClosed):
		status, detail = http.StatusServiceUnavailable, "ACP proxy is shutting down"
	case errors.Is(err, acpproxy.ErrRuntimeCapacity), errors.Is(err, acpruntime.ErrCapacity):
		status, detail = http.StatusTooManyRequests, "ACP capacity exhausted"
	case errors.Is(err, acpruntime.ErrRequestTimeout):
		status, detail = http.StatusGatewayTimeout, "ACP request timed out"
	case errors.Is(err, acpruntime.ErrPersistence), errors.Is(err, acpstore.ErrSequenceExhausted):
		status, detail = http.StatusInsufficientStorage, "ACP persistence failure"
	}

	if status == http.StatusBadGateway {
		if provider, ok := s.deps.ACP.(acpStderrProvider); ok {
			if stderr := provider.Stderr(serverID); stderr != "" {
				writeProblemExt(w, status, detail, map[string]any{"agentStderr": stderr})
				return
			}
		}
	}
	writeProblem(w, status, detail)
}

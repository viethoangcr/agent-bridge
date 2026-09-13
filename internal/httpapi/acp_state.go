package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// Event query policy: the documented defaults and bounds.
const (
	defaultEventLimit = 100
	maxEventLimit     = 1000
	maxSessionIDBytes = 1024
)

// errInvalidEventQuery marks a malformed events query so the handler answers
// with a 400 problem.
var errInvalidEventQuery = errors.New("invalid ACP event query")

type acpServerView struct {
	ServerID    string          `json:"serverId"`
	Agent       string          `json:"agent"`
	Status      acpstore.Status `json:"status"`
	CreatedAtMs int64           `json:"createdAtMs"`
	UpdatedAtMs int64           `json:"updatedAtMs"`
}

// acpStatusView is the status DTO. ServerID, Agent, Status, CreatedAtMs,
// LastEventSeq, SessionIDs, and UpdatedAtMs are durable; PID is a pointer so it
// is omitted unless the proxy confirms a current live generation.
type acpStatusView struct {
	ServerID     string          `json:"serverId"`
	Agent        string          `json:"agent"`
	Status       acpstore.Status `json:"status"`
	CreatedAtMs  int64           `json:"createdAtMs"`
	LastEventSeq int64           `json:"lastEventSeq"`
	SessionIDs   []string        `json:"sessionIds"`
	PID          *int            `json:"pid,omitempty"`
	UpdatedAtMs  int64           `json:"updatedAtMs"`
}

// acpEventView is the events element DTO. Payload is a json.RawMessage so the
// stored bytes embed as raw JSON rather than a quoted string.
type acpEventView struct {
	Seq         int64           `json:"seq"`
	Kind        string          `json:"kind"`
	Method      *string         `json:"method,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	SessionID   *string         `json:"sessionId,omitempty"`
	CreatedAtMs int64           `json:"createdAtMs"`
}

func (s *Server) handleACPList(w http.ResponseWriter, r *http.Request) {
	if s.deps.ACPStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "ACP store is unavailable")
		return
	}
	servers, err := s.deps.ACPStore.Servers(r.Context())
	if err != nil {
		writeStoreReadProblem(w, err)
		return
	}
	views := make([]acpServerView, 0, len(servers))
	for _, server := range servers {
		views = append(views, acpServerView{
			ServerID:    server.ServerID,
			Agent:       server.Agent,
			Status:      server.Status,
			CreatedAtMs: server.CreatedAtMs,
			UpdatedAtMs: server.UpdatedAtMs,
		})
	}
	writeJSON(w, struct {
		Servers []acpServerView `json:"servers"`
	}{Servers: views})
}

// handleACPStatus returns one server's durable status plus a PID only while the
// proxy still owns the current live generation.
func (s *Server) handleACPStatus(w http.ResponseWriter, r *http.Request) {
	serverID, ok := s.acpServerID(w, r)
	if !ok {
		return
	}
	if s.deps.ACPStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "ACP store is unavailable")
		return
	}
	server, err := s.deps.ACPStore.Server(r.Context(), serverID)
	if err != nil {
		writeStoreReadProblem(w, err)
		return
	}
	sessions, err := s.deps.ACPStore.Sessions(r.Context(), serverID)
	if err != nil {
		writeStoreReadProblem(w, err)
		return
	}
	sessionIDs := make([]string, 0, len(sessions))
	for _, session := range sessions {
		sessionIDs = append(sessionIDs, session.SessionID)
	}
	view := acpStatusView{
		ServerID:     server.ServerID,
		Agent:        server.Agent,
		Status:       server.Status,
		CreatedAtMs:  server.CreatedAtMs,
		LastEventSeq: server.LastEventSeq,
		SessionIDs:   sessionIDs,
		UpdatedAtMs:  server.UpdatedAtMs,
	}
	if s.deps.ACP != nil {
		if pid, ok := s.deps.ACP.LivePID(serverID); ok {
			view.PID = &pid
		}
	}
	writeJSON(w, view)
}

func (s *Server) handleACPEvents(w http.ResponseWriter, r *http.Request) {
	serverID, ok := s.acpServerID(w, r)
	if !ok {
		return
	}
	values, err := parseQuery(r, "sessionId", "after", "limit", "order")
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid ACP event query")
		return
	}
	query, err := parseEventQuery(values)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid ACP event query")
		return
	}
	if s.deps.ACPStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "ACP store is unavailable")
		return
	}
	events, err := s.deps.ACPStore.Events(r.Context(), serverID, query)
	if err != nil {
		writeStoreReadProblem(w, err)
		return
	}
	views := make([]acpEventView, 0, len(events))
	for _, event := range events {
		views = append(views, acpEventView{
			Seq:         event.Seq,
			Kind:        event.Kind,
			Method:      event.Method,
			Payload:     event.Payload,
			SessionID:   event.SessionID,
			CreatedAtMs: event.CreatedAtMs,
		})
	}
	writeJSON(w, struct {
		Events []acpEventView `json:"events"`
	}{Events: views})
}

func (s *Server) handleACPDelete(w http.ResponseWriter, r *http.Request) {
	serverID, ok := s.acpServerID(w, r)
	if !ok {
		return
	}
	if s.deps.ACP == nil {
		writeProblem(w, http.StatusServiceUnavailable, "ACP proxy is unavailable")
		return
	}
	if err := s.deps.ACP.Delete(r.Context(), serverID); err != nil {
		writeDeleteProblem(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeDeleteProblem maps a Delete failure: an unknown server is 404; any
// other failure is 500 because the process was already terminated and the
// durable prune can be retried.
func writeDeleteProblem(w http.ResponseWriter, err error) {
	if errors.Is(err, acpstore.ErrNotFound) {
		writeProblem(w, http.StatusNotFound, detailNotFound)
		return
	}
	writeProblem(w, http.StatusInternalServerError, "ACP delete failure")
}

// parseEventQuery strictly parses sessionId, after, limit, and order. Every key
// is single-valued and non-empty; signs, non-decimals, overflow, limit outside
// 1..1000, unknown order values, and session IDs over 1024 bytes are rejected.
// Unknown keys are rejected by parseQuery before this runs.
func parseEventQuery(values url.Values) (acpstore.EventQuery, error) {
	query := acpstore.EventQuery{Limit: defaultEventLimit}
	if values.Has("after") {
		raw, ok := singleQuery(values, "after")
		if !ok {
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
		after, err := parseNonnegativeInt64(raw)
		if err != nil {
			return acpstore.EventQuery{}, err
		}
		query.After = after
	}
	if values.Has("limit") {
		raw, ok := singleQuery(values, "limit")
		if !ok {
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
		limit, err := parseNonnegativeInt64(raw)
		if err != nil || limit < 1 || limit > maxEventLimit {
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
		query.Limit = int(limit)
	}
	if values.Has("order") {
		raw, ok := singleQuery(values, "order")
		if !ok {
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
		switch raw {
		case "asc":
		case "desc":
			query.Desc = true
		default:
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
	}
	if values.Has("sessionId") {
		raw, ok := singleQuery(values, "sessionId")
		if !ok {
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
		if len(raw) > maxSessionIDBytes {
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
		sessionID := raw
		query.SessionID = &sessionID
	}
	return query, nil
}

// parseNonnegativeInt64 accepts only ASCII digits that fit a nonnegative int64
// through math.MaxInt64. Signs, fractions, and overflow are rejected.
func parseNonnegativeInt64(value string) (int64, error) {
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, errInvalidEventQuery
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, errInvalidEventQuery
	}
	return parsed, nil
}

// writeStoreReadProblem maps a durable read failure to a problem response.
func writeStoreReadProblem(w http.ResponseWriter, err error) {
	if errors.Is(err, acpstore.ErrNotFound) {
		writeProblem(w, http.StatusNotFound, detailNotFound)
		return
	}
	writeProblem(w, http.StatusInternalServerError, "ACP store failure")
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(value)
}

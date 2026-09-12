package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// maxACPBodyBytes is the ACP POST body bound: one JSON-RPC object, at most 10MiB.
const maxACPBodyBytes = 10 << 20

// knownACPAgents is the fixed set of query agent identifiers. The resolver also
// rejects unknown identifiers, but rejecting them here keeps an unknown agent a
// client 400 instead of a process 502.
var knownACPAgents = map[string]struct{}{
	"claude": {}, "codex": {}, "opencode": {}, "mock": {},
}

// ACPProxy is the ACP dispatch surface the POST handler consumes, satisfied by
// *acpproxy.Proxy. It is an interface so the HTTP layer can be tested without
// spawning agent processes.
type ACPProxy interface {
	Post(ctx context.Context, serverID string, agent *string, method string, payload json.RawMessage) (acpruntime.PostResult, error)
	LivePID(serverID string) (int, bool)
}

// acpStderrProvider optionally exposes the already capped and redacted agent
// stderr tail for a 502 process-failure response.
type acpStderrProvider interface {
	Stderr(serverID string) string
}

// ACPSubscriber is the SSE dispatch surface the events handler consumes,
// satisfied by *acpproxy.Proxy. It is separate from ACPProxy so existing POST
// fakes need not implement streaming.
type ACPSubscriber interface {
	Subscribe(ctx context.Context, serverID string, after int64) (acpproxy.Subscription, error)
}

// ACPDeleter is the lifecycle deletion surface, satisfied by *acpproxy.Proxy.
type ACPDeleter interface {
	Delete(ctx context.Context, serverID string) error
}

// sseHeartbeatInterval is the documented production SSE keep-alive cadence. It
// is the only HTTP-owned timer; tests inject a fake ticker instead.
const sseHeartbeatInterval = 15 * time.Second

// heartbeatTicker is the injectable SSE keep-alive source.
type heartbeatTicker interface {
	C() <-chan time.Time
	Stop()
}

// realHeartbeatTicker is the production time.Ticker adapter.
type realHeartbeatTicker struct{ ticker *time.Ticker }

func (t realHeartbeatTicker) C() <-chan time.Time { return t.ticker.C }
func (t realHeartbeatTicker) Stop()               { t.ticker.Stop() }

// newHeartbeatTicker returns the production 15-second SSE keep-alive ticker.
// Server.newHeartbeatTicker binds it and tests replace it with a fake.
func newHeartbeatTicker() heartbeatTicker {
	return realHeartbeatTicker{ticker: time.NewTicker(sseHeartbeatInterval)}
}

// errInvalidLastEventID marks a malformed Last-Event-ID header.
var errInvalidLastEventID = errors.New("invalid Last-Event-ID")

// registerACPRoutes installs the ACP endpoints on the shared mux. The
// trailing-slash POST pattern captures the empty server ID so it fails
// validation with 400 instead of falling through to a router 404.
func (s *Server) registerACPRoutes() {
	s.mux.HandleFunc("POST /v1/acp/{serverId}", s.handleACPPost)
	s.mux.HandleFunc("POST /v1/acp/{$}", s.handleACPPost)
	s.mux.HandleFunc("GET /v1/acp", s.handleACPList)
	s.mux.HandleFunc("GET /v1/acp/{serverId}", s.handleACPSSE)
	s.mux.HandleFunc("DELETE /v1/acp/{serverId}", s.handleACPDelete)
	s.mux.HandleFunc("GET /v1/acp/{serverId}/status", s.handleACPStatus)
	s.mux.HandleFunc("GET /v1/acp/{serverId}/events", s.handleACPEvents)
}

// handleACPSSE negotiates Accept, validates Last-Event-ID, subscribes before
// the first durable query, and frames the subscription as Server-Sent Events.
// It ends on request cancellation, subscription closure, or write failure.
func (s *Server) handleACPSSE(w http.ResponseWriter, r *http.Request) {
	serverID := r.PathValue("serverId")
	if !validServerID(serverID) {
		writeProblem(w, http.StatusBadRequest, "invalid ACP server ID")
		return
	}
	if !acceptsEventStream(w, r) {
		return
	}
	after, err := parseLastEventID(r.Header)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid Last-Event-ID")
		return
	}
	subscriber, ok := s.deps.ACP.(ACPSubscriber)
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "ACP proxy is unavailable")
		return
	}
	// Subscribe before writing any header so an unknown server is a plain 404
	// and never a half-open stream.
	sub, err := subscriber.Subscribe(r.Context(), serverID, after)
	if err != nil {
		WriteProblem(w, s.mapACPError(serverID, err))
		return
	}
	defer sub.Close()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // deliver headers before the first event heartbeat

	ticker := s.newHeartbeatTicker()
	defer ticker.Stop()

	// Next is driven from one producer goroutine so the loop can keep writing
	// heartbeats while no event is ready; only the loop writes to the stream.
	type nextResult struct {
		event acpstore.Event
		err   error
	}
	results := make(chan nextResult, 1)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			event, err := sub.Next(r.Context())
			select {
			case results <- nextResult{event: event, err: err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case result := <-results:
			if result.err != nil {
				return
			}
			if !writeSSEEvent(w, result.event) {
				return
			}
			flusher.Flush()
		case <-ticker.C():
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// handleACPDelete terminates and prunes one server, reusing the shared
// server-ID validation and problem mapping.
func (s *Server) handleACPDelete(w http.ResponseWriter, r *http.Request) {
	serverID := r.PathValue("serverId")
	if !validServerID(serverID) {
		writeProblem(w, http.StatusBadRequest, "invalid ACP server ID")
		return
	}
	deleter, ok := s.deps.ACP.(ACPDeleter)
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "ACP proxy is unavailable")
		return
	}
	if err := deleter.Delete(r.Context(), serverID); err != nil {
		writeDeleteProblem(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseLastEventID strictly parses the optional Last-Event-ID header. Absence
// is zero; a single nonnegative decimal int64 through math.MaxInt64 is
// accepted; blank, repeated, signed, non-decimal, and overflowing values are
// rejected. It never converts through a wider or architecture-sized type.
func parseLastEventID(header http.Header) (int64, error) {
	values := header.Values("Last-Event-ID")
	if len(values) == 0 {
		return 0, nil
	}
	if len(values) != 1 || values[0] == "" {
		return 0, errInvalidLastEventID
	}
	after, err := parseNonnegativeInt64(values[0])
	if err != nil {
		return 0, errInvalidLastEventID
	}
	return after, nil
}

// writeSSEEvent frames one event as `event: message`, a decimal int64 `id`,
// and the exact stored payload bytes as `data`, terminated by a blank line. It
// reports false when the underlying write fails so the handler can stop.
func writeSSEEvent(w io.Writer, event acpstore.Event) bool {
	var frame bytes.Buffer
	frame.Grow(len("event: message\nid: \ndata: \n\n") + len(event.Payload) + 20)
	frame.WriteString("event: message\nid: ")
	frame.WriteString(strconv.FormatInt(event.Seq, 10))
	frame.WriteString("\ndata: ")
	frame.Write(event.Payload)
	frame.WriteString("\n\n")
	_, err := w.Write(frame.Bytes())
	return err == nil
}

// acceptsEventStream accepts a missing/empty Accept header or any value whose
// media range allows text/event-stream.
func acceptsEventStream(w http.ResponseWriter, r *http.Request) bool {
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
			case "text/event-stream", "text/*", "*/*":
				return true
			}
		}
	}
	if !sawRange {
		return true
	}
	writeProblem(w, http.StatusNotAcceptable, "Accept must allow text/event-stream")
	return false
}

// parseAcceptRange parses one Accept list element and reports whether its media
// range is acceptable. A missing q defaults to 1; a malformed media range or q
// value, or a quality outside (0,1], makes the range unacceptable.
func parseAcceptRange(part string) (string, bool) {
	mediaType, params, err := mime.ParseMediaType(part)
	if err != nil {
		return "", false
	}
	raw, present := params["q"]
	if !present {
		return mediaType, true
	}
	quality, err := strconv.ParseFloat(raw, 64)
	if err != nil || !(quality > 0 && quality <= 1) {
		return "", false
	}
	return mediaType, true
}

// writeDeleteProblem maps a Delete failure: an unknown server is 404; any
// other failure is 500 because the process was already terminated and the
// durable prune can be retried.
func writeDeleteProblem(w http.ResponseWriter, err error) {
	if errors.Is(err, acpstore.ErrNotFound) {
		writeProblem(w, http.StatusNotFound, "not found")
		return
	}
	writeProblem(w, http.StatusInternalServerError, "ACP delete failure")
}

// handleACPPost negotiates, validates, and dispatches one client envelope. All
// validation happens before any runtime is created or admitted.
func (s *Server) handleACPPost(w http.ResponseWriter, r *http.Request) {
	serverID := r.PathValue("serverId")
	if !validServerID(serverID) {
		writeProblem(w, http.StatusBadRequest, "invalid ACP server ID")
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
		WriteProblem(w, s.mapACPError(serverID, err))
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

// validServerID enforces the decoded server-ID shape: 1-128 bytes of
// [A-Za-z0-9._-]. A decoded slash, non-ASCII byte, space, or NUL is rejected.
func validServerID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
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
// through the Phase 02 classifier. It returns the original raw object and the
// method (empty for a client response) used only for initialize-only recreation
// policy. The raw object is never compacted or re-marshalled here.
func (s *Server) decodeACPEnvelope(w http.ResponseWriter, r *http.Request) (json.RawMessage, string, bool) {
	var payload json.RawMessage
	if !DecodeJSON(w, r, maxACPBodyBytes, &payload) {
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
// agent; an empty, repeated, or unknown value is rejected.
func parseAgentQuery(r *http.Request) (*string, bool) {
	values, present := r.URL.Query()["agent"]
	if !present {
		return nil, true
	}
	if len(values) != 1 || values[0] == "" {
		return nil, false
	}
	if _, known := knownACPAgents[values[0]]; !known {
		return nil, false
	}
	agent := values[0]
	return &agent, true
}

// mapACPError maps a typed Phase 02/acpproxy error to an RFC 9457 problem. Only
// known sentinels are interpreted; anything else is a 502 process failure.
func (s *Server) mapACPError(serverID string, err error) Problem {
	status, detail := http.StatusBadGateway, "ACP agent process failure"
	switch {
	case errors.Is(err, acpstore.ErrNotFound):
		status, detail = http.StatusNotFound, "not found"
	case errors.Is(err, acpruntime.ErrInvalidEnvelope):
		status, detail = http.StatusBadRequest, "invalid ACP envelope"
	case errors.Is(err, acpproxy.ErrMissingAgent):
		status, detail = http.StatusBadRequest, "agent is required for a new server"
	case errors.Is(err, acpruntime.ErrDuplicateID),
		errors.Is(err, acpproxy.ErrAgentConflict),
		errors.Is(err, acpproxy.ErrDeleting),
		errors.Is(err, acpproxy.ErrReinitialize),
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

	problem := Problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
	}
	if status == http.StatusBadGateway {
		if provider, ok := s.deps.ACP.(acpStderrProvider); ok {
			if stderr := provider.Stderr(serverID); stderr != "" {
				problem.Ext = map[string]any{"agentStderr": stderr}
			}
		}
	}
	return problem
}

// writeProblem emits a status/detail problem response.
func writeProblem(w http.ResponseWriter, status int, detail string) {
	WriteProblem(w, Problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
	})
}

// Event query policy: the documented defaults and bounds.
const (
	defaultEventLimit = 100
	maxEventLimit     = 1000
	maxSessionIDBytes = 1024
)

// errInvalidEventQuery marks a malformed events query so the handler answers
// with a 400 problem.
var errInvalidEventQuery = errors.New("invalid ACP event query")

// acpServerView is the list element DTO: exactly the documented fields.
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

// handleACPList returns every durable server ordered by server ID.
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
	serverID := r.PathValue("serverId")
	if !validServerID(serverID) {
		writeProblem(w, http.StatusBadRequest, "invalid ACP server ID")
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

// handleACPEvents returns durable events after an exclusive sequence, filtered
// and ordered by the strictly parsed query.
func (s *Server) handleACPEvents(w http.ResponseWriter, r *http.Request) {
	serverID := r.PathValue("serverId")
	if !validServerID(serverID) {
		writeProblem(w, http.StatusBadRequest, "invalid ACP server ID")
		return
	}
	query, err := parseEventQuery(r.URL.Query())
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

// parseEventQuery strictly parses sessionId, after, limit, and order. Every key
// is single-valued and non-empty; unknown keys, signs, non-decimals, overflow,
// limit outside 1..1000, unknown order values, and session IDs over 1024 bytes
// are rejected.
func parseEventQuery(values url.Values) (acpstore.EventQuery, error) {
	for key := range values {
		switch key {
		case "sessionId", "after", "limit", "order":
		default:
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
	}
	single := func(key string) (string, bool, error) {
		value, present := values[key]
		if !present {
			return "", false, nil
		}
		if len(value) != 1 || value[0] == "" {
			return "", false, errInvalidEventQuery
		}
		return value[0], true, nil
	}

	query := acpstore.EventQuery{Limit: defaultEventLimit}
	if raw, present, err := single("after"); err != nil {
		return acpstore.EventQuery{}, err
	} else if present {
		after, err := parseNonnegativeInt64(raw)
		if err != nil {
			return acpstore.EventQuery{}, err
		}
		query.After = after
	}
	if raw, present, err := single("limit"); err != nil {
		return acpstore.EventQuery{}, err
	} else if present {
		limit, err := parseNonnegativeInt64(raw)
		if err != nil || limit < 1 || limit > maxEventLimit {
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
		query.Limit = int(limit)
	}
	if raw, present, err := single("order"); err != nil {
		return acpstore.EventQuery{}, err
	} else if present {
		switch raw {
		case "asc":
		case "desc":
			query.Desc = true
		default:
			return acpstore.EventQuery{}, errInvalidEventQuery
		}
	}
	if raw, present, err := single("sessionId"); err != nil {
		return acpstore.EventQuery{}, err
	} else if present {
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
		writeProblem(w, http.StatusNotFound, "not found")
		return
	}
	writeProblem(w, http.StatusInternalServerError, "ACP store failure")
}

// writeJSON emits an application/json response body.
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(value)
}

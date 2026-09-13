package httpapi

import (
	"context"
	"encoding/json"
	"mime"
	"net/http"
	"strconv"

	"github.com/viethoangcr/agent-bridge/internal/acpproxy"
	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
)

// knownACPAgents is the fixed set of query agent identifiers. The resolver also
// rejects unknown identifiers, but rejecting them here keeps an unknown agent a
// client 400 instead of a process 502.
var knownACPAgents = map[string]struct{}{
	"claude": {}, "codex": {}, "opencode": {}, "mock": {},
}

// ACPProxy is the complete ACP dispatch surface the registered routes consume:
// POST dispatch and live-PID ownership, SSE subscription, and lifecycle
// deletion. It is satisfied by *acpproxy.Proxy. It is deliberately composite so
// every registered ACP route is satisfied at compile time; a partial
// implementation cannot be injected and cannot silently yield 503 routes.
type ACPProxy interface {
	Post(ctx context.Context, serverID string, agent *string, method string, payload json.RawMessage) (acpruntime.PostResult, error)
	LivePID(serverID string) (int, bool)
	Subscribe(ctx context.Context, serverID string, after int64) (acpproxy.Subscription, error)
	Delete(ctx context.Context, serverID string) error
}

// acpStderrProvider optionally exposes the already capped and redacted agent
// stderr tail for a 502 process-failure response.
type acpStderrProvider interface {
	Stderr(serverID string) string
}

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

// acpServerID extracts and validates the {serverId} path value. An invalid ID
// writes the shared 400 problem and returns false.
func (s *Server) acpServerID(w http.ResponseWriter, r *http.Request) (string, bool) {
	serverID := r.PathValue("serverId")
	if !validServerID(serverID) {
		writeProblem(w, http.StatusBadRequest, "invalid ACP server ID")
		return "", false
	}
	return serverID, true
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

// Compile-time proof that the concrete proxy satisfies the complete surface.
var _ ACPProxy = (*acpproxy.Proxy)(nil)

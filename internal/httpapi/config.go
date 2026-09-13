package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

// registerConfigRoutes installs the method-specific config endpoints. No
// methodless same-path fallbacks are registered, so wrong methods are owned by
// the shared root fallback.
func (s *Server) registerConfigRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/config/mcp", s.handleMCPConfig)
	mux.HandleFunc("PUT /v1/config/mcp", s.handleMCPConfig)
	mux.HandleFunc("DELETE /v1/config/mcp", s.handleMCPConfig)
	mux.HandleFunc("GET /v1/config/skills", s.handleSkillsConfig)
	mux.HandleFunc("PUT /v1/config/skills", s.handleSkillsConfig)
	mux.HandleFunc("DELETE /v1/config/skills", s.handleSkillsConfig)
}

func (s *Server) requireProjectConfig(w http.ResponseWriter) bool {
	if s.deps.Config == nil {
		writeProblem(w, http.StatusServiceUnavailable, "project config service is unavailable")
		return false
	}
	return true
}

func (s *Server) handleMCPConfig(w http.ResponseWriter, r *http.Request) {
	s.handleProjectConfig(w, r, "mcp")
}

func (s *Server) handleSkillsConfig(w http.ResponseWriter, r *http.Request) {
	s.handleProjectConfig(w, r, "skills")
}

// handleProjectConfig owns GET/PUT/DELETE for one fixed config kind. The
// directory is the only query value; PUT preserves the request JSON as a raw
// message until the service validates it, then returns 204 empty.
func (s *Server) handleProjectConfig(w http.ResponseWriter, r *http.Request, kind string) {
	if !s.requireProjectConfig(w) {
		return
	}
	query, ok := fsRequiredQuery(w, r, "directory")
	if !ok {
		return
	}
	directory := query["directory"]

	switch r.Method {
	case http.MethodGet:
		body, err := s.deps.Config.Get(kind, directory)
		if err != nil {
			writeProjectConfigError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	case http.MethodPut:
		var body json.RawMessage
		if !decodeJSONRequest(w, r, s.configJSONLimit, &body) {
			return
		}
		if err := s.deps.Config.Put(kind, directory, body); err != nil {
			writeProjectConfigError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := s.deps.Config.Delete(kind, directory); err != nil {
			writeProjectConfigError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		s.methodNotAllowed(w, []string{http.MethodGet, http.MethodPut, http.MethodDelete})
	}
}

// writeProjectConfigError maps a typed config error to an RFC 9457 problem
// without leaking host paths: invalid is 400, missing is 404, conflicts are
// 409, limits are 413, and anything else is an internal 500.
func writeProjectConfigError(w http.ResponseWriter, err error) {
	status, detail := http.StatusInternalServerError, "project config operation failed"
	var fsErr *filesystem.Error
	if errors.As(err, &fsErr) {
		switch fsErr.Kind {
		case filesystem.ErrorKindInvalid:
			status, detail = http.StatusBadRequest, "invalid project config request"
		case filesystem.ErrorKindNotFound:
			status, detail = http.StatusNotFound, detailNotFound
		case filesystem.ErrorKindConflict:
			status, detail = http.StatusConflict, "project config conflict"
		case filesystem.ErrorKindTooLarge:
			status, detail = http.StatusRequestEntityTooLarge, detailBodyTooLarge
		}
	}
	writeProblem(w, status, detail)
}

package httpapi

import (
	"net/http"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

func (s *Server) requireProcessManager(w http.ResponseWriter) bool {
	if s.deps.Processes == nil {
		writeProblem(w, http.StatusServiceUnavailable, "process manager is unavailable")
		return false
	}
	return true
}

func (s *Server) handleProcessStart(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	var req process.StartRequest
	if !decodeJSONRequest(w, r, maxJSONBodyBytes, &req) {
		return
	}
	snapshot, err := s.deps.Processes.Start(req)
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, snapshot)
}

// handleProcessRun strictly decodes the one-shot request, runs it under the
// caller's request context, and serializes the exact run result with no
// wrapper. The run result carries the exact
// {exitCode?,timedOut,stdout,stderr,stdoutTruncated,stderrTruncated,durationMs}
// members via process.RunResult.
func (s *Server) handleProcessRun(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	var req process.RunRequest
	if !decodeJSONRequest(w, r, maxJSONBodyBytes, &req) {
		return
	}
	result, err := s.deps.Processes.Run(r.Context(), req)
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, result)
}

func (s *Server) handleProcessList(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	writeJSON(w, struct {
		Processes []process.Snapshot `json:"processes"`
	}{s.deps.Processes.List()})
}

func (s *Server) handleProcessGet(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	snapshot, err := s.deps.Processes.Get(r.PathValue("id"))
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, snapshot)
}

func (s *Server) handleProcessStop(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	snapshot, err := s.deps.Processes.Stop(r.PathValue("id"))
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, snapshot)
}

func (s *Server) handleProcessKill(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	snapshot, err := s.deps.Processes.Kill(r.PathValue("id"))
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, snapshot)
}

func (s *Server) handleProcessDelete(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	if err := s.deps.Processes.Delete(r.PathValue("id")); err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleProcessLogs(w http.ResponseWriter, r *http.Request) {
	if !s.requireProcessManager(w) {
		return
	}
	values, err := parseQuery(r, "stream", "tail", "since")
	if err != nil {
		writeProblem(w, http.StatusBadRequest, detailInvalidProcessQuery)
		return
	}
	query, err := parseLogsQuery(values)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, detailInvalidProcessQuery)
		return
	}
	entries, err := s.deps.Processes.Logs(r.PathValue("id"), query)
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, struct {
		Entries []process.LogEntry `json:"entries"`
	}{entries})
}

// handleProcessConfig serves the full-replacement runtime configuration. GET
// returns the active values; POST requires the complete six-field object,
// validates it, and replaces the active configuration atomically. A rejected
// POST leaves the previous configuration untouched.
func (s *Server) handleProcessConfig(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if s.deps.Processes == nil {
		writeProblem(w, http.StatusServiceUnavailable, "process manager is unavailable")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.deps.Processes.Config())
	case http.MethodPost:
		var cfg process.Config
		if !decodeJSONRequest(w, r, maxJSONBodyBytes, &cfg) {
			return
		}
		if err := s.deps.Processes.UpdateConfig(cfg); err != nil {
			WriteProblem(w, mapProcessError(err))
			return
		}
		writeJSON(w, s.deps.Processes.Config())
	default:
		s.methodNotAllowed(w, []string{http.MethodGet, http.MethodPost})
	}
}

package httpapi

import (
	"errors"
	"math"
	"net/http"
	"net/url"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// maxProcessJSONBytes is the master global JSON hard ceiling: 10MiB. Every
// process JSON body is bounded by it before decoding.
const maxProcessJSONBytes = 10 << 20

// processInputEnvelopeBytes is the fixed allowance that covers the exact
// compact `{data,encoding}` input envelope around the base64 expansion.
const processInputEnvelopeBytes = 1024

// errInvalidProcessQuery marks a malformed process query so the caller answers
// with a 400 problem.
var errInvalidProcessQuery = errors.New("invalid process query")

// reservedProcessLiterals maps each reserved literal process path to its exact
// supported methods, in deterministic Allow order. The literals are bound
// per method so an unsupported method can never fall into a /{id} wildcard
// handler, and the fallback Allow derives from this map instead of probing the
// wildcard routes.
var reservedProcessLiterals = map[string][]string{
	"/v1/processes/config": {http.MethodGet, http.MethodPost},
	"/v1/processes/run":    {http.MethodPost},
}

// registerProcessRoutes installs the process endpoints on the shared mux. The
// one-shot run route is added by Task 4.9.
func (s *Server) registerProcessRoutes() {
	s.mux.HandleFunc("GET /v1/processes/config", s.handleProcessConfig)
	s.mux.HandleFunc("POST /v1/processes/config", s.handleProcessConfig)
	// Reserved literals are also bound for every other method that would
	// otherwise match a /{id} wildcard route, so a wrong-method request gets an
	// exact 405 instead of being treated as a process ID.
	s.mux.HandleFunc("DELETE /v1/processes/config", s.handleReservedProcessLiteral)
	s.mux.HandleFunc("GET /v1/processes/run", s.handleReservedProcessLiteral)
	s.mux.HandleFunc("DELETE /v1/processes/run", s.handleReservedProcessLiteral)
	s.mux.HandleFunc("POST /v1/processes/run", s.handleProcessRun)
	s.mux.HandleFunc("POST /v1/processes", s.handleProcessStart)
	s.mux.HandleFunc("GET /v1/processes", s.handleProcessList)
	s.mux.HandleFunc("GET /v1/processes/{id}", s.handleProcessGet)
	s.mux.HandleFunc("POST /v1/processes/{id}/stop", s.handleProcessStop)
	s.mux.HandleFunc("POST /v1/processes/{id}/kill", s.handleProcessKill)
	s.mux.HandleFunc("DELETE /v1/processes/{id}", s.handleProcessDelete)
	s.mux.HandleFunc("GET /v1/processes/{id}/logs", s.handleProcessLogs)
	s.mux.HandleFunc("POST /v1/processes/{id}/input", s.handleProcessInput)
}

// handleReservedProcessLiteral answers a reserved literal bound for a method
// its endpoint does not support with the exact 405 and deterministic Allow.
func (s *Server) handleReservedProcessLiteral(w http.ResponseWriter, r *http.Request) {
	methods, ok := reservedProcessLiterals[r.URL.Path]
	if !ok {
		s.notFound(w)
		return
	}
	s.methodNotAllowed(w, methods)
}

// requireNoProcessQuery rejects any query string on endpoints that document no
// query parameters, using the same invalid-query 400 as the logs parser.
func requireNoProcessQuery(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.RawQuery != "" {
		writeProblem(w, http.StatusBadRequest, "invalid process query")
		return false
	}
	return true
}

// processInputRequest is the exact `{data,encoding}` input envelope. Encoding
// must be exactly base64 or utf8; data is the encoded payload.
type processInputRequest struct {
	Data     string `json:"data"`
	Encoding string `json:"encoding"`
}

// requireProcessManager rejects requests when no manager was injected.
func (s *Server) requireProcessManager(w http.ResponseWriter) bool {
	if s.deps.Processes == nil {
		writeProblem(w, http.StatusServiceUnavailable, "process manager is unavailable")
		return false
	}
	return true
}

// handleProcessStart validates the start body, spawns the process group, and
// returns the running snapshot itself with no wrapper.
func (s *Server) handleProcessStart(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	var req process.StartRequest
	if !decodeProcessJSON(w, r, maxProcessJSONBytes, &req) {
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
	if !requireNoProcessQuery(w, r) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	var req process.RunRequest
	if !decodeProcessJSON(w, r, maxProcessJSONBytes, &req) {
		return
	}
	result, err := s.deps.Processes.Run(r.Context(), req)
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, result)
}

// handleProcessList returns every retained snapshot wrapped in `processes` and
// sorted by ID.
func (s *Server) handleProcessList(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	writeJSON(w, struct {
		Processes []process.Snapshot `json:"processes"`
	}{s.deps.Processes.List()})
}

// handleProcessGet returns one snapshot itself or 404 for an unknown ID.
func (s *Server) handleProcessGet(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
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

// handleProcessStop sends SIGTERM to the process group, waits the fixed bound,
// and returns the resulting snapshot.
func (s *Server) handleProcessStop(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
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

// handleProcessKill sends SIGKILL to the process group, waits the fixed bound,
// and returns the resulting snapshot.
func (s *Server) handleProcessKill(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
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

// handleProcessDelete removes an exited record and returns 204 empty. A running
// process is a 409 and an unknown ID is a 404.
func (s *Server) handleProcessDelete(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
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

// handleProcessLogs strictly parses the log query and returns the selected
// entries wrapped in `entries` with no additional top-level fields.
func (s *Server) handleProcessLogs(w http.ResponseWriter, r *http.Request) {
	if !s.requireProcessManager(w) {
		return
	}
	query, err := parseLogsQuery(r.URL.Query())
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid process query")
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

// handleProcessInput decodes the exact `{data,encoding}` body, enforces the
// active decoded-byte limit, writes to the process stdin, and returns the
// written byte count. The body is bounded by the active encoded ceiling.
func (s *Server) handleProcessInput(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	var req processInputRequest
	active := s.deps.Processes.Config().MaxInputBytesPerRequest
	if !decodeProcessJSON(w, r, inputEncodedBodyLimit(active), &req) {
		return
	}
	decoded, err := process.DecodeInput(req.Encoding, []byte(req.Data))
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	written, err := s.deps.Processes.WriteInput(r.Context(), r.PathValue("id"), decoded)
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, struct {
		BytesWritten int `json:"bytesWritten"`
	}{written})
}

// handleProcessConfig serves the full-replacement runtime configuration. GET
// returns the active values; POST requires the complete six-field object,
// validates it, and replaces the active configuration atomically. A rejected
// POST leaves the previous configuration untouched.
func (s *Server) handleProcessConfig(w http.ResponseWriter, r *http.Request) {
	if !requireNoProcessQuery(w, r) {
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
		if !decodeProcessJSON(w, r, maxProcessJSONBytes, &cfg) {
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

// decodeProcessJSON enforces the public JSON contract shared by every process
// endpoint: a parseable application/json Content-Type, an allowlisted set of
// fields, exactly one JSON value, and the supplied byte ceiling. Unknown
// fields, trailing values, malformed bodies, and wrong types are 400; an
// over-limit body is 413; a missing or non-JSON Content-Type is 415.
func decodeProcessJSON(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	if !requireJSONContentType(w, r) {
		return false
	}
	return DecodeJSON(w, r, limit, dst)
}

// parseLogsQuery strictly parses the logs query. stream, tail, and since are
// allowlisted; every key may appear at most once and with a non-empty value.
// tail and since are strict non-negative decimals; stream is
// stdout|stderr|combined. Unknown, repeated, empty, or malformed values are
// errInvalidProcessQuery. A present tail of zero is preserved distinctly from
// an omitted tail.
func parseLogsQuery(values url.Values) (process.LogQuery, error) {
	for key := range values {
		switch key {
		case "stream", "tail", "since":
		default:
			return process.LogQuery{}, errInvalidProcessQuery
		}
	}

	single := func(key string) (string, bool, error) {
		value, present := values[key]
		if !present {
			return "", false, nil
		}
		if len(value) != 1 || value[0] == "" {
			return "", false, errInvalidProcessQuery
		}
		return value[0], true, nil
	}

	var query process.LogQuery
	if raw, present, err := single("stream"); err != nil {
		return process.LogQuery{}, err
	} else if present {
		switch raw {
		case "stdout", "stderr", "combined":
			query.Stream = raw
		default:
			return process.LogQuery{}, errInvalidProcessQuery
		}
	}
	if raw, present, err := single("since"); err != nil {
		return process.LogQuery{}, err
	} else if present {
		since, err := parseNonnegativeInt64(raw)
		if err != nil {
			return process.LogQuery{}, errInvalidProcessQuery
		}
		query.Since = since
	}
	if raw, present, err := single("tail"); err != nil {
		return process.LogQuery{}, err
	} else if present {
		tail, err := parseNonnegativeInt64(raw)
		if err != nil || tail > int64(math.MaxInt) {
			return process.LogQuery{}, errInvalidProcessQuery
		}
		value := int(tail)
		query.Tail = &value
	}
	return query, nil
}

// inputEncodedBodyLimit returns the encoded JSON body ceiling for a process
// input request with the given active decoded limit: four bytes per three
// decoded bytes of base64 expansion plus a fixed envelope allowance, clamped to
// the global JSON hard ceiling. The block count and 4x expansion are checked in
// int arithmetic before multiplying, so a pathological active limit cannot wrap
// below the clamp.
func inputEncodedBodyLimit(activeDecoded int) int64 {
	if activeDecoded <= 0 {
		return processInputEnvelopeBytes
	}
	blocks, remainder := activeDecoded/3, activeDecoded%3
	if remainder != 0 {
		blocks++
	}
	if blocks > (math.MaxInt-processInputEnvelopeBytes)/4 {
		return maxProcessJSONBytes
	}
	limit := blocks*4 + processInputEnvelopeBytes
	if limit > maxProcessJSONBytes {
		return maxProcessJSONBytes
	}
	return int64(limit)
}

// mapProcessError maps a typed process error to an RFC 9457 problem: validation
// is 400, decoded/body limits are 413, unknown IDs are 404, state/capacity
// conflicts are 409, and spawn/I/O failures are 502. Anything unrecognized is a
// conservative 502 rather than a leaked internal error.
func mapProcessError(err error) Problem {
	status, detail := http.StatusBadGateway, "process failure"
	switch {
	case errors.Is(err, process.ErrValidation):
		status, detail = http.StatusBadRequest, "invalid process request"
	case errors.Is(err, process.ErrPayloadTooLarge):
		status, detail = http.StatusRequestEntityTooLarge, "process payload too large"
	case errors.Is(err, process.ErrNotFound):
		status, detail = http.StatusNotFound, "not found"
	case errors.Is(err, process.ErrConflict), errors.Is(err, process.ErrCapacity):
		status, detail = http.StatusConflict, "process conflict"
	}
	return Problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
	}
}

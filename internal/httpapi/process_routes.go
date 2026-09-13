package httpapi

import "net/http"

// reservedProcessLiterals maps each reserved literal process path to its exact
// supported methods, in deterministic Allow order. The literals are bound
// per method so an unsupported method can never fall into a /{id} wildcard
// handler, and the fallback Allow derives from this map instead of probing the
// wildcard routes.
var reservedProcessLiterals = map[string][]string{
	"/v1/processes/config": {http.MethodGet, http.MethodPost},
	"/v1/processes/run":    {http.MethodPost},
}

// registerProcessRoutes installs the process endpoints on the shared mux.
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

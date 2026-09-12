package httpapi

import (
	"log/slog"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
	"github.com/viethoangcr/agent-bridge/internal/filesystem"
	"github.com/viethoangcr/agent-bridge/internal/process"
	"github.com/viethoangcr/agent-bridge/internal/projectconfig"
)

// rootDocsURL is the stable documentation location served by the root endpoint
// until Phase 06 finalizes public documentation.
const rootDocsURL = "https://github.com/viethoangcr/agent-bridge#readme"

// fallbackPattern is the methodless root pattern that owns 404/405 responses.
const fallbackPattern = "/"

// Dependencies holds the already-constructed values the HTTP server consumes.
// Phase 06 owns the final shape once all services exist; NewServer must not
// construct stores, runtimes, reapers, or other services.
type Dependencies struct {
	Token     string
	Log       *slog.Logger
	ACP       ACPProxy
	ACPStore  *acpstore.Store
	Processes *process.Manager
	Files     *filesystem.Service
	Config    *projectconfig.Service
}

// Server owns the Phase 01 route table and the composed HTTP handler. It is
// assembled only through NewServer so every route shares one middleware chain.
type Server struct {
	mux     *http.ServeMux
	deps    Dependencies
	handler http.Handler

	// newHeartbeatTicker builds the SSE keep-alive source. Tests replace it
	// with a fake ticker so no unit test waits for the 15-second interval.
	newHeartbeatTicker func() heartbeatTicker

	// fsFileLimit and fsUploadLimit bound raw file PUT and upload bodies. They
	// default to the production 512MiB constants and are overridable in tests.
	fsFileLimit   int64
	fsUploadLimit int64

	// configJSONLimit bounds JSON config request bodies. It defaults to the
	// master 10MiB JSON ceiling and is overridable in tests.
	configJSONLimit int64
}

// NewServer builds the route table and composes the middleware chain once, so
// repeatedly returned handlers are the same instance. An empty deps.Token
// leaves /v1/* unauthenticated.
func NewServer(deps Dependencies) *Server {
	s := &Server{
		mux:                http.NewServeMux(),
		deps:               deps,
		newHeartbeatTicker: newHeartbeatTicker,
		fsFileLimit:        maxFSFileBytes,
		fsUploadLimit:      maxFSUploadBytes,
		configJSONLimit:    maxConfigJSONBytes,
	}
	s.registerRoutes()
	s.handler = requestLogger(deps.Log, authenticate(deps.Token, s.rejectUncleanPath(s.mux)))
	return s
}

// Handler returns the stable composed handler. It is the only public HTTP
// assembly path.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// registerRoutes installs the exact-match endpoints and one methodless root
// fallback. The fallback must stay last and methodless; Go 1.22+ ServeMux
// panics on any further methodless same-path conflicts with the root pattern.
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /{$}", s.root)
	s.mux.HandleFunc("GET /v1/health", s.health)
	s.registerACPRoutes()
	s.registerProcessRoutes()
	s.registerFilesystemRoutes(s.mux)
	s.registerConfigRoutes(s.mux)
	s.mux.HandleFunc(fallbackPattern, s.routeFallback)
}

// root serves the public root document.
func (s *Server) root(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"name":"agent-bridge","docs":"` + rootDocsURL + `"}`))
}

// health serves the authenticated health document.
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// routeFallback distinguishes a wrong-method request on a registered path
// (405 with Allow) from an unknown path (404), always as a problem response.
func (s *Server) routeFallback(w http.ResponseWriter, r *http.Request) {
	allow := s.allowedMethods(r)
	if len(allow) == 0 {
		s.notFound(w)
		return
	}
	s.methodNotAllowed(w, allow)
}

// notFound emits an RFC 9457 404 for paths no route matches.
func (s *Server) notFound(w http.ResponseWriter) {
	WriteProblem(w, Problem{
		Type:   "about:blank",
		Title:  http.StatusText(http.StatusNotFound),
		Status: http.StatusNotFound,
		Detail: "not found",
	})
}

// methodNotAllowed emits an RFC 9457 405 with a deterministic Allow header.
func (s *Server) methodNotAllowed(w http.ResponseWriter, allow []string) {
	w.Header().Set("Allow", strings.Join(allow, ", "))
	WriteProblem(w, Problem{
		Type:   "about:blank",
		Title:  http.StatusText(http.StatusMethodNotAllowed),
		Status: http.StatusMethodNotAllowed,
		Detail: "method not allowed",
	})
}

// probeMethods is the deterministic candidate set used to derive Allow. GET
// patterns also match HEAD, so both appear when a GET route owns the path.
var probeMethods = []string{
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodPatch,
	http.MethodDelete,
	http.MethodConnect,
	http.MethodOptions,
	http.MethodTrace,
}

// rejectUncleanPath answers non-canonical request paths with the bridge's RFC
// 9457 404 before ServeMux can emit its own text/html redirect. It wraps the
// mux innermost so the response still travels back through logging and auth.
func (s *Server) rejectUncleanPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != cleanPath(r.URL.Path) {
			s.notFound(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// cleanPath mirrors net/http's internal canonicalization so the guard's notion
// of a canonical path matches ServeMux's own. In particular, a trailing slash
// is preserved, keeping "/v1/health/" non-canonical input for the mux's own
// 404 fallback rather than a bridge redirect.
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	np := path.Clean(p)
	if p[len(p)-1] == '/' && np != "/" {
		np += "/"
	}
	return np
}

// allowedMethods returns the sorted methods a real (non-fallback) route matches
// for r's path. Because the methodless fallback matches every method, a probe
// only counts when mux.Handler returns a pattern other than the fallback's own.
// Reserved literal paths answer from their exact method allowlist instead, so a
// wildcard /{id} route or a wrong-method literal binding can never inflate the
// advertised Allow set.
func (s *Server) allowedMethods(r *http.Request) []string {
	if allow, reserved := reservedProcessLiterals[r.URL.Path]; reserved {
		return allow
	}
	var allow []string
	for _, method := range probeMethods {
		probe := r.Clone(r.Context())
		probe.Method = method
		if _, pattern := s.mux.Handler(probe); pattern != fallbackPattern {
			allow = append(allow, method)
		}
	}
	slices.Sort(allow)
	return allow
}

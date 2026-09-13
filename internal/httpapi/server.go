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

const rootDocsURL = "https://github.com/viethoangcr/agent-bridge#readme"

const fallbackPattern = "/"

// Dependencies supplies services borrowed by NewServer. Nil service values make
// their routes return 503; an empty Token disables /v1 authentication. NewServer
// does not close dependencies.
type Dependencies struct {
	// Token is the bearer credential required on /v1/* requests; an empty Token
	// disables that authentication.
	Token string
	// Log receives one structured record per request; a nil Log discards records.
	Log *slog.Logger
	// ACP dispatches ACP posts, subscriptions, and deletion; a nil ACP makes
	// those runtime routes return 503.
	ACP ACPProxy
	// ACPStore is the durable ACP state store; a nil ACPStore makes every ACP
	// state route return 503.
	ACPStore *acpstore.Store
	// Processes manages child process groups; a nil Processes makes every
	// process route return 503.
	Processes *process.Manager
	// Files serves filesystem routes; a nil Files makes every filesystem route
	// return 503.
	Files *filesystem.Service
	// Config serves project configuration routes; a nil Config makes every
	// project config route return 503.
	Config *projectconfig.Service
}

// Server is the immutable HTTP routing surface assembled by NewServer; Handler
// may serve requests concurrently.
type Server struct {
	mux     *http.ServeMux
	deps    Dependencies
	handler http.Handler

	newHeartbeatTicker func() heartbeatTicker

	fsFileLimit   int64
	fsUploadLimit int64

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
		configJSONLimit:    maxJSONBodyBytes,
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

func (s *Server) root(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"name":"agent-bridge","docs":"` + rootDocsURL + `"}`))
}

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

func (s *Server) notFound(w http.ResponseWriter) {
	writeProblem(w, http.StatusNotFound, detailNotFound)
}

func (s *Server) methodNotAllowed(w http.ResponseWriter, allow []string) {
	w.Header().Set("Allow", strings.Join(allow, ", "))
	writeProblem(w, http.StatusMethodNotAllowed, "method not allowed")
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

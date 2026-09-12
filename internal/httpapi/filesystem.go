package httpapi

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

// maxFilesystemJSONBytes is the shared 10MiB JSON body ceiling for filesystem
// JSON requests.
const maxFilesystemJSONBytes = 10 << 20

// maxFSFileBytes and maxFSUploadBytes are the production 512MiB raw file PUT
// and upload limits. Server copies them into overridable fields so tests can
// inject small limits.
const (
	maxFSFileBytes   int64 = 512 << 20
	maxFSUploadBytes int64 = 512 << 20
)

// errFSBodyTooLarge marks a request body that exceeded the injected limit. The
// body reader returns it at limit+1 so a mutation is aborted before it can
// touch the destination.
var errFSBodyTooLarge = errors.New("filesystem request body too large")

// fsMkdirRequest is the exact `{directory,name}` mkdir body.
type fsMkdirRequest struct {
	Directory string `json:"directory"`
	Name      string `json:"name"`
}

// fsMoveRequest is the exact `{source,destination}` move body.
type fsMoveRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

// registerFilesystemRoutes installs the method-specific filesystem endpoints.
// No methodless same-path fallbacks are registered, so wrong methods are owned
// by the Phase 01 root fallback.
func (s *Server) registerFilesystemRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/fs/entries", s.handleFSEntries)
	mux.HandleFunc("GET /v1/fs/file", s.handleFSFile)
	mux.HandleFunc("PUT /v1/fs/file", s.handleFSFile)
	mux.HandleFunc("DELETE /v1/fs/entry", s.handleFSEntry)
	mux.HandleFunc("POST /v1/fs/mkdir", s.handleFSMkdir)
	mux.HandleFunc("POST /v1/fs/move", s.handleFSMove)
	mux.HandleFunc("GET /v1/fs/stat", s.handleFSStat)
	mux.HandleFunc("POST /v1/fs/upload-batch", s.handleFSUploadBatch)
}

// requireFilesystem rejects requests when no filesystem service was injected.
func (s *Server) requireFilesystem(w http.ResponseWriter) bool {
	if s.deps.Files == nil {
		writeProblem(w, http.StatusServiceUnavailable, "filesystem service is unavailable")
		return false
	}
	return true
}

// fsRequiredQuery parses an allowlisted query whose first key is required.
// Every supplied key may appear at most once with a non-empty value; unknown,
// repeated, empty, or missing-required values write a 400 problem and return
// false. Raw parsing is used so a malformed escape is rejected rather than
// silently dropped with its key.
func fsRequiredQuery(w http.ResponseWriter, r *http.Request, required string, optional ...string) (map[string]string, bool) {
	allowed := make(map[string]struct{}, len(optional)+1)
	allowed[required] = struct{}{}
	for _, key := range optional {
		allowed[key] = struct{}{}
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid filesystem query")
		return nil, false
	}
	for key, list := range values {
		if _, ok := allowed[key]; !ok || len(list) != 1 || list[0] == "" {
			writeProblem(w, http.StatusBadRequest, "invalid filesystem query")
			return nil, false
		}
	}
	if _, present := values[required]; !present {
		writeProblem(w, http.StatusBadRequest, "invalid filesystem query")
		return nil, false
	}
	out := make(map[string]string, len(values))
	for key, list := range values {
		out[key] = list[0]
	}
	return out, true
}

// requireNoFSQuery rejects any query string on endpoints that document none.
func requireNoFSQuery(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.RawQuery != "" {
		writeProblem(w, http.StatusBadRequest, "invalid filesystem query")
		return false
	}
	return true
}

// fsBodyCounter bounds a request body to limit bytes. It returns
// errFSBodyTooLarge at limit+1 so callers can abort a mutation before it
// commits and map the excess to 413.
type fsBodyCounter struct {
	r     io.Reader
	n     int64
	limit int64
}

func (c *fsBodyCounter) Read(p []byte) (int, error) {
	remaining := c.limit + 1 - c.n
	if remaining <= 0 {
		return 0, errFSBodyTooLarge
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := c.r.Read(p)
	c.n += int64(n)
	if c.n > c.limit {
		return n, errFSBodyTooLarge
	}
	return n, err
}

// exceeded reports whether more than the active limit was supplied.
func (c *fsBodyCounter) exceeded() bool { return c.n > c.limit }

// limitFSBody wraps r's body with http.MaxBytesReader as a hard backstop and
// returns a counter that reports the exact over-limit at limit+1.
func limitFSBody(w http.ResponseWriter, r *http.Request, limit int64) *fsBodyCounter {
	r.Body = http.MaxBytesReader(w, r.Body, limit+1)
	return &fsBodyCounter{r: r.Body, limit: limit}
}

// handleFSEntries lists one directory, defaulting type to all.
func (s *Server) handleFSEntries(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	query, ok := fsRequiredQuery(w, r, "directory", "type")
	if !ok {
		return
	}
	entryType := query["type"]
	if entryType == "" {
		entryType = "all"
	}
	entries, err := s.deps.Files.Entries(query["directory"], entryType)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	writeJSON(w, struct {
		Entries []filesystem.Entry `json:"entries"`
	}{entries})
}

// handleFSFile serves GET/HEAD raw file bytes and PUT raw file writes on the
// shared /v1/fs/file path.
func (s *Server) handleFSFile(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		query, ok := fsRequiredQuery(w, r, "path")
		if !ok {
			return
		}
		file, stat, err := s.deps.Files.Open(query["path"])
		if err != nil {
			writeFilesystemError(w, err)
			return
		}
		defer func() { _ = file.Close() }()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(stat.Size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, file)
	case http.MethodPut:
		query, ok := fsRequiredQuery(w, r, "path")
		if !ok {
			return
		}
		counter := limitFSBody(w, r, s.fsFileLimit)
		result, err := s.deps.Files.WriteFile(query["path"], counter)
		if counter.exceeded() {
			writeProblem(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		if err != nil {
			writeFilesystemError(w, err)
			return
		}
		writeJSON(w, result)
	default:
		s.methodNotAllowed(w, []string{http.MethodGet, http.MethodHead, http.MethodPut})
	}
}

// handleFSEntry deletes one file or directory tree, returning 204 empty.
func (s *Server) handleFSEntry(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	query, ok := fsRequiredQuery(w, r, "path")
	if !ok {
		return
	}
	if err := s.deps.Files.Remove(query["path"]); err != nil {
		writeFilesystemError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleFSMkdir creates one path component under a directory.
func (s *Server) handleFSMkdir(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	if !requireNoFSQuery(w, r) {
		return
	}
	if !requireJSONContentType(w, r) {
		return
	}
	var req fsMkdirRequest
	if !DecodeJSON(w, r, maxFilesystemJSONBytes, &req) {
		return
	}
	result, err := s.deps.Files.Mkdir(req.Directory, req.Name)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	writeJSON(w, result)
}

// handleFSMove renames source onto destination, creating destination parents.
func (s *Server) handleFSMove(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	if !requireNoFSQuery(w, r) {
		return
	}
	if !requireJSONContentType(w, r) {
		return
	}
	var req fsMoveRequest
	if !DecodeJSON(w, r, maxFilesystemJSONBytes, &req) {
		return
	}
	result, err := s.deps.Files.Move(req.Source, req.Destination)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	writeJSON(w, result)
}

// handleFSStat returns metadata for one path.
func (s *Server) handleFSStat(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	query, ok := fsRequiredQuery(w, r, "path")
	if !ok {
		return
	}
	stat, err := s.deps.Files.Stat(query["path"])
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	writeJSON(w, stat)
}

// handleFSUploadBatch validates and merges one gzip-compressed tar archive. Any
// Content-Type is accepted; excess compressed bytes are 413 and unsafe archives
// are 400.
func (s *Server) handleFSUploadBatch(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	query, ok := fsRequiredQuery(w, r, "directory")
	if !ok {
		return
	}
	counter := limitFSBody(w, r, s.fsUploadLimit)
	files, err := s.deps.Files.Upload(query["directory"], counter)
	if counter.exceeded() {
		writeProblem(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	writeJSON(w, struct {
		Files []filesystem.UploadedFile `json:"files"`
	}{files})
}

// writeFilesystemError maps a typed filesystem error to RFC 9457 without
// leaking host paths: invalid is 400, missing is 404, conflicts are 409, limits
// are 413, and anything else is an internal 500.
func writeFilesystemError(w http.ResponseWriter, err error) {
	status, detail := http.StatusInternalServerError, "filesystem operation failed"
	var fsErr *filesystem.Error
	if errors.As(err, &fsErr) {
		switch fsErr.Kind {
		case filesystem.ErrorKindInvalid:
			status, detail = http.StatusBadRequest, "invalid filesystem request"
		case filesystem.ErrorKindNotFound:
			status, detail = http.StatusNotFound, "not found"
		case filesystem.ErrorKindConflict:
			status, detail = http.StatusConflict, "destination conflict"
		case filesystem.ErrorKindTooLarge:
			status, detail = http.StatusRequestEntityTooLarge, "request body too large"
		}
	}
	writeProblem(w, status, detail)
}

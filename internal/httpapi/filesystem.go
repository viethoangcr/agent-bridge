package httpapi

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/viethoangcr/agent-bridge/internal/filesystem"
)

// registerFilesystemRoutes installs the method-specific filesystem endpoints.
// No methodless same-path fallbacks are registered, so wrong methods are owned
// by the shared root fallback.
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

func (s *Server) requireFilesystem(w http.ResponseWriter) bool {
	if s.deps.Files == nil {
		writeProblem(w, http.StatusServiceUnavailable, "filesystem service is unavailable")
		return false
	}
	return true
}

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
			writeProblem(w, http.StatusRequestEntityTooLarge, detailBodyTooLarge)
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

func (s *Server) handleFSMkdir(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	if !requireNoQuery(w, r, detailInvalidFilesystemQuery) {
		return
	}
	var req fsMkdirRequest
	if !decodeJSONRequest(w, r, maxJSONBodyBytes, &req) {
		return
	}
	result, err := s.deps.Files.Mkdir(req.Directory, req.Name)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	writeJSON(w, result)
}

func (s *Server) handleFSMove(w http.ResponseWriter, r *http.Request) {
	if !s.requireFilesystem(w) {
		return
	}
	if !requireNoQuery(w, r, detailInvalidFilesystemQuery) {
		return
	}
	var req fsMoveRequest
	if !decodeJSONRequest(w, r, maxJSONBodyBytes, &req) {
		return
	}
	result, err := s.deps.Files.Move(req.Source, req.Destination)
	if err != nil {
		writeFilesystemError(w, err)
		return
	}
	writeJSON(w, result)
}

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
		writeProblem(w, http.StatusRequestEntityTooLarge, detailBodyTooLarge)
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
			status, detail = http.StatusNotFound, detailNotFound
		case filesystem.ErrorKindConflict:
			status, detail = http.StatusConflict, "destination conflict"
		case filesystem.ErrorKindTooLarge:
			status, detail = http.StatusRequestEntityTooLarge, detailBodyTooLarge
		}
	}
	writeProblem(w, status, detail)
}

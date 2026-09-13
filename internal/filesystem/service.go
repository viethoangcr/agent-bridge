package filesystem

import (
	"path/filepath"
	"strings"
	"sync"
)

// Entry describes one directory entry. Type is only "file" or "dir".
type Entry struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	Type       string `json:"type"`
	Size       int64  `json:"size"`
	ModifiedMs int64  `json:"modifiedMs"`
}

// Stat describes one filesystem object's metadata. Type is only "file" or
// "dir"; Mode carries permission bits only.
type Stat struct {
	Path       string `json:"path"`
	Type       string `json:"type"`
	Size       int64  `json:"size"`
	ModifiedMs int64  `json:"modifiedMs"`
	Mode       uint32 `json:"mode"`
}

// FileResult reports a written file's resolved path and byte count.
type FileResult struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// PathResult reports the resolved path of a mutating operation.
type PathResult struct {
	Path string `json:"path"`
}

// Service resolves paths and reports metadata from a HOME captured at
// construction. Concrete methods are safe for concurrent reads; mutations
// serialize through the shared mutation mutex.
type Service struct {
	home          string
	mutations     *sync.Mutex
	maxCompressed int64
	maxExtracted  int64
}

// New captures home for the lifetime of the service. An empty home is allowed;
// only non-empty relative paths then fail resolution. mutations must be non-nil
// and shared by every service that needs cross-service serialization of writes.
// Upload limits default to the production constants.
func New(home string, mutations *sync.Mutex) (*Service, error) {
	return &Service{
		home:          home,
		mutations:     mutations,
		maxCompressed: MaxUploadCompressedBytes,
		maxExtracted:  MaxUploadExtractedBytes,
	}, nil
}

// Resolve turns a client-supplied path into an absolute path. Literal empty
// and NUL paths are invalid. Absolute paths are cleaned; non-empty relative
// paths resolve under the captured HOME, and any exact ".." raw component is
// rejected before cleaning so cleaning cannot hide traversal.
func (s *Service) Resolve(raw string) (string, error) {
	if raw == "" {
		return "", invalidError("path is required")
	}
	if strings.IndexByte(raw, 0) >= 0 {
		return "", invalidError("path must not contain NUL")
	}
	if filepath.IsAbs(raw) {
		return filepath.Clean(raw), nil
	}
	if s.home == "" {
		return "", invalidError("home directory is unavailable")
	}
	if err := validateRelativePath(raw); err != nil {
		return "", err
	}
	return filepath.Join(s.home, raw), nil
}

// validateRelativePath rejects any raw lexical component that is exactly "..".
// It intentionally inspects raw components so filepath.Clean cannot erase a
// parent reference.
func validateRelativePath(raw string) error {
	for _, component := range strings.Split(raw, string(filepath.Separator)) {
		if component == ".." {
			return invalidError("relative path must not contain '..'")
		}
	}
	return nil
}

// Package filesystem provides safe path resolution and deterministic metadata
// for the bridge's /v1/fs endpoints. Paths resolve under a HOME captured once
// at construction; absolute paths are used directly after filepath.Clean.
//
// The service is deliberately lexical: it rejects raw relative parent
// components before any cleaning and never treats path checks as confinement.
// The sandbox itself is the security boundary. The injected mutation mutex
// serializes bridge-originated writes once mutation methods are added.
package filesystem

import (
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// ErrorKind classifies a filesystem failure for the HTTP adapter.
type ErrorKind string

const (
	// ErrorKindInvalid marks malformed input or an unsupported object type.
	ErrorKindInvalid ErrorKind = "invalid"
	// ErrorKindNotFound marks a missing path.
	ErrorKindNotFound ErrorKind = "not_found"
	// ErrorKindConflict marks a destination or type collision.
	ErrorKindConflict ErrorKind = "conflict"
	// ErrorKindTooLarge marks input that exceeds an enforced limit.
	ErrorKindTooLarge ErrorKind = "too_large"
	// ErrorKindInternal marks an unexpected I/O failure.
	ErrorKindInternal ErrorKind = "internal"
)

// Error is a filesystem failure carrying a machine-readable Kind. Err holds
// the underlying cause for server-side inspection and is never surfaced to
// clients; Error's message is safe to reuse as a problem detail.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

// Error implements error.
func (e *Error) Error() string { return e.Message }

// Unwrap exposes the underlying cause to errors.Is/errors.As.
func (e *Error) Unwrap() error { return e.Err }

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
// construction. Concrete methods are safe for concurrent reads; future
// mutation methods serialize through mutations.
type Service struct {
	home          string
	mutations     *sync.Mutex
	maxCompressed int64
	maxExtracted  int64
}

// New captures home for the lifetime of the service. An empty home is allowed;
// only non-empty relative paths then fail resolution. Upload limits default to
// the production constants and are overridable only inside the package.
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

// Entries lists a resolved directory sorted by absolute path. entryType is one
// of "", "all", "file", or "dir". Symlinks are followed via os.Stat; dangling
// links and non-file/non-directory targets are skipped, while other stat
// failures abort the listing.
func (s *Service) Entries(directory, entryType string) ([]Entry, error) {
	switch entryType {
	case "", "all", "file", "dir":
	default:
		return nil, invalidError("entry type must be all, file, or dir")
	}

	resolved, err := s.Resolve(directory)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, mapPathError(err)
	}
	if !info.IsDir() {
		return nil, invalidError("path is not a directory")
	}
	dirEntries, err := os.ReadDir(resolved)
	if err != nil {
		return nil, mapPathError(err)
	}

	entries := make([]Entry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		joined := filepath.Join(resolved, dirEntry.Name())
		info, err := os.Stat(joined)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, mapPathError(err)
		}
		st, ok := statFromInfo(joined, info)
		if !ok {
			continue
		}
		if entryType != "" && entryType != "all" && st.Type != entryType {
			continue
		}
		entries = append(entries, Entry{
			Name:       dirEntry.Name(),
			Path:       st.Path,
			Type:       st.Type,
			Size:       st.Size,
			ModifiedMs: st.ModifiedMs,
		})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}

// Stat reports metadata for a resolved path. Regular files map to "file",
// directories map to "dir", and other object types are invalid.
func (s *Service) Stat(path string) (Stat, error) {
	resolved, err := s.Resolve(path)
	if err != nil {
		return Stat{}, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return Stat{}, mapPathError(err)
	}
	st, ok := statFromInfo(resolved, info)
	if !ok {
		return Stat{}, invalidError("unsupported file type")
	}
	return st, nil
}

// Open returns an open descriptor plus metadata for a regular file. Opening a
// directory or any non-regular object is invalid. The descriptor is stat'ed
// directly so the caller does not re-resolve a possibly changed path.
func (s *Service) Open(path string) (*os.File, Stat, error) {
	resolved, err := s.Resolve(path)
	if err != nil {
		return nil, Stat{}, err
	}
	file, err := os.Open(resolved)
	if err != nil {
		return nil, Stat{}, mapPathError(err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, Stat{}, mapPathError(err)
	}
	st, ok := statFromInfo(resolved, info)
	if !ok || st.Type != "file" {
		_ = file.Close()
		return nil, Stat{}, invalidError("path is not a file")
	}
	return file, st, nil
}

// WriteFile streams src into a same-directory temporary file and renames it
// over the target, so a read failure never truncates an existing destination.
// Missing parents are created; created files are mode 0644 subject to umask.
func (s *Service) WriteFile(path string, src io.Reader) (FileResult, error) {
	s.mutations.Lock()
	defer s.mutations.Unlock()

	resolved, err := s.Resolve(path)
	if err != nil {
		return FileResult{}, err
	}
	dir := filepath.Dir(resolved)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return FileResult{}, mapMutationError(err)
	}
	tmpName := filepath.Join(dir, ".agent-bridge-write-"+rand.Text())
	tmp, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return FileResult{}, mapMutationError(err)
	}
	defer func() { _ = os.Remove(tmpName) }()

	size, err := io.Copy(tmp, src)
	if err != nil {
		_ = tmp.Close()
		return FileResult{}, &Error{Kind: ErrorKindInternal, Message: "write failed", Err: err}
	}
	if err := tmp.Close(); err != nil {
		return FileResult{}, mapMutationError(err)
	}
	if err := os.Rename(tmpName, resolved); err != nil {
		return FileResult{}, mapMutationError(err)
	}
	return FileResult{Path: resolved, Size: size}, nil
}

// Remove deletes a resolved file, symlink, or directory tree. A symlink is
// unlinked itself rather than followed; directories are removed recursively.
func (s *Service) Remove(path string) error {
	s.mutations.Lock()
	defer s.mutations.Unlock()

	resolved, err := s.Resolve(path)
	if err != nil {
		return err
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return mapMutationError(err)
	}
	if info.IsDir() {
		err = os.RemoveAll(resolved)
	} else {
		err = os.Remove(resolved)
	}
	return mapMutationError(err)
}

// Mkdir creates name, exactly one non-dot path component, under directory.
// Creating an existing directory succeeds.
func (s *Service) Mkdir(directory, name string) (PathResult, error) {
	s.mutations.Lock()
	defer s.mutations.Unlock()

	resolved, err := s.Resolve(directory)
	if err != nil {
		return PathResult{}, err
	}
	if err := validateName(name); err != nil {
		return PathResult{}, err
	}
	target := filepath.Join(resolved, name)
	if err := os.MkdirAll(target, 0o755); err != nil {
		return PathResult{}, mapMutationError(err)
	}
	return PathResult{Path: target}, nil
}

// Move renames source onto destination after creating destination parents.
// os.Rename is called directly: compatible targets are replaced atomically,
// while type conflicts, non-empty directory targets, and cross-device moves
// fail with conflict and leave the destination untouched.
func (s *Service) Move(source, destination string) (PathResult, error) {
	s.mutations.Lock()
	defer s.mutations.Unlock()

	src, err := s.Resolve(source)
	if err != nil {
		return PathResult{}, err
	}
	dst, err := s.Resolve(destination)
	if err != nil {
		return PathResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return PathResult{}, mapMutationError(err)
	}
	if err := os.Rename(src, dst); err != nil {
		return PathResult{}, mapMutationError(err)
	}
	return PathResult{Path: dst}, nil
}

// validateName rejects names that are not exactly one non-dot path component.
func validateName(name string) error {
	if name == "" || name == "." || name == ".." {
		return invalidError("name must be a single path component")
	}
	if strings.IndexByte(name, 0) >= 0 {
		return invalidError("name must not contain NUL")
	}
	if strings.ContainsRune(name, filepath.Separator) || filepath.Base(name) != name {
		return invalidError("name must be a single path component")
	}
	return nil
}

// statFromInfo reports whether info is a supported file or directory and
// converts it to a Stat with permission-only mode.
func statFromInfo(path string, info fs.FileInfo) (Stat, bool) {
	entryType := ""
	switch {
	case info.Mode().IsRegular():
		entryType = "file"
	case info.IsDir():
		entryType = "dir"
	default:
		return Stat{}, false
	}
	return Stat{
		Path:       path,
		Type:       entryType,
		Size:       info.Size(),
		ModifiedMs: info.ModTime().UnixMilli(),
		Mode:       uint32(info.Mode().Perm()),
	}, true
}

func invalidError(message string) *Error {
	return &Error{Kind: ErrorKindInvalid, Message: message}
}

func mapPathError(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return &Error{Kind: ErrorKindNotFound, Message: "path not found", Err: err}
	}
	return &Error{Kind: ErrorKindInternal, Message: "filesystem operation failed", Err: err}
}

// mapMutationError classifies a mutation failure. Missing paths are not_found,
// incompatible targets and cross-device renames are conflict, and anything
// else is an internal I/O failure. Underlying causes stay server-side.
func mapMutationError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return &Error{Kind: ErrorKindNotFound, Message: "path not found", Err: err}
	case errors.Is(err, syscall.EXDEV),
		errors.Is(err, syscall.ENOTEMPTY),
		errors.Is(err, syscall.ENOTDIR),
		errors.Is(err, syscall.EISDIR),
		errors.Is(err, fs.ErrExist):
		return &Error{Kind: ErrorKindConflict, Message: "destination conflict", Err: err}
	default:
		return &Error{Kind: ErrorKindInternal, Message: "filesystem operation failed", Err: err}
	}
}

package filesystem

import (
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// WriteFile streams src into a same-directory temporary file and renames it
// over the target, so a read failure never truncates an existing destination.
// Missing parents are created; created files are mode 0644 subject to umask.
//
// Only a regular file is replaced. Lstat immediately before the rename rejects
// an existing directory, symlink, or special file with conflict, leaving the
// destination untouched and removing the temporary data. Rename would unlink a
// symlink rather than follow it, so rejecting symlinks keeps PUT at upload's
// regular-file-only replacement rule even though read endpoints follow symlinks.
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
	if info, err := os.Lstat(resolved); err == nil {
		if !info.Mode().IsRegular() {
			return FileResult{}, &Error{Kind: ErrorKindConflict, Message: "destination is not a regular file"}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
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
// fail with conflict. A failed rename can leave newly created destination
// parents behind.
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

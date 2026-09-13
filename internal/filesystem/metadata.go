package filesystem

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

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
// directory or any non-regular object is invalid. The caller must close the
// returned file; on failure Open closes it itself. The descriptor is stat'ed
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

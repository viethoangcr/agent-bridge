package filesystem

import (
	"archive/tar"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

// archiveTypeRegA is the legacy NUL regular-file typeflag. The plan requires
// accepting it, but archive/tar's TypeRegA constant is deprecated, so name the
// literal rather than referencing the deprecated symbol.
const archiveTypeRegA byte = 0

// archiveItem is one validated tar entry. name is the cleaned archive-relative
// slash path; path is the final absolute destination path.
type archiveItem struct {
	name string
	path string
	size int64
	mode os.FileMode
	dir  bool
}

// archiveManifest accumulates validated entries and rejects duplicates and
// file/parent conflicts before anything reaches the destination.
type archiveManifest struct {
	items    []archiveItem
	seen     map[string]bool
	hasChild map[string]bool
}

func newArchiveManifest() archiveManifest {
	return archiveManifest{seen: map[string]bool{}, hasChild: map[string]bool{}}
}

// add records item or reports a duplicate path or an ancestor/descendant type
// conflict. A regular file may not be a parent, and no path may repeat.
func (m *archiveManifest) add(item archiveItem) error {
	if _, exists := m.seen[item.name]; exists {
		return invalidError("archive contains a duplicate path")
	}
	if !item.dir && m.hasChild[item.name] {
		return invalidError("archive path is both a file and a parent directory")
	}
	for parent := path.Dir(item.name); parent != "."; parent = path.Dir(parent) {
		if isDir, exists := m.seen[parent]; exists && !isDir {
			return invalidError("archive path has a file parent")
		}
		m.hasChild[parent] = true
	}
	m.seen[item.name] = item.dir
	m.items = append(m.items, item)
	return nil
}

// uploadedFiles returns regular files sorted by final absolute path. The result
// is never nil so an empty archive encodes as an empty list.
func (m archiveManifest) uploadedFiles() []UploadedFile {
	files := make([]UploadedFile, 0, len(m.items))
	for _, item := range m.items {
		if !item.dir {
			files = append(files, UploadedFile{Path: item.path, Size: item.size})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files
}

// validateArchivePath rejects absolute, empty, NUL, and parent-escaping names
// and returns the cleaned archive-relative slash path. "." components are
// dropped before cleaning so cleaning cannot hide a raw parent component.
func validateArchivePath(name string) (string, error) {
	if name == "" {
		return "", invalidError("archive entry has an empty name")
	}
	if strings.IndexByte(name, 0) >= 0 {
		return "", invalidError("archive entry name contains NUL")
	}
	if filepath.IsAbs(name) {
		return "", invalidError("archive entry name is absolute")
	}
	parts := make([]string, 0, strings.Count(name, "/")+1)
	for _, component := range strings.Split(name, "/") {
		switch component {
		case "", ".":
			continue
		case "..":
			return "", invalidError("archive entry name escapes the destination")
		default:
			parts = append(parts, component)
		}
	}
	if len(parts) == 0 {
		return "", invalidError("archive entry has an empty name")
	}
	return path.Join(parts...), nil
}

// extractArchive validates every tar entry and extracts it into staging. It
// accepts only regular files and directories, enforces the cumulative
// declared size limit, and rejects short entries. destination is used only to
// compute the final absolute paths recorded in the manifest.
func extractArchive(tr *tar.Reader, destination, staging string, extractedLimit int64) (archiveManifest, error) {
	manifest := newArchiveManifest()
	var extracted int64

	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return manifest, nil
		}
		if err != nil {
			return archiveManifest{}, invalidError("archive is not a valid tar stream")
		}

		name, err := validateArchivePath(header.Name)
		if err != nil {
			return archiveManifest{}, err
		}

		dir := false
		switch header.Typeflag {
		case tar.TypeReg, archiveTypeRegA:
		case tar.TypeDir:
			dir = true
		default:
			return archiveManifest{}, invalidError("archive entry type is not a regular file or directory")
		}
		if header.Size < 0 {
			return archiveManifest{}, invalidError("archive entry has a negative size")
		}
		if !dir {
			if header.Size > extractedLimit-extracted {
				return archiveManifest{}, tooLargeError("upload exceeds the extracted size limit")
			}
			extracted += header.Size
		}

		item := archiveItem{
			name: name,
			path: filepath.Join(destination, name),
			size: header.Size,
			mode: header.FileInfo().Mode().Perm() & 0o777,
			dir:  dir,
		}
		if err := manifest.add(item); err != nil {
			return archiveManifest{}, err
		}

		target := filepath.Join(staging, name)
		if dir {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return archiveManifest{}, mapMutationError(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return archiveManifest{}, mapMutationError(err)
		}
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, item.mode)
		if err != nil {
			return archiveManifest{}, mapMutationError(err)
		}
		limited := &io.LimitedReader{R: tr, N: header.Size}
		_, copyErr := io.Copy(file, limited)
		closeErr := file.Close()
		if copyErr != nil {
			if errors.Is(copyErr, io.ErrUnexpectedEOF) || errors.Is(copyErr, io.EOF) {
				return archiveManifest{}, invalidError("archive entry is shorter than its header")
			}
			return archiveManifest{}, &Error{Kind: ErrorKindInternal, Message: "extraction failed", Err: copyErr}
		}
		if closeErr != nil {
			return archiveManifest{}, mapMutationError(closeErr)
		}
		if limited.N != 0 {
			return archiveManifest{}, invalidError("archive entry is shorter than its header")
		}
	}
}

// rejectSymlinkComponents walks every existing component of path with os.Lstat
// and rejects any symlink, including one that points back inside the
// destination. A component that exists but is not a directory (ENOTDIR) is a
// destination type conflict. Missing components are allowed; they are created
// later.
func rejectSymlinkComponents(target string) error {
	cleaned := filepath.Clean(target)
	if !filepath.IsAbs(cleaned) {
		return invalidError("destination path must be absolute")
	}
	current := string(filepath.Separator)
	rest := strings.TrimPrefix(cleaned, string(filepath.Separator))
	for _, component := range strings.Split(rest, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if errors.Is(err, syscall.ENOTDIR) {
				return &Error{Kind: ErrorKindConflict, Message: "destination component is not a directory"}
			}
			return mapPathError(err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return invalidError("destination contains a symlink component")
		}
	}
	return nil
}

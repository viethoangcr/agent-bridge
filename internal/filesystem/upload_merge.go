package filesystem

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// preflightMerge rejects every destination conflict before the first mutation:
// archive files over existing directories, archive directories over existing
// regular files, non-regular destination objects, symlink components, and
// non-directory ancestors. It performs no writes, so a rejected destination
// stays byte-for-byte unchanged.
func preflightMerge(manifest archiveManifest) error {
	for _, item := range manifest.items {
		if err := rejectSymlinkComponents(item.path); err != nil {
			return err
		}
		info, err := os.Lstat(item.path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return mapPathError(err)
		}
		if item.dir {
			if !info.IsDir() {
				return &Error{Kind: ErrorKindConflict, Message: "archive directory conflicts with an existing file"}
			}
			continue
		}
		if info.IsDir() {
			return &Error{Kind: ErrorKindConflict, Message: "archive file conflicts with an existing directory"}
		}
		if !info.Mode().IsRegular() {
			return &Error{Kind: ErrorKindConflict, Message: "archive file conflicts with an existing non-regular file"}
		}
	}
	return nil
}

// mergeStaging applies a preflighted manifest inside the staging lifetime:
// directories are created shallowest-first, then regular files are replaced in
// final-path order. Matching directories are reused and unrelated destination
// content is left untouched. Merge is not transactional; once it begins, a
// mid-merge I/O failure can leave earlier items applied.
func mergeStaging(manifest archiveManifest, staging string) error {
	directories := make([]archiveItem, 0, len(manifest.items))
	files := make([]archiveItem, 0, len(manifest.items))
	for _, item := range manifest.items {
		if item.dir {
			directories = append(directories, item)
			continue
		}
		files = append(files, item)
	}
	sort.Slice(directories, func(i, j int) bool {
		left, right := strings.Count(directories[i].name, "/"), strings.Count(directories[j].name, "/")
		if left != right {
			return left < right
		}
		return directories[i].name < directories[j].name
	})
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })

	for _, item := range directories {
		if err := mergeItem(staging, item); err != nil {
			return err
		}
	}
	for _, item := range files {
		if err := mergeItem(staging, item); err != nil {
			return err
		}
	}
	return nil
}

// mergeItem creates one directory or replaces one regular file. A file is
// rechecked for symlink components and for a non-regular destination, then
// renamed to a same-directory temporary name and finally over the destination
// so replacement is one rename.
func mergeItem(staging string, item archiveItem) error {
	if item.dir {
		if err := rejectSymlinkComponents(item.path); err != nil {
			return err
		}
		return mapMutationError(os.MkdirAll(item.path, 0o755))
	}
	if err := rejectSymlinkComponents(item.path); err != nil {
		return err
	}
	parent := filepath.Dir(item.path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return mapMutationError(err)
	}
	source := filepath.Join(staging, filepath.FromSlash(item.name))
	temporary, err := os.CreateTemp(parent, ".agent-bridge-merge-")
	if err != nil {
		return mapMutationError(err)
	}
	temporaryName := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryName)
		return mapMutationError(err)
	}
	if err := os.Rename(source, temporaryName); err != nil {
		_ = os.Remove(temporaryName)
		return mapMutationError(err)
	}
	// Recheck the destination type immediately before the replacing rename: a
	// concurrent writer could have created a non-regular object since preflight.
	if info, err := os.Lstat(item.path); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(temporaryName)
			return mapMutationError(err)
		}
	} else if !info.Mode().IsRegular() {
		_ = os.Remove(temporaryName)
		return &Error{Kind: ErrorKindConflict, Message: "archive file conflicts with an existing non-regular file"}
	}
	if err := os.Rename(temporaryName, item.path); err != nil {
		_ = os.Remove(temporaryName)
		return mapMutationError(err)
	}
	return nil
}

// stagingParent returns the nearest existing directory at or above
// destination, so the staging tree shares a filesystem with the final renames.
func stagingParent(destination string) (string, error) {
	current := filepath.Clean(destination)
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return "", &Error{Kind: ErrorKindConflict, Message: "upload destination is not a directory"}
			}
			return current, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", mapPathError(err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", &Error{Kind: ErrorKindInternal, Message: "cannot locate an existing destination parent"}
		}
		current = parent
	}
}

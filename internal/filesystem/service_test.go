package filesystem

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"
)

func newTestService(t *testing.T, home string) *Service {
	t.Helper()
	svc, err := New(home, &sync.Mutex{})
	if err != nil {
		t.Fatalf("New(%q) error = %v", home, err)
	}
	return svc
}

func mustWriteFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func mustSymlink(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Symlink(oldname, newname); err != nil {
		t.Fatalf("symlink %s -> %s: %v", newname, oldname, err)
	}
}

func errorKind(t *testing.T, err error) ErrorKind {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("error %v is not *filesystem.Error", err)
	}
	return e.Kind
}

func entryNames(entries []Entry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name)
	}
	return names
}

func TestServiceResolve(t *testing.T) {
	const home = "/home/bridge"
	tests := []struct {
		name     string
		home     string
		raw      string
		want     string
		wantKind ErrorKind
	}{
		{name: "absolute already clean", home: home, raw: "/var/tmp", want: "/var/tmp"},
		{name: "absolute cleaned", home: home, raw: "/a/./b/../c//", want: "/a/c"},
		{name: "absolute with nul rejected", home: home, raw: "/a\x00b", wantKind: ErrorKindInvalid},
		{name: "absolute without home", home: "", raw: "/srv/data", want: "/srv/data"},
		{name: "dot resolves to home", home: home, raw: ".", want: home},
		{name: "curdir component", home: home, raw: "a/./b", want: "/home/bridge/a/b"},
		{name: "repeated slash", home: home, raw: "a//b", want: "/home/bridge/a/b"},
		{name: "trailing slash", home: home, raw: "a/b/", want: "/home/bridge/a/b"},
		{name: "backslash is ordinary", home: home, raw: `dir\..\file`, want: `/home/bridge/dir\..\file`},
		{name: "dotdot substring allowed", home: home, raw: "foo..bar/baz", want: "/home/bridge/foo..bar/baz"},
		{name: "empty", home: home, raw: "", wantKind: ErrorKindInvalid},
		{name: "relative empty home", home: "", raw: "a/b", wantKind: ErrorKindInvalid},
		{name: "nul", home: home, raw: "a\x00b", wantKind: ErrorKindInvalid},
		{name: "leading parent", home: home, raw: "../a", wantKind: ErrorKindInvalid},
		{name: "inner parent", home: home, raw: "a/../b", wantKind: ErrorKindInvalid},
		{name: "trailing parent", home: home, raw: "a/b/..", wantKind: ErrorKindInvalid},
		{name: "repeated slash parent", home: home, raw: "a//../b", wantKind: ErrorKindInvalid},
		{name: "dot parent", home: home, raw: "./../a", wantKind: ErrorKindInvalid},
		{name: "parent then normal", home: home, raw: "..//..", wantKind: ErrorKindInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestService(t, tc.home)
			got, err := svc.Resolve(tc.raw)
			if tc.wantKind != "" {
				if kind := errorKind(t, err); kind != tc.wantKind {
					t.Fatalf("Resolve(%q) kind = %q, want %q", tc.raw, kind, tc.wantKind)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve(%q) error = %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("Resolve(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestServiceResolveCapturesHomeOnce(t *testing.T) {
	t.Setenv("HOME", "/env/home")
	svc := newTestService(t, "/captured/home")

	got, err := svc.Resolve(".")
	if err != nil {
		t.Fatalf("Resolve(.) error = %v", err)
	}
	if got != "/captured/home" {
		t.Fatalf("Resolve(.) = %q, want captured /captured/home", got)
	}

	got, err = svc.Resolve("sub")
	if err != nil {
		t.Fatalf("Resolve(sub) error = %v", err)
	}
	if got != "/captured/home/sub" {
		t.Fatalf("Resolve(sub) = %q, want /captured/home/sub", got)
	}
}

func TestServiceEntriesDefaultsToAllAndSorts(t *testing.T) {
	home := t.TempDir()
	mustWriteFile(t, filepath.Join(home, "a.txt"), "alpha", 0o644)
	mustWriteFile(t, filepath.Join(home, "m.txt"), "mu", 0o644)
	mustMkdir(t, filepath.Join(home, "zdir"))
	mustSymlink(t, filepath.Join(home, "a.txt"), filepath.Join(home, "link_file"))
	mustSymlink(t, filepath.Join(home, "zdir"), filepath.Join(home, "link_dir"))
	mustSymlink(t, filepath.Join(home, "missing"), filepath.Join(home, "dangling"))
	if err := syscall.Mkfifo(filepath.Join(home, "fifo"), 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	fixed := time.UnixMilli(1_700_000_000_123)
	if err := os.Chtimes(filepath.Join(home, "a.txt"), fixed, fixed); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	svc := newTestService(t, home)
	entries, err := svc.Entries(home, "")
	if err != nil {
		t.Fatalf("Entries error = %v", err)
	}

	if got, want := entryNames(entries), []string{"a.txt", "link_dir", "link_file", "m.txt", "zdir"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries names = %v, want %v", got, want)
	}
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path }) {
		t.Fatalf("Entries not sorted by path: %v", entryNames(entries))
	}
	for _, entry := range entries {
		if want := filepath.Join(home, entry.Name); entry.Path != want {
			t.Fatalf("entry %q path = %q, want %q", entry.Name, entry.Path, want)
		}
	}

	byName := map[string]Entry{}
	for _, entry := range entries {
		byName[entry.Name] = entry
	}
	if got, want := byName["a.txt"].Type, "file"; got != want {
		t.Fatalf("a.txt type = %q, want %q", got, want)
	}
	if got, want := byName["a.txt"].Size, int64(len("alpha")); got != want {
		t.Fatalf("a.txt size = %d, want %d", got, want)
	}
	if got, want := byName["a.txt"].ModifiedMs, fixed.UnixMilli(); got != want {
		t.Fatalf("a.txt modifiedMs = %d, want %d", got, want)
	}
	if got, want := byName["zdir"].Type, "dir"; got != want {
		t.Fatalf("zdir type = %q, want %q", got, want)
	}
	if _, ok := byName["dangling"]; ok {
		t.Fatal("dangling symlink should be skipped")
	}
	if _, ok := byName["fifo"]; ok {
		t.Fatal("special file should be skipped")
	}

	allEntries, err := svc.Entries(".", "all")
	if err != nil {
		t.Fatalf("Entries(all) error = %v", err)
	}
	if got, want := entryNames(allEntries), entryNames(entries); !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries(all) names = %v, want %v", got, want)
	}
}

func TestServiceEntriesFiltersByType(t *testing.T) {
	home := t.TempDir()
	mustWriteFile(t, filepath.Join(home, "a.txt"), "alpha", 0o644)
	mustWriteFile(t, filepath.Join(home, "m.txt"), "mu", 0o644)
	mustMkdir(t, filepath.Join(home, "zdir"))
	mustSymlink(t, filepath.Join(home, "a.txt"), filepath.Join(home, "link_file"))
	mustSymlink(t, filepath.Join(home, "zdir"), filepath.Join(home, "link_dir"))

	svc := newTestService(t, home)

	files, err := svc.Entries(home, "file")
	if err != nil {
		t.Fatalf("Entries(file) error = %v", err)
	}
	if got, want := entryNames(files), []string{"a.txt", "link_file", "m.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries(file) names = %v, want %v", got, want)
	}
	for _, entry := range files {
		if entry.Type != "file" {
			t.Fatalf("Entries(file) returned type %q", entry.Type)
		}
	}

	dirs, err := svc.Entries(home, "dir")
	if err != nil {
		t.Fatalf("Entries(dir) error = %v", err)
	}
	if got, want := entryNames(dirs), []string{"link_dir", "zdir"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries(dir) names = %v, want %v", got, want)
	}
	for _, entry := range dirs {
		if entry.Type != "dir" {
			t.Fatalf("Entries(dir) returned type %q", entry.Type)
		}
	}
}

func TestServiceEntriesRejectsInvalidType(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)
	for _, entryType := range []string{"symlink", "FILE", "all,file", " "} {
		if _, err := svc.Entries(home, entryType); errorKind(t, err) != ErrorKindInvalid {
			t.Fatalf("Entries type %q kind = %q, want invalid", entryType, errorKind(t, err))
		}
	}
}

func TestServiceEntriesFollowsSymlinksAndSkipsSpecial(t *testing.T) {
	home := t.TempDir()
	mustWriteFile(t, filepath.Join(home, "target.txt"), "data", 0o644)
	mustMkdir(t, filepath.Join(home, "targetdir"))
	mustSymlink(t, "target.txt", filepath.Join(home, "link_file"))
	mustSymlink(t, "targetdir", filepath.Join(home, "link_dir"))
	mustSymlink(t, "gone", filepath.Join(home, "dangling"))

	svc := newTestService(t, home)
	entries, err := svc.Entries(home, "all")
	if err != nil {
		t.Fatalf("Entries error = %v", err)
	}
	byName := map[string]Entry{}
	for _, entry := range entries {
		byName[entry.Name] = entry
	}
	if got, want := byName["link_file"].Type, "file"; got != want {
		t.Fatalf("link_file type = %q, want %q", got, want)
	}
	if got, want := byName["link_dir"].Type, "dir"; got != want {
		t.Fatalf("link_dir type = %q, want %q", got, want)
	}
	if _, ok := byName["dangling"]; ok {
		t.Fatal("dangling symlink should be skipped")
	}
}

func TestServiceEntriesOtherStatFailureIsInternal(t *testing.T) {
	home := t.TempDir()
	loopDir := filepath.Join(home, "loopdir")
	mustMkdir(t, loopDir)
	mustSymlink(t, "loop", filepath.Join(loopDir, "loop"))

	svc := newTestService(t, home)
	_, err := svc.Entries(loopDir, "all")
	if kind := errorKind(t, err); kind != ErrorKindInternal {
		t.Fatalf("Entries over symlink loop kind = %q, want internal", kind)
	}
}

func TestServiceEntriesErrors(t *testing.T) {
	home := t.TempDir()
	mustWriteFile(t, filepath.Join(home, "a.txt"), "alpha", 0o644)
	svc := newTestService(t, home)

	if _, err := svc.Entries(filepath.Join(home, "missing"), "all"); errorKind(t, err) != ErrorKindNotFound {
		t.Fatalf("missing directory kind = %q, want not_found", errorKind(t, err))
	}
	if _, err := svc.Entries(filepath.Join(home, "a.txt"), "all"); errorKind(t, err) != ErrorKindInvalid {
		t.Fatalf("file as directory kind = %q, want invalid", errorKind(t, err))
	}
	if _, err := svc.Entries("", "all"); errorKind(t, err) != ErrorKindInvalid {
		t.Fatalf("empty directory kind = %q, want invalid", errorKind(t, err))
	}
}

package filesystem

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
)

func TestUploadInvalidLeavesDestinationUnchanged(t *testing.T) {
	home := t.TempDir()
	dest := filepath.Join(home, "out")
	mustMkdir(t, dest)
	mustWriteFile(t, filepath.Join(dest, "keep.txt"), "original", 0o644)

	svc := newTestService(t, home)
	archive := uploadGzip(t, rawTarArchive(rawTarHeader("../escape", tar.TypeReg, octalField(0), ""), true))
	_, err := svc.Upload("out", bytes.NewReader(archive))
	if kind := errorKind(t, err); kind != ErrorKindInvalid {
		t.Fatalf("kind = %q, want invalid", kind)
	}

	if got, readErr := os.ReadFile(filepath.Join(dest, "keep.txt")); readErr != nil || string(got) != "original" {
		t.Fatalf("destination changed: content = %q err=%v", got, readErr)
	}
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "keep.txt" {
		t.Fatalf("destination entries = %v, want only keep.txt", entries)
	}
	if leftovers := stagingLeftovers(t, dest); len(leftovers) != 0 {
		t.Fatalf("staging left behind: %v", leftovers)
	}
}

func TestUploadRejectsDestinationSymlinks(t *testing.T) {
	t.Run("destination root is a symlink", func(t *testing.T) {
		home := t.TempDir()
		real := filepath.Join(home, "real")
		mustMkdir(t, real)
		mustSymlink(t, real, filepath.Join(home, "out"))

		svc := newTestService(t, home)
		archive := uploadGzip(t, uploadTar(t, tarEntry{name: "f", body: []byte("x")}))
		assertRejected(t, svc, home, "out", archive, ErrorKindInvalid)
		if entries, err := os.ReadDir(real); err != nil || len(entries) != 0 {
			t.Fatalf("symlink target mutated: entries=%v err=%v", entries, err)
		}
	})

	t.Run("symlink component points outside", func(t *testing.T) {
		home := t.TempDir()
		dest := filepath.Join(home, "out")
		mustMkdir(t, dest)
		outside := filepath.Join(home, "outside")
		mustMkdir(t, outside)
		mustSymlink(t, outside, filepath.Join(dest, "sub"))

		svc := newTestService(t, home)
		archive := uploadGzip(t, uploadTar(t, tarEntry{name: "sub/file", body: []byte("x")}))
		assertRejected(t, svc, home, "out", archive, ErrorKindInvalid)
		if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
			t.Fatalf("outside target mutated: entries=%v err=%v", entries, err)
		}
	})

	t.Run("symlink points back inside destination", func(t *testing.T) {
		home := t.TempDir()
		dest := filepath.Join(home, "out")
		mustMkdir(t, dest)
		mustSymlink(t, dest, filepath.Join(dest, "loop"))

		svc := newTestService(t, home)
		archive := uploadGzip(t, uploadTar(t, tarEntry{name: "loop/evil", body: []byte("x")}))
		assertRejected(t, svc, home, "out", archive, ErrorKindInvalid)
		if _, err := os.Lstat(filepath.Join(dest, "evil")); !os.IsNotExist(err) {
			t.Fatalf("file written through symlink: %v", err)
		}
	})
}

func TestUploadRejectsNonRegularDestination(t *testing.T) {
	home := t.TempDir()
	dest := filepath.Join(home, "out")
	mustMkdir(t, dest)
	fifo := filepath.Join(dest, "pipe")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	svc := newTestService(t, home)
	archive := uploadGzip(t, uploadTar(t, tarEntry{name: "pipe", body: []byte("data")}))
	_, err := svc.Upload("out", bytes.NewReader(archive))
	if kind := errorKind(t, err); kind != ErrorKindConflict {
		t.Fatalf("kind = %q, want conflict", kind)
	}
	info, statErr := os.Lstat(fifo)
	if statErr != nil {
		t.Fatalf("lstat fifo: %v", statErr)
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("fifo was replaced: mode = %v", info.Mode())
	}
	if leftovers := stagingLeftovers(t, home); len(leftovers) != 0 {
		t.Fatalf("staging left behind: %v", leftovers)
	}
}

func TestUploadMergeOverwritesReusesAndPreserves(t *testing.T) {
	home := t.TempDir()
	dest := filepath.Join(home, "out")
	mustMkdir(t, dest)
	mustMkdir(t, filepath.Join(dest, "sub"))
	mustMkdir(t, filepath.Join(dest, "topdir"))
	mustWriteFile(t, filepath.Join(dest, "keep.txt"), "original", 0o644)
	mustWriteFile(t, filepath.Join(dest, "sub", "old.txt"), "old", 0o644)
	mustWriteFile(t, filepath.Join(dest, "sub", "extra.txt"), "extra", 0o644)
	mustWriteFile(t, filepath.Join(dest, "topdir", "unrelated.txt"), "unrelated", 0o644)

	svc := newTestService(t, home)
	archive := uploadGzip(t, uploadTar(t,
		tarEntry{name: "keep.txt", body: []byte("replaced")},
		tarEntry{name: "sub/", typeflag: tar.TypeDir},
		tarEntry{name: "sub/old.txt", body: []byte("new")},
		tarEntry{name: "sub/nested/new.txt", body: []byte("nested")},
		tarEntry{name: "fresh.txt", body: []byte("fresh")},
	))

	files, err := svc.Upload("out", bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("Upload error = %v", err)
	}

	for path, content := range map[string]string{
		"keep.txt":             "replaced",
		"sub/old.txt":          "new",
		"sub/nested/new.txt":   "nested",
		"fresh.txt":            "fresh",
		"sub/extra.txt":        "extra",
		"topdir/unrelated.txt": "unrelated",
	} {
		got, readErr := os.ReadFile(filepath.Join(dest, path))
		if readErr != nil || string(got) != content {
			t.Fatalf("destination %s = %q err=%v, want %q", path, got, readErr, content)
		}
	}

	wantPaths := []string{
		filepath.Join(dest, "fresh.txt"),
		filepath.Join(dest, "keep.txt"),
		filepath.Join(dest, "sub/nested/new.txt"),
		filepath.Join(dest, "sub/old.txt"),
	}
	if len(files) != len(wantPaths) {
		t.Fatalf("files = %#v, want %d regular files", files, len(wantPaths))
	}
	if !sort.SliceIsSorted(files, func(i, j int) bool { return files[i].Path < files[j].Path }) {
		t.Fatalf("files not sorted by final path: %#v", files)
	}
	for i, path := range wantPaths {
		if files[i].Path != path {
			t.Fatalf("files[%d].Path = %q, want %q", i, files[i].Path, path)
		}
	}
	if leftovers := stagingLeftovers(t, home); len(leftovers) != 0 {
		t.Fatalf("staging left behind: %v", leftovers)
	}
}

func TestUploadMergePreflightRejectsWithoutChanges(t *testing.T) {
	t.Run("archive file over destination directory", func(t *testing.T) {
		home := t.TempDir()
		dest := filepath.Join(home, "out")
		mustMkdir(t, dest)
		mustMkdir(t, filepath.Join(dest, "blocked"))
		mustWriteFile(t, filepath.Join(dest, "blocked", "inside.txt"), "inside", 0o644)

		svc := newTestService(t, home)
		archive := uploadGzip(t, uploadTar(t,
			tarEntry{name: "ok.txt", body: []byte("ok")},
			tarEntry{name: "blocked", body: []byte("file")},
		))
		_, err := svc.Upload("out", bytes.NewReader(archive))
		if kind := errorKind(t, err); kind != ErrorKindConflict {
			t.Fatalf("kind = %q, want conflict", kind)
		}
		if _, statErr := os.Lstat(filepath.Join(dest, "ok.txt")); !os.IsNotExist(statErr) {
			t.Fatalf("destination mutated before rejection: ok.txt exists")
		}
		if got, readErr := os.ReadFile(filepath.Join(dest, "blocked", "inside.txt")); readErr != nil || string(got) != "inside" {
			t.Fatalf("destination directory changed: %q err=%v", got, readErr)
		}
		if leftovers := stagingLeftovers(t, home); len(leftovers) != 0 {
			t.Fatalf("staging left behind: %v", leftovers)
		}
	})

	t.Run("archive directory over destination file", func(t *testing.T) {
		home := t.TempDir()
		dest := filepath.Join(home, "out")
		mustMkdir(t, dest)
		mustWriteFile(t, filepath.Join(dest, "blocked"), "original", 0o644)

		svc := newTestService(t, home)
		archive := uploadGzip(t, uploadTar(t,
			tarEntry{name: "ok.txt", body: []byte("ok")},
			tarEntry{name: "blocked/", typeflag: tar.TypeDir},
			tarEntry{name: "blocked/x", body: []byte("x")},
		))
		_, err := svc.Upload("out", bytes.NewReader(archive))
		if kind := errorKind(t, err); kind != ErrorKindConflict {
			t.Fatalf("kind = %q, want conflict", kind)
		}
		if _, statErr := os.Lstat(filepath.Join(dest, "ok.txt")); !os.IsNotExist(statErr) {
			t.Fatalf("destination mutated before rejection: ok.txt exists")
		}
		if got, readErr := os.ReadFile(filepath.Join(dest, "blocked")); readErr != nil || string(got) != "original" {
			t.Fatalf("destination file changed: %q err=%v", got, readErr)
		}
		if leftovers := stagingLeftovers(t, home); len(leftovers) != 0 {
			t.Fatalf("staging left behind: %v", leftovers)
		}
	})

	t.Run("symlink destination component", func(t *testing.T) {
		home := t.TempDir()
		dest := filepath.Join(home, "out")
		outside := filepath.Join(home, "outside")
		mustMkdir(t, dest)
		mustMkdir(t, outside)
		mustSymlink(t, outside, filepath.Join(dest, "link"))

		svc := newTestService(t, home)
		archive := uploadGzip(t, uploadTar(t,
			tarEntry{name: "ok.txt", body: []byte("ok")},
			tarEntry{name: "link/file.txt", body: []byte("x")},
		))
		_, err := svc.Upload("out", bytes.NewReader(archive))
		if kind := errorKind(t, err); kind != ErrorKindInvalid {
			t.Fatalf("kind = %q, want invalid", kind)
		}
		if _, statErr := os.Lstat(filepath.Join(dest, "ok.txt")); !os.IsNotExist(statErr) {
			t.Fatalf("destination mutated before rejection: ok.txt exists")
		}
		if entries, readErr := os.ReadDir(outside); readErr != nil || len(entries) != 0 {
			t.Fatalf("symlink target mutated: entries=%v err=%v", entries, readErr)
		}
		if leftovers := stagingLeftovers(t, home); len(leftovers) != 0 {
			t.Fatalf("staging left behind: %v", leftovers)
		}
	})
}

func TestUploadMergeForcedIOFailureRemovesStaging(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based merge failure requires a non-root user")
	}
	home := t.TempDir()
	dest := filepath.Join(home, "out")
	readonly := filepath.Join(dest, "readonly")
	mustMkdir(t, dest)
	mustMkdir(t, readonly)
	if err := os.Chmod(readonly, 0o500); err != nil {
		t.Fatalf("chmod readonly: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(readonly, 0o755) })

	svc := newTestService(t, home)
	archive := uploadGzip(t, uploadTar(t, tarEntry{name: "readonly/f.txt", body: []byte("data")}))
	_, err := svc.Upload("out", bytes.NewReader(archive))
	if kind := errorKind(t, err); kind != ErrorKindInternal {
		t.Fatalf("kind = %q, want internal", kind)
	}
	if leftovers := stagingLeftovers(t, dest); len(leftovers) != 0 {
		t.Fatalf("staging left behind: %v", leftovers)
	}
	entries, readErr := os.ReadDir(readonly)
	if readErr != nil {
		t.Fatalf("read readonly: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("readonly directory mutated: %v", entries)
	}
}

// firstReadSignal closes first on the first Read call so a test can observe
// when Upload starts consuming the compressed input.
type firstReadSignal struct {
	r     io.Reader
	first chan struct{}
	once  sync.Once
}

func (r *firstReadSignal) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.first) })
	return r.r.Read(p)
}

func TestUploadAcquiresMutationMutexBeforeReading(t *testing.T) {
	home := t.TempDir()
	mutations := &sync.Mutex{}
	svc, err := New(home, mutations)
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	archive := uploadGzip(t, uploadTar(t, tarEntry{name: "f.txt", body: []byte("data")}))
	src := &firstReadSignal{r: bytes.NewReader(archive), first: make(chan struct{})}

	mutations.Lock()
	done := make(chan error, 1)
	go func() {
		_, uploadErr := svc.Upload("out", src)
		done <- uploadErr
	}()
	for range 1000 {
		runtime.Gosched()
		select {
		case <-src.first:
			mutations.Unlock()
			t.Fatal("Upload read the compressed input before acquiring the mutation mutex")
		case uploadErr := <-done:
			mutations.Unlock()
			t.Fatalf("Upload returned while the mutation mutex was held (err=%v)", uploadErr)
		default:
		}
	}
	mutations.Unlock()
	if uploadErr := <-done; uploadErr != nil {
		t.Fatalf("Upload error = %v", uploadErr)
	}
}

func TestUploadMergeSerializedByMutationMutex(t *testing.T) {
	home := t.TempDir()
	mutations := &sync.Mutex{}
	svc, err := New(home, mutations)
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	archive := uploadGzip(t, uploadTar(t, tarEntry{name: "f.txt", body: []byte("data")}))

	mutations.Lock()
	done := make(chan error, 1)
	go func() {
		_, uploadErr := svc.Upload("out", bytes.NewReader(archive))
		done <- uploadErr
	}()
	for range 1000 {
		runtime.Gosched()
		select {
		case uploadErr := <-done:
			mutations.Unlock()
			t.Fatalf("Upload did not block on the injected mutation mutex (err=%v)", uploadErr)
		default:
		}
	}
	mutations.Unlock()
	if uploadErr := <-done; uploadErr != nil {
		t.Fatalf("Upload error = %v", uploadErr)
	}
	if got, readErr := os.ReadFile(filepath.Join(home, "out", "f.txt")); readErr != nil || string(got) != "data" {
		t.Fatalf("merged content = %q err=%v", got, readErr)
	}
}

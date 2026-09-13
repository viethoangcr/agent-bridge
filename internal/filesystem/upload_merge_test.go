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
	"testing"
)

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

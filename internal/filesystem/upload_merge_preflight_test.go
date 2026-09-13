package filesystem

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
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

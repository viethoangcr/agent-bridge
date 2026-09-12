package filesystem

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func TestServiceMkdir(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)

	wantA := filepath.Join(home, "a")
	res, err := svc.Mkdir(".", "a")
	if err != nil {
		t.Fatalf("Mkdir error = %v", err)
	}
	if res.Path != wantA {
		t.Fatalf("Mkdir path = %q, want %q", res.Path, wantA)
	}
	if info, err := os.Stat(wantA); err != nil || !info.IsDir() {
		t.Fatalf("a is not a directory: %v", err)
	}

	res, err = svc.Mkdir(".", "a")
	if err != nil || res.Path != wantA {
		t.Fatalf("Mkdir idempotent = %+v, %v", res, err)
	}

	if _, err := svc.Mkdir("a", "b"); err != nil {
		t.Fatalf("Mkdir nested error = %v", err)
	}
	if info, err := os.Stat(filepath.Join(home, "a", "b")); err != nil || !info.IsDir() {
		t.Fatalf("a/b is not a directory: %v", err)
	}

	for _, name := range []string{"", ".", "..", "a/b", "/", "a\x00b", "../x"} {
		if _, err := svc.Mkdir(".", name); errorKind(t, err) != ErrorKindInvalid {
			t.Fatalf("Mkdir(%q) kind = %q, want invalid", name, errorKind(t, err))
		}
	}

	mustWriteFile(t, filepath.Join(home, "blocker"), "x", 0o644)
	if _, err := svc.Mkdir(".", "blocker"); errorKind(t, err) != ErrorKindConflict {
		t.Fatalf("Mkdir over file kind = %q, want conflict", errorKind(t, err))
	}
}

func TestServiceMove(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)

	mustWriteFile(t, filepath.Join(home, "src.txt"), "source", 0o644)
	res, err := svc.Move("src.txt", "sub/dir/dst.txt")
	if err != nil {
		t.Fatalf("Move error = %v", err)
	}
	wantDst := filepath.Join(home, "sub/dir/dst.txt")
	if res.Path != wantDst {
		t.Fatalf("Move path = %q, want %q", res.Path, wantDst)
	}
	if _, err := os.Lstat(filepath.Join(home, "src.txt")); !os.IsNotExist(err) {
		t.Fatalf("source still present: %v", err)
	}
	if got, err := os.ReadFile(wantDst); err != nil || string(got) != "source" {
		t.Fatalf("moved content = %q err=%v", got, err)
	}

	mustWriteFile(t, filepath.Join(home, "a.txt"), "A", 0o644)
	mustWriteFile(t, filepath.Join(home, "b.txt"), "B", 0o644)
	if _, err := svc.Move("a.txt", "b.txt"); err != nil {
		t.Fatalf("Move replace error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(home, "b.txt")); err != nil || string(got) != "A" {
		t.Fatalf("replaced content = %q err=%v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(home, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("replaced source still present: %v", err)
	}

	mustMkdir(t, filepath.Join(home, "srcdir"))
	mustWriteFile(t, filepath.Join(home, "srcdir", "x"), "x", 0o644)
	movedDir := filepath.Join(home, "moveddir")
	if _, err := svc.Move("srcdir", "moveddir"); err != nil {
		t.Fatalf("Move directory error = %v", err)
	}
	if info, err := os.Stat(movedDir); err != nil || !info.IsDir() {
		t.Fatalf("moveddir is not a directory: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(movedDir, "x")); err != nil || string(got) != "x" {
		t.Fatalf("moved dir content = %q err=%v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(home, "srcdir")); !os.IsNotExist(err) {
		t.Fatalf("srcdir still present: %v", err)
	}

	// os.Rename refuses to replace any existing directory, so an empty
	// directory destination is a conflict that leaves both trees in place.
	mustMkdir(t, filepath.Join(home, "d1"))
	mustMkdir(t, filepath.Join(home, "d2"))
	if _, err := svc.Move("d1", "d2"); errorKind(t, err) != ErrorKindConflict {
		t.Fatalf("dir over dir kind = %q, want conflict", errorKind(t, err))
	}
	if info, err := os.Stat(filepath.Join(home, "d2")); err != nil || !info.IsDir() {
		t.Fatalf("d2 changed: %v", err)
	}
	if info, err := os.Stat(filepath.Join(home, "d1")); err != nil || !info.IsDir() {
		t.Fatalf("d1 consumed: %v", err)
	}

	if _, err := svc.Move("nope", "elsewhere"); errorKind(t, err) != ErrorKindNotFound {
		t.Fatalf("Move missing source kind = %q, want not_found", errorKind(t, err))
	}
}

func TestServiceMoveRejectsConflictsWithoutChangingDestination(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)

	mustWriteFile(t, filepath.Join(home, "file1"), "file", 0o644)
	mustMkdir(t, filepath.Join(home, "dir1"))
	mustWriteFile(t, filepath.Join(home, "dir1", "keep"), "keep", 0o644)
	if _, err := svc.Move("file1", "dir1"); errorKind(t, err) != ErrorKindConflict {
		t.Fatalf("file over dir kind = %q, want conflict", errorKind(t, err))
	}
	if got, err := os.ReadFile(filepath.Join(home, "dir1", "keep")); err != nil || string(got) != "keep" {
		t.Fatalf("destination dir changed: got %q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(home, "file1")); err != nil {
		t.Fatalf("source consumed: %v", err)
	}

	mustMkdir(t, filepath.Join(home, "dir2"))
	mustWriteFile(t, filepath.Join(home, "file2"), "file", 0o644)
	if _, err := svc.Move("dir2", "file2"); errorKind(t, err) != ErrorKindConflict {
		t.Fatalf("dir over file kind = %q, want conflict", errorKind(t, err))
	}
	if got, err := os.ReadFile(filepath.Join(home, "file2")); err != nil || string(got) != "file" {
		t.Fatalf("destination file changed: got %q err=%v", got, err)
	}
	if info, err := os.Stat(filepath.Join(home, "dir2")); err != nil || !info.IsDir() {
		t.Fatalf("source dir consumed: %v", err)
	}

	mustMkdir(t, filepath.Join(home, "srcdir"))
	mustMkdir(t, filepath.Join(home, "fulldir"))
	mustWriteFile(t, filepath.Join(home, "fulldir", "keep"), "keep", 0o644)
	if _, err := svc.Move("srcdir", "fulldir"); errorKind(t, err) != ErrorKindConflict {
		t.Fatalf("dir over non-empty dir kind = %q, want conflict", errorKind(t, err))
	}
	if got, err := os.ReadFile(filepath.Join(home, "fulldir", "keep")); err != nil || string(got) != "keep" {
		t.Fatalf("non-empty destination changed: got %q err=%v", got, err)
	}
	if info, err := os.Stat(filepath.Join(home, "srcdir")); err != nil || !info.IsDir() {
		t.Fatalf("source dir consumed: %v", err)
	}
}

func TestServiceMoveCrossDeviceConflict(t *testing.T) {
	shm, err := os.MkdirTemp("/dev/shm", "agent-bridge-exdev-*")
	if err != nil {
		t.Skipf("requires /dev/shm: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shm) })

	home := t.TempDir()
	sourceDev, err := deviceID(shm)
	if err != nil {
		t.Fatalf("device id for shm: %v", err)
	}
	destinationDev, err := deviceID(home)
	if err != nil {
		t.Fatalf("device id for home: %v", err)
	}
	if sourceDev == destinationDev {
		t.Skip("requires two distinct filesystems for EXDEV")
	}

	source := filepath.Join(shm, "source.txt")
	mustWriteFile(t, source, "source", 0o644)
	destination := filepath.Join(home, "destination.txt")
	mustWriteFile(t, destination, "destination", 0o644)

	svc := newTestService(t, home)
	if _, err := svc.Move(source, destination); errorKind(t, err) != ErrorKindConflict {
		t.Fatalf("cross-device kind = %q, want conflict", errorKind(t, err))
	}
	if got, err := os.ReadFile(source); err != nil || string(got) != "source" {
		t.Fatalf("source after EXDEV = %q err=%v; copy fallback occurred", got, err)
	}
	if got, err := os.ReadFile(destination); err != nil || string(got) != "destination" {
		t.Fatalf("destination after EXDEV = %q err=%v", got, err)
	}
}

func deviceID(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("stat_t unavailable")
	}
	return uint64(stat.Dev), nil
}

func TestServiceMutationsShareMutex(t *testing.T) {
	ops := []struct {
		name    string
		prepare func(t *testing.T, home string)
		call    func(svc *Service) error
	}{
		{
			name: "write",
			call: func(svc *Service) error {
				_, err := svc.WriteFile("write.txt", strings.NewReader("payload"))
				return err
			},
		},
		{
			name:    "remove",
			prepare: func(t *testing.T, home string) { mustWriteFile(t, filepath.Join(home, "remove.txt"), "x", 0o644) },
			call:    func(svc *Service) error { return svc.Remove("remove.txt") },
		},
		{
			name: "mkdir",
			call: func(svc *Service) error {
				_, err := svc.Mkdir(".", "made")
				return err
			},
		},
		{
			name:    "move",
			prepare: func(t *testing.T, home string) { mustWriteFile(t, filepath.Join(home, "from.txt"), "x", 0o644) },
			call: func(svc *Service) error {
				_, err := svc.Move("from.txt", "to.txt")
				return err
			},
		},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			home := t.TempDir()
			if op.prepare != nil {
				op.prepare(t, home)
			}
			mutations := &sync.Mutex{}
			svc, err := New(home, mutations)
			if err != nil {
				t.Fatalf("New error = %v", err)
			}

			mutations.Lock()
			done := make(chan error, 1)
			go func() { done <- op.call(svc) }()
			for range 1000 {
				runtime.Gosched()
				select {
				case err := <-done:
					mutations.Unlock()
					t.Fatalf("%s did not block on the injected mutation mutex (err=%v)", op.name, err)
				default:
				}
			}
			mutations.Unlock()
			if err := <-done; err != nil {
				t.Fatalf("%s error = %v", op.name, err)
			}
		})
	}
}

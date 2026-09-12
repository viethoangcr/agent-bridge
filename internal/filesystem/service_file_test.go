package filesystem

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestServiceStat(t *testing.T) {
	home := t.TempDir()
	filePath := filepath.Join(home, "a.txt")
	mustWriteFile(t, filePath, "alpha", 0o640)
	mustMkdir(t, filepath.Join(home, "zdir"))
	if err := syscall.Mkfifo(filepath.Join(home, "fifo"), 0o644); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	mustSymlink(t, "a.txt", filepath.Join(home, "link_file"))

	fixed := time.UnixMilli(1_700_000_000_456)
	if err := os.Chtimes(filePath, fixed, fixed); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	svc := newTestService(t, home)

	st, err := svc.Stat("a.txt")
	if err != nil {
		t.Fatalf("Stat(a.txt) error = %v", err)
	}
	if got, want := st.Path, filePath; got != want {
		t.Fatalf("Stat path = %q, want %q", got, want)
	}
	if got, want := st.Type, "file"; got != want {
		t.Fatalf("Stat type = %q, want %q", got, want)
	}
	if got, want := st.Size, int64(len("alpha")); got != want {
		t.Fatalf("Stat size = %d, want %d", got, want)
	}
	if got, want := st.ModifiedMs, fixed.UnixMilli(); got != want {
		t.Fatalf("Stat modifiedMs = %d, want %d", got, want)
	}
	if got, want := st.Mode, uint32(0o640); got != want {
		t.Fatalf("Stat mode = %#o, want %#o", got, want)
	}
	if st.Mode&^uint32(0o777) != 0 {
		t.Fatalf("Stat mode %#o includes non-permission bits", st.Mode)
	}

	if st, err := svc.Stat("zdir"); err != nil {
		t.Fatalf("Stat(zdir) error = %v", err)
	} else if st.Type != "dir" {
		t.Fatalf("Stat(zdir) type = %q, want dir", st.Type)
	}

	if st, err := svc.Stat("link_file"); err != nil {
		t.Fatalf("Stat(link_file) error = %v", err)
	} else if st.Type != "file" {
		t.Fatalf("Stat(link_file) type = %q, want file", st.Type)
	}

	if _, err := svc.Stat("fifo"); errorKind(t, err) != ErrorKindInvalid {
		t.Fatalf("Stat(fifo) kind = %q, want invalid", errorKind(t, err))
	}
	if _, err := svc.Stat("missing"); errorKind(t, err) != ErrorKindNotFound {
		t.Fatalf("Stat(missing) kind = %q, want not_found", errorKind(t, err))
	}
	if _, err := svc.Stat(""); errorKind(t, err) != ErrorKindInvalid {
		t.Fatalf("Stat(empty) kind = %q, want invalid", errorKind(t, err))
	}
}

func TestServiceStatAbsoluteWithoutHome(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "a.txt")
	mustWriteFile(t, filePath, "alpha", 0o600)

	svc := newTestService(t, "")
	st, err := svc.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat(absolute) error = %v", err)
	}
	if got, want := st.Path, filePath; got != want {
		t.Fatalf("Stat path = %q, want %q", got, want)
	}
	if got, want := st.Mode, uint32(0o600); got != want {
		t.Fatalf("Stat mode = %#o, want %#o", got, want)
	}
}

type failingReader struct {
	data []byte
	err  error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestServiceOpen(t *testing.T) {
	home := t.TempDir()
	want := make([]byte, 512)
	for i := range want {
		want[i] = byte(i)
	}
	target := filepath.Join(home, "blob.bin")
	mustWriteFile(t, target, string(want), 0o640)
	mustMkdir(t, filepath.Join(home, "zdir"))

	svc := newTestService(t, home)

	f, st, err := svc.Open("blob.bin")
	if err != nil {
		t.Fatalf("Open error = %v", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Open bytes length = %d, want %d", len(got), len(want))
	}
	if st.Path != target || st.Type != "file" || st.Size != int64(len(want)) {
		t.Fatalf("Open stat = %+v, want path %q type file size %d", st, target, len(want))
	}
	if st.Mode != uint32(0o640) {
		t.Fatalf("Open mode = %#o, want %#o", st.Mode, 0o640)
	}

	if _, _, err := svc.Open("zdir"); errorKind(t, err) != ErrorKindInvalid {
		t.Fatalf("Open(dir) kind = %q, want invalid", errorKind(t, err))
	}
	if _, _, err := svc.Open("missing"); errorKind(t, err) != ErrorKindNotFound {
		t.Fatalf("Open(missing) kind = %q, want not_found", errorKind(t, err))
	}
	if _, _, err := svc.Open(""); errorKind(t, err) != ErrorKindInvalid {
		t.Fatalf("Open(empty) kind = %q, want invalid", errorKind(t, err))
	}
}

func TestServiceWriteFile(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)

	want := make([]byte, 300)
	for i := range want {
		want[i] = byte(i * 7)
	}
	resolved := filepath.Join(home, "nested", "deep", "blob.bin")

	res, err := svc.WriteFile("nested/deep/blob.bin", bytes.NewReader(want))
	if err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}
	if res.Path != resolved || res.Size != int64(len(want)) {
		t.Fatalf("WriteFile result = %+v, want path %q size %d", res, resolved, len(want))
	}
	got, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("written bytes mismatch: got %d bytes", len(got))
	}

	small := []byte("short")
	res, err = svc.WriteFile("nested/deep/blob.bin", bytes.NewReader(small))
	if err != nil {
		t.Fatalf("overwrite error = %v", err)
	}
	if res.Size != int64(len(small)) {
		t.Fatalf("overwrite size = %d, want %d", res.Size, len(small))
	}
	got, err = os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("read overwritten file: %v", err)
	}
	if !bytes.Equal(got, small) {
		t.Fatalf("overwrite bytes = %q, want %q", got, small)
	}

	info, err := os.Stat(resolved)
	if err != nil {
		t.Fatalf("stat written file: %v", err)
	}
	if info.Mode().Perm()&0o600 != 0o600 {
		t.Fatalf("mode %#o missing owner read/write", info.Mode().Perm())
	}
	if info.Mode().Perm()&0o111 != 0 {
		t.Fatalf("mode %#o is executable", info.Mode().Perm())
	}
}

func TestServiceWriteFileReadFailurePreservesTarget(t *testing.T) {
	home := t.TempDir()
	target := filepath.Join(home, "existing.txt")
	mustWriteFile(t, target, "original", 0o644)
	svc := newTestService(t, home)

	src := &failingReader{data: []byte("partial"), err: errors.New("boom")}
	if _, err := svc.WriteFile("existing.txt", src); errorKind(t, err) != ErrorKindInternal {
		t.Fatalf("write failure kind = %q, want internal", errorKind(t, err))
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("target after failed write = %q, want original", got)
	}
	entries, err := os.ReadDir(home)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "existing.txt" {
		t.Fatalf("temporary file residue after failed write: %d entries", len(entries))
	}
}

func TestServiceRemove(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)

	mustWriteFile(t, filepath.Join(home, "file.txt"), "x", 0o644)
	if err := svc.Remove("file.txt"); err != nil {
		t.Fatalf("Remove file error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(home, "file.txt")); !os.IsNotExist(err) {
		t.Fatalf("file still present: %v", err)
	}

	nested := filepath.Join(home, "tree", "deep")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir tree: %v", err)
	}
	mustWriteFile(t, filepath.Join(nested, "a"), "a", 0o644)
	if err := svc.Remove("tree"); err != nil {
		t.Fatalf("Remove tree error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(home, "tree")); !os.IsNotExist(err) {
		t.Fatalf("tree still present: %v", err)
	}

	mustMkdir(t, filepath.Join(home, "targetdir"))
	mustWriteFile(t, filepath.Join(home, "targetdir", "keep"), "keep", 0o644)
	mustSymlink(t, "targetdir", filepath.Join(home, "link"))
	if err := svc.Remove("link"); err != nil {
		t.Fatalf("Remove symlink error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(home, "link")); !os.IsNotExist(err) {
		t.Fatalf("symlink still present: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(home, "targetdir", "keep")); err != nil || string(got) != "keep" {
		t.Fatalf("symlink target was followed or removed: got %q err=%v", got, err)
	}

	if err := svc.Remove("missing"); errorKind(t, err) != ErrorKindNotFound {
		t.Fatalf("Remove(missing) kind = %q, want not_found", errorKind(t, err))
	}
	if err := svc.Remove(""); errorKind(t, err) != ErrorKindInvalid {
		t.Fatalf("Remove(empty) kind = %q, want invalid", errorKind(t, err))
	}
}

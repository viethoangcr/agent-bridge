package filesystem

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestUploadValid(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)
	dest := filepath.Join(home, "out")
	archive := uploadGzip(t, uploadTar(t,
		tarEntry{name: "a.txt", body: []byte("alpha")},
		tarEntry{name: "dir/", typeflag: tar.TypeDir},
		tarEntry{name: "dir/b.txt", body: []byte("bravo")},
		tarEntry{name: "dir/nested/c.txt", body: []byte("charlie")},
		tarEntry{name: "empty.txt"},
	))

	files, err := svc.Upload("out", bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("Upload error = %v", err)
	}
	want := []UploadedFile{
		{Path: filepath.Join(dest, "a.txt"), Size: 5},
		{Path: filepath.Join(dest, "dir/b.txt"), Size: 5},
		{Path: filepath.Join(dest, "dir/nested/c.txt"), Size: 7},
		{Path: filepath.Join(dest, "empty.txt"), Size: 0},
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("Upload files = %#v, want %#v", files, want)
	}
	for path, content := range map[string]string{
		"a.txt":            "alpha",
		"dir/b.txt":        "bravo",
		"dir/nested/c.txt": "charlie",
		"empty.txt":        "",
	} {
		got, err := os.ReadFile(filepath.Join(dest, path))
		if err != nil || string(got) != content {
			t.Fatalf("merged %s = %q err=%v, want %q", path, got, err, content)
		}
	}
	if leftovers := stagingLeftovers(t, home); len(leftovers) != 0 {
		t.Fatalf("staging left behind: %v", leftovers)
	}
}

func TestUploadEmptyArchive(t *testing.T) {
	home := t.TempDir()
	svc := newTestService(t, home)
	files, err := svc.Upload("out", bytes.NewReader(uploadGzip(t, uploadTar(t))))
	if err != nil {
		t.Fatalf("Upload error = %v", err)
	}
	if files == nil || len(files) != 0 {
		t.Fatalf("empty archive files = %#v, want non-nil empty", files)
	}
}

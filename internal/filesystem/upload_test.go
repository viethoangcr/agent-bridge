package filesystem

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// --- archive construction helpers ---

type tarEntry struct {
	name     string
	typeflag byte
	mode     int64
	linkname string
	body     []byte
}

func uploadTar(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		header := &tar.Header{
			Name:     entry.name,
			Mode:     entry.mode,
			Typeflag: entry.typeflag,
			Linkname: entry.linkname,
			Size:     int64(len(entry.body)),
		}
		if header.Typeflag == 0 {
			header.Typeflag = tar.TypeReg
		}
		if header.Mode == 0 {
			header.Mode = 0o644
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("write tar header %q: %v", entry.name, err)
		}
		if len(entry.body) > 0 {
			if _, err := tw.Write(entry.body); err != nil {
				t.Fatalf("write tar body %q: %v", entry.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return buf.Bytes()
}

func uploadGzip(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func octalField(n int64) []byte {
	return append([]byte(fmt.Sprintf("%011o", n)), 0)
}

// rawTarHeader hand-builds one checksummed ustar header so tests can produce
// entry types and size encodings archive/tar refuses to write.
func rawTarHeader(name string, typeflag byte, sizeField []byte, linkname string) []byte {
	var block [512]byte
	copy(block[0:100], name)
	copy(block[100:108], "0000644\x00")
	copy(block[108:116], "0000000\x00")
	copy(block[116:124], "0000000\x00")
	copy(block[124:136], sizeField)
	copy(block[136:148], "00000000000\x00")
	for i := 148; i < 156; i++ {
		block[i] = ' '
	}
	block[156] = typeflag
	copy(block[157:257], linkname)
	copy(block[257:263], "ustar\x00")
	copy(block[263:265], "00")
	sum := 0
	for _, b := range block {
		sum += int(b)
	}
	copy(block[148:154], fmt.Sprintf("%06o", sum))
	block[154] = 0
	block[155] = ' '
	return block[:]
}

// rawTarArchive concatenates blocks and optionally appends the required two
// zero end-of-archive blocks.
func rawTarArchive(blocks []byte, end bool) []byte {
	out := append([]byte(nil), blocks...)
	if end {
		out = append(out, make([]byte, 1024)...)
	}
	return out
}

func stagingLeftovers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	var found []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), stagingPrefix) {
			found = append(found, entry.Name())
		}
	}
	return found
}

func assertRejected(t *testing.T, svc *Service, parent, directory string, body []byte, want ErrorKind) {
	t.Helper()
	_, err := svc.Upload(directory, bytes.NewReader(body))
	if err == nil {
		t.Fatalf("Upload(%q) accepted a rejected archive", directory)
	}
	if kind := errorKind(t, err); kind != want {
		t.Fatalf("Upload(%q) kind = %q, want %q", directory, kind, want)
	}
	if leftovers := stagingLeftovers(t, parent); len(leftovers) != 0 {
		t.Fatalf("staging left behind: %v", leftovers)
	}
}

// --- tests ---

func TestValidateArchivePath(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		want     string
		wantKind ErrorKind
	}{
		{name: "plain", raw: "a/b", want: "a/b"},
		{name: "curdir dropped", raw: "./a/b", want: "a/b"},
		{name: "double slash", raw: "a//b/", want: "a/b"},
		{name: "inner curdir", raw: "a/./b", want: "a/b"},
		{name: "backslash ordinary", raw: `dir\..\file`, want: `dir\..\file`},
		{name: "triple dot ordinary", raw: "foo..bar/baz", want: "foo..bar/baz"},
		{name: "empty", raw: "", wantKind: ErrorKindInvalid},
		{name: "nul", raw: "a\x00b", wantKind: ErrorKindInvalid},
		{name: "absolute", raw: "/etc/passwd", wantKind: ErrorKindInvalid},
		{name: "leading parent", raw: "../a", wantKind: ErrorKindInvalid},
		{name: "inner parent", raw: "a/../b", wantKind: ErrorKindInvalid},
		{name: "only curdir", raw: ".", wantKind: ErrorKindInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateArchivePath(tc.raw)
			if tc.wantKind != "" {
				if kind := errorKind(t, err); kind != tc.wantKind {
					t.Fatalf("validateArchivePath(%q) kind = %q, want %q", tc.raw, kind, tc.wantKind)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateArchivePath(%q) error = %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("validateArchivePath(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

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

func TestUploadRejectsUnsafeEntries(t *testing.T) {
	tests := []struct {
		name    string
		archive []byte
	}{
		{
			name:    "absolute",
			archive: rawTarArchive(rawTarHeader("/etc/passwd", tar.TypeReg, octalField(0), ""), true),
		},
		{
			name:    "leading parent",
			archive: rawTarArchive(rawTarHeader("../evil", tar.TypeReg, octalField(0), ""), true),
		},
		{
			name:    "inner parent",
			archive: rawTarArchive(rawTarHeader("dir/../evil", tar.TypeReg, octalField(0), ""), true),
		},
		{
			name:    "double inner parent",
			archive: rawTarArchive(rawTarHeader("a/b/../../evil", tar.TypeReg, octalField(0), ""), true),
		},
		{
			name:    "empty name",
			archive: rawTarArchive(rawTarHeader("", tar.TypeReg, octalField(0), ""), true),
		},
		{
			name:    "symlink",
			archive: rawTarArchive(rawTarHeader("link", tar.TypeSymlink, octalField(0), "/etc/passwd"), true),
		},
		{
			name:    "hardlink",
			archive: rawTarArchive(rawTarHeader("link", tar.TypeLink, octalField(0), "target"), true),
		},
		{
			name:    "char device",
			archive: rawTarArchive(rawTarHeader("dev", tar.TypeChar, octalField(0), ""), true),
		},
		{
			name:    "block device",
			archive: rawTarArchive(rawTarHeader("dev", tar.TypeBlock, octalField(0), ""), true),
		},
		{
			name:    "fifo",
			archive: rawTarArchive(rawTarHeader("pipe", tar.TypeFifo, octalField(0), ""), true),
		},
		{
			name:    "sparse",
			archive: rawTarArchive(rawTarHeader("sparse", tar.TypeGNUSparse, octalField(0), ""), true),
		},
		{
			name:    "unknown type",
			archive: rawTarArchive(rawTarHeader("weird", 'Z', octalField(0), ""), true),
		},
		{
			name:    "negative size",
			archive: rawTarArchive(rawTarHeader("neg", tar.TypeReg, bytes.Repeat([]byte{0xff}, 12), ""), true),
		},
		{
			name: "duplicate path",
			archive: rawTarArchive(append(
				rawTarHeader("dup", tar.TypeReg, octalField(0), ""),
				rawTarHeader("dup", tar.TypeReg, octalField(0), "")...,
			), true),
		},
		{
			name: "file then child",
			archive: rawTarArchive(append(
				rawTarHeader("a", tar.TypeReg, octalField(0), ""),
				rawTarHeader("a/b", tar.TypeReg, octalField(0), "")...,
			), true),
		},
		{
			name: "child then file",
			archive: rawTarArchive(append(
				rawTarHeader("a/b", tar.TypeReg, octalField(0), ""),
				rawTarHeader("a", tar.TypeReg, octalField(0), "")...,
			), true),
		},
		{
			name: "directory and file same name",
			archive: rawTarArchive(append(
				rawTarHeader("a", tar.TypeDir, octalField(0), ""),
				rawTarHeader("a", tar.TypeReg, octalField(0), "")...,
			), true),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			svc := newTestService(t, home)
			assertRejected(t, svc, home, "out", uploadGzip(t, tc.archive), ErrorKindInvalid)
		})
	}
}

func TestUploadCompressedLimit(t *testing.T) {
	home := t.TempDir()
	archive := uploadGzip(t, uploadTar(t, tarEntry{name: "f", body: []byte("hello world")}))

	atLimit := newTestService(t, home)
	atLimit.maxCompressed = int64(len(archive))
	files, err := atLimit.Upload("out", bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("Upload at compressed limit error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("Upload at limit files = %#v", files)
	}

	over := newTestService(t, home)
	over.maxCompressed = int64(len(archive)) - 1
	assertRejected(t, over, home, "over", archive, ErrorKindTooLarge)
}

func TestUploadExtractedLimit(t *testing.T) {
	home := t.TempDir()
	archive := uploadGzip(t, uploadTar(t,
		tarEntry{name: "a", body: []byte("12345")},
		tarEntry{name: "b", body: []byte("67890")},
	))

	atLimit := newTestService(t, home)
	atLimit.maxExtracted = 10
	files, err := atLimit.Upload("out", bytes.NewReader(archive))
	if err != nil {
		t.Fatalf("Upload at extracted limit error = %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("Upload at limit files = %#v", files)
	}

	over := newTestService(t, home)
	over.maxExtracted = 9
	assertRejected(t, over, home, "over", archive, ErrorKindTooLarge)
}

func TestUploadProductionLimitConstants(t *testing.T) {
	if MaxUploadCompressedBytes != int64(512)<<20 {
		t.Fatalf("MaxUploadCompressedBytes = %d, want %d", MaxUploadCompressedBytes, int64(512)<<20)
	}
	if MaxUploadExtractedBytes != int64(512)<<20 {
		t.Fatalf("MaxUploadExtractedBytes = %d, want %d", MaxUploadExtractedBytes, int64(512)<<20)
	}
	home := t.TempDir()
	svc := newTestService(t, home)
	if svc.maxCompressed != MaxUploadCompressedBytes || svc.maxExtracted != MaxUploadExtractedBytes {
		t.Fatalf("service limits = (%d,%d), want production constants", svc.maxCompressed, svc.maxExtracted)
	}

	// Boundary arithmetic only: a header declaring 512MiB+1 is rejected without
	// ever allocating or transferring 512MiB.
	over := rawTarArchive(rawTarHeader("big", tar.TypeReg, octalField(MaxUploadExtractedBytes+1), ""), false)
	assertRejected(t, svc, home, "big", uploadGzip(t, over), ErrorKindTooLarge)

	// Exactly at the limit passes the size check and fails later only because
	// the entry body is missing.
	atLimit := rawTarArchive(rawTarHeader("big", tar.TypeReg, octalField(MaxUploadExtractedBytes), ""), false)
	_, err := svc.Upload("at", bytes.NewReader(uploadGzip(t, atLimit)))
	if err == nil {
		t.Fatal("short at-limit entry unexpectedly accepted")
	}
	if kind := errorKind(t, err); kind == ErrorKindTooLarge {
		t.Fatalf("at-limit declared size reported too_large: %v", err)
	}
}

func TestUploadMalformed(t *testing.T) {
	valid := uploadGzip(t, uploadTar(t, tarEntry{name: "f", body: []byte("hello")}))

	tests := []struct {
		name    string
		archive []byte
		want    ErrorKind
	}{
		{name: "truncated gzip", archive: valid[:len(valid)-5], want: ErrorKindInvalid},
		{name: "corrupt tar", archive: uploadGzip(t, []byte("this is not a tar stream at all")), want: ErrorKindInvalid},
		{
			name: "short entry",
			archive: uploadGzip(t, append(
				rawTarHeader("short", tar.TypeReg, octalField(100), ""),
				[]byte("tiny")...,
			)),
			want: ErrorKindInvalid,
		},
		{
			name:    "concatenated gzip members",
			archive: append(append([]byte(nil), valid...), valid...),
			want:    ErrorKindInvalid,
		},
		{
			name:    "trailing byte",
			archive: append(append([]byte(nil), valid...), 0x00),
			want:    ErrorKindInvalid,
		},
		{
			name:    "trailing decompressed data",
			archive: uploadGzip(t, append(uploadTar(t, tarEntry{name: "f", body: []byte("hello")}), []byte("garbage")...)),
			want:    ErrorKindInvalid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			svc := newTestService(t, home)
			assertRejected(t, svc, home, "out", tc.archive, tc.want)
		})
	}
}

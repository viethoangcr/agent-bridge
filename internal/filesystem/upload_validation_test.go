package filesystem

import (
	"archive/tar"
	"bytes"
	"testing"
)

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

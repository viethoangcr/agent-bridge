package filesystem

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
	"testing"
)

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

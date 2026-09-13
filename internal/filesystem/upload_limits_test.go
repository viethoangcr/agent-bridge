package filesystem

import (
	"archive/tar"
	"bytes"
	"testing"
)

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
	if MaxFileBytes != int64(512)<<20 {
		t.Fatalf("MaxFileBytes = %d, want %d", MaxFileBytes, int64(512)<<20)
	}
	if MaxUploadCompressedBytes != MaxFileBytes {
		t.Fatalf("MaxUploadCompressedBytes = %d, want owner constant %d", MaxUploadCompressedBytes, MaxFileBytes)
	}
	if MaxUploadExtractedBytes != MaxFileBytes {
		t.Fatalf("MaxUploadExtractedBytes = %d, want owner constant %d", MaxUploadExtractedBytes, MaxFileBytes)
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

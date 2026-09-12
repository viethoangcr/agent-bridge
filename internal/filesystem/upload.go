package filesystem

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"io"
	"os"
)

// MaxUploadCompressedBytes and MaxUploadExtractedBytes bound tar.gz uploads.
const (
	MaxUploadCompressedBytes int64 = 512 << 20
	MaxUploadExtractedBytes  int64 = 512 << 20
)

// stagingPrefix names every temporary extraction tree so a rejected upload is
// identifiable and always removed.
const stagingPrefix = "agent-bridge-upload-"

// UploadedFile reports one regular file the archive intends to place at the
// resolved absolute path.
type UploadedFile struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// countingReader counts bytes read from r and refuses to read more than
// limit+1, so callers can detect an over-limit input without reading it all.
// It implements io.ByteReader so compress/gzip reads it directly instead of
// wrapping it in a read-ahead buffer, which preserves trailing-byte detection.
type countingReader struct {
	r     io.Reader
	n     int64
	limit int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	remaining := c.limit + 1 - c.n
	if remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReader) ReadByte() (byte, error) {
	var b [1]byte
	n, err := c.Read(b[:])
	if n == 1 {
		return b[0], nil
	}
	if err == nil {
		err = io.ErrNoProgress
	}
	return 0, err
}

// exceeded reports whether more than limit bytes were consumed. A full read of
// limit+1 is the largest possible, so this is exactly "over the limit".
func (c *countingReader) exceeded() bool { return c.n > c.limit }

// Upload validates a gzip-compressed tar stream, extracts it into a staging
// directory beside the destination, then preflights and merges it. Validation
// and extraction never touch the destination. The injected process-wide
// mutation mutex is held from the first destination validation through staging
// cleanup and merge, so a concurrent mutation cannot remove the staging tree or
// invalidate the destination checks; it does not serialize arbitrary commands
// or external OS writers and is not a security boundary. Staging is always
// removed on exit.
func (s *Service) Upload(directory string, src io.Reader) ([]UploadedFile, error) {
	destination, err := s.Resolve(directory)
	if err != nil {
		return nil, err
	}

	s.mutations.Lock()
	defer s.mutations.Unlock()

	if err := rejectSymlinkComponents(destination); err != nil {
		return nil, err
	}
	parent, err := stagingParent(destination)
	if err != nil {
		return nil, err
	}

	compressed := &countingReader{r: src, limit: s.maxCompressed}
	gz, err := gzip.NewReader(compressed)
	if err != nil {
		if compressed.exceeded() {
			return nil, tooLargeError("upload exceeds the compressed size limit")
		}
		return nil, invalidError("upload body is not a valid gzip stream")
	}
	defer func() { _ = gz.Close() }()
	gz.Multistream(false)

	staging, err := os.MkdirTemp(parent, stagingPrefix)
	if err != nil {
		return nil, mapMutationError(err)
	}
	defer func() { _ = os.RemoveAll(staging) }()

	manifest, err := extractArchive(tar.NewReader(gz), destination, staging, s.maxExtracted)
	if err != nil {
		if compressed.exceeded() {
			return nil, tooLargeError("upload exceeds the compressed size limit")
		}
		return nil, err
	}

	// After tar EOF the member must end immediately: read exactly one more
	// decompressed byte, require gzip EOF, then prove the compressed input has
	// no trailing member or byte left in the counting reader.
	var trailing [1]byte
	if n, err := gz.Read(trailing[:]); n > 0 {
		return nil, invalidError("upload body has trailing data")
	} else if err != nil && !errors.Is(err, io.EOF) {
		if compressed.exceeded() {
			return nil, tooLargeError("upload exceeds the compressed size limit")
		}
		return nil, invalidError("upload body is not a valid gzip stream")
	}
	if _, err := compressed.ReadByte(); err == nil {
		if compressed.exceeded() {
			return nil, tooLargeError("upload exceeds the compressed size limit")
		}
		return nil, invalidError("upload body has trailing data")
	} else if !errors.Is(err, io.EOF) {
		if compressed.exceeded() {
			return nil, tooLargeError("upload exceeds the compressed size limit")
		}
		return nil, &Error{Kind: ErrorKindInternal, Message: "upload read failed", Err: err}
	}
	if compressed.exceeded() {
		return nil, tooLargeError("upload exceeds the compressed size limit")
	}

	for _, item := range manifest.items {
		if err := rejectSymlinkComponents(item.path); err != nil {
			return nil, err
		}
	}
	if err := preflightMerge(manifest); err != nil {
		return nil, err
	}
	if err := mergeStaging(manifest, staging); err != nil {
		return nil, err
	}
	return manifest.uploadedFiles(), nil
}

func tooLargeError(message string) *Error {
	return &Error{Kind: ErrorKindTooLarge, Message: message}
}

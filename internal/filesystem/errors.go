package filesystem

import (
	"errors"
	"io/fs"
	"syscall"
)

// ErrorKind classifies a filesystem failure for the HTTP adapter.
type ErrorKind string

const (
	// ErrorKindInvalid marks malformed input or an unsupported object type.
	ErrorKindInvalid ErrorKind = "invalid"
	// ErrorKindNotFound marks a missing path.
	ErrorKindNotFound ErrorKind = "not_found"
	// ErrorKindConflict marks a destination or type collision.
	ErrorKindConflict ErrorKind = "conflict"
	// ErrorKindTooLarge marks input that exceeds an enforced limit.
	ErrorKindTooLarge ErrorKind = "too_large"
	// ErrorKindInternal marks an unexpected I/O failure.
	ErrorKindInternal ErrorKind = "internal"
)

// Error is a filesystem failure carrying a machine-readable Kind. Err holds
// the underlying cause for server-side inspection and is never surfaced to
// clients; Error's message is safe to reuse as a problem detail.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

// Error implements error.
func (e *Error) Error() string { return e.Message }

// Unwrap exposes the underlying cause to errors.Is/errors.As.
func (e *Error) Unwrap() error { return e.Err }

func invalidError(message string) *Error {
	return &Error{Kind: ErrorKindInvalid, Message: message}
}

func mapPathError(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return &Error{Kind: ErrorKindNotFound, Message: "path not found", Err: err}
	}
	return &Error{Kind: ErrorKindInternal, Message: "filesystem operation failed", Err: err}
}

// mapMutationError classifies a mutation failure. Missing paths are not_found,
// incompatible targets and cross-device renames are conflict, and anything
// else is an internal I/O failure. Underlying causes stay server-side.
func mapMutationError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return &Error{Kind: ErrorKindNotFound, Message: "path not found", Err: err}
	case errors.Is(err, syscall.EXDEV),
		errors.Is(err, syscall.ENOTEMPTY),
		errors.Is(err, syscall.ENOTDIR),
		errors.Is(err, syscall.EISDIR),
		errors.Is(err, fs.ErrExist):
		return &Error{Kind: ErrorKindConflict, Message: "destination conflict", Err: err}
	default:
		return &Error{Kind: ErrorKindInternal, Message: "filesystem operation failed", Err: err}
	}
}

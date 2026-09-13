package acpproxy

import (
	"context"
	"errors"
	"os"
	"os/exec"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// persistenceFailure marks a store-operation failure as a persistence failure
// (HTTP 507) while leaving the lifecycle sentinels that carry their own HTTP
// status exactly untouched.
func persistenceFailure(err error) error {
	if err == nil ||
		errors.Is(err, acpstore.ErrNotFound) ||
		errors.Is(err, acpstore.ErrConflict) ||
		errors.Is(err, acpstore.ErrDeleted) {
		return err
	}
	return errors.Join(acpruntime.ErrPersistence, err)
}

// startupFailure classifies a runtime-factory failure: process spawn,
// cancellation, and exited failures stay process failures (502), while the
// startup live-row publication maps to 507. ponytail: spawn detection is
// type-based over exec.Cmd's error surface; an unknown spawn failure shape
// would be read as persistence.
func startupFailure(err error) error {
	var (
		execErr *exec.Error
		pathErr *os.PathError
	)
	if errors.As(err, &execErr) || errors.As(err, &pathErr) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, acpruntime.ErrExited) {
		return err
	}
	return persistenceFailure(err)
}

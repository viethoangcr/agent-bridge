package acpruntime

import (
	"context"
	"errors"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// publishLive performs the initial creating-to-idle publication under the same
// status mutex as every later transition, so a terminal mark can never be
// overwritten by the initial row write. It also records the child PID.
func (r *Runtime) publishLive(ctx context.Context, pid int) error {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	if r.statusExited {
		return ErrExited
	}
	if r.store == nil {
		return nil
	}
	return r.store.SetLive(ctx, r.serverID, pid)
}

// reconcileStatus serializes one runtime status write. It holds statusMu,
// briefly reads the current correlation total under corrMu, releases corrMu
// before any SQL, writes busy when the total is positive or idle when it is
// zero, then releases statusMu. A terminal runtime performs no write, so a
// stale completion can never overwrite exited.
func (r *Runtime) reconcileStatus() error {
	r.statusMu.Lock()
	if r.statusExited {
		r.statusMu.Unlock()
		return nil
	}
	r.corrMu.Lock()
	count := len(r.corr)
	r.corrMu.Unlock()

	status := acpstore.StatusIdle
	if count > 0 {
		status = acpstore.StatusBusy
	}
	var err error
	if r.store != nil {
		err = r.store.SetStatus(context.Background(), r.serverID, status)
	}
	r.statusMu.Unlock()
	if err != nil {
		return errors.Join(ErrPersistence, err)
	}
	return nil
}

// reconcileStatusAndFail reconciles and, on failure, fails the runtime.
func (r *Runtime) reconcileStatusAndFail() {
	if err := r.reconcileStatus(); err != nil {
		r.failPersistence(err)
	}
}

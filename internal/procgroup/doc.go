// Package procgroup holds the process-group observation primitive shared by
// the managed-process and ACP runtime waiters.
//
// Production is Linux-only: ObserveExit peeks a terminated direct child with
// waitid(2) WNOWAIT so its caller can kill the captured group while the child's
// PID is still owned by its zombie. The non-Linux fallback reports that no
// unreaped observation is available, so callers keep the reap-then-group-kill
// order.
package procgroup

//go:build !linux

package acpruntime

// observeExit reports false: waitid(WNOWAIT) is unavailable, so the waiter
// keeps the reap-then-group-kill order. Production is Linux-only.
func observeExit(_ int) bool {
	return false
}

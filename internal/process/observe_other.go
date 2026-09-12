//go:build !linux

package process

// observeExit reports false: waitid(WNOWAIT) is unavailable, so the waiters
// keep the reap-then-group-kill order. Production is Linux-only.
func observeExit(_ int) bool {
	return false
}

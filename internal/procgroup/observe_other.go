//go:build !linux

package procgroup

// ObserveExit reports false: waitid(WNOWAIT) is unavailable, so callers keep
// the reap-then-group-kill order. Production is Linux-only.
func ObserveExit(_ int) bool {
	return false
}

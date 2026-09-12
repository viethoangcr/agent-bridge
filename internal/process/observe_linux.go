//go:build linux

package process

import (
	"syscall"
	"unsafe"
)

// pPid is waitid(2)'s P_PID idtype. The public syscall package exposes neither
// waitid nor its constants, so the helper issues the raw syscall.
const pPid = 1

// siginfoSize is the kernel's siginfo_t size (SI_MAX_SIZE) on Linux. Only the
// wait result matters here, not the siginfo contents.
const siginfoSize = 128

// observeExit blocks until pid has terminated, peeking its exit with
// waitid(P_PID, pid, ..., WEXITED|WNOWAIT) so the child stays waitable. The
// caller can then SIGKILL the captured process group while the direct child's
// PID is still owned by its zombie and cannot be recycled, and reap the real
// status with Wait afterwards.
//
// wait4(2) rejects WNOWAIT with EINVAL on Linux; waitid(2) is the kernel's
// no-reap observation point, the same one os.Process uses before its Wait.
//
// It reports false when the exit cannot be observed without reaping: an
// invalid pid or a non-child or already reaped pid (ECHILD). The caller then
// falls back to reaping before the group kill.
func observeExit(pid int) bool {
	if pid <= 1 {
		return false
	}
	// The kernel writes a full siginfo_t; Syscall6's //go:uintptrkeepalive
	// keeps this buffer alive for the duration of the call.
	var info [siginfoSize]byte
	for {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_WAITID,
			uintptr(pPid),
			uintptr(pid),
			uintptr(unsafe.Pointer(&info[0])),
			uintptr(syscall.WEXITED|syscall.WNOWAIT),
			0,
			0,
		)
		switch {
		case errno == 0:
			// WEXITED reports terminated children only, so a successful
			// waitid means the child has exited and is still waitable.
			return true
		case errno == syscall.EINTR:
			continue
		default:
			return false
		}
	}
}

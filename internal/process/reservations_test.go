package process

import (
	"context"
	"errors"
	"os"
	"testing"
)

// assertReservationState asserts the reservation counters are non-negative and
// exactly equal the expected arithmetic. Exact equality is the invariant: an
// unmatched release would drive the clamped counter below the expected value,
// so normal flows must never rely on the clamps.
func assertReservationState(t *testing.T, m *Manager, wantActive, wantPeak int) {
	t.Helper()
	m.reserveMu.Lock()
	active, peak := m.activeProcesses, m.activeRunPeak
	m.reserveMu.Unlock()
	if active < 0 || peak < 0 {
		t.Fatalf("reservation underflow: activeProcesses=%d activeRunPeak=%d", active, peak)
	}
	if active != wantActive || peak != wantPeak {
		t.Fatalf("reservation state = (%d,%d), want (%d,%d)", active, peak, wantActive, wantPeak)
	}
}

func TestReservationsManagedStartExitAccountExact(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)
	assertReservationState(t, m, 0, 0)

	snap := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	waitForActiveProcesses(t, m, 1)
	assertReservationState(t, m, 1, 0)

	if _, err := m.Kill(snap.ID); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitProcessDone(t, m, snap.ID)
	assertReservationState(t, m, 0, 0)
}

func TestReservationsRunAccountExact(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 200, 4)
	assertReservationState(t, m, 0, 0)

	// A rejected request reserves nothing.
	if _, err := m.Run(context.Background(), RunRequest{Command: "   "}); !errors.Is(err, ErrValidation) {
		t.Fatalf("invalid run error = %v, want ErrValidation", err)
	}
	assertReservationState(t, m, 0, 0)

	// A completed run holds its peak only while in flight.
	done := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RunRequest{
			Command: os.Args[0], Env: helperEnv(helperBlocking), TimeoutMs: ptrI64(60), MaxOutputBytes: ptrI64(10),
		})
		done <- err
	}()
	waitForActiveProcesses(t, m, 1)
	assertReservationState(t, m, 1, 200)
	if err := <-done; err != nil {
		t.Fatalf("timeout run error = %v", err)
	}
	assertReservationState(t, m, 0, 0)
}

func TestReservationsCapacityRejectionLeavesCountsUnchanged(t *testing.T) {
	m := newRunManager(t, 200, 1)

	if err := m.reserveRun(200); err != nil {
		t.Fatalf("reserveRun(200) = %v, want nil", err)
	}
	assertReservationState(t, m, 1, 200)

	// Process-count rejection must not touch peak accounting.
	if err := m.reserveRun(200); !errors.Is(err, ErrCapacity) {
		t.Fatalf("over-capacity reserveRun error = %v, want ErrCapacity", err)
	}
	assertReservationState(t, m, 1, 200)

	m.releaseRun(200)
	assertReservationState(t, m, 0, 0)

	// Peak rejection must not consume a process slot.
	if err := m.reserveRun(201); !errors.Is(err, ErrCapacity) {
		t.Fatalf("over-peak reserveRun error = %v, want ErrCapacity", err)
	}
	assertReservationState(t, m, 0, 0)
}

func TestReservationsShutdownReleasesExactlyOnce(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 4)

	startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	waitForActiveProcesses(t, m, 2)
	assertReservationState(t, m, 2, 0)

	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	assertReservationState(t, m, 0, 0)

	// A second shutdown must not release again.
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown = %v, want nil", err)
	}
	assertReservationState(t, m, 0, 0)
}

func TestReservationsShutdownReleasesActiveRun(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 200, 4)

	done := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RunRequest{
			Command: os.Args[0], Env: helperEnv(helperBlocking), MaxOutputBytes: ptrI64(10),
		})
		done <- err
	}()
	waitForActiveProcesses(t, m, 1)
	assertReservationState(t, m, 1, 200)

	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("run after shutdown error = %v, want nil", err)
	}
	assertReservationState(t, m, 0, 0)
}

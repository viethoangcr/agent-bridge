package process

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestManager_Run_DirectChildExitKillsDescendant(t *testing.T) {
	requireLinuxProcess(t)

	pidPath := filepath.Join(t.TempDir(), "pids.json")
	t.Cleanup(func() {
		if pids, ok := tryReadHelperPIDs(pidPath); ok {
			killHelperPIDs(pids)
		}
	})
	env := helperEnv(helperChildExits)
	env[processHelperPIDEnv] = pidPath

	m := newRunManager(t, 20*MaxOutputBytes, 4)
	res, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: env})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.TimedOut {
		t.Fatal("TimedOut = true, want false for a direct child that exits on its own")
	}
	if res.ExitCode == nil || *res.ExitCode != 0 {
		t.Fatalf("exitCode = %v, want 0 after the direct child's own exit", res.ExitCode)
	}

	// The direct child exited immediately; the descendant inherited the pipes,
	// so Run can only complete if the captured group was SIGKILLed before the
	// captures were joined.
	pids := readHelperPIDs(t, pidPath)
	waitGoneOrZombie(t, pids.Grandchild)
	if got := activeRunPeak(m); got != 0 {
		t.Fatalf("activeRunPeak = %d after direct-child exit, want 0", got)
	}
}

func TestManager_Run_SharesConcurrencyBudget(t *testing.T) {
	requireLinuxProcess(t)
	m := newTestManager(t, 1)

	managed := startHelper(t, m, StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
	if _, err := m.Run(context.Background(), RunRequest{
		Command: os.Args[0], Env: helperEnv(helperOutput), MaxOutputBytes: ptrI64(64),
	}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Run while a managed process is running error = %v, want ErrCapacity", err)
	}
	if _, err := m.Kill(managed.ID); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	res, err := m.Run(context.Background(), RunRequest{
		Command: os.Args[0], Env: helperEnv(helperOutput), MaxOutputBytes: ptrI64(64),
	})
	if err != nil {
		t.Fatalf("Run after capacity freed: %v", err)
	}
	if res.Stdout != "stdout-payload" {
		t.Fatalf("stdout = %q, want stdout-payload", res.Stdout)
	}

	// A live run blocks managed starts through the same shared counter.
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(ctx, RunRequest{
			Command: os.Args[0], Env: helperEnv(helperBlocking), MaxOutputBytes: ptrI64(64),
		})
		runDone <- err
	}()
	waitForActiveProcesses(t, m, 1)
	if _, err := m.Start(StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Start while a run is active error = %v, want ErrCapacity", err)
	}
	cancel()
	if err := <-runDone; !errors.Is(err, ErrGateway) {
		t.Fatalf("canceled run error = %v, want ErrGateway", err)
	}
	if got := activeRunPeak(m); got != 0 {
		t.Fatalf("activeRunPeak = %d after canceled run, want 0", got)
	}
}

func TestManager_Run_ReservesTwentyTimesOutputCap(t *testing.T) {
	requireLinuxProcess(t)

	// cap 10 reserves exactly 200, so a 200-byte budget admits it.
	exact := newRunManager(t, 200, 4)
	res, err := exact.Run(context.Background(), RunRequest{
		Command: os.Args[0], Env: runEnvExit("0"), MaxOutputBytes: ptrI64(10),
	})
	if err != nil {
		t.Fatalf("Run at exact peak boundary: %v", err)
	}
	_ = res
	if got := activeRunPeak(exact); got != 0 {
		t.Fatalf("activeRunPeak = %d after release, want 0", got)
	}

	// One byte less cannot admit cap 10; nothing is spawned or reserved.
	tight := newRunManager(t, 199, 4)
	marker := filepath.Join(t.TempDir(), "spawned")
	_, err = tight.Run(context.Background(), RunRequest{
		Command:        "/bin/sh",
		Args:           []string{"-c", "touch " + marker},
		MaxOutputBytes: ptrI64(10),
	})
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("Run with insufficient peak error = %v, want ErrCapacity", err)
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("command spawned despite peak conflict (stat err = %v)", statErr)
	}
	if got := activeProcesses(tight); got != 0 {
		t.Fatalf("activeProcesses = %d after peak conflict, want 0 (no partial reservation)", got)
	}
	if got := activeRunPeak(tight); got != 0 {
		t.Fatalf("activeRunPeak = %d after peak conflict, want 0 (no partial reservation)", got)
	}
}

func TestManager_Run_AggregatePeakCannotOversubscribe(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 200, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	go func() {
		_, err := m.Run(ctx, RunRequest{
			Command: os.Args[0], Env: helperEnv(helperBlocking), MaxOutputBytes: ptrI64(10),
		})
		firstDone <- err
	}()
	waitForActiveProcesses(t, m, 1)
	if got := activeRunPeak(m); got != 200 {
		t.Fatalf("activeRunPeak = %d, want exactly 200", got)
	}

	marker := filepath.Join(t.TempDir(), "spawned")
	if _, err := m.Run(context.Background(), RunRequest{
		Command:        "/bin/sh",
		Args:           []string{"-c", "touch " + marker},
		MaxOutputBytes: ptrI64(10),
	}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("oversubscribed run error = %v, want ErrCapacity", err)
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("oversubscribed run spawned the command (stat err = %v)", statErr)
	}
	if got := activeRunPeak(m); got != 200 {
		t.Fatalf("activeRunPeak = %d after a rejected run, want unchanged 200", got)
	}

	cancel()
	if err := <-firstDone; !errors.Is(err, ErrGateway) {
		t.Fatalf("canceled run error = %v, want ErrGateway", err)
	}
	if got := activeRunPeak(m); got != 0 {
		t.Fatalf("activeRunPeak = %d after run released, want 0", got)
	}
}

func TestManager_Run_CheckedPeakMultiplication(t *testing.T) {
	if _, err := runPeakBytes(math.MaxInt/20 + 1); err == nil {
		t.Fatal("runPeakBytes accepted a value that overflows 20x")
	}
	got, err := runPeakBytes(math.MaxInt / 20)
	if err != nil {
		t.Fatalf("runPeakBytes(MaxInt/20): %v", err)
	}
	if want := 20 * (math.MaxInt / 20); got != want {
		t.Fatalf("runPeakBytes(MaxInt/20) = %d, want %d", got, want)
	}
}

func TestManager_Run_ProductionArithmetic(t *testing.T) {
	peak, err := runPeakBytes(MaxOutputBytes)
	if err != nil {
		t.Fatalf("runPeakBytes(MaxOutputBytes): %v", err)
	}
	if peak != 320<<20 {
		t.Fatalf("16MiB reserves %d bytes, want 320MiB", peak)
	}
	if MaxActiveRunPeakBytes != 512<<20 {
		t.Fatalf("MaxActiveRunPeakBytes = %d, want 512MiB", MaxActiveRunPeakBytes)
	}
	if headroom := MaxActiveRunPeakBytes - peak; headroom != 192<<20 {
		t.Fatalf("headroom = %d bytes, want 192MiB", headroom)
	}
	// The 20x model is 2x raw captures + 6x UTF-8/result strings + 12x JSON
	// escaping/encoder buffers, with no large allocation in this test.
	if 2*MaxOutputBytes+6*MaxOutputBytes+12*MaxOutputBytes != peak {
		t.Fatal("conservative peak components do not sum to the 20x reservation")
	}
}

func TestManager_Run_ReleasesReservationsOnEveryPath(t *testing.T) {
	requireLinuxProcess(t)
	m := newRunManager(t, 20*MaxOutputBytes, 64)
	missing := filepath.Join(t.TempDir(), "definitely-missing")

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 4 {
			case 0:
				if _, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: runEnvExit("0"), MaxOutputBytes: ptrI64(64)}); err != nil {
					t.Errorf("success path: %v", err)
				}
			case 1:
				if _, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: runEnvExit("3"), MaxOutputBytes: ptrI64(64)}); err != nil {
					t.Errorf("non-zero path: %v", err)
				}
			case 2:
				if _, err := m.Run(context.Background(), RunRequest{Command: missing, MaxOutputBytes: ptrI64(64)}); err == nil {
					t.Error("spawn-error path: got nil error")
				}
			case 3:
				if _, err := m.Run(context.Background(), RunRequest{Command: os.Args[0], Env: helperEnv(helperBlocking), TimeoutMs: ptrI64(60), MaxOutputBytes: ptrI64(64)}); err != nil {
					t.Errorf("timeout path: %v", err)
				}
			}
		}(i)
	}

	// In-flight cancellation releases through the same path.
	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, err := m.Run(ctx, RunRequest{Command: os.Args[0], Env: helperEnv(helperBlocking), MaxOutputBytes: ptrI64(64)})
		canceled <- err
	}()
	waitForActiveProcesses(t, m, 1)
	cancel()
	if err := <-canceled; !errors.Is(err, ErrGateway) {
		t.Fatalf("in-flight cancellation error = %v, want ErrGateway", err)
	}
	wg.Wait()

	if got := activeProcesses(m); got != 0 {
		t.Fatalf("activeProcesses = %d after all paths, want 0", got)
	}
	if got := activeRunPeak(m); got != 0 {
		t.Fatalf("activeRunPeak = %d after all paths, want 0", got)
	}
}

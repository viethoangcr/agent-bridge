package process

import (
	"errors"
	"os"
	"sync"
	"testing"
)

func TestManager_Capacity_RacesExactLimit(t *testing.T) {
	requireLinuxProcess(t)

	const limit = 4
	const attempts = 24
	m := newTestManager(t, limit)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var successes int
	var capacity int
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.Start(StartRequest{Command: os.Args[0], Env: helperEnv(helperBlocking)})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrCapacity):
				capacity++
			default:
				t.Errorf("Start: unexpected error %v", err)
			}
		}()
	}
	wg.Wait()

	if successes != limit {
		t.Fatalf("successful starts = %d, want exactly %d", successes, limit)
	}
	if capacity != attempts-limit {
		t.Fatalf("capacity rejections = %d, want %d", capacity, attempts-limit)
	}
	if got := activeProcesses(m); got != limit {
		t.Fatalf("activeProcesses = %d, want %d", got, limit)
	}
}

func TestManager_Capacity_Budgets(t *testing.T) {
	invalid := []struct {
		name    string
		budgets managerBudgets
	}{
		{"zero log budget", managerBudgets{retainedLogMemoryBytes: 0, activeRunPeakBytes: 1}},
		{"negative log budget", managerBudgets{retainedLogMemoryBytes: -1, activeRunPeakBytes: 1}},
		{"zero run peak", managerBudgets{retainedLogMemoryBytes: 1, activeRunPeakBytes: 0}},
		{"negative run peak", managerBudgets{retainedLogMemoryBytes: 1, activeRunPeakBytes: -1}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newManagerWithBudgets(nil, t.TempDir(), tc.budgets); err == nil {
				t.Fatal("newManagerWithBudgets accepted a non-positive budget")
			}
		})
	}

	m := NewManager(nil, t.TempDir())
	if m.budgets.retainedLogMemoryBytes != MaxRetainedLogMemoryBytes {
		t.Fatalf("production retained log budget = %d, want %d", m.budgets.retainedLogMemoryBytes, MaxRetainedLogMemoryBytes)
	}
	if m.budgets.activeRunPeakBytes != MaxActiveRunPeakBytes {
		t.Fatalf("production active run peak budget = %d, want %d", m.budgets.activeRunPeakBytes, MaxActiveRunPeakBytes)
	}
	if MaxRetainedLogMemoryBytes != 256<<20 {
		t.Fatalf("MaxRetainedLogMemoryBytes = %d, want %d", MaxRetainedLogMemoryBytes, 256<<20)
	}
	if MaxActiveRunPeakBytes != 512<<20 {
		t.Fatalf("MaxActiveRunPeakBytes = %d, want %d", MaxActiveRunPeakBytes, 512<<20)
	}
	if MaxOutputBytes != 16<<20 {
		t.Fatalf("MaxOutputBytes = %d, want %d", MaxOutputBytes, 16<<20)
	}
}

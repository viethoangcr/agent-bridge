package process

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestManager_Logs_GlobalEvictionOrderAndMonotonicSequences(t *testing.T) {
	const budget = 3000
	m := newTestManagerWithLogBudget(t, budget, 4)
	p1 := testManagedProcess("p1", 1<<20)
	p2 := testManagedProcess("p2", 1<<20)
	registerTestProcess(m, p1)
	registerTestProcess(m, p2)

	plan := []struct {
		p      *managedProcess
		stream string
	}{
		{p1, "stdout"}, {p1, "stderr"}, {p2, "stdout"}, {p2, "stderr"},
		{p1, "stdout"}, {p2, "stdout"}, {p1, "stderr"}, {p2, "stderr"},
	}
	const n = 8
	for i, step := range plan {
		m.appendLog(step.p, step.stream, int64(i+1), logRingChunk(1000, byte('a'+i)))
	}

	m.logMu.Lock()
	charge := m.logCharge
	fifo := append([]*logEntry(nil), m.logFIFO...)
	m.logMu.Unlock()

	if charge > budget {
		t.Fatalf("logCharge = %d, want <= %d", charge, budget)
	}
	if len(fifo) < 1 || len(fifo) >= n {
		t.Fatalf("retained entries = %d, want a strict suffix of %d", len(fifo), n)
	}

	// The global FIFO is exactly the oldest-first suffix of the acceptance
	// order, regardless of owning process or stream.
	sum := 0
	for i, e := range fifo {
		want := int64(n - len(fifo) + i + 1)
		if e.globalSequence != want {
			t.Fatalf("fifo[%d].globalSequence = %d, want %d", i, e.globalSequence, want)
		}
		if !ringContains(e) {
			t.Fatalf("retained entry %d is missing from its owning ring", e.globalSequence)
		}
		sum += e.charge
	}
	if sum != charge {
		t.Fatalf("logCharge = %d, want summed retained charge %d", charge, sum)
	}

	// Per-process public sequences stay strictly increasing but show gaps once
	// their oldest entries are globally evicted.
	gapSeen := false
	for _, p := range []*managedProcess{p1, p2} {
		entries := p.ring.query(LogQuery{})
		var last int64
		for _, e := range entries {
			if e.Sequence <= last {
				t.Fatalf("process %s sequences not increasing: %d after %d", p.id, e.Sequence, last)
			}
			last = e.Sequence
		}
		if len(entries) > 0 && entries[0].Sequence > 1 {
			gapSeen = true
		}
	}
	if !gapSeen {
		t.Fatal("expected at least one per-process sequence gap after global eviction")
	}
}

func TestManager_Logs_PerProcessEvictionUpdatesGlobal(t *testing.T) {
	m := newTestManagerWithLogBudget(t, 1<<20, 4)
	p := testManagedProcess("p", 2500)
	registerTestProcess(m, p)
	for i := range 3 {
		m.appendLog(p, "stdout", int64(i+1), logRingChunk(1000, byte('a'+i)))
	}

	m.logMu.Lock()
	fifo := append([]*logEntry(nil), m.logFIFO...)
	charge := m.logCharge
	m.logMu.Unlock()

	if len(fifo) != len(p.ring.entries) {
		t.Fatalf("fifo=%d ring=%d, per-process eviction leaked into global FIFO", len(fifo), len(p.ring.entries))
	}
	if len(fifo) != 2 || fifo[0].sequence != 2 {
		t.Fatalf("retained sequences = %v, want [2 3]", logRingSequencesFromEntries(fifo))
	}
	sum := 0
	for _, e := range fifo {
		sum += e.charge
	}
	if charge != sum {
		t.Fatalf("logCharge = %d, want %d", charge, sum)
	}
	if want := cap(fifo[0].raw) + logEntryCharge; fifo[0].charge != want {
		t.Fatalf("retained charge = %d, want cap+%d = %d", fifo[0].charge, logEntryCharge, want)
	}
}

func TestManager_Logs_ExitedCompetesAndDeleteReleases(t *testing.T) {
	m := newTestManagerWithLogBudget(t, 3000, 4)
	exited := testManagedProcess("exited", 1<<20)
	registerTestProcess(m, exited)
	m.appendLog(exited, "stdout", 1, logRingChunk(1000, 'e'))
	if charge := retainedLogCharge(m); charge <= 0 {
		t.Fatalf("exited record charge = %d, want > 0", charge)
	}

	live := testManagedProcess("live", 1<<20)
	registerTestProcess(m, live)
	m.appendLog(live, "stderr", 2, logRingChunk(1000, 'l'))
	m.appendLog(live, "stderr", 3, logRingChunk(1000, 'l'))

	m.logMu.Lock()
	retained := make(map[int64]bool, len(m.logFIFO))
	for _, e := range m.logFIFO {
		retained[e.globalSequence] = true
	}
	m.logMu.Unlock()
	if retained[1] {
		t.Fatal("exited record's globally oldest entry was not evicted under aggregate pressure")
	}

	// DELETE releases the remaining charge immediately.
	m2 := newTestManagerWithLogBudget(t, 1<<20, 4)
	gone := testManagedProcess("gone", 1<<20)
	registerTestProcess(m2, gone)
	m2.appendLog(gone, "stdout", 1, logRingChunk(1000, 'g'))
	m2.appendLog(gone, "stdout", 2, logRingChunk(1000, 'g'))
	if charge := retainedLogCharge(m2); charge <= 0 {
		t.Fatalf("charge before delete = %d, want > 0", charge)
	}
	if err := m2.Delete(gone.id); err != nil {
		t.Fatalf("Delete(exited): %v", err)
	}
	if charge := retainedLogCharge(m2); charge != 0 {
		t.Fatalf("charge after delete = %d, want 0", charge)
	}
	if _, err := m2.Get(gone.id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(deleted) error = %v, want ErrNotFound", err)
	}
	if _, err := m2.Logs(gone.id, LogQuery{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Logs(deleted) error = %v, want ErrNotFound", err)
	}
}

func TestManager_Logs_ConcurrentAppendQueryDelete(t *testing.T) {
	m := newTestManagerWithLogBudget(t, 4000, 8)
	a := testManagedProcess("a", 1<<20)
	b := testManagedProcess("b", 1<<20)
	c := testManagedProcess("c", 1<<20)
	registerTestProcess(m, a)
	registerTestProcess(m, b)
	registerTestProcess(m, c)
	m.appendLog(c, "stdout", 1, logRingChunk(500, 'c'))

	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := a
			if i%2 == 0 {
				p = b
			}
			m.appendLog(p, "stdout", int64(i), logRingChunk(300, byte('a'+i%26)))
		}(i)
	}
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = m.Logs("a", LogQuery{})
			_, _ = m.Logs("b", LogQuery{})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = m.Delete("c")
	}()
	wg.Wait()

	// Each retained entry is charged exactly once: aggregate equals the summed
	// FIFO charges, every FIFO entry is still present in its ring, and the
	// deleted record contributed nothing.
	m.logMu.Lock()
	want := 0
	for _, e := range m.logFIFO {
		if !ringContains(e) {
			t.Fatalf("retained entry %d is missing from its ring", e.globalSequence)
		}
		want += e.charge
	}
	charge := m.logCharge
	m.logMu.Unlock()
	if charge != want {
		t.Fatalf("logCharge = %d, want %d", charge, want)
	}
	if charge < 0 {
		t.Fatalf("logCharge = %d, want non-negative", charge)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestManager_Logs_StreamReadErrorDoesNotDeadlockCompletion(t *testing.T) {
	m := newTestManager(t, 4)
	p := testManagedProcess("p", 1<<20)
	p.manager = m
	p.pumps.Add(1)

	finished := make(chan struct{})
	go func() {
		p.pump("stdout", errorReader{err: errors.New("read failed")})
		p.pumps.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("pump did not finish after a stream read error")
	}
	if charge := retainedLogCharge(m); charge != 0 {
		t.Fatalf("read error appended %d charged bytes, want 0", charge)
	}
}

func logRingSequencesFromEntries(entries []*logEntry) []int64 {
	seqs := make([]int64, len(entries))
	for i, e := range entries {
		seqs[i] = e.sequence
	}
	return seqs
}

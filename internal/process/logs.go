package process

import (
	"encoding/base64"
	"fmt"
	"math"
)

// releaseLogCharge removes every retained entry owned by p from the global
// FIFO and returns its charge to the aggregate budget immediately. DELETE calls
// it so an exited record stops competing for retained log memory.
func (m *Manager) releaseLogCharge(p *managedProcess) {
	if p.ring == nil {
		return
	}
	m.logMu.Lock()
	defer m.logMu.Unlock()
	kept := m.logFIFO[:0]
	for _, e := range m.logFIFO {
		if e.ring == p.ring {
			m.logCharge -= e.charge
			continue
		}
		kept = append(kept, e)
	}
	for i := len(kept); i < len(m.logFIFO); i++ {
		m.logFIFO[i] = nil
	}
	m.logFIFO = kept
}

// Logs returns copied public log DTOs for id, or ErrNotFound. Selection and the
// raw chunk copy happen under the single log lock; base64 encoding happens
// after it is released, so no lock is held during encoding.
func (m *Manager) Logs(id string, q LogQuery) ([]LogEntry, error) {
	m.mu.Lock()
	p, ok := m.processes[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if p.ring == nil {
		return []LogEntry{}, nil
	}

	type rawChunk struct {
		sequence    int64
		stream      string
		timestampMs int64
		raw         []byte
	}
	m.logMu.Lock()
	matched := p.ring.selectEntries(q)
	chunks := make([]rawChunk, len(matched))
	for i, e := range matched {
		chunks[i] = rawChunk{
			sequence:    e.sequence,
			stream:      e.stream,
			timestampMs: e.timestampMs,
			raw:         append([]byte(nil), e.raw...),
		}
	}
	m.logMu.Unlock()

	out := make([]LogEntry, len(chunks))
	for i, c := range chunks {
		out[i] = LogEntry{
			Sequence:    c.sequence,
			Stream:      c.stream,
			TimestampMs: c.timestampMs,
			Data:        base64.StdEncoding.EncodeToString(c.raw),
			Encoding:    "base64",
		}
	}
	return out, nil
}

// appendLog records one bridge-observed raw chunk under the single manager log
// lock. The lock assigns both the private manager-global sequence and the
// owning ring's per-process sequence, charges cap(raw)+logEntryCharge against
// the injected retained-memory budget, and evicts globally oldest entries while
// the budget is exceeded. Raw bytes are copied; no base64 is produced here.
func (m *Manager) appendLog(p *managedProcess, stream string, timestampMs int64, raw []byte) {
	if p.ring == nil || len(raw) == 0 {
		return
	}
	m.logMu.Lock()
	defer m.logMu.Unlock()

	m.globalSequence++
	entry, perProcessEvicted := p.ring.append(stream, timestampMs, raw, m.globalSequence)
	m.logFIFO = append(m.logFIFO, entry)
	m.logCharge = chargeAdd(m.logCharge, entry.charge)

	for _, e := range perProcessEvicted {
		m.removeFIFOLocked(e)
	}
	m.evictLogs()
}

// evictLogs drops the globally oldest whole entries until the aggregate charge
// is within budget, removing each from its owning ring exactly once. The caller
// must hold logMu.
func (m *Manager) evictLogs() {
	for m.logCharge > m.budgets.retainedLogMemoryBytes && len(m.logFIFO) > 0 {
		e := m.logFIFO[0]
		m.logFIFO[0] = nil
		m.logFIFO = m.logFIFO[1:]
		e.ring.remove(e)
		m.logCharge -= e.charge
	}
}

// removeFIFOLocked locates e in the global FIFO and drops it, returning its
// charge to the aggregate exactly once. The caller must hold logMu.
func (m *Manager) removeFIFOLocked(e *logEntry) {
	for i, cur := range m.logFIFO {
		if cur != e {
			continue
		}
		copy(m.logFIFO[i:], m.logFIFO[i+1:])
		m.logFIFO[len(m.logFIFO)-1] = nil
		m.logFIFO = m.logFIFO[:len(m.logFIFO)-1]
		m.logCharge -= e.charge
		return
	}
}

// chargeAdd returns total+charge, saturating at math.MaxInt instead of wrapping
// so a pathological charge cannot defeat the aggregate budget.
func chargeAdd(total, charge int) int {
	if charge > math.MaxInt-total {
		return math.MaxInt
	}
	return total + charge
}

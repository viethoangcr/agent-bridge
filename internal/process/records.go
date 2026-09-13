package process

import (
	"fmt"
	"slices"
	"strings"
)

// List returns independent snapshots of every retained process, sorted by ID.
func (m *Manager) List() []Snapshot {
	m.mu.Lock()
	out := make([]Snapshot, 0, len(m.processes))
	for _, p := range m.processes {
		out = append(out, p.snapshot())
	}
	m.mu.Unlock()

	slices.SortFunc(out, func(a, b Snapshot) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// Get returns a copied snapshot for id, or ErrNotFound.
func (m *Manager) Get(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.processes[id]
	if !ok {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return p.snapshot(), nil
}

// Delete removes an exited process record. A running process is ErrConflict and
// an unknown ID is ErrNotFound.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	p, ok := m.processes[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if p.status == StatusRunning {
		m.mu.Unlock()
		return fmt.Errorf("%w: process %s is still running", ErrConflict, id)
	}
	delete(m.processes, id)
	m.mu.Unlock()
	m.releaseLogCharge(p)
	return nil
}

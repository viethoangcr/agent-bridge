package process

// isClosing reports whether the manager has been marked closing.
func (m *Manager) isClosing() bool {
	m.reserveMu.Lock()
	defer m.reserveMu.Unlock()
	return m.closing
}

// reserveProcess atomically consumes one managed-process slot from the shared
// concurrency budget. It returns ErrCapacity when the active configuration's
// limit is already reached.
func (m *Manager) reserveProcess() error {
	m.reserveMu.Lock()
	defer m.reserveMu.Unlock()
	if m.closing {
		return errShuttingDown
	}
	if m.activeProcesses >= m.config.load().MaxConcurrentProcesses {
		return ErrCapacity
	}
	m.activeProcesses++
	m.wg.Add(1)
	return nil
}

// releaseProcess returns one managed-process slot to the shared concurrency
// budget. Callers release exactly once per successful reservation.
func (m *Manager) releaseProcess() {
	m.reserveMu.Lock()
	if m.activeProcesses > 0 {
		m.activeProcesses--
	}
	m.reserveMu.Unlock()
	m.wg.Done()
}

// reserveRun atomically reserves one shared process slot and peak bytes for a
// one-shot run before it spawns. Insufficient process-count or peak capacity is
// ErrCapacity with nothing reserved. Run-peak tracking stays on the manager so
// shutdown can account for active runs.
func (m *Manager) reserveRun(peak int) error {
	m.reserveMu.Lock()
	defer m.reserveMu.Unlock()
	if m.closing {
		return errShuttingDown
	}
	if m.activeProcesses >= m.config.load().MaxConcurrentProcesses {
		return ErrCapacity
	}
	if peak < 0 || m.activeRunPeak > m.budgets.activeRunPeakBytes-peak {
		return ErrCapacity
	}
	m.activeProcesses++
	m.activeRunPeak += peak
	m.wg.Add(1)
	return nil
}

// releaseRun returns one shared process slot and peak bytes for a completed
// one-shot run. Callers release exactly once per successful reservation.
func (m *Manager) releaseRun(peak int) {
	m.reserveMu.Lock()
	if m.activeProcesses > 0 {
		m.activeProcesses--
	}
	m.activeRunPeak -= peak
	if m.activeRunPeak < 0 {
		m.activeRunPeak = 0
	}
	m.reserveMu.Unlock()
	m.wg.Done()
}

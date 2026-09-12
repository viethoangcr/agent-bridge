package process

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

// maxDurationMillis is the largest millisecond count that fits in a
// time.Duration without overflowing the nanosecond conversion.
const maxDurationMillis = math.MaxInt64 / int64(time.Millisecond)

// productionBounds are the operational maxima from the specification. Tests
// inject smaller bounds through Config.validate.
var productionBounds = bounds{
	maxConcurrentProcesses:  1024,
	maxTimeoutMs:            86400000,
	maxOutputBytes:          16777216,
	maxLogBytesPerProcess:   268435456,
	maxInputBytesPerRequest: 7340032,
}

// bounds are the operational maxima a Config is validated against.
type bounds struct {
	maxConcurrentProcesses  int
	maxTimeoutMs            int
	maxOutputBytes          int
	maxLogBytesPerProcess   int
	maxInputBytesPerRequest int
}

// Config is the full-replacement runtime process configuration. Every value
// is a positive integer bounded by the operational maxima; a configuration is
// installed as a whole or not at all.
type Config struct {
	MaxConcurrentProcesses  int `json:"maxConcurrentProcesses"`
	DefaultRunTimeoutMs     int `json:"defaultRunTimeoutMs"`
	MaxRunTimeoutMs         int `json:"maxRunTimeoutMs"`
	MaxOutputBytes          int `json:"maxOutputBytes"`
	MaxLogBytesPerProcess   int `json:"maxLogBytesPerProcess"`
	MaxInputBytesPerRequest int `json:"maxInputBytesPerRequest"`
}

// DefaultConfig returns the documented default runtime configuration.
func DefaultConfig() Config {
	return Config{
		MaxConcurrentProcesses:  64,
		DefaultRunTimeoutMs:     30000,
		MaxRunTimeoutMs:         300000,
		MaxOutputBytes:          1048576,
		MaxLogBytesPerProcess:   10485760,
		MaxInputBytesPerRequest: 65536,
	}
}

// Validate reports whether c is a valid full replacement configuration under
// the production operational maxima. Callers validate before replacing the
// active configuration, so a failed validation leaves it unchanged.
func (c Config) Validate() error {
	return c.validate(productionBounds)
}

// validate applies b as the operational maxima. Timeouts are checked against
// the largest representable time.Duration so a later checked conversion
// cannot overflow.
func (c Config) validate(b bounds) error {
	fields := [...]struct {
		name  string
		value int
		max   int
	}{
		{"maxConcurrentProcesses", c.MaxConcurrentProcesses, b.maxConcurrentProcesses},
		{"defaultRunTimeoutMs", c.DefaultRunTimeoutMs, b.maxTimeoutMs},
		{"maxRunTimeoutMs", c.MaxRunTimeoutMs, b.maxTimeoutMs},
		{"maxOutputBytes", c.MaxOutputBytes, b.maxOutputBytes},
		{"maxLogBytesPerProcess", c.MaxLogBytesPerProcess, b.maxLogBytesPerProcess},
		{"maxInputBytesPerRequest", c.MaxInputBytesPerRequest, b.maxInputBytesPerRequest},
	}
	for _, f := range fields {
		if f.value <= 0 {
			return fmt.Errorf("%s must be positive", f.name)
		}
		if f.value > f.max {
			return fmt.Errorf("%s exceeds maximum %d", f.name, f.max)
		}
	}

	if c.DefaultRunTimeoutMs > c.MaxRunTimeoutMs {
		return fmt.Errorf("defaultRunTimeoutMs must not exceed maxRunTimeoutMs")
	}
	if int64(c.DefaultRunTimeoutMs) > maxDurationMillis || int64(c.MaxRunTimeoutMs) > maxDurationMillis {
		return fmt.Errorf("timeout is too large to represent as a duration")
	}
	return nil
}

// Config returns a copy of the active runtime configuration.
func (m *Manager) Config() Config {
	return m.config.load()
}

// UpdateConfig validates cfg and, on success, atomically replaces the active
// runtime configuration. A failed update leaves the previous configuration
// unchanged and reports ErrValidation.
func (m *Manager) UpdateConfig(cfg Config) error {
	if err := m.config.update(cfg); err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}
	return nil
}

// configStore holds the active runtime configuration as one immutable value.
// Readers observe either the previous complete configuration or a newly
// installed complete configuration, never a partial update.
type configStore struct {
	current atomic.Pointer[Config]
}

// newConfigStore returns a store holding initial, which must already be valid.
func newConfigStore(initial Config) *configStore {
	s := &configStore{}
	s.current.Store(&initial)
	return s
}

// load returns a copy of the active configuration.
func (s *configStore) load() Config {
	return *s.current.Load()
}

// update validates cfg and, on success, atomically replaces the active
// configuration. On failure the previous configuration is unchanged.
func (s *configStore) update(cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	s.current.Store(&cfg)
	return nil
}

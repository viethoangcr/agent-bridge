package process

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"testing"
)

func TestConfigDefaultsExact(t *testing.T) {
	want := Config{
		MaxConcurrentProcesses:  64,
		DefaultRunTimeoutMs:     30000,
		MaxRunTimeoutMs:         300000,
		MaxOutputBytes:          1048576,
		MaxLogBytesPerProcess:   10485760,
		MaxInputBytesPerRequest: 65536,
	}

	got := DefaultConfig()
	if got != want {
		t.Fatalf("DefaultConfig() = %+v, want %+v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() = %v, want nil", err)
	}
}

func TestConfigValidate(t *testing.T) {
	b := bounds{
		maxConcurrentProcesses:  8,
		maxTimeoutMs:            1000,
		maxOutputBytes:          64,
		maxLogBytesPerProcess:   128,
		maxInputBytesPerRequest: 32,
	}
	base := Config{
		MaxConcurrentProcesses:  4,
		DefaultRunTimeoutMs:     500,
		MaxRunTimeoutMs:         800,
		MaxOutputBytes:          32,
		MaxLogBytesPerProcess:   64,
		MaxInputBytesPerRequest: 16,
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"zero concurrent", func(c *Config) { c.MaxConcurrentProcesses = 0 }, true},
		{"negative concurrent", func(c *Config) { c.MaxConcurrentProcesses = -1 }, true},
		{"concurrent above max", func(c *Config) { c.MaxConcurrentProcesses = 9 }, true},
		{"zero default timeout", func(c *Config) { c.DefaultRunTimeoutMs = 0 }, true},
		{"negative default timeout", func(c *Config) { c.DefaultRunTimeoutMs = -1 }, true},
		{"default timeout above bound", func(c *Config) { c.DefaultRunTimeoutMs = 1001 }, true},
		{"zero max timeout", func(c *Config) { c.MaxRunTimeoutMs = 0 }, true},
		{"negative max timeout", func(c *Config) { c.MaxRunTimeoutMs = -1 }, true},
		{"max timeout above bound", func(c *Config) { c.MaxRunTimeoutMs = 1001 }, true},
		{"zero output", func(c *Config) { c.MaxOutputBytes = 0 }, true},
		{"negative output", func(c *Config) { c.MaxOutputBytes = -1 }, true},
		{"output above max", func(c *Config) { c.MaxOutputBytes = 65 }, true},
		{"zero log", func(c *Config) { c.MaxLogBytesPerProcess = 0 }, true},
		{"negative log", func(c *Config) { c.MaxLogBytesPerProcess = -1 }, true},
		{"log above max", func(c *Config) { c.MaxLogBytesPerProcess = 129 }, true},
		{"zero input", func(c *Config) { c.MaxInputBytesPerRequest = 0 }, true},
		{"negative input", func(c *Config) { c.MaxInputBytesPerRequest = -1 }, true},
		{"input above max", func(c *Config) { c.MaxInputBytesPerRequest = 33 }, true},
		{"default above max timeout", func(c *Config) { c.DefaultRunTimeoutMs = c.MaxRunTimeoutMs + 1 }, true},
		{"default equal max timeout", func(c *Config) {
			c.DefaultRunTimeoutMs = c.MaxRunTimeoutMs
		}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if err := cfg.validate(b); (err != nil) != tc.wantErr {
				t.Fatalf("validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestConfigTimeoutOverflow proves the millisecond-to-duration conversion is
// checked even when injected bounds would otherwise admit the value.
func TestConfigTimeoutOverflow(t *testing.T) {
	b := bounds{
		maxConcurrentProcesses:  math.MaxInt,
		maxTimeoutMs:            math.MaxInt,
		maxOutputBytes:          math.MaxInt,
		maxLogBytesPerProcess:   math.MaxInt,
		maxInputBytesPerRequest: math.MaxInt,
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"max run timeout", func(c *Config) { c.MaxRunTimeoutMs = math.MaxInt }},
		{"default run timeout", func(c *Config) {
			c.DefaultRunTimeoutMs = math.MaxInt
			c.MaxRunTimeoutMs = math.MaxInt
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(&cfg)
			if err := cfg.validate(b); err == nil {
				t.Fatalf("validate() = nil, want duration overflow error")
			}
		})
	}

	cfg := DefaultConfig()
	cfg.DefaultRunTimeoutMs = int(maxDurationMillis)
	cfg.MaxRunTimeoutMs = int(maxDurationMillis)
	if err := cfg.validate(b); err != nil {
		t.Fatalf("largest representable timeout rejected: %v", err)
	}
}

// TestConfigValidateProductionBoundary checks the real operational maxima
// without allocating maximum-sized buffers.
func TestConfigValidateProductionBoundary(t *testing.T) {
	atMax := Config{
		MaxConcurrentProcesses:  1024,
		DefaultRunTimeoutMs:     86400000,
		MaxRunTimeoutMs:         86400000,
		MaxOutputBytes:          16777216,
		MaxLogBytesPerProcess:   268435456,
		MaxInputBytesPerRequest: 7340032,
	}
	if err := atMax.Validate(); err != nil {
		t.Fatalf("configuration at every maximum rejected: %v", err)
	}

	out := DefaultConfig()
	out.MaxOutputBytes = 16777216
	if err := out.Validate(); err != nil {
		t.Fatalf("maxOutputBytes=16777216 rejected: %v", err)
	}
	out.MaxOutputBytes = 16777217
	if err := out.Validate(); err == nil {
		t.Fatalf("maxOutputBytes=16777217 accepted, want rejection")
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"concurrent", func(c *Config) { c.MaxConcurrentProcesses = 1025 }},
		{"default timeout", func(c *Config) { c.DefaultRunTimeoutMs = 86400001 }},
		{"max timeout", func(c *Config) { c.MaxRunTimeoutMs = 86400001 }},
		{"output", func(c *Config) { c.MaxOutputBytes = 16777217 }},
		{"log", func(c *Config) { c.MaxLogBytesPerProcess = 268435457 }},
		{"input", func(c *Config) { c.MaxInputBytesPerRequest = 7340033 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("validate() = nil, want rejection above production maximum")
			}
		})
	}
}

func TestConfigStoreUpdateReplacesAllFields(t *testing.T) {
	store := newConfigStore(DefaultConfig())
	next := Config{
		MaxConcurrentProcesses:  2,
		DefaultRunTimeoutMs:     10,
		MaxRunTimeoutMs:         20,
		MaxOutputBytes:          30,
		MaxLogBytesPerProcess:   40,
		MaxInputBytesPerRequest: 50,
	}

	if err := store.update(next); err != nil {
		t.Fatalf("update() = %v, want nil", err)
	}
	if got := store.load(); got != next {
		t.Fatalf("load() = %+v, want complete replacement %+v", got, next)
	}
}

func TestConfigStoreRejectsInvalidUpdateAtomically(t *testing.T) {
	store := newConfigStore(DefaultConfig())
	before := store.load()

	invalid := DefaultConfig()
	invalid.MaxInputBytesPerRequest = 0
	if err := store.update(invalid); err == nil {
		t.Fatalf("update() = nil, want validation error")
	}
	if got := store.load(); got != before {
		t.Fatalf("load() = %+v after failed update, want prior %+v", got, before)
	}
}

func TestConfigStoreReturnsCopies(t *testing.T) {
	store := newConfigStore(DefaultConfig())
	before := store.load()

	leaked := store.load()
	leaked.MaxOutputBytes = 1
	if got := store.load(); got != before {
		t.Fatalf("load() = %+v after caller mutation, want unchanged %+v", got, before)
	}
}

// TestConfigStoreConcurrentReadersSeeCompleteConfigs asserts readers never
// observe a mix of two valid configurations.
func TestConfigStoreConcurrentReadersSeeCompleteConfigs(t *testing.T) {
	a := DefaultConfig()
	b := Config{
		MaxConcurrentProcesses:  1,
		DefaultRunTimeoutMs:     1,
		MaxRunTimeoutMs:         1,
		MaxOutputBytes:          1,
		MaxLogBytesPerProcess:   1,
		MaxInputBytesPerRequest: 1,
	}
	store := newConfigStore(a)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if got := store.load(); got != a && got != b {
					t.Errorf("observed partial configuration: %+v", got)
					return
				}
			}
		})
	}

	for i := range 1000 {
		cfg := a
		if i%2 == 0 {
			cfg = b
		}
		if err := store.update(cfg); err != nil {
			t.Fatalf("update() = %v, want nil", err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestJSONConfigExactFields(t *testing.T) {
	got, err := json.Marshal(DefaultConfig())
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	const want = `{"maxConcurrentProcesses":64,"defaultRunTimeoutMs":30000,"maxRunTimeoutMs":300000,"maxOutputBytes":1048576,"maxLogBytesPerProcess":10485760,"maxInputBytesPerRequest":65536}`
	if string(got) != want {
		t.Fatalf("config JSON = %s, want %s", got, want)
	}
}

func TestJSONConfigStrictDecode(t *testing.T) {
	const full = `{"maxConcurrentProcesses":8,"defaultRunTimeoutMs":100,"maxRunTimeoutMs":200,"maxOutputBytes":1024,"maxLogBytesPerProcess":2048,"maxInputBytesPerRequest":512}`
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"full replacement", full, false},
		{"missing field", `{"maxConcurrentProcesses":8}`, true},
		{"unknown field", `{"maxConcurrentProcesses":8,"defaultRunTimeoutMs":100,"maxRunTimeoutMs":200,"maxOutputBytes":1024,"maxLogBytesPerProcess":2048,"maxInputBytesPerRequest":512,"owner":"me"}`, true},
		{"fractional value", `{"maxConcurrentProcesses":8.5,"defaultRunTimeoutMs":100,"maxRunTimeoutMs":200,"maxOutputBytes":1024,"maxLogBytesPerProcess":2048,"maxInputBytesPerRequest":512}`, true},
		{"integer overflow", `{"maxConcurrentProcesses":9223372036854775808,"defaultRunTimeoutMs":100,"maxRunTimeoutMs":200,"maxOutputBytes":1024,"maxLogBytesPerProcess":2048,"maxInputBytesPerRequest":512}`, true},
		{"trailing value", full + ` {}`, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeConfigJSON([]byte(tc.body)); (err != nil) != tc.wantErr {
				t.Fatalf("decodeConfigJSON() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// decodeConfigJSON applies the process JSON contract to a config body: reject
// unknown fields and trailing values, then require a fully valid replacement.
func decodeConfigJSON(data []byte) (Config, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("trailing JSON value")
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func TestJSONSnapshotOmitsUnset(t *testing.T) {
	pid := 1234
	zero := 0
	created := int64(1000)
	exited := int64(2000)

	tests := []struct {
		name string
		snap Snapshot
		want string
	}{
		{
			"running omits exit fields",
			Snapshot{
				ID:          "proc_1",
				Command:     "sleep",
				Args:        []string{"1"},
				Cwd:         "/tmp",
				Status:      StatusRunning,
				PID:         &pid,
				CreatedAtMs: created,
			},
			`{"id":"proc_1","command":"sleep","args":["1"],"cwd":"/tmp","status":"running","pid":1234,"createdAtMs":1000}`,
		},
		{
			"exited with code omits pid",
			Snapshot{
				ID:          "proc_2",
				Command:     "true",
				Args:        []string{},
				Cwd:         "/tmp",
				Status:      StatusExited,
				ExitCode:    &zero,
				CreatedAtMs: created,
				ExitedAtMs:  &exited,
			},
			`{"id":"proc_2","command":"true","args":[],"cwd":"/tmp","status":"exited","exitCode":0,"createdAtMs":1000,"exitedAtMs":2000}`,
		},
		{
			"signal exit omits exit code",
			Snapshot{
				ID:          "proc_3",
				Command:     "killed",
				Args:        []string{},
				Cwd:         "/tmp",
				Status:      StatusExited,
				CreatedAtMs: created,
				ExitedAtMs:  &exited,
			},
			`{"id":"proc_3","command":"killed","args":[],"cwd":"/tmp","status":"exited","createdAtMs":1000,"exitedAtMs":2000}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.snap)
			if err != nil {
				t.Fatalf("marshal snapshot: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("snapshot JSON = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestJSONLogEntryExactFields(t *testing.T) {
	got, err := json.Marshal(LogEntry{
		Sequence:    7,
		Stream:      "stdout",
		TimestampMs: 1000,
		Data:        "aGk=",
		Encoding:    "base64",
	})
	if err != nil {
		t.Fatalf("marshal log entry: %v", err)
	}
	const want = `{"sequence":7,"stream":"stdout","timestampMs":1000,"data":"aGk=","encoding":"base64"}`
	if string(got) != want {
		t.Fatalf("log entry JSON = %s, want %s", got, want)
	}
}

func TestJSONRunResultOmitsMissingExitCode(t *testing.T) {
	zero := 0
	tests := []struct {
		name   string
		result RunResult
		want   string
	}{
		{
			"success includes exit code",
			RunResult{
				ExitCode:        &zero,
				Stdout:          "out",
				Stderr:          "err",
				StdoutTruncated: true,
				DurationMs:      12,
			},
			`{"exitCode":0,"timedOut":false,"stdout":"out","stderr":"err","stdoutTruncated":true,"stderrTruncated":false,"durationMs":12}`,
		},
		{
			"timeout omits exit code",
			RunResult{
				TimedOut:   true,
				DurationMs: 5,
			},
			`{"timedOut":true,"stdout":"","stderr":"","stdoutTruncated":false,"stderrTruncated":false,"durationMs":5}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.result)
			if err != nil {
				t.Fatalf("marshal run result: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("run result JSON = %s, want %s", got, tc.want)
			}
		})
	}
}

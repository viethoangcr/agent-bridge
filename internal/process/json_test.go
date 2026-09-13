package process

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
)

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

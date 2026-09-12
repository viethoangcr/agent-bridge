// Package process owns managed process groups, their bounded output logs, and
// one-shot command execution for the agent-bridge process API.
package process

// Status is the lifecycle state of a managed process.
type Status string

const (
	// StatusRunning marks a process whose direct child has started and has not
	// yet been observed to exit.
	StatusRunning Status = "running"
	// StatusExited marks a process whose direct child has exited. Exited
	// records are retained until they are explicitly deleted.
	StatusExited Status = "exited"
)

// Snapshot is the public, immutable view of one managed process. PID is
// present only while the process is running, ExitCode is present only when
// the direct child exited normally, and ExitedAtMs is present only after exit.
type Snapshot struct {
	ID          string   `json:"id"`
	Command     string   `json:"command"`
	Args        []string `json:"args"`
	Cwd         string   `json:"cwd"`
	Status      Status   `json:"status"`
	PID         *int     `json:"pid,omitempty"`
	ExitCode    *int     `json:"exitCode,omitempty"`
	CreatedAtMs int64    `json:"createdAtMs"`
	ExitedAtMs  *int64   `json:"exitedAtMs,omitempty"`
}

// LogEntry is one bridge-observed stdout or stderr chunk. Data is standard
// padded base64 and Encoding is always "base64".
type LogEntry struct {
	Sequence    int64  `json:"sequence"`
	Stream      string `json:"stream"`
	TimestampMs int64  `json:"timestampMs"`
	Data        string `json:"data"`
	Encoding    string `json:"encoding"`
}

// LogQuery selects log entries for one managed process. Since is an exclusive
// per-process sequence lower bound. Stream is "stdout", "stderr", or
// "combined"; the empty value is treated as "combined". When Tail is non-nil,
// at most *Tail entries are returned; a Tail of zero is valid.
type LogQuery struct {
	Since  int64
	Stream string
	Tail   *int
}

// StartRequest is the managed-start request body. Args, Cwd, and Env are
// optional; an omitted Cwd resolves to the bridge startup directory and Env
// overlays the sanitized inherited environment.
type StartRequest struct {
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Cwd     string            `json:"cwd"`
	Env     map[string]string `json:"env"`
}

// RunRequest is the one-shot request body. TimeoutMs and MaxOutputBytes are
// optional and fall back to the active configuration when nil.
type RunRequest struct {
	Command        string            `json:"command"`
	Args           []string          `json:"args"`
	Cwd            string            `json:"cwd"`
	Env            map[string]string `json:"env"`
	TimeoutMs      *int64            `json:"timeoutMs,omitempty"`
	MaxOutputBytes *int64            `json:"maxOutputBytes,omitempty"`
}

// RunResult is the one-shot response. ExitCode is present only when the
// direct child exited normally; timed-out or signal-terminated runs omit it.
type RunResult struct {
	ExitCode        *int   `json:"exitCode,omitempty"`
	TimedOut        bool   `json:"timedOut"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdoutTruncated"`
	StderrTruncated bool   `json:"stderrTruncated"`
	DurationMs      int64  `json:"durationMs"`
}

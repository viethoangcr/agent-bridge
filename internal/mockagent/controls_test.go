package mockagent

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestControlHookDelay checks the delay hook waits for the requested duration
// through the injected sleep seam, then replies.
func TestControlHookDelay(t *testing.T) {
	old := sleepFn
	defer func() { sleepFn = old }()
	var got time.Duration
	sleepFn = func(d time.Duration) { got = d }

	out, _, err := run(t, requestLine(t, "1", methodDelay, map[string]any{"ms": 25})+"\n")
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	if got != 25*time.Millisecond {
		t.Fatalf("delay = %v, want 25ms", got)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 1 || string(msgs[0].Result) != "{}" {
		t.Fatalf("delay response = %+v, want result {}", msgs)
	}
}

// TestControlHookInvalidStdout checks the invalid-stdout hook writes a
// malformed line before its own response.
func TestControlHookInvalidStdout(t *testing.T) {
	line := requestLine(t, "1", methodInvalidStdout, map[string]any{"line": "this is not json"}) + "\n"
	out, _, err := run(t, line)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("output lines = %d, want malformed line plus response:\n%s", len(lines), out)
	}
	if lines[0] != "this is not json" {
		t.Fatalf("invalid stdout line = %q, want raw malformed line", lines[0])
	}
	var m rpcMessage
	if err := json.Unmarshal([]byte(lines[1]), &m); err != nil {
		t.Fatalf("hook response %q: %v", lines[1], err)
	}
	if string(m.Result) != "{}" {
		t.Fatalf("hook response result = %s, want {}", m.Result)
	}
}

// TestControlHookStderr checks the stderr hook writes only to stderr.
func TestControlHookStderr(t *testing.T) {
	input := requestLine(t, "1", methodStderr, map[string]any{"line": "secret=abc"}) + "\n"
	out, errOut, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	if errOut != "secret=abc\n" {
		t.Fatalf("stderr = %q, want %q", errOut, "secret=abc\n")
	}
	if strings.Contains(out, "secret=abc") {
		t.Fatalf("stderr hook leaked to stdout: %q", out)
	}
}

// TestControlHookExit checks the exit hook replies once and then stops the loop
// without processing later lines.
func TestControlHookExit(t *testing.T) {
	input := strings.Join([]string{
		requestLine(t, "1", methodExit, map[string]any{}),
		initLine(t, "2"),
	}, "\n") + "\n"
	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 1 {
		t.Fatalf("got %d responses after exit, want 1:\n%s", len(msgs), out)
	}
	if string(msgs[0].ID) != "1" {
		t.Fatalf("exit response id = %s, want 1", msgs[0].ID)
	}
}

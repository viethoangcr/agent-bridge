package acpruntime

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/viethoangcr/agent-bridge/internal/mockagent"
)

func TestRedactSensitiveLine(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "token colon", in: "token: abc", want: "token[REDACTED]"},
		{name: "token equals", in: "token=abc", want: "token[REDACTED]"},
		{name: "key equals", in: "key=value", want: "key[REDACTED]"},
		{name: "secret colon", in: "secret: hunter2", want: "secret[REDACTED]"},
		{name: "password equals", in: "password=p@ss", want: "password[REDACTED]"},
		{name: "mixed case", in: "SeCrEt=abc", want: "SeCrEt[REDACTED]"},
		{name: "upper case", in: "PASSWORD: abc", want: "PASSWORD[REDACTED]"},
		{name: "prefix preserved", in: "connecting token=SECRET now", want: "connecting token[REDACTED]"},
		{name: "whitespace after delimiter", in: "token:   secret value   ", want: "token[REDACTED]"},
		{name: "whitespace before delimiter", in: "password = secret", want: "password[REDACTED]"},
		{name: "underscore boundary", in: "api_key=abc", want: "api_key[REDACTED]"},
		{name: "earliest label wins", in: "secret=abc token=def", want: "secret[REDACTED]"},
		{name: "tokenizer unchanged", in: "tokenizer: nope", want: "tokenizer: nope"},
		{name: "keynote unchanged", in: "keynote: nope", want: "keynote: nope"},
		{name: "monkey unchanged", in: "monkey = banana", want: "monkey = banana"},
		{name: "other delimiter slash", in: "key/value pair", want: "key/value pair"},
		{name: "other delimiter dash", in: "key-value pair", want: "key-value pair"},
		{name: "space delimiter", in: "key value", want: "key value"},
		{name: "trailing label only", in: "a token", want: "a token"},
		{name: "no label", in: "just diagnostics", want: "just diagnostics"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactSensitiveLine(tt.in); got != tt.want {
				t.Errorf("redactSensitiveLine(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestStderrTailBound(t *testing.T) {
	const limit = 8 * 1024

	t.Run("exact limit retained", func(t *testing.T) {
		var tail stderrTail
		want := strings.Repeat("a", limit)
		tail.append([]byte(want))
		if got := tail.string(); got != want {
			t.Errorf("tail length = %d, want %d; content mismatch", len(got), limit)
		}
	})

	t.Run("overflow keeps latest complete tail", func(t *testing.T) {
		var tail stderrTail
		const marker = "TAIL-MARKER"
		tail.append([]byte(strings.Repeat("a", limit)))
		tail.append([]byte(marker))
		got := tail.string()
		if len(got) > limit {
			t.Errorf("tail length = %d, want <= %d", len(got), limit)
		}
		if !strings.HasSuffix(got, marker) {
			t.Errorf("tail = %q, want suffix %q", got, marker)
		}
		if !utf8.ValidString(got) {
			t.Errorf("tail is not valid UTF-8: %q", got)
		}
	})

	t.Run("split code point normalized", func(t *testing.T) {
		var tail stderrTail
		// The Euro sign U+20AC is E2 82 AC; split it across appends.
		tail.append([]byte("ok "))
		tail.append([]byte{0xE2, 0x82})
		tail.append([]byte{0xAC, '\n'})
		got := tail.string()
		if !utf8.ValidString(got) {
			t.Fatalf("tail is not valid UTF-8: %q", got)
		}
		if !strings.Contains(got, "ok ") {
			t.Errorf("tail = %q, want preserved leading text", got)
		}
	})

	t.Run("trim does not emit invalid utf8", func(t *testing.T) {
		var tail stderrTail
		// Place a multi-byte rune so the naive 8 KiB trim lands mid-rune.
		buf := append([]byte("a"), []byte("\u20ac")...)
		buf = append(buf, bytes.Repeat([]byte("b"), limit)...)
		tail.append(buf)
		got := tail.string()
		if len(got) > limit {
			t.Errorf("tail length = %d, want <= %d", len(got), limit)
		}
		if !utf8.ValidString(got) {
			t.Errorf("tail is not valid UTF-8 after trim: %q", got)
		}
	})
}

func TestStderrReadMultiLineChunked(t *testing.T) {
	const secret = "SECRET-c0ffee"
	r := &Runtime{serverID: "srv", log: testLogger()}
	r.pumps.Add(1)
	r.readStderr(io.NopCloser(strings.NewReader("alpha\nconnecting token=" + secret + " now\nomega\n")))

	got := r.Stderr()
	if strings.Contains(got, secret) {
		t.Errorf("Stderr() = %q, leaked secret", got)
	}
	for _, want := range []string{"alpha", "connecting", "[REDACTED]", "omega"} {
		if !strings.Contains(got, want) {
			t.Errorf("Stderr() = %q, want %q", got, want)
		}
	}
}

// TestStderrUnterminatedStreamBounded proves an unterminated stderr line is
// processed with bounded memory and no partial-line leak: once the pending line
// exceeds stderrPendingLimit its content is discarded and only a content-free
// truncation marker is retained. Partial-line retention before a newline is
// deliberately not required; redaction correctness and the no-leak bound are.
func TestStderrUnterminatedStreamBounded(t *testing.T) {
	const secret = "SECRET-unterminated-c0ffee"

	var logs bytes.Buffer
	r := &Runtime{serverID: "srv", log: slog.New(slog.NewTextHandler(&logs, nil))}
	r.pumps.Add(1)

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		r.readStderr(pr)
	}()

	// The unterminated line crosses the pending cap, so the secret near its
	// start must be discarded rather than retained or logged.
	stream := "connecting token=" + secret + " now " + strings.Repeat("pad ", 40*1024)
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		_, _ = io.WriteString(pw, stream)
	}()

	// The truncation marker must appear while the line is still open, and the
	// secret must never be retained.
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(r.Stderr(), stderrTruncatedMarker) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := r.Stderr(); !strings.Contains(got, stderrTruncatedMarker) {
		t.Fatalf("retained tail = %q, want the truncation marker while the line is open", got)
	} else {
		if len(got) > stderrTailLimit {
			t.Errorf("retained tail length = %d, want <= %d", len(got), stderrTailLimit)
		}
		if strings.Contains(got, secret) {
			t.Errorf("retained tail leaked the secret: %q", got)
		}
		if !utf8.ValidString(got) {
			t.Errorf("retained tail is not valid UTF-8: %q", got)
		}
	}

	select {
	case <-writeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stderr writer did not finish")
	}
	_ = pw.Close()
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("readStderr did not return after the pipe closed")
	}

	got := r.Stderr()
	logged := logs.String()
	if strings.Contains(got, secret) || strings.Contains(logged, secret) {
		t.Errorf("overlong line leaked the secret: tail=%q logs=%q", got, logged)
	}
	if !strings.Contains(got, stderrTruncatedMarker) || !strings.Contains(logged, stderrTruncatedMarker) {
		t.Errorf("want the content-free truncation marker: tail=%q logs=%q", got, logged)
	}
	if strings.Contains(got, "pad ") {
		t.Errorf("overlong line content was retained: %q", got)
	}
	if len(got) > stderrTailLimit {
		t.Errorf("retained tail length = %d, want <= %d", len(got), stderrTailLimit)
	}
	if !utf8.ValidString(got) {
		t.Errorf("retained tail is not valid UTF-8: %q", got)
	}
}

// TestStderrRedactsLabelSplitAcrossFragments drives a label across a
// buffer-sized read boundary and proves the completed line is redacted whole.
func TestStderrRedactsLabelSplitAcrossFragments(t *testing.T) {
	const secret = "SECRET-split-label"
	// The first ReadSlice returns exactly one buffer ending in "to", so "ken="
	// arrives in the next fragment.
	stream := strings.Repeat(" ", stderrFragmentBytes-2) + "to" + "ken=" + secret + " now\n"

	var logs bytes.Buffer
	r := &Runtime{serverID: "srv", log: slog.New(slog.NewTextHandler(&logs, nil))}
	r.pumps.Add(1)
	r.readStderr(io.NopCloser(strings.NewReader(stream)))

	for name, got := range map[string]string{"Stderr()": r.Stderr(), "logs": logs.String()} {
		if strings.Contains(got, secret) {
			t.Errorf("%s = %q, leaked secret", name, got)
		}
		if !strings.Contains(got, "token[REDACTED]") {
			t.Errorf("%s = %q, want redacted label", name, got)
		}
	}
}

// TestStderrRedactsSecretSpanningFragments proves a label completed at the very
// end of one fragment still redacts the secret that arrives in the next.
func TestStderrRedactsSecretSpanningFragments(t *testing.T) {
	const secret = "SECRET-spanning"
	// The first buffer-sized fragment ends exactly at the '=' delimiter.
	stream := strings.Repeat(" ", stderrFragmentBytes-len("token=")) + "token=" + secret + " now\n"

	var logs bytes.Buffer
	r := &Runtime{serverID: "srv", log: slog.New(slog.NewTextHandler(&logs, nil))}
	r.pumps.Add(1)
	r.readStderr(io.NopCloser(strings.NewReader(stream)))

	for name, got := range map[string]string{"Stderr()": r.Stderr(), "logs": logs.String()} {
		if strings.Contains(got, secret) {
			t.Errorf("%s = %q, leaked secret", name, got)
		}
		if !strings.Contains(got, "token[REDACTED]") {
			t.Errorf("%s = %q, want redacted label", name, got)
		}
	}
}

// TestStderrOverlongLineDroppedWithoutLeak proves an overlong unterminated line
// is discarded in bounded chunks without retaining or logging any content.
func TestStderrOverlongLineDroppedWithoutLeak(t *testing.T) {
	const secret = "SECRET-overlong"

	var logs bytes.Buffer
	r := &Runtime{serverID: "srv", log: slog.New(slog.NewTextHandler(&logs, nil))}
	r.pumps.Add(1)
	stream := "token=" + secret + " " + strings.Repeat("x", stderrPendingLimit)
	r.readStderr(io.NopCloser(strings.NewReader(stream)))

	got := r.Stderr()
	logged := logs.String()
	if strings.Contains(got, secret) || strings.Contains(logged, secret) {
		t.Errorf("overlong line leaked the secret: tail=%q logs=%q", got, logged)
	}
	if !strings.Contains(got, stderrTruncatedMarker) || !strings.Contains(logged, stderrTruncatedMarker) {
		t.Errorf("want the content-free truncation marker: tail=%q logs=%q", got, logged)
	}
	if strings.Contains(got, "xxx") {
		t.Errorf("overlong line content was retained: %q", got)
	}
	if len(got) > stderrTailLimit {
		t.Errorf("retained tail length = %d, want <= %d", len(got), stderrTailLimit)
	}
	if !utf8.ValidString(got) {
		t.Errorf("retained tail is not valid UTF-8: %q", got)
	}
}

// TestStderrMultibyteRuneSplitAcrossFragments proves a rune split across a read
// boundary survives once the line completes instead of becoming a replacement
// character.
func TestStderrMultibyteRuneSplitAcrossFragments(t *testing.T) {
	// The buffer boundary lands on the first byte of the three-byte Euro sign.
	stream := strings.Repeat("a", stderrFragmentBytes-1) + "\u20ac" + "tail\n"

	r := &Runtime{serverID: "srv", log: testLogger()}
	r.pumps.Add(1)
	r.readStderr(io.NopCloser(strings.NewReader(stream)))

	got := r.Stderr()
	if !utf8.ValidString(got) {
		t.Fatalf("Stderr() is not valid UTF-8: %q", got)
	}
	if !strings.Contains(got, "\u20ac"+"tail") {
		t.Errorf("split rune not preserved once the line completed: %q", got)
	}
}

func TestStderrReplacesInvalidUTF8(t *testing.T) {
	r := &Runtime{serverID: "srv", log: testLogger()}
	r.pumps.Add(1)
	r.readStderr(io.NopCloser(bytes.NewReader([]byte("before \xff\xfe after\n"))))

	got := r.Stderr()
	if !utf8.ValidString(got) {
		t.Fatalf("Stderr() is not valid UTF-8: %q", got)
	}
	if bytes.Contains([]byte(got), []byte{0xff}) || bytes.Contains([]byte(got), []byte{0xfe}) {
		t.Errorf("Stderr() = %q, want invalid bytes replaced", got)
	}
	for _, want := range []string{"before", "after"} {
		if !strings.Contains(got, want) {
			t.Errorf("Stderr() = %q, want %q", got, want)
		}
	}
}

func TestStderrRedactsMockSecret(t *testing.T) {
	const secret = "sk-super-secret-value-123"

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mockDone := make(chan error, 1)
	go func() {
		err := mockagent.Run(ctx, inR, outW, errW)
		_ = inR.Close()
		_ = outW.Close()
		_ = errW.Close()
		mockDone <- err
	}()

	var logs bytes.Buffer
	r := &Runtime{
		serverID: "srv",
		log:      slog.New(slog.NewTextHandler(&logs, nil)),
	}
	r.pumps.Add(1)
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		r.readStderr(errR)
	}()

	request := `{"jsonrpc":"2.0","id":1,"method":"_mock/stderr","params":{"line":"connecting token=` + secret + ` now"}}` + "\n"
	if _, err := io.WriteString(inW, request); err != nil {
		t.Fatalf("write _mock/stderr request: %v", err)
	}
	_ = inW.Close()

	response, err := bufio.NewReader(outR).ReadString('\n')
	if err != nil {
		t.Fatalf("read mock response: %v", err)
	}
	if !strings.Contains(response, `"id":1`) {
		t.Fatalf("mock response = %q, want the _mock/stderr result", response)
	}

	select {
	case <-stderrDone:
	case <-time.After(5 * time.Second):
		t.Fatal("readStderr did not finish after the mock closed stderr")
	}
	if err := <-mockDone; err != nil {
		t.Fatalf("mockagent.Run: %v", err)
	}

	for name, got := range map[string]string{"Stderr()": r.Stderr(), "logs": logs.String()} {
		if strings.Contains(got, secret) {
			t.Errorf("%s = %q, leaked secret", name, got)
		}
		if !strings.Contains(got, "connecting") {
			t.Errorf("%s = %q, want surrounding diagnostic", name, got)
		}
		if !strings.Contains(got, "[REDACTED]") {
			t.Errorf("%s = %q, want [REDACTED]", name, got)
		}
	}
}

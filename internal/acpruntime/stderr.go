package acpruntime

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"unicode/utf8"
)

// stderrTailLimit is the maximum number of stderr bytes retained for future 502
// problem extensions.
const stderrTailLimit = 8 * 1024

// stderrFragmentBytes bounds one stderr read so an unterminated line cannot
// allocate without limit.
const stderrFragmentBytes = 4 * 1024

// stderrPendingLimit bounds one pending stderr line. A line that grows past the
// cap before its newline is discarded entirely and replaced by a content-free
// truncation marker, so no partial line is ever retained or logged.
const stderrPendingLimit = 64 * 1024

// stderrTruncatedMarker is the content-free replacement retained and logged for
// an overlong stderr line whose bytes were discarded.
const stderrTruncatedMarker = "[stderr line dropped: exceeded pending limit]"

// utf8Replacement is the standard-library replacement rune used to normalize
// split or otherwise invalid stderr bytes.
const utf8Replacement = "\uFFFD"

// sensitiveKeywords are the case-insensitive labels whose value is redacted.
var sensitiveKeywords = []string{"token", "key", "secret", "password"}

// stderrTail is a mutex-protected bounded tail of redacted agent stderr.
type stderrTail struct {
	mu  sync.Mutex
	buf []byte
}

// append retains b as the newest part of the tail. The retained bytes are
// normalized to valid UTF-8 and trimmed to the newest complete runes within
// stderrTailLimit bytes, so a chunk boundary or a trim can never emit invalid
// UTF-8.
func (t *stderrTail) append(b []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, b...)
	t.buf = trimTailToRuneBoundary(t.buf, stderrTailLimit)
	t.buf = []byte(strings.ToValidUTF8(string(t.buf), utf8Replacement))
	t.buf = trimTailToRuneBoundary(t.buf, stderrTailLimit)
}

// trimTailToRuneBoundary keeps at most limit trailing bytes, starting at the
// first UTF-8 rune boundary so the retained tail never begins with a partial
// rune.
func trimTailToRuneBoundary(buf []byte, limit int) []byte {
	if len(buf) <= limit {
		return buf
	}
	start := len(buf) - limit
	for start < len(buf) && !utf8.RuneStart(buf[start]) {
		start++
	}
	return buf[start:]
}

// string returns a copy of the retained tail.
func (t *stderrTail) string() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

// Stderr returns the retained, redacted stderr tail.
func (r *Runtime) Stderr() string {
	return r.stderr.string()
}

// redactSensitiveLine replaces the value of the first case-insensitive
// token|key|secret|password label with [REDACTED]. The label must be a
// standalone word (so "tokenizer", "keynote", and "monkey" are untouched) and
// may be separated from its ':' or '=' delimiter by spaces or tabs; everything
// from the label onward, including following whitespace, is discarded.
func redactSensitiveLine(line string) string {
	for i := 0; i < len(line); i++ {
		if !isLabelBoundary(line, i) {
			continue
		}
		for _, keyword := range sensitiveKeywords {
			end := i + len(keyword)
			if end > len(line) || !strings.EqualFold(line[i:end], keyword) {
				continue
			}
			delim := end
			for delim < len(line) && (line[delim] == ' ' || line[delim] == '\t') {
				delim++
			}
			if delim < len(line) && (line[delim] == ':' || line[delim] == '=') {
				return line[:end] + "[REDACTED]"
			}
		}
	}
	return line
}

// isLabelBoundary reports whether position i begins a word, so a label embedded
// in a longer identifier is not matched.
func isLabelBoundary(s string, i int) bool {
	if i == 0 {
		return true
	}
	c := s[i-1]
	return !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9')
}

// readStderr consumes the child's stderr with line-granular bounded
// processing. Fragments accumulate into a pending line of at most
// stderrPendingLimit bytes; only complete newline-terminated lines are
// redacted, normalized, retained, and logged, so a label, secret, or multibyte
// rune split across fragment boundaries is handled on the whole line. A line
// that exceeds the cap before its newline is discarded entirely and replaced by
// a content-free truncation marker. Stderr is never persisted.
func (r *Runtime) readStderr(pipe io.ReadCloser) {
	defer r.pumps.Done()
	defer closePipe(pipe)

	reader := bufio.NewReaderSize(pipe, stderrFragmentBytes)
	pending := make([]byte, 0, stderrFragmentBytes)
	discarding := false
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) > 0 {
			pending, discarding = r.consumeStderrFragment(pending, discarding, fragment)
		}
		if err == nil || errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		// EOF or a read error: process the remaining bytes as a final line.
		if len(pending) > 0 && !discarding {
			r.recordStderrLine(pending, false)
		}
		return
	}
}

// consumeStderrFragment appends one raw fragment to the pending line and
// processes every complete newline-terminated line it contains. When the
// pending line exceeds stderrPendingLimit before a newline, its bytes are
// discarded and only a content-free marker is retained; discarding then
// continues until the next newline or EOF, so an overlong line can never leak a
// partial label. It returns the retained pending bytes and the discarding state
// for the next fragment.
func (r *Runtime) consumeStderrFragment(pending []byte, discarding bool, fragment []byte) ([]byte, bool) {
	for {
		newline := bytes.IndexByte(fragment, '\n')
		chunk := fragment
		if newline >= 0 {
			chunk = fragment[:newline]
		}
		if !discarding {
			if len(pending)+len(chunk) > stderrPendingLimit {
				pending = pending[:0]
				discarding = true
				r.recordStderrMarker()
			} else {
				pending = append(pending, chunk...)
			}
		}
		if newline < 0 {
			return pending, discarding
		}
		if !discarding {
			r.recordStderrLine(pending, true)
		}
		pending = pending[:0]
		discarding = false
		fragment = fragment[newline+1:]
		if len(fragment) == 0 {
			return pending, discarding
		}
	}
}

// recordStderrLine redacts one complete raw stderr line before retaining and
// logging it. Redaction runs on the whole line, so a label or secret split
// across read fragments is still caught; invalid UTF-8 is normalized once the
// line is complete, so a rune split across fragments is preserved. The retained
// tail keeps the newline; the log value omits it.
func (r *Runtime) recordStderrLine(line []byte, newline bool) {
	redacted := strings.ToValidUTF8(redactSensitiveLine(string(line)), utf8Replacement)
	if newline {
		redacted += "\n"
	}
	r.stderr.append([]byte(redacted))

	logged := strings.TrimSuffix(redacted, "\n")
	if logged == "" {
		return
	}
	r.log.Info("agent stderr", "server_id", r.serverID, "line", logged)
}

// recordStderrMarker retains and logs the content-free truncation marker for an
// overlong line whose bytes were discarded.
func (r *Runtime) recordStderrMarker() {
	r.recordStderrLine([]byte(stderrTruncatedMarker), true)
}

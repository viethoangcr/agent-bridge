package process

import (
	"bytes"
	"encoding/base64"
	"math"
	"testing"
)

func logRingChunk(n int, fill byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = fill
	}
	return b
}

func intPtr(v int) *int { return &v }

func TestLogRingAppendAssignsSequenceStreamTimestampAndGlobalSequence(t *testing.T) {
	r := newLogRing(1 << 20)

	e1, evicted := r.append("stdout", 111, []byte("one"), 10)
	if len(evicted) != 0 {
		t.Fatalf("first append evicted %d entries, want 0", len(evicted))
	}
	e2, _ := r.append("stderr", 222, []byte("two"), 11)

	if e1.sequence != 1 {
		t.Fatalf("e1.sequence = %d, want 1", e1.sequence)
	}
	if e2.sequence != 2 {
		t.Fatalf("e2.sequence = %d, want 2", e2.sequence)
	}
	if e1.globalSequence != 10 || e2.globalSequence != 11 {
		t.Fatalf("globalSequence = %d,%d, want 10,11", e1.globalSequence, e2.globalSequence)
	}
	if e1.stream != "stdout" || e2.stream != "stderr" {
		t.Fatalf("stream = %q,%q, want stdout,stderr", e1.stream, e2.stream)
	}
	if e1.timestampMs != 111 || e2.timestampMs != 222 {
		t.Fatalf("timestamp = %d,%d, want 111,222", e1.timestampMs, e2.timestampMs)
	}
	if !bytes.Equal(e1.raw, []byte("one")) || !bytes.Equal(e2.raw, []byte("two")) {
		t.Fatalf("raw = %q,%q, want one,two", e1.raw, e2.raw)
	}
	if e1.ring != r || e2.ring != r {
		t.Fatal("entries do not point back at their owning ring")
	}
}

func TestLogRingAppendCopiesRawChunk(t *testing.T) {
	r := newLogRing(1 << 20)

	src := []byte("hello")
	e, _ := r.append("stdout", 1, src, 1)
	src[0] = 'X'

	if !bytes.Equal(e.raw, []byte("hello")) {
		t.Fatalf("retained raw = %q, want hello (source mutation leaked)", e.raw)
	}
	got := r.query(LogQuery{})
	if len(got) != 1 {
		t.Fatalf("query returned %d entries, want 1", len(got))
	}
	if want := base64.StdEncoding.EncodeToString([]byte("hello")); got[0].Data != want {
		t.Fatalf("data = %q, want %q", got[0].Data, want)
	}
}

func TestLogRingPerProcessEvictionUsesLen(t *testing.T) {
	r := newLogRing(10) // not divisible by the 4-byte chunk size

	e1, _ := r.append("stdout", 1, logRingChunk(4, 'a'), 1)
	if r.rawBytes != 4 {
		t.Fatalf("rawBytes = %d, want 4", r.rawBytes)
	}
	e2, ev := r.append("stdout", 2, logRingChunk(4, 'b'), 2)
	if len(ev) != 0 {
		t.Fatalf("second append evicted %d entries, want 0", len(ev))
	}
	e3, ev := r.append("stdout", 3, logRingChunk(4, 'c'), 3)
	if len(ev) != 1 || ev[0] != e1 {
		t.Fatalf("third append evicted %v, want [e1]", ev)
	}
	if r.rawBytes != 8 {
		t.Fatalf("rawBytes = %d, want 8", r.rawBytes)
	}
	if want := e2.charge + e3.charge; r.chargedBytes != want {
		t.Fatalf("chargedBytes = %d, want %d", r.chargedBytes, want)
	}

	got := r.query(LogQuery{})
	if len(got) != 2 || got[0].Sequence != 2 || got[1].Sequence != 3 {
		t.Fatalf("query sequences = %v, want [2 3]", logRingSequences(got))
	}
}

func TestLogRingRetainsSingle8KiBChunkAtLimit(t *testing.T) {
	r := newLogRing(8192)

	e1, ev := r.append("stdout", 1, logRingChunk(8192, 'x'), 1)
	if len(ev) != 0 {
		t.Fatalf("8KiB append evicted %d entries, want 0", len(ev))
	}
	if r.rawBytes != 8192 {
		t.Fatalf("rawBytes = %d, want 8192", r.rawBytes)
	}

	e2, ev := r.append("stdout", 2, []byte("y"), 2)
	if len(ev) != 1 || ev[0] != e1 {
		t.Fatalf("overflow append evicted %v, want [e1]", ev)
	}
	if r.rawBytes != 1 {
		t.Fatalf("rawBytes = %d, want 1", r.rawBytes)
	}
	if want := e2.charge; r.chargedBytes != want {
		t.Fatalf("chargedBytes = %d, want %d", r.chargedBytes, want)
	}
}

func TestLogRingChargeIsCapPlusMetadataSurcharge(t *testing.T) {
	r := newLogRing(1 << 20)

	raw := make([]byte, 100) // the retained copy rounds up to a larger cap
	e, _ := r.append("stdout", 1, raw, 1)

	if len(e.raw) != 100 {
		t.Fatalf("retained len = %d, want 100", len(e.raw))
	}
	if cap(e.raw) == len(e.raw) {
		t.Fatal("test precondition failed: retained copy cap equals len")
	}
	want := cap(e.raw) + logEntryCharge
	if e.charge != want {
		t.Fatalf("charge = %d, want cap(%d)+%d = %d", e.charge, cap(e.raw), logEntryCharge, want)
	}
	if r.chargedBytes != want {
		t.Fatalf("chargedBytes = %d, want %d", r.chargedBytes, want)
	}
}

// TestChunkChargeSaturatesWithoutWrapping covers the checked cap+256 charge at
// its boundaries: the exact threshold stays exact and anything above it clamps
// to MaxInt instead of wrapping negative.
func TestChunkChargeSaturatesWithoutWrapping(t *testing.T) {
	if got := chunkCharge(0); got != logEntryCharge {
		t.Fatalf("chunkCharge(0) = %d, want %d", got, logEntryCharge)
	}
	if got := chunkCharge(math.MaxInt - logEntryCharge); got != math.MaxInt {
		t.Fatalf("chunkCharge(MaxInt-%d) = %d, want MaxInt", logEntryCharge, got)
	}
	for _, capacity := range []int{math.MaxInt - logEntryCharge + 1, math.MaxInt} {
		got := chunkCharge(capacity)
		if got != math.MaxInt {
			t.Fatalf("chunkCharge(%d) = %d, want saturation at MaxInt", capacity, got)
		}
	}
}

func TestLogRingRemoveUpdatesAccountingExactlyOnce(t *testing.T) {
	r := newLogRing(1 << 20)

	e1, _ := r.append("stdout", 1, logRingChunk(100, 'a'), 1)
	e2, _ := r.append("stdout", 2, logRingChunk(20, 'b'), 2)
	e3, _ := r.append("stderr", 3, logRingChunk(30, 'c'), 3)

	rawBefore, chargedBefore := r.rawBytes, r.chargedBytes
	if !r.remove(e2) {
		t.Fatal("remove(e2) = false, want true")
	}
	if r.rawBytes != rawBefore-len(e2.raw) {
		t.Fatalf("rawBytes = %d, want %d", r.rawBytes, rawBefore-len(e2.raw))
	}
	if r.chargedBytes != chargedBefore-e2.charge {
		t.Fatalf("chargedBytes = %d, want %d", r.chargedBytes, chargedBefore-e2.charge)
	}

	if r.remove(e2) {
		t.Fatal("second remove(e2) = true, want false")
	}
	if r.rawBytes != rawBefore-len(e2.raw) || r.chargedBytes != chargedBefore-e2.charge {
		t.Fatal("second remove changed accounting")
	}

	got := r.query(LogQuery{})
	if want := []int64{e1.sequence, e3.sequence}; !logRingSequencesEqual(got, want) {
		t.Fatalf("query sequences = %v, want %v", logRingSequences(got), want)
	}
}

func TestLogRingQueryFiltersSinceStreamAndTail(t *testing.T) {
	r := newLogRing(1 << 20)
	streams := []string{"stdout", "stderr", "stdout", "stderr", "stdout"}
	for i, stream := range streams {
		r.append(stream, int64(i+1), []byte{byte('a' + i)}, int64(i+1))
	}

	tests := []struct {
		name   string
		query  LogQuery
		stream string
		want   []int64
	}{
		{name: "combined default", query: LogQuery{}, want: []int64{1, 2, 3, 4, 5}},
		{name: "combined explicit", query: LogQuery{Stream: "combined"}, want: []int64{1, 2, 3, 4, 5}},
		{name: "exclusive since", query: LogQuery{Since: 2}, want: []int64{3, 4, 5}},
		{name: "stdout", query: LogQuery{Stream: "stdout"}, want: []int64{1, 3, 5}},
		{name: "stderr with since", query: LogQuery{Stream: "stderr", Since: 1}, want: []int64{2, 4}},
		{name: "tail", query: LogQuery{Tail: intPtr(2)}, want: []int64{4, 5}},
		{name: "tail zero", query: LogQuery{Tail: intPtr(0)}, want: []int64{}},
		{name: "tail larger than available", query: LogQuery{Tail: intPtr(99)}, want: []int64{1, 2, 3, 4, 5}},
		{name: "tail with stream and since", query: LogQuery{Stream: "stdout", Since: 1, Tail: intPtr(1)}, want: []int64{5}},
		{name: "no matches", query: LogQuery{Stream: "bogus"}, want: []int64{}},
		{name: "since beyond", query: LogQuery{Since: 5}, want: []int64{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.query(tt.query)
			if seqs := logRingSequences(got); !logRingSeqEqual(seqs, tt.want) {
				t.Fatalf("sequences = %v, want %v", seqs, tt.want)
			}
			for i, e := range got {
				if e.Encoding != "base64" {
					t.Fatalf("entry %d encoding = %q, want base64", i, e.Encoding)
				}
				if _, err := base64.StdEncoding.DecodeString(e.Data); err != nil {
					t.Fatalf("entry %d data is not standard padded base64: %v", i, err)
				}
			}
		})
	}
}

func TestLogRingQueryDoesNotMutateRing(t *testing.T) {
	r := newLogRing(1 << 20)

	src := []byte("payload")
	_, _ = r.append("stdout", 1, src, 1)
	rawBytes, chargedBytes := r.rawBytes, r.chargedBytes

	got := r.query(LogQuery{})
	if len(got) != 1 {
		t.Fatalf("query returned %d entries, want 1", len(got))
	}
	got[0].Data = "tampered"
	got[0].Sequence = 999
	got[0].Stream = "stderr"

	again := r.query(LogQuery{})
	if len(again) != 1 {
		t.Fatalf("second query returned %d entries, want 1", len(again))
	}
	if again[0].Sequence != 1 || again[0].Stream != "stdout" {
		t.Fatalf("ring mutated through DTO: %+v", again[0])
	}
	if want := base64.StdEncoding.EncodeToString(src); again[0].Data != want {
		t.Fatalf("ring data mutated: got %q, want %q", again[0].Data, want)
	}
	if r.rawBytes != rawBytes || r.chargedBytes != chargedBytes {
		t.Fatal("query changed ring accounting")
	}
}

func logRingSequences(entries []LogEntry) []int64 {
	seqs := make([]int64, len(entries))
	for i, e := range entries {
		seqs[i] = e.Sequence
	}
	return seqs
}

func logRingSeqEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func logRingSequencesEqual(entries []LogEntry, want []int64) bool {
	return logRingSeqEqual(logRingSequences(entries), want)
}

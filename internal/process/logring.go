package process

import (
	"encoding/base64"
	"math"
)

// logEntryCharge is the fixed conservative surcharge added to a raw chunk's
// capacity when charging retained log memory. It covers the entry, list, and
// index metadata that the chunk's backing array does not.
const logEntryCharge = 256

// chunkCharge returns capacity+logEntryCharge, saturating at math.MaxInt so a
// pathological capacity cannot wrap the per-entry charge.
func chunkCharge(capacity int) int {
	if capacity > math.MaxInt-logEntryCharge {
		return math.MaxInt
	}
	return capacity + logEntryCharge
}

// logEntry is one retained bridge-observed stdout/stderr chunk. It stores only
// a private copy of the raw bytes plus a private manager-assigned
// globalSequence; neither is serialized. The public base64 DTO is built on
// demand by logRing.query.
type logEntry struct {
	ring           *logRing
	sequence       int64
	globalSequence int64
	stream         string
	timestampMs    int64
	raw            []byte
	charge         int
}

// logRing is a per-process bounded FIFO of raw log chunks. It is not safe for
// concurrent use; the manager serializes every ring operation under its log
// lock so stream merge order is lock-acquisition order. A ring captures its
// creation-time byte cap, so later configuration changes do not affect it.
type logRing struct {
	maxRawBytes  int
	entries      []*logEntry
	rawBytes     int
	chargedBytes int
	nextSequence int64
}

// newLogRing returns an empty ring bounded to maxRawBytes of retained raw
// chunk bytes. maxRawBytes comes from a validated, positive configuration.
func newLogRing(maxRawBytes int) *logRing {
	return &logRing{maxRawBytes: maxRawBytes}
}

// append copies raw into the ring in call order, assigns the next monotonic
// per-process sequence (starting at 1), records the caller-supplied
// manager-global sequence, charges cap(raw)+logEntryCharge, and evicts whole
// oldest entries until the retained raw length is within maxRawBytes. It
// returns the inserted entry and the per-process entries it evicted so the
// manager can drop them from its global FIFO. Only len(raw) counts against the
// per-process cap; capacity is charged conservatively.
func (r *logRing) append(stream string, timestampMs int64, raw []byte, globalSequence int64) (*logEntry, []*logEntry) {
	chunk := append([]byte(nil), raw...)
	r.nextSequence++
	e := &logEntry{
		ring:           r,
		sequence:       r.nextSequence,
		globalSequence: globalSequence,
		stream:         stream,
		timestampMs:    timestampMs,
		raw:            chunk,
		charge:         chunkCharge(cap(chunk)),
	}
	r.entries = append(r.entries, e)
	r.rawBytes += len(chunk)
	r.chargedBytes = chargeAdd(r.chargedBytes, e.charge)

	var evicted []*logEntry
	for r.rawBytes > r.maxRawBytes && len(r.entries) > 0 {
		evicted = append(evicted, r.evictOldest())
	}
	return e, evicted
}

// remove drops e if the ring still retains it, subtracting its raw length and
// charge exactly once and preserving the order of the remaining entries. It
// reports whether e was found. The manager calls it when aggregate memory
// pressure evicts e from the global FIFO.
func (r *logRing) remove(e *logEntry) bool {
	for i, cur := range r.entries {
		if cur != e {
			continue
		}
		copy(r.entries[i:], r.entries[i+1:])
		r.entries[len(r.entries)-1] = nil
		r.entries = r.entries[:len(r.entries)-1]
		r.rawBytes -= len(e.raw)
		r.chargedBytes -= e.charge
		return true
	}
	return false
}

// selectEntries returns the retained entries matching q in order. It applies
// the exclusive since bound, then the stream filter (stdout|stderr|combined,
// with empty meaning combined), then tail by entry count. A nil Tail returns
// every match and a Tail of zero returns none. The returned pointers alias ring
// state and are only safe while the manager log lock is held.
func (r *logRing) selectEntries(q LogQuery) []*logEntry {
	combined := q.Stream == "" || q.Stream == "combined"
	matched := make([]*logEntry, 0, len(r.entries))
	for _, e := range r.entries {
		if e.sequence <= q.Since {
			continue
		}
		if !combined && e.stream != q.Stream {
			continue
		}
		matched = append(matched, e)
	}
	if q.Tail != nil {
		n := *q.Tail
		if n <= 0 {
			matched = matched[:0]
		} else if n < len(matched) {
			matched = matched[len(matched)-n:]
		}
	}
	return matched
}

// query returns independent public DTOs for the entries matching q in retained
// order. Raw bytes are base64-encoded only into the copies, so the ring is
// never mutated. Callers that must not encode under the manager log lock use
// selectEntries and encode after unlocking.
func (r *logRing) query(q LogQuery) []LogEntry {
	matched := r.selectEntries(q)
	out := make([]LogEntry, 0, len(matched))
	for _, e := range matched {
		out = append(out, LogEntry{
			Sequence:    e.sequence,
			Stream:      e.stream,
			TimestampMs: e.timestampMs,
			Data:        base64.StdEncoding.EncodeToString(e.raw),
			Encoding:    "base64",
		})
	}
	return out
}

// evictOldest drops the head entry and returns it. The caller owns the returned
// entry; the ring's backing slot is cleared so no evicted chunk stays reachable.
func (r *logRing) evictOldest() *logEntry {
	e := r.entries[0]
	r.entries[0] = nil
	r.entries = r.entries[1:]
	r.rawBytes -= len(e.raw)
	r.chargedBytes -= e.charge
	return e
}

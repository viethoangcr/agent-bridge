package httpapi

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// sseHeartbeatInterval is the documented production SSE keep-alive cadence. It
// is the only HTTP-owned timer; tests inject a fake ticker instead.
const sseHeartbeatInterval = 15 * time.Second

// heartbeatTicker is the injectable SSE keep-alive source.
type heartbeatTicker interface {
	C() <-chan time.Time
	Stop()
}

// realHeartbeatTicker is the production time.Ticker adapter.
type realHeartbeatTicker struct{ ticker *time.Ticker }

func (t realHeartbeatTicker) C() <-chan time.Time { return t.ticker.C }
func (t realHeartbeatTicker) Stop()               { t.ticker.Stop() }

// newHeartbeatTicker returns the production 15-second SSE keep-alive ticker.
// Server.newHeartbeatTicker binds it and tests replace it with a fake.
func newHeartbeatTicker() heartbeatTicker {
	return realHeartbeatTicker{ticker: time.NewTicker(sseHeartbeatInterval)}
}

// errInvalidLastEventID marks a malformed Last-Event-ID header.
var errInvalidLastEventID = errors.New("invalid Last-Event-ID")

// handleACPSSE negotiates Accept, validates Last-Event-ID, subscribes before
// the first durable query, and frames the subscription as Server-Sent Events.
// It ends on request cancellation, subscription closure, or write failure.
func (s *Server) handleACPSSE(w http.ResponseWriter, r *http.Request) {
	serverID, ok := s.acpServerID(w, r)
	if !ok {
		return
	}
	if !acceptsEventStream(w, r) {
		return
	}
	after, err := parseLastEventID(r.Header)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid Last-Event-ID")
		return
	}
	// A nil dependency is a wiring error, not a request error; answer as an
	// unavailable service rather than dereferencing the interface.
	if s.deps.ACP == nil {
		writeProblem(w, http.StatusServiceUnavailable, "ACP proxy is unavailable")
		return
	}
	// Subscribe before writing any header so an unknown server is a plain 404
	// and never a half-open stream.
	sub, err := s.deps.ACP.Subscribe(r.Context(), serverID, after)
	if err != nil {
		s.writeACPError(w, serverID, err)
		return
	}
	defer sub.Close()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush() // deliver headers before the first event heartbeat

	ticker := s.newHeartbeatTicker()
	defer ticker.Stop()

	// Next is driven from one producer goroutine so the loop can keep writing
	// heartbeats while no event is ready; only the loop writes to the stream.
	type nextResult struct {
		event acpstore.Event
		err   error
	}
	results := make(chan nextResult, 1)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			event, err := sub.Next(r.Context())
			select {
			case results <- nextResult{event: event, err: err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case result := <-results:
			if result.err != nil {
				return
			}
			if !writeSSEEvent(w, result.event) {
				return
			}
			flusher.Flush()
		case <-ticker.C():
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// parseLastEventID strictly parses the optional Last-Event-ID header. Absence
// is zero; a single nonnegative decimal int64 through math.MaxInt64 is
// accepted; blank, repeated, signed, non-decimal, and overflowing values are
// rejected. It never converts through a wider or architecture-sized type.
func parseLastEventID(header http.Header) (int64, error) {
	values := header.Values("Last-Event-ID")
	if len(values) == 0 {
		return 0, nil
	}
	if len(values) != 1 || values[0] == "" {
		return 0, errInvalidLastEventID
	}
	after, err := parseNonnegativeInt64(values[0])
	if err != nil {
		return 0, errInvalidLastEventID
	}
	return after, nil
}

// writeSSEEvent frames one event as `event: message`, a decimal int64 `id`,
// and the exact stored payload bytes as `data`, terminated by a blank line. It
// reports false when the underlying write fails so the handler can stop.
func writeSSEEvent(w io.Writer, event acpstore.Event) bool {
	var frame bytes.Buffer
	frame.Grow(len("event: message\nid: \ndata: \n\n") + len(event.Payload) + 20)
	frame.WriteString("event: message\nid: ")
	frame.WriteString(strconv.FormatInt(event.Seq, 10))
	frame.WriteString("\ndata: ")
	frame.Write(event.Payload)
	frame.WriteString("\n\n")
	_, err := w.Write(frame.Bytes())
	return err == nil
}

// acceptsEventStream accepts a missing/empty Accept header or any value whose
// media range allows text/event-stream.
func acceptsEventStream(w http.ResponseWriter, r *http.Request) bool {
	values := r.Header.Values("Accept")
	sawRange := false
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			sawRange = true
			mediaType, acceptable := parseAcceptRange(part)
			if !acceptable {
				continue
			}
			switch mediaType {
			case "text/event-stream", "text/*", "*/*":
				return true
			}
		}
	}
	if !sawRange {
		return true
	}
	writeProblem(w, http.StatusNotAcceptable, "Accept must allow text/event-stream")
	return false
}

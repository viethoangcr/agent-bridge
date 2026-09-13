//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testMockSSE covers subscribe-before-watermark replay, live continuity without
// gaps, heartbeat framing, reconnect lag catch-up, and closure on DELETE.
func testMockSSE(t *testing.T, image string) {
	c := startMock(t, image, uniqueToken("sse"), nil)
	initialize(t, c, "sse", "mock")
	sessionID, _ := sessionNewResult(t, c, "sse", mockCWD)

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	stream, err := c.subscribe(ctx, "sse", "0")
	if err != nil {
		t.Fatalf("subscribe: %v\n%s", err, c.diagnostics())
	}
	defer stream.close()

	var ids []int64
	readIDs := func(want int) {
		t.Helper()
		for len(ids) < want {
			event, err := stream.nextMessage(c)
			if err != nil {
				t.Fatalf("read sse event %d: %v\n%s", len(ids)+1, err, c.diagnostics())
			}
			if !json.Valid(event.Data) {
				t.Fatalf("sse data %d is not raw JSON: %q", event.ID, event.Data)
			}
			ids = append(ids, event.ID)
		}
	}

	// Replay from Last-Event-ID: 0 delivers the initialize response first.
	readIDs(1)

	// Live events produced after subscription must continue with no gap.
	if code, body := prompt(t, c, "sse", sessionID, "hello"); code != http.StatusOK {
		t.Fatalf("prompt = %d: %s", code, body)
	}
	readIDs(5)

	for i, id := range ids {
		if id != int64(i+1) {
			t.Fatalf("SSE ids not strict monotonic from 1: %v", ids)
		}
	}

	// Heartbeat framing is the `: heartbeat` comment line.
	comment, err := stream.waitComment(20 * time.Second)
	if err != nil {
		t.Fatalf("heartbeat: %v\n%s", err, c.diagnostics())
	}
	if !strings.Contains(comment.Text, "heartbeat") {
		t.Fatalf("comment frame = %q, want heartbeat", comment.Text)
	}

	// Reconnect and lag catch-up: events created while disconnected are
	// replayed exactly once after the prior watermark.
	stream.close()
	last := ids[len(ids)-1]
	if code, body := prompt(t, c, "sse", sessionID, "lag"); code != http.StatusOK {
		t.Fatalf("lag prompt = %d: %s", code, body)
	}
	reconnected, err := c.subscribe(ctx, "sse", strconv.FormatInt(last, 10))
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer reconnected.close()
	for i := 0; i < 3; i++ {
		event, err := reconnected.nextMessage(c)
		if err != nil {
			t.Fatalf("lag read %d: %v", i, err)
		}
		want := last + int64(i) + 1
		if event.ID != want {
			t.Fatalf("lag event id = %d, want %d", event.ID, want)
		}
	}

	// DELETE closes the live SSE stream.
	initialize(t, c, "sse-del", "mock")
	delStream, err := c.subscribe(ctx, "sse-del", "0")
	if err != nil {
		t.Fatalf("delete subscribe: %v", err)
	}
	defer delStream.close()
	if _, err := delStream.nextMessage(c); err != nil {
		t.Fatalf("delete replay: %v", err)
	}
	closed := make(chan error, 1)
	go func() {
		// Read until the hard close terminates the stream.
		for {
			event, err := delStream.next()
			if err != nil {
				closed <- err
				return
			}
			_ = event
		}
	}()
	if resp, body := c.request(http.MethodDelete, "/v1/acp/sse-del", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE = %d: %s", resp.StatusCode, body)
	}
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("SSE stream did not close on DELETE")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("SSE stream did not close on DELETE\n%s", c.diagnostics())
	}
}

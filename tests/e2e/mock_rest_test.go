//go:build e2e

package e2e

import (
	"net/http"
	"testing"
)

// testMockREST covers status/event endpoints, sorted server/session lists,
// event filtering/pagination/order, unknown session 404, and DELETE pruning.
func testMockREST(t *testing.T, image string) {
	c := startMock(t, image, uniqueToken("rest"), nil)
	initialize(t, c, "zeta", "mock")
	initialize(t, c, "alpha", "mock")

	var list serverListView
	if code, body := getJSON(t, c, "/v1/acp", &list); code != http.StatusOK {
		t.Fatalf("list = %d: %s", code, body)
	}
	if len(list.Servers) != 2 || list.Servers[0].ServerID != "alpha" || list.Servers[1].ServerID != "zeta" {
		t.Fatalf("server list order = %+v, want alpha then zeta", list.Servers)
	}

	s1, _ := sessionNewResult(t, c, "alpha", "/s1")
	s2, _ := sessionNewResult(t, c, "alpha", "/s2")
	view, code := status(t, c, "alpha")
	if code != http.StatusOK {
		t.Fatalf("alpha status = %d", code)
	}
	if len(view.SessionIDs) != 2 || view.SessionIDs[0] != s1 || view.SessionIDs[1] != s2 {
		t.Fatalf("sessionIds = %v, want [%s %s] sorted", view.SessionIDs, s1, s2)
	}

	// Attribute events to s1 only.
	if code, body := prompt(t, c, "alpha", s1, "hello"); code != http.StatusOK {
		t.Fatalf("prompt = %d: %s", code, body)
	}
	filtered, code := events(t, c, "alpha", "sessionId="+s1)
	if code != http.StatusOK || len(filtered) != 4 {
		t.Fatalf("session-filtered events = %d, want 4", len(filtered))
	}
	for _, event := range filtered {
		if event.SessionID == nil || *event.SessionID != s1 {
			t.Fatalf("filtered event has session %v, want %s", event.SessionID, s1)
		}
	}

	// Pagination and ordering.
	one, code := events(t, c, "alpha", "limit=1")
	if code != http.StatusOK || len(one) != 1 {
		t.Fatalf("limit=1 events = %d, want 1", len(one))
	}
	after, code := events(t, c, "alpha", "after=1")
	if code != http.StatusOK {
		t.Fatalf("after=1 = %d", code)
	}
	for _, event := range after {
		if event.Seq <= 1 {
			t.Fatalf("after=1 returned seq %d", event.Seq)
		}
	}
	desc, code := events(t, c, "alpha", "order=desc")
	if code != http.StatusOK || len(desc) == 0 {
		t.Fatalf("order=desc = %d", code)
	}
	for i := 1; i < len(desc); i++ {
		if desc[i].Seq >= desc[i-1].Seq {
			t.Fatalf("desc order not strict: %v", desc)
		}
	}

	// Unknown session and unknown server are 404.
	if _, code := events(t, c, "alpha", "sessionId=nope"); code != http.StatusNotFound {
		t.Fatalf("unknown session events = %d, want 404", code)
	}
	if _, code := events(t, c, "ghost", ""); code != http.StatusNotFound {
		t.Fatalf("unknown server events = %d, want 404", code)
	}

	// DELETE prunes durable state.
	if resp, body := c.request(http.MethodDelete, "/v1/acp/alpha", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE = %d: %s", resp.StatusCode, body)
	}
	if _, code := status(t, c, "alpha"); code != http.StatusNotFound {
		t.Fatalf("status after DELETE = %d, want 404", code)
	}
	if _, code := events(t, c, "alpha", ""); code != http.StatusNotFound {
		t.Fatalf("events after DELETE = %d, want 404", code)
	}
	if code, body := getJSON(t, c, "/v1/acp", &list); code != http.StatusOK {
		t.Fatalf("list after DELETE = %d: %s", code, body)
	}
	for _, server := range list.Servers {
		if server.ServerID == "alpha" {
			t.Fatalf("alpha still listed after DELETE: %+v", list.Servers)
		}
	}
}

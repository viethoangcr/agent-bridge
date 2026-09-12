package mockagent

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestPromptNotifiesBeforeResponse asserts prompt updates precede the matching
// response and that string/numeric raw ID tokens round-trip unchanged.
func TestPromptNotifiesBeforeResponse(t *testing.T) {
	stringID := `"a\"b"`
	input := strings.Join([]string{
		initLine(t, "1"),
		newSessionLine(t, "2", "/work"),
		requestLine(t, "1e0", "session/prompt", map[string]any{
			"sessionId": "mock-session-1",
			"prompt":    []any{map[string]any{"type": "text", "text": "hello"}},
		}),
		requestLine(t, stringID, "session/prompt", map[string]any{
			"sessionId": "mock-session-1",
			"prompt":    []any{map[string]any{"type": "text", "text": "again"}},
		}),
	}, "\n") + "\n"

	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 8 {
		t.Fatalf("got %d messages, want 8:\n%s", len(msgs), out)
	}
	for _, i := range []int{2, 3, 5, 6} {
		if msgs[i].Method != "session/update" {
			t.Fatalf("message %d method = %q, want session/update", i, msgs[i].Method)
		}
	}
	if msgs[4].Method != "" || string(msgs[4].ID) != "1e0" {
		t.Fatalf("first prompt response = method %q id %s, want id 1e0", msgs[4].Method, msgs[4].ID)
	}
	if msgs[7].Method != "" || string(msgs[7].ID) != stringID {
		t.Fatalf("second prompt response = method %q id %s, want id %s", msgs[7].Method, msgs[7].ID, stringID)
	}
	for _, i := range []int{4, 7} {
		var result struct {
			StopReason string `json:"stopReason"`
		}
		if err := json.Unmarshal(msgs[i].Result, &result); err != nil {
			t.Fatalf("prompt result %s: %v", msgs[i].Result, err)
		}
		if result.StopReason != "end_turn" {
			t.Fatalf("stopReason = %q, want end_turn", result.StopReason)
		}
	}
}

// TestPermissionReverseCall covers the permission flow: a designated trigger
// emits a reverse-call, the matching client response unblocks the withheld
// prompt response, and updates finish before that response.
func TestPermissionReverseCall(t *testing.T) {
	input := strings.Join([]string{
		initLine(t, "1"),
		newSessionLine(t, "2", "/work"),
		requestLine(t, "10", "session/prompt", map[string]any{
			"sessionId": "mock-session-1",
			"prompt":    []any{map[string]any{"type": "text", "text": "run " + permissionTrigger}},
		}),
		responseLine(t, `"mock-permission-1"`, map[string]any{"outcome": "selected"}),
	}, "\n") + "\n"

	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 6 {
		t.Fatalf("got %d messages, want 6:\n%s", len(msgs), out)
	}
	if msgs[2].Method != "session/update" {
		t.Fatalf("message 2 method = %q, want update before reverse-call", msgs[2].Method)
	}
	if msgs[3].Method != "session/request_permission" {
		t.Fatalf("message 3 method = %q, want request_permission", msgs[3].Method)
	}
	if string(msgs[3].ID) != `"mock-permission-1"` {
		t.Fatalf("reverse-call id = %s, want \"mock-permission-1\"", msgs[3].ID)
	}
	if msgs[4].Method != "session/update" {
		t.Fatalf("message 4 method = %q, want post-permission update", msgs[4].Method)
	}
	if string(msgs[5].ID) != "10" || msgs[5].Method != "" {
		t.Fatalf("final message = method %q id %s, want prompt response id 10", msgs[5].Method, msgs[5].ID)
	}
}

// TestPermissionMismatchedIDDoesNotComplete proves a non-matching id cannot
// complete the reverse-call; meeting EOF while pending exits cleanly with no
// prompt response.
func TestPermissionMismatchedIDDoesNotComplete(t *testing.T) {
	input := strings.Join([]string{
		initLine(t, "1"),
		newSessionLine(t, "2", "/work"),
		requestLine(t, "10", "session/prompt", map[string]any{
			"sessionId": "mock-session-1",
			"prompt":    []any{map[string]any{"type": "text", "text": "run " + permissionTrigger}},
		}),
		responseLine(t, `"wrong"`, map[string]any{"outcome": "selected"}),
	}, "\n") + "\n"

	out, _, err := run(t, input)
	if err != nil {
		t.Fatalf("Run() = %v, want nil on EOF while pending", err)
	}
	msgs := parseOutput(t, out)
	if len(msgs) != 4 {
		t.Fatalf("got %d messages, want 4 (init, new, update, reverse-call):\n%s", len(msgs), out)
	}
	for _, m := range msgs {
		if string(m.ID) == "10" {
			t.Fatalf("prompt completed despite mismatched response id:\n%s", out)
		}
	}
}

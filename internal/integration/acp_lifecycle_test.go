package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestACPLifecycle verifies the end-to-end ACP lifecycle through the app.
func TestACPLifecycle(t *testing.T) {
	t.Run("session-prompt-attribution-and-events", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()

		b.initialize("life", "mock")
		sessionID := b.sessionNew("life", "/work")
		code, body := b.prompt("life", sessionID, "hello")
		if code != http.StatusOK {
			t.Fatalf("session/prompt = %d: %s", code, body)
		}

		events, code := b.events("life", "sessionId="+url.QueryEscape(sessionID))
		if code != http.StatusOK {
			t.Fatalf("events = %d", code)
		}
		if len(events) < 3 {
			t.Fatalf("session events = %d, want at least the two updates and the response", len(events))
		}
		var sawResponse bool
		var sawUpdate int
		for _, event := range events {
			if event.SessionID == nil || *event.SessionID != sessionID {
				t.Errorf("event seq %d sessionId = %v, want %q", event.Seq, event.SessionID, sessionID)
			}
			switch event.Kind {
			case "response":
				sawResponse = true
				var envelope struct {
					ID json.RawMessage `json:"id"`
				}
				if err := json.Unmarshal(event.Payload, &envelope); err == nil && string(envelope.ID) == `"prompt"` {
					if !bytes.Equal(event.Payload, body) {
						t.Errorf("response event payload = %s, want exact HTTP response %s", event.Payload, body)
					}
				}
			case "notification":
				if event.Method != nil && *event.Method == "session/update" {
					sawUpdate++
				}
			}
		}
		if !sawResponse {
			t.Error("no response event carried the retained request sessionId")
		}
		if sawUpdate != 2 {
			t.Errorf("session/update notifications = %d, want 2", sawUpdate)
		}

		status, code := b.status("life")
		if code != http.StatusOK || status.Status != string(acpstore.StatusIdle) {
			t.Fatalf("status = %d %+v, want idle", code, status)
		}
		if status.PID == nil {
			t.Error("live status omitted the current PID")
		}
		if len(status.SessionIDs) != 1 || status.SessionIDs[0] != sessionID {
			t.Errorf("status sessionIds = %v, want [%s]", status.SessionIDs, sessionID)
		}
	})

	t.Run("raw-compaction-and-exact-response", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()

		// The multi-line object is not one JSONL record; only Runtime.Post's
		// whitespace-only compaction lets the mock answer it.
		pretty := "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": \"init\",\n  \"method\": \"initialize\",\n  \"params\": {\"protocolVersion\": 1, \"clientCapabilities\": {}}\n}"
		code, body := b.postEnvelope("compact", "mock", pretty)
		want := `{"jsonrpc":"2.0","id":"init","result":{"agentCapabilities":{},"authMethods":[],"protocolVersion":1}}`
		if code != http.StatusOK || string(body) != want {
			t.Fatalf("compacted response = %d %q, want 200 %q", code, body, want)
		}
	})

	t.Run("reverse-call-and-notification", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()

		b.initialize("call", "mock")
		sessionID := b.sessionNew("call", "/call")

		type result struct {
			code int
			body []byte
		}
		done := make(chan result, 1)
		go func() {
			code, body := b.prompt("call", sessionID, "[mock:request_permission]")
			done <- result{code: code, body: body}
		}()

		request := b.waitEventMethod("call", "session/request_permission", 10*time.Second)
		var envelope struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(request.Payload, &envelope); err != nil || len(envelope.ID) == 0 {
			t.Fatalf("permission request payload = %s (%v)", request.Payload, err)
		}
		clientResponse := `{"jsonrpc":"2.0","id":` + string(envelope.ID) + `,"result":{"outcome":{"outcome":"selected","optionId":"allow"}}}`
		if code, body := b.postEnvelope("call", "", clientResponse); code != http.StatusAccepted {
			t.Fatalf("client response = %d: %s", code, body)
		}
		if code, body := b.postEnvelope("call", "", `{"jsonrpc":"2.0","method":"notifications/initialized"}`); code != http.StatusAccepted {
			t.Fatalf("notification = %d: %s", code, body)
		}

		select {
		case got := <-done:
			if got.code != http.StatusOK {
				t.Fatalf("reverse-call prompt = %d: %s", got.code, got.body)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("prompt did not complete after the client response")
		}
	})

	t.Run("duplicate-numeric-ids", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()
		b.initialize("dup", "mock")

		go func() {
			_, _ = b.postEnvelope("dup", "", `{"jsonrpc":"2.0","id":1,"method":"_mock/delay","params":{"ms":3000}}`)
		}()
		b.waitFor(5*time.Second, "busy after numeric request", func() bool {
			status, code := b.status("dup")
			return code == http.StatusOK && status.Status == string(acpstore.StatusBusy)
		})
		code, body := b.postEnvelope("dup", "", `{"jsonrpc":"2.0","id":1.0,"method":"_mock/delay","params":{"ms":3000}}`)
		if code != http.StatusConflict {
			t.Fatalf("lexically duplicate numeric ID = %d: %s", code, body)
		}
	})

	t.Run("invalid-metadata", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()

		longID := strings.Repeat("a", 129)
		longSession := strings.Repeat("s", 1025)
		cases := []struct {
			name string
			body string
		}{
			{"null-id", `{"jsonrpc":"2.0","id":null,"method":"initialize","params":{"protocolVersion":1}}`},
			{"over-limit-id", `{"jsonrpc":"2.0","id":"` + longID + `","method":"initialize","params":{"protocolVersion":1}}`},
			{"over-limit-session", `{"jsonrpc":"2.0","id":"p","method":"session/prompt","params":{"sessionId":"` + longSession + `","prompt":[]}}`},
		}
		for _, tc := range cases {
			code, body := b.postEnvelope("meta", "", tc.body)
			if code != http.StatusBadRequest {
				t.Errorf("%s = %d: %s, want 400", tc.name, code, body)
			}
		}
	})

	t.Run("correlation-capacity", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()
		b.initialize("corr", "mock")

		const total = 300
		results := make(chan int, total)
		for i := 0; i < total; i++ {
			go func(i int) {
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"_mock/delay","params":{"ms":30000}}`, i)
				code, _ := b.postEnvelope("corr", "", body)
				results <- code
			}(i)
		}
		overflow, rejected := 0, 0
		deadline := time.After(20 * time.Second)
		for overflow < total-256 && rejected < total-256 {
			select {
			case code := <-results:
				switch code {
				case http.StatusTooManyRequests:
					overflow++
				case http.StatusOK:
					// Still blocked; the response only arrives after cleanup.
				default:
					t.Errorf("unexpected correlation result %d", code)
				}
			case <-deadline:
				t.Fatalf("saw %d capacity rejections, want %d", overflow, total-256)
			}
		}
	})

	t.Run("runtime-capacity", func(t *testing.T) {
		if testing.Short() {
			t.Skip("spawns 64 real mock runtimes")
		}
		b := startBridge(t, bridgeOptions{})
		defer b.stop()

		for i := 0; i < 64; i++ {
			serverID := fmt.Sprintf("cap-%02d", i)
			code, body := b.postEnvelope(serverID, "mock", `{"jsonrpc":"2.0","id":"d","method":"_mock/delay","params":{"ms":0}}`)
			if code != http.StatusOK {
				t.Fatalf("runtime %d = %d: %s", i, code, body)
			}
		}
		code, body := b.postEnvelope("cap-over", "mock", `{"jsonrpc":"2.0","id":"d","method":"_mock/delay","params":{"ms":0}}`)
		if code != http.StatusTooManyRequests {
			t.Fatalf("65th runtime = %d: %s, want 429", code, body)
		}
	})

	t.Run("timeout-grace-busy-late-event", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{env: map[string]string{"AGENT_BRIDGE_ACP_REQUEST_TIMEOUT_MS": "150"}})
		defer b.stop()
		b.initialize("slow", "mock")

		// Block the single mock loop so the lifecycle request cannot be read
		// until its request deadline has already moved it into grace.
		go func() {
			_, _ = b.postEnvelope("slow", "", `{"jsonrpc":"2.0","id":"delay","method":"_mock/delay","params":{"ms":900}}`)
		}()
		b.waitFor(5*time.Second, "mock loop busy", func() bool {
			status, code := b.status("slow")
			return code == http.StatusOK && status.Status == string(acpstore.StatusBusy)
		})

		code, body := b.postEnvelope("slow", "", `{"jsonrpc":"2.0","id":"new","method":"session/new","params":{"cwd":"/late"}}`)
		if code != http.StatusGatewayTimeout {
			t.Fatalf("lifecycle timeout = %d: %s, want 504", code, body)
		}
		status, code := b.status("slow")
		if code != http.StatusOK || status.Status != string(acpstore.StatusBusy) {
			t.Fatalf("status during grace = %d %+v, want durable busy", code, status)
		}

		// The mock's late response commits the request and becomes observable.
		var sessionID string
		b.waitFor(10*time.Second, "late session/new response", func() bool {
			events, _ := b.events("slow", "limit=1000")
			for _, event := range events {
				if event.Kind == "response" && event.SessionID != nil && *event.SessionID != "" {
					sessionID = *event.SessionID
					return true
				}
			}
			return false
		})
		if sessionID == "" {
			t.Fatal("late response did not carry a committed session")
		}
	})

	t.Run("invalid-stdout-synthetic", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()
		b.initialize("badout", "mock")

		code, body := b.postEnvelope("badout", "", `{"jsonrpc":"2.0","id":"bad","method":"_mock/invalid_stdout","params":{"line":"{not json"}}`)
		if code != http.StatusOK {
			t.Fatalf("invalid_stdout = %d: %s", code, body)
		}
		b.waitEventMethod("badout", "_adapter/invalid_stdout", 5*time.Second)
	})

	t.Run("process-exit-reinitialize", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()
		b.initialize("exit", "mock")

		if code, body := b.postEnvelope("exit", "", `{"jsonrpc":"2.0","id":"bye","method":"_mock/exit","params":{}}`); code != http.StatusOK {
			t.Fatalf("_mock/exit = %d: %s", code, body)
		}
		// Wait until the exited runtime is no longer the live generation.
		b.waitFor(10*time.Second, "exited server without a live PID", func() bool {
			status, code := b.status("exit")
			return code == http.StatusOK && status.Status == string(acpstore.StatusExited) && status.PID == nil
		})
		code, body := b.postEnvelope("exit", "", `{"jsonrpc":"2.0","id":"again","method":"_mock/delay","params":{"ms":0}}`)
		if code != http.StatusConflict {
			t.Fatalf("non-initialize after exit = %d: %s, want 409", code, body)
		}
		b.initialize("exit", "mock")
	})

	t.Run("list-status-events-sorted", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{})
		defer b.stop()
		b.initialize("zeta", "mock")
		b.initialize("alpha", "mock")

		var list listView
		if code := b.getJSON("/v1/acp", &list); code != http.StatusOK {
			t.Fatalf("list = %d", code)
		}
		if len(list.Servers) != 2 || list.Servers[0].ServerID != "alpha" || list.Servers[1].ServerID != "zeta" {
			t.Fatalf("list order = %+v, want alpha then zeta", list.Servers)
		}

		events, _ := b.events("alpha", "order=desc")
		for i := 1; i < len(events); i++ {
			if events[i].Seq >= events[i-1].Seq {
				t.Fatalf("desc events not strictly descending: %v", events)
			}
		}
	})

	t.Run("stderr-safe-502", func(t *testing.T) {
		b := startBridge(t, bridgeOptions{env: map[string]string{
			"AGENT_BRIDGE_CLAUDE_BIN":  "/bin/sh",
			"AGENT_BRIDGE_CLAUDE_ARGS": `["-c","printf 'password=hunter2\n' 1>&2; sleep 0.3; exit 1"]`,
		}})
		defer b.stop()

		code, body := b.postEnvelope("secret", "claude", `{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}`)
		if code != http.StatusBadGateway {
			t.Fatalf("failing agent = %d: %s, want 502", code, body)
		}
		if bytes.Contains(body, []byte("hunter2")) {
			t.Fatalf("502 leaked the secret: %s", body)
		}
		var problem struct {
			AgentStderr string `json:"agentStderr"`
		}
		if err := json.Unmarshal(body, &problem); err != nil {
			t.Fatalf("502 body = %s (%v)", body, err)
		}
		if problem.AgentStderr == "" {
			t.Fatalf("agentStderr omitted after a process had started: %s", body)
		}
		if !strings.Contains(problem.AgentStderr, "[REDACTED]") {
			t.Fatalf("agentStderr not redacted: %q", problem.AgentStderr)
		}
		if strings.Contains(problem.AgentStderr, "hunter2") {
			t.Fatalf("agentStderr leaked the secret: %q", problem.AgentStderr)
		}
	})
}

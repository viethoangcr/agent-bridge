// Package childenv builds sanitized environments for child processes. It
// removes bridge-only variables so bridge credentials and private dispatch
// state never leak into agent or managed subprocesses.
package childenv

import "strings"

// Sanitized returns a newly allocated copy of environ with every occurrence
// of the bridge-only variables removed. Keys are compared exactly against the
// text before the first '='; entries without '=' are retained. Order and all
// other entries, including credentials, are preserved, and the result shares
// no backing array with environ.
func Sanitized(environ []string) []string {
	sanitized := make([]string, 0, len(environ))
	for _, entry := range environ {
		key, _, _ := strings.Cut(entry, "=")
		if isBridgeOnly(key) {
			continue
		}
		sanitized = append(sanitized, entry)
	}
	return sanitized
}

// isBridgeOnly reports whether key is one of the variables that must never be
// inherited by a child process. AGENT_BRIDGE_TOKEN is a credential;
// AGENT_BRIDGE_PID_FILE and AGENT_BRIDGE_INTERNAL_MOCK_AGENT are private
// lifecycle state; AGENT_BRIDGE_ALLOW_INSECURE_REMOTE would re-enable the
// unsafe unauthenticated-remote override inside a child.
func isBridgeOnly(key string) bool {
	switch key {
	case "AGENT_BRIDGE_TOKEN",
		"AGENT_BRIDGE_PID_FILE",
		"AGENT_BRIDGE_INTERNAL_MOCK_AGENT",
		"AGENT_BRIDGE_ALLOW_INSECURE_REMOTE":
		return true
	default:
		return false
	}
}

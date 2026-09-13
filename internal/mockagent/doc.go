// Package mockagent implements the private, deterministic ACP JSONL mock
// agent. It is reachable only through the private
// AGENT_BRIDGE_INTERNAL_MOCK_AGENT=1 dispatch in internal/app and keeps all
// state in process memory.
//
// Private wire-level control hooks (undocumented, not a public contract):
//
//	_mock/delay           request {"ms":<int>}       sleeps, then replies {}
//	_mock/invalid_stdout  request {"line":"<raw>"}   writes one malformed stdout line, then replies {}
//	_mock/stderr          request {"line":"<raw>"}   writes one stderr line, then replies {}
//	_mock/exit            request                    replies {} then stops the loop cleanly
//
// A session/prompt whose prompt content contains the literal
// "[mock:request_permission]" emits a session/request_permission reverse-call
// and withholds the prompt response until a client response echoes the exact
// reverse-call id.
package mockagent

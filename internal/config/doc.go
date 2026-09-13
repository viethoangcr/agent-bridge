// Package config parses and validates the agent-bridge startup environment.
//
// Load is the only interpreter of public AGENT_BRIDGE_* variables. It applies
// the documented defaults, rejects malformed or out-of-range values, and
// enforces the loopback/token safety rule before the server starts.
package config

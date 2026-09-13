// Package filesystem provides safe path resolution, metadata, and mutation
// helpers for the bridge's /v1/fs endpoints. Paths resolve under a HOME captured
// once at construction; absolute paths are used directly after filepath.Clean.
//
// The service is deliberately lexical: it rejects raw relative parent
// components before any cleaning and never treats path checks as confinement.
// The sandbox itself is the security boundary.
package filesystem

// Package httpapi provides the HTTP routing, middleware, problem responses,
// and request-body utilities for the agent bridge server.
package httpapi

import (
	"encoding/json"
	"net/http"
)

const (
	detailNotFound     = "not found"
	detailBodyTooLarge = "request body too large"
)

// Problem is an RFC 9457 problem detail. Ext holds top-level extension
// members; reserved members (type, title, status, detail) always win.
type Problem struct {
	// Type is the RFC 9457 problem type URI.
	Type string `json:"type"`
	// Title is the short human-readable problem summary.
	Title string `json:"title"`
	// Status is both the problem status member and the HTTP response code.
	Status int `json:"status"`
	// Detail explains this occurrence of the problem.
	Detail string `json:"detail"`
	// Ext carries top-level extension members; reserved members always win.
	Ext map[string]any `json:"-"`
}

// reservedProblemMembers are the RFC 9457 members that extension keys must
// never replace.
var reservedProblemMembers = map[string]struct{}{
	"type":   {},
	"title":  {},
	"status": {},
	"detail": {},
}

// problemMap flattens p's extension members into top-level members while
// keeping the reserved RFC 9457 members authoritative.
func problemMap(p Problem) map[string]any {
	m := make(map[string]any, 4+len(p.Ext))
	m["type"] = p.Type
	m["title"] = p.Title
	m["status"] = p.Status
	m["detail"] = p.Detail
	for key, value := range p.Ext {
		if _, reserved := reservedProblemMembers[key]; reserved {
			continue
		}
		m[key] = value
	}
	return m
}

// MarshalJSON renders p as a flat JSON object, merging Ext into the top level.
func (p Problem) MarshalJSON() ([]byte, error) {
	return json.Marshal(problemMap(p))
}

func newProblem(status int, detail string) Problem {
	return Problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
	}
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	WriteProblem(w, newProblem(status, detail))
}

// writeProblemExt emits a status/detail problem response carrying extension
// members. Reserved RFC 9457 members always win over conflicting ext keys.
func writeProblemExt(w http.ResponseWriter, status int, detail string, ext map[string]any) {
	problem := newProblem(status, detail)
	problem.Ext = ext
	WriteProblem(w, problem)
}

// WriteProblem writes p as an application/problem+json response using p.Status
// as the HTTP status code.
func WriteProblem(w http.ResponseWriter, p Problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p) // best effort; response is already committed
}

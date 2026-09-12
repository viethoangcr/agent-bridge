package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// authenticate guards /v1/* requests with a constant-time bearer-token check.
// An empty token disables authentication. The public root and any non-/v1 path
// are passed through so the router can serve the root and problem 404s.
//
// Expected and supplied credentials are hashed with sha256.Sum256 before
// crypto/subtle.ConstantTimeCompare so comparison time cannot disclose token
// length. Authorization is never logged, echoed, or placed in a context.
func authenticate(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	expected := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !protectedPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		supplied, ok := bearerToken(r)
		if !ok {
			writeUnauthorized(w)
			return
		}
		got := sha256.Sum256([]byte(supplied))
		if subtle.ConstantTimeCompare(got[:], expected[:]) != 1 {
			writeUnauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// protectedPath reports whether path belongs to the authenticated /v1 subtree.
func protectedPath(path string) bool {
	return path == "/v1" || strings.HasPrefix(path, "/v1/")
}

// bearerToken extracts an exact "Bearer <token>" credential from a single
// Authorization header value. The scheme is case-sensitive and a missing,
// duplicated, empty, or non-Bearer header yields ok=false.
func bearerToken(r *http.Request) (string, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	token, found := strings.CutPrefix(values[0], "Bearer ")
	if !found || token == "" {
		return "", false
	}
	return token, true
}

// writeUnauthorized emits an RFC 9457 401 without reflecting any credential.
func writeUnauthorized(w http.ResponseWriter) {
	WriteProblem(w, Problem{
		Type:   "about:blank",
		Title:  http.StatusText(http.StatusUnauthorized),
		Status: http.StatusUnauthorized,
		Detail: "missing or invalid bearer token",
	})
}

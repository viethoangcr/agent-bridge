package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// DecodeJSON decodes exactly one JSON object from r into dst while bounding the
// body to limit bytes. It rejects malformed or empty bodies and any trailing
// JSON value, writing an RFC 9457 problem response and returning false on
// failure. An over-limit body maps to 413.
func DecodeJSON(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		writeDecodeProblem(w, err)
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeDecodeProblem(w, err)
		return false
	}
	return true
}

// writeDecodeProblem maps a decode failure to an RFC 9457 response.
func writeDecodeProblem(w http.ResponseWriter, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		WriteProblem(w, Problem{
			Type:   "about:blank",
			Title:  http.StatusText(http.StatusRequestEntityTooLarge),
			Status: http.StatusRequestEntityTooLarge,
			Detail: "request body too large",
		})
		return
	}
	WriteProblem(w, Problem{
		Type:   "about:blank",
		Title:  http.StatusText(http.StatusBadRequest),
		Status: http.StatusBadRequest,
		Detail: "invalid JSON body",
	})
}

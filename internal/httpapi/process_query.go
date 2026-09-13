package httpapi

import (
	"errors"
	"math"
	"net/url"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// detailInvalidProcessQuery is the canonical 400 detail for a malformed process
// query.
const detailInvalidProcessQuery = "invalid process query"

// errInvalidProcessQuery marks a malformed process query so the caller answers
// with a 400 problem.
var errInvalidProcessQuery = errors.New("invalid process query")

// parseLogsQuery strictly parses the logs query. stream, tail, and since are
// allowlisted; every key may appear at most once and with a non-empty value.
// tail and since are strict non-negative decimals; stream is
// stdout|stderr|combined. Repeated, empty, or malformed values are
// errInvalidProcessQuery; unknown keys are rejected by parseQuery first. A
// present tail of zero is preserved distinctly from an omitted tail.
func parseLogsQuery(values url.Values) (process.LogQuery, error) {
	var query process.LogQuery
	if values.Has("stream") {
		raw, ok := singleQuery(values, "stream")
		if !ok {
			return process.LogQuery{}, errInvalidProcessQuery
		}
		switch raw {
		case "stdout", "stderr", "combined":
			query.Stream = raw
		default:
			return process.LogQuery{}, errInvalidProcessQuery
		}
	}
	if values.Has("since") {
		raw, ok := singleQuery(values, "since")
		if !ok {
			return process.LogQuery{}, errInvalidProcessQuery
		}
		since, err := parseNonnegativeInt64(raw)
		if err != nil {
			return process.LogQuery{}, errInvalidProcessQuery
		}
		query.Since = since
	}
	if values.Has("tail") {
		raw, ok := singleQuery(values, "tail")
		if !ok {
			return process.LogQuery{}, errInvalidProcessQuery
		}
		tail, err := parseNonnegativeInt64(raw)
		if err != nil || tail > int64(math.MaxInt) {
			return process.LogQuery{}, errInvalidProcessQuery
		}
		value := int(tail)
		query.Tail = &value
	}
	return query, nil
}

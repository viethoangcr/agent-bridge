package httpapi

import (
	"errors"
	"net/http"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// mapProcessError maps a typed process error to an RFC 9457 problem: validation
// is 400, decoded/body limits are 413, unknown IDs are 404, state/capacity
// conflicts are 409, and spawn/I/O failures are 502. Anything unrecognized is a
// conservative 502 rather than a leaked internal error.
func mapProcessError(err error) Problem {
	status, detail := http.StatusBadGateway, "process failure"
	switch {
	case errors.Is(err, process.ErrValidation):
		status, detail = http.StatusBadRequest, "invalid process request"
	case errors.Is(err, process.ErrPayloadTooLarge):
		status, detail = http.StatusRequestEntityTooLarge, "process payload too large"
	case errors.Is(err, process.ErrNotFound):
		status, detail = http.StatusNotFound, detailNotFound
	case errors.Is(err, process.ErrConflict), errors.Is(err, process.ErrCapacity):
		status, detail = http.StatusConflict, "process conflict"
	}
	return newProblem(status, detail)
}

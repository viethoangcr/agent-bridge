package httpapi

import (
	"math"
	"net/http"

	"github.com/viethoangcr/agent-bridge/internal/process"
)

// processInputEnvelopeBytes is the fixed allowance that covers the exact
// compact `{data,encoding}` input envelope around the base64 expansion.
const processInputEnvelopeBytes = 1024

type processInputRequest struct {
	Data     string `json:"data"`
	Encoding string `json:"encoding"`
}

func (s *Server) handleProcessInput(w http.ResponseWriter, r *http.Request) {
	if !requireNoQuery(w, r, detailInvalidProcessQuery) {
		return
	}
	if !s.requireProcessManager(w) {
		return
	}
	var req processInputRequest
	active := s.deps.Processes.Config().MaxInputBytesPerRequest
	if !decodeJSONRequest(w, r, inputEncodedBodyLimit(active), &req) {
		return
	}
	decoded, err := process.DecodeInput(req.Encoding, []byte(req.Data))
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	written, err := s.deps.Processes.WriteInput(r.Context(), r.PathValue("id"), decoded)
	if err != nil {
		WriteProblem(w, mapProcessError(err))
		return
	}
	writeJSON(w, struct {
		BytesWritten int `json:"bytesWritten"`
	}{written})
}

// inputEncodedBodyLimit returns the encoded JSON body ceiling for a process
// input request with the given active decoded limit: four bytes per three
// decoded bytes of base64 expansion plus a fixed envelope allowance, clamped to
// the global JSON hard ceiling. The block count and 4x expansion are checked in
// int arithmetic before multiplying, so a pathological active limit cannot wrap
// below the clamp.
func inputEncodedBodyLimit(activeDecoded int) int64 {
	if activeDecoded <= 0 {
		return processInputEnvelopeBytes
	}
	blocks, remainder := activeDecoded/3, activeDecoded%3
	if remainder != 0 {
		blocks++
	}
	if blocks > (math.MaxInt-processInputEnvelopeBytes)/4 {
		return maxJSONBodyBytes
	}
	limit := blocks*4 + processInputEnvelopeBytes
	if int64(limit) > maxJSONBodyBytes {
		return maxJSONBodyBytes
	}
	return int64(limit)
}

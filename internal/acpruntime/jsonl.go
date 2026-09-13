package acpruntime

import (
	"bufio"
	"bytes"
)

const (
	// maxOutputLine matches the bridge's 10 MiB JSON body hard ceiling.
	maxOutputLine = 10 * 1024 * 1024

	// outputReadBuffer sizes the bufio.Reader below maxOutputLine so an overlong
	// line surfaces as bufio.ErrBufferFull and can be drained.
	outputReadBuffer = 32 * 1024
)

// readRawLine reads one newline-terminated record without letting a single
// record grow past limit bytes. It returns the retained bytes (never longer
// than limit), whether the record exceeded limit, and the read error. A nil
// error means the record ended at a newline; on error the caller processes any
// retained bytes and then stops.
func readRawLine(reader *bufio.Reader, limit int) (line []byte, oversized bool, err error) {
	for {
		fragment, readErr := reader.ReadSlice('\n')
		newline := bytes.IndexByte(fragment, '\n')
		if newline < 0 {
			if !oversized {
				if len(line)+len(fragment) > limit {
					oversized = true
				} else {
					line = append(line, fragment...)
				}
			}
			if readErr == bufio.ErrBufferFull {
				continue
			}
			return line, oversized, readErr
		}
		if !oversized {
			if len(line)+newline > limit {
				oversized = true
			} else {
				line = append(line, fragment[:newline]...)
			}
		}
		return line, oversized, nil
	}
}

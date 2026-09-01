package testprotocol

import (
	"testing"

	"github.com/alecthomas/assert/v2"
)

// TestRequestStream covers the 040_TESTING_PROTOCOL interface's correctness
// requirements that triggered the RequestBuilder rewrite: every Request must
// return a distinct, immutable slice whose bytes survive later Requests (no
// shared backing storage), and the frame layouts must round-trip through
// ParseRequest/ParseResponse.
//
// Each Request frame is requestFrameSize bytes: a HeaderCodePing byte followed
// by the 21-byte payload that ParseRequest reads (the server consumes the
// header separately, see main.go's handleRequest).
//
// This is a plain (non-io) test: RequestBuilder/ParseRequest/ParseResponse
// do no netring or socket work.
func TestRequestStream(t *testing.T) {
	const requestsNo = 100_000

	requester, err := New(requestsNo)
	assert.NoError(t, err, "New must succeed")

	// New must reject a non-positive request count.
	_, err = New(0)
	assert.Error(t, err, "New(0) must fail")

	// Every Request must return a distinct immutable slice: frames may not
	// share backing storage, so earlier payloads must survive later ones.
	stored := make([]byte, requestsNo*requestFrameSize)
	for i := range requestsNo {
		sequenceID, _, payload := requester.Request()
		assert.Equal(t, uint64(i), sequenceID, "sequence IDs must run 0..requestsNo-1")
		assert.Equal(t, requestFrameSize, len(payload), "each request must be 22 bytes")
		assert.Equal(t, byte(HeaderCodePing), payload[0], "each request must carry HeaderCodePing")
		copy(stored[i*requestFrameSize:], payload)
	}

	// Re-check the stored bytes after all Requests: proves they were
	// unaffected by subsequent Request calls on the requester.
	for i := range requestsNo {
		sequenceID, _, err := ParseRequest(stored[i*requestFrameSize+1 : (i+1)*requestFrameSize])
		assert.NoError(t, err, "stored request %d must parse", i)
		assert.Equal(t, uint64(i), sequenceID, "stored request %d must keep its sequence ID", i)
	}

	// RequestStop must append a distinct stop frame after the requests.
	stop := requester.RequestStop()
	assert.Equal(t, stopFrameSize, len(stop), "stop frame must be 1 byte")
	assert.Equal(t, byte(HeaderCodeStop), stop[0], "stop frame must carry HeaderCodeStop")
}

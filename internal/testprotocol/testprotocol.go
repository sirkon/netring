package testprotocol

import (
	"encoding/binary"
	"time"
	"unsafe"

	"github.com/sirkon/blog/beer"
)

// HeaderCode each message
type HeaderCode byte

const (
	// HeaderCodeStop this is the last message. The server need to stop that connection.
	HeaderCodeStop HeaderCode = 0
	// HeaderCodePing got a valid message. Need to read it and reply accordingly.
	HeaderCodePing HeaderCode = 1
)

func (h HeaderCode) Code() byte {
	return byte(h)
}

// ParseRequest server side parser for incoming ping requests.
func ParseRequest(input []byte) (sequenceID uint64, clientTime uint64, err error) {
	if len(input) < 21 {
		return 0, clientTime, beer.Newf("must be 21 bytes long, got %d bytes", len(input))
	}

	inputPtr := unsafe.Pointer(unsafe.SliceData(input))
	if unsafe.String((*byte)(unsafe.Add(inputPtr, 8)), 5) != "Hello" {
		return 0, clientTime, beer.New("expected 'Hello' text in the request bytes 8..13").
			Bytes("unexpected-text-payload", input[8:13])
	}

	sequenceID = binary.LittleEndian.Uint64(input[:8])
	clientTime = binary.LittleEndian.Uint64(input[13:])
	now := time.Now()
	if clientTime > uint64(now.UnixNano()) {
		return 0, 0, beer.Newf("request time must not be greater than the current time").
			Uint64("sequence-id", sequenceID).
			Time("request-time", time.Unix(0, int64(clientTime))).
			Time("current-time", now)
	}

	return sequenceID, clientTime, nil
}

type ResponseBuilder struct {
	buf []byte
}

// Response creates
func (p *ResponseBuilder) Response(sequenceID uint64) []byte {
	buf := p.buf[:0]
	buf = binary.LittleEndian.AppendUint64(buf, sequenceID)
	buf = append(buf, "World"...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(time.Now().UnixNano()))
	p.buf = buf

	return buf
}

func ParseResponse(output []byte) (sequenceID uint64, serverTime uint64, err error) {
	if len(output) < 21 {
		return 0, 0, beer.Newf("must be 21 bytes long, got %d bytes", len(output))
	}

	inputPtr := unsafe.Pointer(unsafe.SliceData(output))
	if unsafe.String((*byte)(unsafe.Add(inputPtr, 8)), 5) != "World" {
		return 0, 0, beer.New("expected 'Hello' text in the response bytes 8..13").
			Bytes("unexpected-text-payload", output[8:13])
	}

	sequenceID = binary.LittleEndian.Uint64(output[:8])
	serverTime = binary.LittleEndian.Uint64(output[13:])

	return sequenceID, serverTime, nil
}

type RequestBuilder struct {
	sequenceID uint64
	buf        []byte
}

// New allocates the whole request stream (requestsNo 22-byte ping frames plus
// one 1-byte stop frame) up front, so each Request/RequestStop returns a
// distinct, immutable slice of that storage. The caller must keep every
// returned slice alive and unmodified until the kernel has finished with it
// (Send takes no copies).
func New(requestsNo int) (*RequestBuilder, error) {
	if requestsNo < 1 {
		return nil, beer.Newf("RequestBuilder: requestsNo must be >= 1, got %d", requestsNo)
	}

	size := requestsNo*requestFrameSize + stopFrameSize + slackSize

	p := new(RequestBuilder)
	p.buf = make([]byte, size)

	return p, nil
}

func (p *RequestBuilder) Request() (sequenceID uint64, clientTime uint64, requestPayload []byte) {
	off := len(p.buf)
	sequenceID = p.sequenceID
	now := uint64(time.Now().UnixNano())

	p.buf = append(p.buf, HeaderCodePing.Code())
	p.buf = binary.LittleEndian.AppendUint64(p.buf, sequenceID)
	p.buf = append(p.buf, "Hello"...)
	p.buf = binary.LittleEndian.AppendUint64(p.buf, now)

	p.sequenceID++

	return sequenceID, now, p.buf[off:]
}

func (p *RequestBuilder) RequestStop() []byte {
	off := len(p.buf)
	p.buf = append(p.buf, HeaderCodeStop.Code())

	return p.buf[off:]
}

const (
	requestFrameSize = 22
	stopFrameSize    = 1
	// slackSize keeps the appends in Request and RequestStop from
	// reallocating p.buf once it reaches the final stream size (see New).
	slackSize = 64
)

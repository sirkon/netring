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

func (p *RequestBuilder) Request() (sequenceID uint64, clientTime uint64, requestPayload []byte) {
	buf := p.buf[:0]
	sequenceID = p.sequenceID
	now := uint64(time.Now().UnixNano())

	buf = append(buf, HeaderCodePing.Code())
	buf = binary.LittleEndian.AppendUint64(buf, sequenceID)
	buf = append(buf, "Hello"...)
	buf = binary.LittleEndian.AppendUint64(buf, now)

	p.sequenceID++
	p.buf = buf
	return sequenceID, now, buf
}

func (p *RequestBuilder) RequestStop() []byte {
	buf := p.buf[:0]
	buf = append(buf, HeaderCodeStop.Code())
	p.buf = buf

	return p.buf
}

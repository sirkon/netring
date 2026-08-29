# Integration Testing Protocol Specification

## 1. Frame Binary Layouts

### Client Request Frame (Total: 37 bytes)
* `session_id` (16 bytes / `uint128`): Split as `goroutine_id (uint64)` + `local_index (uint64)`.
* `payload` (5 bytes / `string`): Fixed ASCII characters `"Hello"`.
* `timestamp` (8 bytes / `uint64`): Unix timestamp in nanoseconds when the client sent the request.

### Server Response Frame (Total: 37 bytes)
* `session_id` (16 bytes / `uint128`): Mirrored directly from the client request.
* `payload` (5 bytes / `string`): Fixed ASCII characters `"World"`.
* `timestamp` (8 bytes / `uint64`): Unix timestamp in nanoseconds captured on the server during processing.

---

## 2. Reference Debug Echo Server (`testserver/main.go`)

This standalone implementation executes via the standard blocking network API. It enforces strict binary unmarshalling, structural checks, and mirrors payloads back to target pipes.

```go
package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9099", "Target TCP listen address")
	flag.Parse()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to initialize verification anchor: %v", err)
	}
	defer listener.Close()
	log.Printf("Debug test server anchor actively listening on %s", *addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleProtocolClient(conn)
	}
}

func handleProtocolClient(conn net.Conn) {
	defer conn.Close()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	// 37 bytes constant frame size
	buf := make([]byte, 37)

	for {
		// Read entire fixed client frame
		_, err := io.ReadFull(conn, buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}

		// Validation step: Verify payload matches "Hello"
		if string(buf[16:21]) != "Hello" {
			log.Printf("Protocol violation: unexpected client payload %q", buf[16:21])
			return
		}

		// Update payload chunk from "Hello" to "World" in-place
		copy(buf[16:21], "World")

		// Overwrite the client timestamp with current server nanoseconds timestamp
		serverTime := uint64(time.Now().UnixNano())
		binary.BigEndian.PutUint64(buf[21:29], serverTime)

		// Echo the modified 37-byte frame back into the pipe
		_, err = conn.Write(buf)
		if err != nil {
			return
		}
	}
}
```

---

## 3. High-Load Client Verification Routine (`client_worker.go`)

This blueprint outlines how your N goroutines will simultaneously stress-test the `netring` layer.

```go
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/sirkon/netring"
)

func RunVerificationTest(nr *netring.NetRing, fd int, totalGoroutines int, requestsPerGoroutine int) error {
	var wg sync.WaitGroup
	errChan := make(chan error, totalGoroutines)

	for gID := uint64(0); gID < uint64(totalGoroutines); gID++ {
		wg.Add(1)
		go func(goroutineID uint64) {
			defer wg.Done()

			// Local outbound/inbound scratch buffer allocations to achieve 0 heap growth inside loop
			reqBuf := make([]byte, 37)
			copy(reqBuf[16:21], "Hello")

			for mIdx := uint64(0); mIdx < uint64(requestsPerGoroutine); mIdx++ {
				// 1. Pack Session ID: || goroutineID (8B) | localIndex (8B) ||
				binary.BigEndian.PutUint64(reqBuf[0:8], goroutineID)
				binary.BigEndian.PutUint64(reqBuf[8:16], mIdx)

				// 2. Set Client Timestamp
				clientTime := uint64(time.Now().UnixNano())
				binary.BigEndian.PutUint64(reqBuf[21:29], clientTime)

				// 3. Dispatch raw memory write over netring surface
				_, err := nr.Send(fd, reqBuf)
				if err != nil {
					errChan <- fmt.Errorf("[G-%d][M-%d] send failed: %w", goroutineID, mIdx, err)
					return
				}

				// 4. Await streaming package delivery leveraging kernel buffer selection
				respBytes, err := nr.Recv(fd, netring.SizeClassTiny) // SizeClassTiny is 128B (holds 37B frame easily)
				if err != nil {
					errChan <- fmt.Errorf("[G-%d][M-%d] recv failed: %w", goroutineID, mIdx, err)
					return
				}

				// 5. Assert frame size accuracy
				if len(respBytes) != 37 {
					errChan <- fmt.Errorf("[G-%d][M-%d] protocol size mismatch, expected 37, got %d", goroutineID, mIdx, len(respBytes))
					return
				}

				// 6. Assert Session ID Integrity
				respG := binary.BigEndian.Uint64(respBytes[0:8])
				respM := binary.BigEndian.Uint64(respBytes[8:16])
				if respG != goroutineID || respM != mIdx {
					errChan <- fmt.Errorf("[CRITICAL] Race condition detected! Expected Session ID (%d, %d), got (%d, %d)", goroutineID, mIdx, respG, respM)
					return
				}

				// 7. Assert Response Payload Context
				if !bytes.Equal(respBytes[16:21], []byte("World")) {
					errChan <- fmt.Errorf("[G-%d][M-%d] payload corruption, expected 'World', got %q", goroutineID, mIdx, respBytes[16:21])
					return
				}

				// 8. Assert Temporal Invariant (Server time >= Client time)
				serverTime := binary.BigEndian.Uint64(respBytes[21:29])
				if serverTime < clientTime {
					errChan <- fmt.Errorf("[G-%d][M-%d] time-travel paradox: server timestamp (%d) is earlier than client send timestamp (%d)", goroutineID, mIdx, serverTime, clientTime)
					return
				}
				
				// 9. Return the kernel buffer immediately to prevent ring starvation
				// nr.ReleaseBuffer(netring.SizeClassTiny, respBytes)
			}
		}(gID)
	}

	wg.Wait()
	close(errChan)

	// Harvest the first encountered failure across the concurrent execution space
	if err, ok := <-errChan; ok {
		return err
	}
	return nil
}
```

package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
	"golang.org/x/sync/errgroup"

	"github.com/sirkon/netring/internal/testeasy"
	"github.com/sirkon/netring/internal/testprotocol"
)

const (
	CONN_HOST = "localhost"
	CONN_PORT = "8080"
	CONN_TYPE = "tcp"
)

func main() {
	clientImpl := flag.String("client", "stdlib", "client implementation: stdlib|netring")
	enablePProf := flag.Bool("pprof", false, "enable profiling")
	flag.Parse()

	if enablePProf != nil && *enablePProf {
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)

		fCPU, err := os.Create("cpu.pprof")
		if err != nil {
			panic(err)
		}
		defer fCPU.Close()

		if err := pprof.StartCPUProfile(fCPU); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile() // Важно: остановит запись перед выходом из main

		// Дальше идет ваш код netringClient...
		// ...

		// В самом конце main (перед выходом) пишем дампы блокировок
		fBlock, _ := os.Create("block.pprof")
		pprof.Lookup("block").WriteTo(fBlock, 0)
		fBlock.Close()

		fMutex, _ := os.Create("mutex.pprof")
		pprof.Lookup("mutex").WriteTo(fMutex, 0)
		fMutex.Close()
	}

	logger := testeasy.NewLogger(os.Stdout)

	eg, ctx := errgroup.WithContext(context.Background())
	barrier := make(chan struct{})
	noMoreRequests := make(chan struct{})

	eg.Go(func() error {
		// Server logic.
		logger := logger.With(blog.Str("side", "server"))

		listener, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
		if err != nil {
			return beer.Wrap(err, "start listener")
		}

		go func() {
			select {
			case <-ctx.Done():
			case <-noMoreRequests:
			}

			if err := listener.Close(); err != nil {
				logger.Error(nil, "failed to close listener", blog.Err(err))
			}
		}()

		logger.Info(nil, "server is listening on "+CONN_HOST+":"+CONN_PORT)
		close(barrier)

		clientErrors := make(chan error, 1)

		// Запускаем Accept в цикле прямо тут, без промежуточных каналов для conns
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Проверяем, было ли закрытие листенера преднамеренным
				select {
				case <-ctx.Done():
					return nil
				case <-noMoreRequests:
					return nil
				default:
					return beer.Wrap(err, "accept connection")
				}
			}

			go func(c net.Conn) {
				defer func() {
					_ = c.Close()
				}()

				deadline := time.Now().Add(10 * time.Second)
				if err := c.SetReadDeadline(deadline); err != nil {
					clientErrors <- beer.Wrap(err, "set read deadline to "+deadline.String())
					return
				}

				clientLogger := logger.With(blog.Stg("client-addr", c.RemoteAddr()))
				clientLogger.Info(nil, "client connected")

				if err := handleRequest(c, clientLogger); err != nil {
					clientErrors <- beer.Wrap(err, "interact with the client").
						Stg("client-addr", c.RemoteAddr())
				}
			}(conn)

			select {
			case err := <-clientErrors:
				return err
			default:
			}
		}
	})

	eg.Go(func() error {
		// Client logic.

		client := stdlibClient
		if *clientImpl == "netring" {
			client = netringClient
		}

		if err := client(ctx, logger, barrier); err != nil {
			return err
		}

		close(noMoreRequests)

		return nil
	})

	if err := eg.Wait(); err != nil {
		logger.Error(nil, "client-server interaction failed", blog.Err(err))
	}
}

// Function to read data from the client socket
func handleRequest(conn net.Conn, logger *blog.Logger) (err error) {
	// Close the connection when this function exits
	defer func() {
		if cErr := conn.Close(); cErr != nil {
			if err != nil {
				logger.Error(nil, "failed to close connection", blog.Err(cErr))
				return
			}

			err = beer.Wrap(cErr, "close connection")
		}
	}()

	// Use a buffered reader for efficient line-by-line reading
	reader := bufio.NewReader(conn)

	var requestsHandled int
	buf := make([]byte, 21)
	var responser testprotocol.ResponseBuilder

	sequences := map[uint64]struct{}{}

	for {
		header, err := reader.ReadByte()
		if err != nil {
			return beer.Wrap(err, "read header")
		}

		switch testprotocol.HeaderCode(header) {
		case testprotocol.HeaderCodeStop:
			logger.Info(
				nil, "client wants to disconnect",
				blog.Int("requests-processed", requestsHandled),
			)
			return nil
		case testprotocol.HeaderCodePing:
		default:
			return beer.Newf("got incorrect header code %d", header)
		}

		if _, err := io.ReadFull(reader, buf); err != nil {
			return beer.Wrap(err, "read ping request")
		}

		sequenceID, clientTime, err := testprotocol.ParseRequest(buf)
		if err != nil {
			return beer.Wrap(err, "parse request")
		}
		_ = clientTime
		if _, ok := sequences[sequenceID]; ok {
			logger.Warn(nil, "got duplicate sequence ID", blog.Uint64("sequenceID", sequenceID))
		}
		sequences[sequenceID] = struct{}{}

		//logger.Debug(nil, "request data",
		//	blog.Uint64("sequence-id", sequenceID),
		//	blog.Time("client-time", time.Unix(0, int64(clientTime))),
		//)

		response := responser.Response(sequenceID)
		if _, err := conn.Write(response); err != nil {
			return beer.Wrap(err, "write response").Uint64("sequence-id", sequenceID)
		}

		requestsHandled++
	}
}

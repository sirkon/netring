package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
	"golang.org/x/sync/errgroup"

	"github.com/sirkon/netring/internal/testprotocol"
)

func stdlibClient(ctx context.Context, logger *blog.Logger, barrier chan struct{}) error {
	logger = logger.With(blog.Str("side", "client"))
	<-barrier

	conn, err := net.Dial(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		logger.Error(nil, "failed to connect", blog.Err(err))
		os.Exit(1)
	}

	defer func() {
		if err := conn.Close(); err != nil {
			logger.Error(nil, "failed to close client connection", blog.Err(err))
		}
	}()

	deadline := 10 * time.Second
	if err := conn.SetDeadline(time.Now().Add(deadline)); err != nil {
		return beer.Wrap(err, "set test deadline to "+deadline.String())
	}

	sequences := sync.Map{}
	var requester testprotocol.RequestBuilder

	egg, egCtx := errgroup.WithContext(ctx)

	start := time.Now()

	const requestsNo = 100_000

	egg.Go(func() error {
		for range requestsNo {
			select {
			case <-egCtx.Done():
				return nil
			default:
			}

			sequenceID, clientTime, payload := requester.Request()
			sequences.Store(sequenceID, clientTime)

			if _, err := conn.Write(payload); err != nil {
				return beer.Wrap(err, "send request").Uint64("sequence-id", sequenceID)
			}
		}

		stop := requester.RequestStop()
		if _, err := conn.Write(stop); err != nil {
			return beer.Wrap(err, "send stop request")
		}

		logger.Info(nil, "sent all requests")

		return nil
	})

	egg.Go(func() error {
		reader := bufio.NewReader(conn)
		buf := make([]byte, 21)

		for range requestsNo {
			select {
			case <-egCtx.Done():
				return nil
			default:
			}

			if _, err := io.ReadFull(reader, buf); err != nil {
				return beer.Wrap(err, "read response")
			}

			sequenceID, serverTime, err := testprotocol.ParseResponse(buf)
			if err != nil {
				return beer.Wrap(err, "process response")
			}

			clientTimeBoxed, ok := sequences.Load(sequenceID)
			if !ok {
				return beer.Wrapf(err, "unknown sequence ID %d in response", sequenceID)
			}

			clientTime := clientTimeBoxed.(uint64)
			if serverTime < clientTime {
				return beer.Wrap(err, "server time is lower than client time").
					Time("client-time", time.Unix(0, int64(clientTime))).
					Time("server-time", time.Unix(0, int64(serverTime)))
			}

			sequences.Delete(sequenceID)
		}

		var notEmpty bool
		sequences.Range(func(key, value interface{}) bool {
			notEmpty = true
			return false
		})

		if notEmpty {
			return beer.New("some requests have not been responded")
		}

		logger.Info(nil, "response checker done")
		return nil
	})

	if err := egg.Wait(); err != nil {
		return err
	}

	logger.Info(nil, "echo client stop", blog.Duration("elapsed", time.Since(start)))

	return nil
}

package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/sirkon/netring"
	"github.com/sirkon/netring/internal/testprotocol"
)

// netringClient mirrors stdlibClient's echo workload (100k ping requests plus
// a stop frame over one TCP connection, with response verification), driven
// exclusively through the public github.com/sirkon/netring API.
//
// The local protocol framing duplicates the byte layouts of the internal
// testprotocol package (040-TESTING-PROTOCOL): nothing from internal/ is
// reachable here, so the frames are built and parsed by hand.
//
// Deadline handling reproduces SetDeadline(now+10s) with two layers: a
// watchdog goroutine around the whole workload (primary, exact semantic
// match) plus SO_RCVTIMEO/SO_SNDTIMEO socket timeouts (best effort, lets a
// blocked io-wq SEND/RECV fail with -EAGAIN on its own). If the watchdog
// fires, the error is returned and the possibly still parked workers are
// abandoned: there is no way to cancel a posted SQE with the current API.
// The abandoned goroutines die with the process when main returns, exactly
// like stdlibClient's os.Exit(1) path. The proper fix (async cancel / linked
// timeouts) is future work.
func netringClient(ctx context.Context, logger *blog.Logger, barrier chan struct{}) error {
	logger = logger.With(blog.Str("side", "client"))
	<-barrier

	// The Connect design (037-TASK-CONNECT) supports *unix.SockaddrInet4
	// only, so force IPv4 resolution.
	target, err := net.ResolveTCPAddr("tcp4", CONN_HOST+":"+CONN_PORT)
	if err != nil {
		return beer.Wrapf(err, "resolve server address %s", CONN_HOST+":"+CONN_PORT)
	}

	nr, err := netring.New(256, logger)
	if err != nil {
		return beer.Wrap(err, "create netring")
	}

	// Caller-owned CQ poller, exactly like newTestNetRing.
	var stop atomic.Bool
	finish := make(chan struct{})
	go nr.Poll(&stop, finish)

	// Response frames are 21 bytes; the 128 byte tiny class holds up to 6
	// frames per view, so capacity 4 is ample for one outstanding Recv.
	if err := nr.RegisterBufferRing(netring.SizeClassTiny, 4); err != nil {
		return beer.Wrap(err, "provision tiny buffer ring")
	}

	if err := nr.RegisterBufferRing(netring.SizeClassHuge, 1024); err != nil {
		return beer.Wrap(err, "provision huge buffer ring")
	}

	// The fd stays blocking: io_uring punts blocking sockets to io-wq, which
	// is what we want for the timeout behavior below.
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return beer.Wrap(err, "create client socket")
	}

	// Defense-in-depth socket timeouts: a blocked io-wq SEND/RECV fails with
	// -EAGAIN after 10 idle seconds on kernels that honor socket timeouts for
	// io_uring.
	timeout := &unix.Timeval{Sec: 10}
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, timeout); err != nil {
		return beer.Wrap(err, "set socket timeouts")
	}
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, timeout); err != nil {
		return beer.Wrap(err, "set socket timeouts")
	}

	var ip [4]byte
	copy(ip[:], target.IP.To4())
	if err := nr.Connect(fd, &unix.SockaddrInet4{Addr: ip, Port: target.Port}); err != nil {
		return beer.Wrapf(err, "connect to %s:%d", CONN_HOST, target.Port)
	}

	// Watchdog: run the workload in a goroutine and select on it. A fired
	// watchdog abandons the (possibly still parked) workers; the process is
	// about to exit via main, and tearing down a ring with abandoned parked
	// workers would be unsound.
	const clientDeadline = 10 * time.Second

	done := make(chan error, 1)
	go func() {
		done <- runWorkload(ctx, nr, fd, logger)
	}()

	select {
	case workloadErr := <-done:
		if workloadErr != nil {
			return beer.Wrap(workloadErr, "run echo workload")
		}
	case <-ctx.Done():
		return beer.Wrap(ctx.Err(), "client context cancelled")
	case <-time.After(clientDeadline):
		return beer.New("client deadline exceeded")
	}

	// Teardown (success path only), each failure logged and swallowed
	// (ERRORS.md rules 2/3): the fd dies through the per-fd shard so the
	// close lands after in-flight ops, then the poller stops before any
	// munmap, then the ring itself is destroyed.
	if err := nr.Close(fd); err != nil {
		logger.Error(nil, "failed to close client connection", blog.Err(err))
	}
	stop.Store(true)
	<-finish
	if err := nr.Stop(); err != nil {
		logger.Error(nil, "failed to stop netring", blog.Err(err))
	}

	return nil
}

const (
	requestFrameSize  = 22
	responseFrameSize = 21
	requestsNo        = 100_000
)

// runWorkload is the errgroup with the sender and receiver workers, watching
// egCtx for cancellation like stdlibClient.
func runWorkload(ctx context.Context, nr *netring.NetRing, fd int, logger *blog.Logger) error {
	sequences := sync.Map{}
	requester, err := testprotocol.New(requestsNo)
	if err != nil {
		return beer.Wrap(err, "create requester")
	}

	egg, egCtx := errgroup.WithContext(ctx)

	start := time.Now()

	egg.Go(func() error {
		for range requestsNo {
			select {
			case <-egCtx.Done():
				return nil
			default:
			}

			sequenceID, clientTime, payload := requester.Request()
			sequences.Store(sequenceID, clientTime)

			if err := nr.Send(fd, payload); err != nil {
				return beer.Wrap(err, "send request").Uint64("sequence-id", sequenceID)
			}
		}

		if err := nr.Send(fd, requester.RequestStop()); err != nil {
			return beer.Wrap(err, "send stop request").Uint64("sequence-id", 0)
		}

		if err := nr.FlushFDSends(fd); err != nil {
			return beer.Wrap(err, "flush outgoing requests")
		}

		logger.Info(nil, "sent all requests")
		return nil
	})

	egg.Go(func() error {
		// Stream reassembly over provided-buffer views: a Recv is a single
		// recv(2)-like completion, one 128 byte view may carry several
		// coalesced response frames or a fraction of one, so leftovers live
		// in the carry buffer (always < responseFrameSize). The loop runs
		// until requestsNo frames were verified, not until requestsNo Recv
		// calls: views and frames are not 1:1.
		var carry []byte
		var processed int
		deletedSequences := map[uint64]struct{}{}

		for processed < requestsNo {
			select {
			case <-egCtx.Done():
				return nil
			default:
			}

			view, err := nr.Read(fd, netring.SizeClassHuge)
			if errors.Is(err, syscall.ENOBUFS) {
				continue // ring was empty; data stays queued
			}
			if err != nil {
				return beer.Wrap(err, "read response")
			}
			if view == nil {
				return beer.New("server closed the connection early")
			}

			// Copy out before releasing the loan...
			work := carry
			work = append(work, view...)
			nr.ReleaseBuffer(netring.SizeClassTiny, view) // ...then hand the kernel buffer back

			for len(work) >= responseFrameSize && processed < requestsNo {
				frame := work[:responseFrameSize]
				work = work[responseFrameSize:]
				processed++

				sequenceID, serverTime, err := testprotocol.ParseResponse(frame)
				if err != nil {
					return beer.Wrap(err, "process response")
				}

				clientTimeBoxed, ok := sequences.Load(sequenceID)
				if !ok {
					if _, ok := deletedSequences[sequenceID]; ok {
						return beer.Newf("server sent deleted sequence id %d again", sequenceID)
					}
					return beer.Newf("unknown sequence ID %d in response", sequenceID)
				}

				clientTime := clientTimeBoxed.(uint64)
				if serverTime < clientTime {
					return beer.New("server time is lower than client time").
						Time("client-time", time.Unix(0, int64(clientTime))).
						Time("server-time", time.Unix(0, int64(serverTime)))
				}

				sequences.Delete(sequenceID)
				deletedSequences[sequenceID] = struct{}{}
			}
			carry = append(carry[:0], work...)
		}

		var notEmpty bool
		sequences.Range(func(key, value any) bool {
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

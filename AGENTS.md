# AGENTS.md

## Project overview

`netring` (module `github.com/sirkon/netring`) is a work-in-progress Go envelope for
Linux `io_uring`, aimed at a high-performance networking subsystem with zero allocations
per request. It bypasses the standard `netpoll`/epoll path. Linux-only (uses `unix` syscalls
and `SYS_IO_URING_*`). Go 1.27.

**Status: early WIP.** See commits `db8da48 fix: wip`, `2934fbc Initial commit`. There are
`TODO`s in the code and `fmt.Println` side effects (details below). Do not assume features
described in `ARCH.md` exist yet.

## ARCH.md vs reality (read this first)

`ARCH.md` describes the *target* architecture, most of which is **not implemented**:

- `NetTask`, `sync.Pool` task pooling, `runtime.gopark` + `unlockf` state machine,
  the Translator goroutine, Task channels, and `runtime.goready` wakeups: **absent**.
- What exists today is only the io_uring ring foundation: the unexported `IOUring` struct
  (`New` in `internal/iouring/iouring.go`), raw SQ push, CQ polling loop (which does **not** dispatch
  results yet), and helpers (`accept`, `wakeup`, `close`).
- `ARCH.md`'s `TaskSlots` description does match `internal/taskslots` closely; the rest is a
  design doc.

So: use `ARCH.md` as design intent, but grep the code before assuming a symbol exists.

## Commands

```sh
go build ./...          # everything compiles (linux/amd64 verified)
go test -count=1 ./...  # no cache; the only real test is internal/taskslots
go vet ./...            # passes -- but see "unsafe.Pointer warnings" below
go test ./internal/taskslots/ -bench=. -benchmem   # perf micro-benchmarks
```

- No Makefile, no CI, no `.golangci.yml`, no Dockerfile. Plain Go toolchain.
- Benchmarks use `b.Loop()` (Go 1.24+ feature) and compare `Slots[T]` vs `map[uint64]T`,
  asserting `0 allocs/op` for the slots path. Run with `-benchmem`.
- `go vet ./...` prints a dozen `possible misuse of unsafe.Pointer` in `internal/iouring/iouring.go` and
  `internal/iouring/method_close.go`. These are **expected and accepted** — deliberate raw pointer
  arithmetic over mmap'ed shared ring memory. Do not "fix" them by removing the unsafe code.

## Layout

| Path | Package | What it is |
|---|---|---|
| `*.go` (root) | `netring` | The io_uring envelope: `IOUring` struct + ring mmap setup + methods. Split into `internal/iouring/iouring.go` (setup), `iouring_method_*.go` (Push/Accept/Close/Wakeup), `netring_poller.go`, `internal/iouring/q_entries.go` (SQEntry/CQEntry), `internal/iouring/consts.go`, `internal/iouring/params.go`, `internal/iouring/errors.go`, `poller_options.go` |
| `internal/sysnet` | `sysnet` | Raw syscalls that must return fd values (stdlib would swallow them). Only `Listen(ip, port) (int, error)` — **IPv4 only** |
| `internal/taskslots` | `taskslots` | Zero-allocation bytable for in-flight tasks keyed by `uint64` index. The only package with tests/benchmarks |
| `internal/timingwheel` | `timingwheel` | SoA (struct-of-arrays) resource-expiry wheel for connection TTL timeouts |

The root package was recently moved out of `internal/iouring/` (git shows renames `RM` /
`AM`); the refactor is mid-flight in the working tree.

## Conventions & patterns

- **Errors**: wrap with `github.com/sirkon/blog/beer` — `beer.New(...)`, `beer.Wrap(err, "...")`.
  The single exception: the CQ mmap branch in `internal/iouring/iouring.go` uses `fmt.Errorf`. Logging is done
  via `*blog.Logger` passed into `New`; the poller logs errors via
  `r.logger.Error(nil, ..., blog.Err(errno))`.
- **Kernel constants are hand-copied** from `linux/io_uring.h`: opcodes (`OPAccept = 13`,
  `OPRead = 22`), setup flags (`setupSQPoll`, `setupSQAff`, `featSingleMMap`), enter flags
  (`enterGetEvents`, `enterSQWakeup`). `x/sys/unix` does not export these; if the kernel ABI
  changes, update `internal/iouring/consts.go` and the `SQEntry` layout *together*.
- **SQEntry must stay exactly 64 bytes** (mirrors `struct io_uring_sqe`, including `Pad2`).
  `CQEntry` is 16 bytes. The ring code assumes both sizes.
- **Style**: short unexported helpers, inline comments explaining kernel quirks (e.g. the C-
  union field reuse — in `io_uring_accept` the `off` field carries a `socklen_t*` pointer,
  see `loiring_method_accept.go`). Comments are chatty; match that tone.
- go.mod deps: `kelindar/bitmap` (bitmap used by taskslots), `sirkon/blog` (+`/beer`),
  `sirkon/deepequal` (test equality), `golang.org/x/sys` (unix).

## taskslots (the important one)

`Slots[T]` is a preallocated flat table + bit bitmap; indexes come from a "wave" pointer
(the last visited word) to make allocation O(1)-ish with `bitmap.MinZero()`.

- Constraints: capacity must be a power of 2 and **strictly greater than 4096** (>4096 —
  minimum `8192`).
- **Not thread-safe.** No locks, no atomics — by design it is driven from a single
  Translator goroutine (per ARCH.md). If accessed concurrently this is a data race. Don't
  add locking; the design relies on it being exclusively single-threaded.
- **The "ULTRA-HACK"**: `Add`/`Get`/`Del` compute raw `unsafe.Add(basePtr, wordIdx<<3)`
  pointers and do non-checked bit writes (skipping bounds checks). The defensive slice
  `allocAlign` gives 64-byte cache-line alignment for the words. Preconditions: 1) all words
  are within the real `[]uint64` allocation, 2) `idx` within `cap` when hitting the bitmap
  path. If you widen the idx math, `allocAlign`/`bitmapLenMask` interplay, re-read
  `Get`'s fallback branch.
- **Fallback cliff**: indexes `>= cap` route to `map[uint64]T` — allocations and a per-op
  map lookup; that's the pathological path for when the slots are exhausted plus the
  `fullCount` monotonic counter on overflow. The map will blow up memory/allocations,
  watch it in benchmarks.
- `Reset()` clears in place with `clear()`, zero-alloc reset for pool reuse.
- Empty test file `internal/taskslots/taskslots_test.go` uses `deepequal`+`SideBySide`,
  `reflect.DeepEqual`; benchmarks assert 0 allocs on the hot path (`b.ReportAllocs`).

## Poller

- `Poll(stop *atomic.Bool, finish chan struct{}, options ...CQPollerOption)` must run on a
  **dedicated OS thread** — it calls `runtime.LockOSThread()` itself and closes `finish`
  on exit. Don't run it on a shared goroutine.
- Spin strategy: on empty CQ it spins `runtime.Gosched()` up to `maxSpinLimit` (50k), then
  calls `io_uring_enter(fd, 0, 1, IORING_ENTER_GETEVENTS)`; `EINTR` -> retry, other errors
  are logged and it continues.
- **It does not yet dispatch CQEs**: the code is `_ = resp` with a `TODO` describing how the
  periodic timer task (`periodicalTimerTaskID` in consts, `WithCQPollerOptionTimer` in
  options) and goready slot dispatch should land. If you wire this up, the ARCH.md
  `stateInCore` / `stateParked` / `stateDone` CAS handshake is the design to follow to avoid
  the `goready: bad g state` panic.
- `NeedWakeup()` exists but is unused; `Push` checks `SQFlags & sqNeedWakup` and calls
  `Wakeup()` inline instead.

## ioUring setup gotchas

- `newIOUring(entries, logger)`: entries must be a power of 2 and >= 256 (check when
  `entries < 256`). Uses `IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF` — the SQPOLL kernel
  thread requires elevated privileges (CAP_SYS_ADMIN/CAP_SYS_NICE); in restricted containers
  the setup syscall can fail with `EPERM`. Tests do not hit this path (no netring tests), so
  `go test ./...` will pass even in privileged/unprivileged containers.
- `SINGLE_MMAP` feature probing: SQ and CQ are mmap'ed with sizes rounded from SQOff.Array /
  CQOff.Cqes, falling back to a **separate CQ mmap** on old kernels. `Close()` unmaps with
  the *same* size computation and keeps the mapping; if you change `SetupRing`, check both
  the setup *and* `Close` paths — they must stay symmetric.
- `Push` returns `ErrSQFull` **value as-is** (not wrapped) — documented contract, because
  callers do `errors.Is`/`==` on it. But note it's a custom `ErrorSQ` type, not sentinel
  built-in; if you add more SQ errors keep `iota+1` so `ErrSQFull == 1` doesn't change.

## sysnet

- Only `Listen` exists: `unix.Socket(AF_INET, SOCK_STREAM)`, `SO_REUSEADDR`, `Bind`,
  `Listen(SOMAXCONN)`, returns the bare fd. IPv4-only (`net.ParseIP(ip).To4()`); no IPv6
  path. Error messages wrap with `beer.Wrap` using lowercase short phrases.

## timingwheel

- Structure-of-arrays TTL expiration: `TimingWheel.Tick()` must be called once per second
  (externally — from your event loop or timerfd). `Add(ttlSec, fd, cb)` with invalid index
  + generation pair in `TimerId` for stale-entry detection; free indices come from a stack
  slice. Callbacks fire during `Tick` and are expected to submit the async close into
  io_uring. Not tested.

## Misc

- `internal/iouring/q_entries.go` has `func init() { fmt.Println(unsafe.Sizeof(SQEntry{})) }` —
  prints `64` on any process that imports this package. Debug leftover; don't rely on it,
  feel free to remove once it doesn't break anything (the tests do not depend on it).
- `go.mod`: `go 1.27`, requires `x/sys v0.47.0`, `kelindar/bitmap v1.5.5`,
  `sirkon/blog` + `sirkon/deepequal` (test-only for deep equality).
- Benchmarks currently show `Slots` ~2-5x faster than map and always `0 allocs/op` — any
  change that introduces allocations in `Add`/`Get`/`Del` on the hot path will be caught
  by `-benchmem`; that's the acceptance bar for this code.
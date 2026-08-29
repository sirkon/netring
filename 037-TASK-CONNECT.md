# 037-TASK-CONNECT (package netring)

## 0. Mission
Implement the outbound connection primitive `Connect` across the entire pipeline. Enable Go workers to initiate asynchronous, non-blocking network handshakes through the kernel, suspending via `gopark` until `io_uring` reports the connection is established or failed.

---

## 1. Low-Level Connect Builder Extension (internal/iouring)
Ensure your context assumes the existence or adds a non-blocking `ExpectConnect` helper inside the low-level `IOUring` subsystem:
- **Signature**: `func (u *IOUring) ExpectConnect(fd int, sockaddr unsafe.Pointer, socklen uint32, userData uint64) error`
- It must grab a free SQE, set `opcode = IORING_OP_CONNECT`, bind `fd`, map `addr = uint64(sockaddr)`, `len = socklen`, and assign `user_data = userData`.

---

## 2. High-Level Connect API (netring/method_connect.go)

Implement the public blocking `Connect` method on `NetRing`:
- **Signature**: `func (nr *NetRing) Connect(fd int, sa unix.Sockaddr) error`
- **Validation**: If `fd < 0` or `sa == nil`, return a `beer` error instantly.
- **Sockaddr Marshalling**: Convert high-level `unix.Sockaddr` into raw system `sockaddr_in` layout with Network Byte Order (Big-Endian) alignment:
  ```go
  var rawAddr unsafe.Pointer
  var sockLen uint32

  switch v := sa.(type) {
  case *unix.SockaddrInet4:
      var raw unix.RawSockaddrInet4
      raw.Family = unix.AF_INET
      raw.Port = uint16((v.Port >> 8) | (v.Port << 8)) // Big-Endian byte swap
      raw.Addr = v.Addr
      rawAddr = unsafe.Pointer(&raw)
      sockLen = uint32(unsafe.Sizeof(raw))
  default:
      return beer.Newf("connect: unsupported sockaddr type %T", sa)
  }
  ```
- **Execution Lifecycle**:
    1. Acquire a `*taskCell` via `nr.taskCell()` (or `nr.pool.Get().(*taskCell)` depending on your current layout). Initialize `taskState.Store(taskStateInCore)`, `res = 0`, `flags = 0`, and set `g = getg()`.
    2. Build a local stack POD `ringTask` with `Opcode: opcodeTypeConnect`, carrying `Payload: rawAddr` and `Addr: uint64(sockLen)`.
    3. Push to `nr.chans[fd & (len(nr.chans) - 1)]`.
    4. Block via `gopark(netringParkUnlock, unsafe.Pointer(cell),...)`.
    5. Call `runtime.KeepAlive(sa)` right after the wake-up path to prevent premature GC sweeps during the in-flight kernel operation.
    6. Return cell to pool via `nr.pool.Put(cell)`.
    7. If `cell.res < 0`, return `kernelResultToError(cell.res)`. Else, return `nil`.

---

## 3. Translator Integration (netring_translator.go)

Add `opcodeTypeConnect` to the switch block inside `dispatch(task ringTask)` :
```go
case opcodeTypeConnect:
    task.Ctx.g = task.G
    slotIdx := nr.slots.Add(task.Ctx)
    if err := nr.r.ExpectConnect(int(task.FD), task.Payload, uint32(task.Addr), slotIdx); err != nil {
        nr.abortIssuer(slotIdx, err)
    }
```

---

## 4. Non-Negotiables
- Keep `Connect` fully blocking and mapped to the standard cell/slot atomic state machine.
- All code must pass `go test -race./...`.

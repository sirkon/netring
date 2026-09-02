# netring
Go-frendly envelope for io_uring and network operations.

## The goal.

The goal was to implement "native" interface for the io_uring. Meaning something like

```go
data, err := nr.Reac(netring.SizeClass4Kb)
```

## Status

Failed. Because M:N model is absolutely incompatible with SQ/CQ async. The cost of making synchronous API
is just way too high. Thread safe access to SQ alone is too expensive. And the way back to task issuer
takes even more.

If it were be possible to pin a goroutine to an OS thread and then the switch became a lot cheaper and
a dedicated ring per thread would make them 100% lockless. Then it would work.

## Netring client and stdlib server profile.
![netring](./resources/netring-client.png)

## Stdlib client and server profile.
![stdlib](./resources/stdlib-client.png)

Take a look at the `Go.func1` of the first picture. It is Go runtime cost of the io_uring logic.
The `main.main.func1.2` are the same on both pictures - this is the server written with Go's `net`, stdlib
I mean. You see, the `io_uring` logic is cheap. It is the cost of interaction between incompatible
finite state machines of io_uring and Go's runtime that sinks it - the huge red area on the first pic.

## The perspective.

It is clear using this library for network interaction is meaningless. It is slower than stdlib and
somewhat harder to use. But the disk IO seems a target that will benefit from it.
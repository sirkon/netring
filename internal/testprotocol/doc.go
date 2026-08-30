// Package testprotocol provides means for an echo protocol with multiplexing.
//
// # Protocol
//
// Each message looks like either
//
//	|| 0 ||
//
// or
//
//	|| 1 | SequenceID: u64(LE) | "Hello" | ClientTimeNS: u64(LE) ||
//
// where the first one means the client wants to disconnect. We should oblige it then.
// The second message should be processed and returned as
//
//	|| SequenceID: u64(LE) /* Copied from request */ | "World" | ServerTimeNS: u64(LE) ||
//
// With no header in it. The responses come byte to byte as
//
//	| responseX | responseY | ...
//
// As this all is meant to run on a single machine (and even the single process):
//   - the server must check if the client time is lower than its current time.
//   - the client must check whether the returned server time for the given SequenceID is
//     not smaller than the time in the request.
//
// Any error on any side means everything stops right now.
package testprotocol

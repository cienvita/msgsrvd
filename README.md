# msgsrvd

Near real-time messaging server with strong durability guarantees, for
bare-metal clusters.

## Design

Durability. Every WRITE hits a WAL before the client can observe it.
ACK is withheld until the requested durability level is met: local
append, local fsync, or replicated-and-fsync'd on a quorum.

Replication. 2+1 minimum (one primary, two replicas). Two-phase commit:
prepare on the primary and replicas, then commit once a quorum has the
prepare record on stable storage. ACK carries the WAL sequence number.

Intelligent clients. Each WRITE carries a client-assigned monotonic
sequence ID, used by the server for dedup on retry. Clients own retry,
reconnect, and ordering.

## Implementation

C99, Linux x86-64 only. Raw syscalls via inline asm instead of libc
wrappers; io_uring for all I/O. Kernel ABI structs declared locally,
no kernel-header dependency. The only libc reference in code is
`printf`, gated behind the debug build.

Static memory only. Per-connection arena allocators, no `malloc`, no
`free`. Internal references are index-based spans, not pointers.

Disciplined style. Fixed-size error stacks, no hidden allocations, no
exceptions or longjmp, every syscall returns a structured result.

Wire protocol. Fixed 32-byte header, little-endian, memcpy-decoded.
Small fixed verb set: WRITE, READ, DELETE, SUBSCRIBE, NOTIFY, ACK, ERR,
PING, PONG.

## Running

    msgsrvd --dir PATH [--port N] [--segment-size BYTES]

Serves the write-ahead log in PATH, which must already exist. Port
defaults to 7400 and segments to 256 MiB. SIGINT or SIGTERM stops the
loop at the end of the pass that receives it, so a shutdown never
lands between an append and the flush that makes it durable.

    msgsrvd --selfcheck

Runs the in-process checks and exits. There is no external test
harness: a binary with no libc is easier to exercise from inside
itself than to link into one.

## Status

Single node, and durable on that node. The wire protocol, WAL,
crash recovery, segment rollover, retention, and an io_uring event
loop with group commit all work end to end: a write is acknowledged
only after the flush that covers it, and survives a restart.

Not built yet. Replication, so a write that asks for a second copy is
refused rather than accepted on a promise. The Rust client. Sessions
live in memory, so deduplication does not survive a restart; a client
resuming one is told the session is unknown and opens a new one, which
means a retry spanning a restart can be stored twice. Reads and
subscriptions are refused as unimplemented.

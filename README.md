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

## Status

Foundations only. Core types, arena, error stack, wire protocol,
connection state machine, and io_uring bindings are in place. WAL,
replication, and the event loop are not yet implemented.
`make debug && ./build/msgsrvd` runs the in-process self-checks.

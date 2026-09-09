# msgsrvd

Near real-time messaging server with strong durability guarantees, for
bare-metal clusters.

## Design

Durability. Every WRITE hits a WAL before the client can observe it.
ACK is withheld until the requested durability level is met: local
append, local fsync, or replicated-and-fsync'd on a quorum.

Replication. One leader, up to two replicas, each streaming the
leader's log into its own. A write that asks for a second copy is
acknowledged once the leader and at least one replica have both
flushed it, which puts it on two nodes in two data centres. There is
no prepare and commit: what a replica holds is the leader's record
unchanged, and the acknowledgement is the only thing that waits.

Intelligent clients. Each WRITE carries a client-assigned monotonic
sequence ID, used by the server for dedup on retry. Clients own retry,
reconnect, and ordering.

## What it stores

A record is a payload of bytes the server never looks at, a
`partition_key` you choose, and what the log adds: a sequence, dense
from 1, ordering every record on the node against every other; the
session and client sequence that identify who wrote it; the durability
that was asked for; and a checksum.

The key is a label and a filter, not a namespace. There is no registry
and there are no names, and every key shares one log, one sequence,
one flush and one replication stream. What the log does keep per key
is where it starts and where it ends, which is what stops a read for
one key walking the whole log. Making a key a unit of storage instead
was costed and rejected: it would trade one flush per batch for one
per key written to, and keeping slow flushes off the acknowledgement
path is the reason the leader sits where it does.

Records are around 8 KB at most. The protocol allows 1 MiB but a
connection's receive buffer is 8 KiB, and the design's advice is that
clients coalesce small records rather than send large ones.

There is no schema. `record_type` is in the header to route a record to
a projector that would interpret it, and no projector exists, so it is
always zero.

**Everything is an append.** There is no update, and DELETE is a verb
that answers "not built". Nothing compacts, so the current value for a
key is whatever its history says, read back, and nothing queries a
payload, because nothing parses one. What bounds a node is retention,
which deletes whole segments off the back of the log once nothing
needs them.

That suits a log of things that happened: entries in a ledger, state
changes to be replayed, work to be consumed in order, a feed for other
processes to follow. It does not suit anything mutable, anything
searched by content, or anything past one node's write rate, since one
leader takes every write.

## Implementation

C99, Linux x86-64 only. Raw syscalls via inline asm instead of libc
wrappers; epoll for all I/O. Kernel ABI structs declared locally,
no kernel-header dependency. The only libc reference in code is
`printf`, gated behind the debug build.

Static memory only. Per-connection arena allocators, no `malloc`, no
`free`. Internal references are index-based spans, not pointers.

Statically linked. The syscalls are the program's own, so nothing is
taken from libc but the startup, and the binary the control node
builds runs on the servers without a glibc version having to match.

Disciplined style. Fixed-size error stacks, no hidden allocations, no
exceptions or longjmp, every syscall returns a structured result.

Wire protocol. Fixed 32-byte header, little-endian, memcpy-decoded.
Small fixed verb set: WRITE, READ, DELETE, SUBSCRIBE, NOTIFY, ACK, ERR,
PING, PONG, HELLO, and three more for the replication stream. The
metrics port is the one thing that does not speak it, and the least
HTTP that a scraper accepts is all it speaks instead.

Nothing asks a node about itself. There is no verb for what a node is,
whether it leads, or what keys it holds, so a client finds out that it
reached a replica by having a write refused, and what a log holds is
read off its files.

## Running

    msgsrvd --dir PATH [--bind ADDR] [--port N] [--segment-size BYTES]
            [--leader ADDR:PORT] [--retain-segments N] [--replicas N]
            [--metrics-port N]

Serves the write-ahead log in PATH, which must already exist. Port
defaults to 7400 and segments to 256 MiB. SIGINT or SIGTERM stops the
loop at the end of the pass that receives it, so a shutdown never
lands between an append and the flush that makes it durable.

`--bind` is the one address the node listens on, and the metrics port
takes the same one. It defaults to 127.0.0.1, so a node anything else
has to reach must be given an address, and that address is numeric
because there is no resolver here. The port carries client writes and
the replication stream both, which is why it belongs on a private
interface rather than on every one.

`--retain-segments` is how many segments the node keeps, and without
it the log grows for ever. A node also keeps whatever the replicas
named by `--replicas` have not yet confirmed, whichever is more, so a
leader expecting replicas that are not all attached deletes nothing.
`--replicas` says nothing on a replica, which has nothing downstream
of it, and is ignored there rather than refused, so one set of flags
serves every node. Sizing is arithmetic the operator does: segments
times segment size is the volume, and it has to hold the window a
replica may be away for.

`--metrics-port` opens a second listener that answers any HTTP request
with the node's counters in Prometheus text format. It is off unless
asked for. What it reports is the log's sequences and size, per
replica what has been confirmed and how long ago, acknowledgements and
refusals by kind, and a histogram of flush latency.

With `--leader` the node is a replica: it dials that address, asks for
the first sequence it does not hold, and writes what it is sent.
Clients that reach it are answered with the leader's address rather
than served. Without it the node is the leader, and replicas stream
from the same port clients use. Which node is which is configuration;
there is no election.

    msgsrvd --dir PATH --inspect

Scans the log, prints what is in it, and exits without listening for
anything. Of two stopped nodes, this is what says which one is further
along, which is what a promotion has to know.

    msgsrvd --selfcheck

Runs the in-process checks and exits. There is no external test
harness: a binary with no libc is easier to exercise from inside
itself than to link into one.

## Status

Durable on the node that takes the write, and on a replica when the
write asks for one. The wire protocol, WAL, crash recovery, segment
rollover, and an epoll event loop with group commit all work end to
end: a write is acknowledged only after the flush that covers it, and
survives a restart.

Deduplication survives a restart too. The session table is not stored
beside the log, it is counted back out of it: every record carries the
session that wrote it and that session's own sequence, and recovery
already reads every record. A client that resumes its session after a
restart is told the highest sequence the log holds for it and resends
from there, and anything at or below that mark is acknowledged rather
than stored again.

An id is drawn from the kernel rather than counted up, which is what
lets the table be derived and still be safe. A counter would have to
remember what it had already issued, and the only place to remember
that which survives both retention and a promotion is the log, meaning
a record format for it and a flush before any id could be handed out.
Drawing needs neither. A draw is checked against the sessions the node
holds, so a collision would have to be with one that exists only in
the log, at odds far longer than the checksum this log already trusts
to tell a good record from a corrupted one.

What retention does reach is the table. A session whose records have
all been deleted leaves nothing for the scan to count, so a restart
forgets it, and a client resuming it is told the session is unknown
rather than answered from a mark that is no longer there. It opens a
new session and resends, which is what an expired one gets too.

Retention deletes whole segments, and only ones the node has no reason
to keep: below the count it was given, and below what every replica it
expects has confirmed. A reader standing where retention has been is
refused rather than left waiting, which is the difference between
being told the history is gone and being told nothing at all.

Metrics are a second listener that answers any HTTP request with one
Prometheus text document and closes. It never interprets the request,
only finds where the head ends, and it has its own connection slots so
a stuck scraper cannot take one a client needs. A document that would
not fit the buffer is refused rather than cut short, since a metric
missing from a scrape reads as one that never existed.

Replication is a stream and an acknowledgement. The replica names the
sequence it wants, the leader sends whole records from its log, and
the replica answers with what its own flush has covered. A write
asking for a second copy waits for that answer; one that cannot have
it is refused, or told it has a single copy if the client said it
would take one. A replica that has been away asks from where it left
off and the leader reads the rest out of its log, so a restart costs a
catch-up rather than a rebuild.

Reading is a cursor over the same log. A subscription follows it and a
read is the same thing bounded at both ends, ending with an empty
record that says so. Both are served by any node from its own copy, so
a client can follow a replica while writing to the leader, and both
send only what the node has flushed. A client that asks not to be
shown a view older than its own write passes the sequence its
acknowledgement gave it, and is refused rather than answered late if
the node cannot catch up in time.

The durability claims above were checked by killing things: a leader,
a replica and a client each killed under load, and a filesystem filled
on purpose, each asking whether every acknowledged record is still
there exactly once. That suite runs real processes against a
particular deployment, so it is kept with the deployment rather than
here. Four of the five bugs listed in this repository's history came
out of it.

Not built yet. Automatic failover: losing the leader needs an operator
and a runbook, which lives with the deployment rather than here.
Backups: the segments are the backup and nothing copies them off the
node. DELETE, which no projector defines. And this has not been
deployed anywhere yet.

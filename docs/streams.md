# Keys, and whether they should be streams

A record carries a `partition_key`, a 64-bit number the client picks.
Today it is a label: it decides what a reader is shown and nothing
else. Every key shares one log, one sequence, one flush and one
replication stream.

The question this note answers is what it would take for a key to be a
unit of storage instead, a directory of its own under the data
directory, which is how Kafka and NATS JetStream arrange the same
idea. The short version: it would cost a flush per key per batch,
which is measured below and is the number the whole topology was
chosen for. So the index came first, and the split is waiting for a
reason.

## What exists

`wal/index.h` holds, for each key, the first sequence it appears at,
the last, and how many records it has. It is built by the recovery
scan, which already reads every record, and kept current by appends.
Nothing writes it to disk; it is derived.

That bounds a read at both ends. A read for a key starts at the key's
first record rather than at sequence 1 and stops at its last rather
than at the end of the log, and a key the log has never held is
answered without reading anything at all. `msgsrvd --dir PATH
--inspect` lists what a log holds, which nothing could answer before:

    msgsrvd: log /var/lib/msgsrvd/wal segments=1 records=9 ...
    msgsrvd:   key 10 records=3 first=1 last=3
    msgsrvd:   key 20 records=3 first=4 last=6
    msgsrvd:   key 30 records=3 first=7 last=9

From a client, `msgsrv keys --dir PATH` counts the same summary out of
the files, which works on a stopped node and does not need a login on
the one that owns them. Nothing asks a *running* node this over the
network: there is no verb that lists keys, the same gap as there being
no verb that asks a node whether it leads.

It is an accelerator and never an authority. Its table is a fixed
size, and a key it had no room for is answered with "no idea" so the
caller reads what it meant to. It stops claiming to know what is
absent once it has run out of room, which is the difference between
being unhelpful and being wrong.

## What the index cannot do

It holds no position per record, deliberately: that is the thing that
would let a reader touch only its own key's records, and it cannot be
a fixed size because it grows with the log.

So a key whose records are spread evenly through the log still costs a
full walk, and one subscriber per key means each of them walking the
whole log separately. Sparse waypoints between first and last were
tried and removed: a waypoint is a place a record happens to be, not a
boundary, so a reader wanting everything from some sequence onwards
cannot skip to one.

The fix for that is not a bigger index. It is separate storage.

## What separate storage would cost

One log means one `fdatasync` per batch however many keys it touched.
A log per key means one per key touched, so the cost scales with the
number of keys written to at once, not with how many exist.

Measured on the workstation, a batch of 64 records spread across N
preallocated files, median of 40 rounds:

| files | ms per batch | against one |
|-------|-------------|-------------|
| 1  |  5.0 | 1x |
| 2  | 10.0 | 2.0x |
| 4  | 20.0 | 4.0x |
| 8  | 18.1 | 3.6x |
| 16 | 34.9 | 7.0x |
| 32 | 67.1 | 13.4x |

Roughly linear. There is some coalescing above eight files, but not
enough to change the shape: splitting a batch across K files costs
somewhere between K/2 and K flushes.

Put that on real hardware. On the machines this is built for a flush
costs 57 us on one node, whose drives have a power-loss-protected
write cache, and 3.2 ms on another, whose do not. A replicated write
waits for the leader's flush and one replica's, which is why it is
about 3.5 ms today. With eight keys being written at once that becomes
roughly 0.5 ms on the fast node plus 26 ms on the slow one: about 26
ms, against 3.5. The leader is on the fast node precisely to keep one
slow flush off the acknowledgement path, and this would put eight of
them back.

It costs more than flushes. The single dense sequence is what makes
replication a few hundred lines: a replica refuses anything that is not
exactly its next sequence, and recovery is one scan in order. Per-key
logs mean either a replication stream per key or a shared physical log
underneath, and if it is shared then the storage is not really
separate.

## What would force the split anyway

**Retention per key.** Retention drops whole segments and a segment
mixes keys, so "keep orders for a year and telemetry for a week" means
keeping everything for a year. Fixing that inside one log means
compaction, which is a subsystem of its own. This is the strongest
argument for separate storage and the one to watch for.

**A leader per key.** `03-replication.md` names per-partition leaders
as the way this scales past one node's write rate. That needs
placement per key, which needs storage per key.

**Many keys, each with a subscriber.** N subscribers each walking the
whole log is N times the work, and the index does not help.

None of those is true yet, because there is no client yet. When one of
them becomes true, the shape to reach for is probably not a directory
per key but a directory per *group* of keys, few enough that a batch
touches one or two of them: the flush cost is per file touched, so the
number of files is the thing to keep small, not the number of keys.

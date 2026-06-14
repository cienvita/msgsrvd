# msgsrvd design notes

Working notes on internals not covered by the README. Add sections as
the pieces land.

## Open topics

- Replication: prepare/commit on 2+1, ACK with WAL sequence
- Event loop on top of io_uring
- Connection-arena sizing and per-conn buffer limits

## WAL

The WAL is the storage module behind every mutation. `msg.h` already
fixes the contract: each mutation is one WAL record, `op` stays a small
fixed verb set, and `record_type` routes to a projector. The wire
header's `sequence` field is dual-use, client-assigned monotonic id on
a WRITE (for dedup on retry), server-assigned WAL sequence on the ACK.
The durability flags (ACK_REQ, SYNC, REPLICATED, ALLOW_DEGRADED,
DEGRADED) are the ACK gates the WAL has to honour.

The module lives behind a small surface and depends only on `core/` and
`sys/os.h`. It knows nothing about sockets, the event loop, or
replication.

### Record format

Fixed prefix, memcpy-decoded, same idiom as the wire header. crc32c
covers the prefix (with the crc field zeroed) plus the payload, and the
payload is padded to an 8-byte boundary.

```
[0..3]   rec_magic     u32   frames a record, distinct from MSG_MAGIC
[4..7]   len           u32   payload length following the prefix
[8..15]  wal_seq       u64   server-assigned, monotonic, gap-free
[16..23] partition_key u64   copied from the wire header
[24..31] client_seq    u64   from the wire header sequence, for dedup
[32..33] record_type   u16
[34..35] flags         u16
[36..39] crc32c        u32   over prefix (crc zeroed) + payload
```

### Segments

Append-only, split into fixed-size segment files under a WAL directory,
named by their base `wal_seq`. `os_fallocate` preallocates each segment
so appends never extend-on-write. The active segment rolls over when it
fills. Retention and GC of old segments are out of scope for now.

### Append path

1. Encode the record into an arena-backed staging buffer.
2. uring_prep_write at the active segment's current offset.
3. On SYNC, fsync and withhold the ACK until the fsync completes.
4. Assign `wal_seq` and hand it back so the caller fills the ACK
   sequence field.

The first cut runs synchronously via uring_submit_and_wait. Group commit
(many appends behind one fsync, ACK the batch together) lands later with
the event loop.

### Recovery

On open, scan segments in `wal_seq` order and validate each record's
crc32c and length. Stop at the first torn or invalid record, ftruncate
the tail, and resume `next_wal_seq` from there. A replay callback feeds
surviving records to projectors; the projectors themselves are out of
scope here.

### Out of scope (later)

- Replication and two-phase commit. The WAL only exposes the sequence
  and the record bytes to ship.
- Event loop integration and group commit.
- Dedup index. `client_seq` is reserved now; the in-memory
  (partition_key, client_seq) -> wal_seq map that re-ACKs retries comes
  with dispatch.
- Segment GC, checkpointing, compaction.
- Projectors and the READ path.

### Prerequisites

- core: crc32c (done).
- sys: ftruncate, fstat or lseek, mkdir. open, fsync, fdatasync,
  fallocate, and mmap are already present.

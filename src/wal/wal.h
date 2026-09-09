#ifndef MSGSRVD_WAL_H
#define MSGSRVD_WAL_H

#include "core/types.h"
#include "core/err.h"
#include "wal/segment.h"
#include "wal/index.h"

/*
 * The write-ahead log: a directory of segments, one of them active.
 *
 * Segment boundaries are sequence boundaries. A segment's name is the
 * sequence of its first record, and the next segment begins where the
 * previous one stopped, so the set of names alone describes which
 * sequences live where without any of the files being read.
 *
 * That invariant is what forces the rollover order. The active segment
 * is flushed before a new one is created, so the sequences it consumed
 * are on disk before the next segment claims to start after them.
 * Rolling over first would let a crash lose unflushed records and
 * leave the new segment's name asserting a starting point that nothing
 * reaches, which is a hole rather than a short log.
 *
 * Recovery treats the last segment differently from the rest. A torn
 * tail there is the ordinary result of a crash during an append and is
 * dropped. A torn record or a broken boundary anywhere earlier is
 * corruption in the middle of the log: the records after it were
 * acknowledged and are still readable, so discarding them would throw
 * away data a client was promised. The log refuses to open instead,
 * leaving an operator free to restore that segment from a replica.
 * Availability is the thing given up there, deliberately.
 */

/*
 * Bounds the segment table held in memory. At the default segment size
 * this is far more log than a node will hold between retention runs.
 */
#define WAL_MAX_SEGMENTS 1024

typedef struct {
    int32_t     dir_fd;
    int32_t     _pad;
    int64_t     seg_capacity;
    wal_seg_t   active;

    /* Base sequence of every segment on disk, ascending. */
    uint64_t    seg_base[WAL_MAX_SEGMENTS];
    int32_t     seg_count;
    int32_t     _pad2;

    /*
     * Where each key lives. Built by the recovery scan and kept up to
     * date by appends, so a reader after one key does not have to walk
     * the whole log to find where it starts and ends.
     */
    wal_index_t index;
} wal_t;

/* What opening the log found. */
typedef struct {
    int32_t     segments;       /* segment files present */
    bool_t      created;        /* the log was empty and got its first */
    bool_t      torn;           /* the last segment had a tail dropped */
    uint8_t     _pad[2];
    uint64_t    first_seq;      /* lowest sequence still on disk */
    uint64_t    last_seq;       /* highest recovered, first_seq - 1 if none */
    uint64_t    records;        /* records recovered across all segments */
} wal_open_t;

/*
 * Open the log in dir_path, creating the directory's first segment if
 * there is none. Every segment is scanned, so this is where a crash is
 * paid for.
 *
 * scratch is used for both the directory listing and the record scan,
 * so it must be able to hold the largest record present.
 *
 * cb, when given, sees every record recovered across every segment, in
 * sequence order. This is how state that lives in the log rather than
 * beside it, the session table above all, is rebuilt without a second
 * pass over the same bytes.
 */
result_t wal_open(wal_t *w, err_t *e, const char *dir_path,
                  int64_t seg_capacity, uint8_t *scratch, int32_t scratch_len,
                  wal_open_t *out, wal_rec_cb_t cb, void *ctx);

/*
 * Append a record, rolling over to a new segment if it does not fit.
 * The log assigns the sequence and writes it into r. Nothing is
 * durable until wal_sync returns.
 *
 * A record too large for an empty segment is rejected rather than
 * triggering a rollover that could not help.
 */
result_t wal_append(wal_t *w, err_t *e, wal_rec_t *r, const uint8_t *payload,
                    uint8_t *scratch, int32_t scratch_len);

/* Flush the active segment and advance the durable sequence. */
result_t wal_sync(wal_t *w, err_t *e);

/* Highest sequence a completed flush covers. */
uint64_t wal_durable_seq(const wal_t *w);

/* Sequence the next append will be given. */
uint64_t wal_next_seq(const wal_t *w);

/* Lowest sequence still on disk. */
uint64_t wal_first_seq(const wal_t *w);

/* Segments on disk, the active one included. */
int32_t wal_segments(const wal_t *w);

/* Bytes each segment is preallocated to. */
int64_t wal_seg_capacity(const wal_t *w);

/*
 * The keep_from that would leave the newest keep_segments in place, or
 * 0 when the log is no longer than that and nothing can go.
 *
 * Segment boundaries are sequence boundaries, so this is a name in the
 * table rather than anything read off the disk. It answers only what
 * the count allows; a caller with another reason to keep more takes
 * the lower of the two.
 */
uint64_t wal_retain_mark(const wal_t *w, int32_t keep_segments);

/*
 * Delete whole segments that end before keep_from. The active segment
 * is never deleted, and neither is one holding any sequence at or
 * above keep_from, so retention can only remove what the caller has
 * said it no longer needs.
 */
result_t wal_retain(wal_t *w, err_t *e, uint64_t keep_from,
                    int32_t *removed_out);

/*
 * A read position in the log.
 *
 * Replication is the first reader: a replica names the sequence it
 * wants and the leader sends whole records from there. The cursor
 * holds its own read-only descriptor rather than borrowing the log's,
 * which is the append point and moves under it.
 *
 * There is no index from sequence to offset, so seeking scans the
 * segment that holds the sequence from its start. The cursor is what
 * keeps that a one-off: it remembers where it stopped, and streaming
 * on from there is a read at a known offset. A reader that seeks per
 * batch instead of holding a cursor would be rescanning the log for
 * every batch it sends.
 */
typedef struct {
    int32_t     fd;             /* -1 when nothing is open */
    int32_t     _pad;
    uint64_t    seg_base;       /* segment the descriptor refers to */
    int64_t     off;            /* offset of the next record */
    uint64_t    next_seq;       /* sequence of the next record */
} wal_cursor_t;

void wal_cursor_init(wal_cursor_t *c);

/*
 * Place the cursor at seq. ERR_INVALID if the log does not hold it,
 * which is either a replica behind the retained log or one ahead of
 * this one, and neither is recoverable by streaming.
 *
 * scratch must hold the largest record present, as for recovery.
 */
result_t wal_cursor_seek(wal_t *w, err_t *e, wal_cursor_t *c, uint64_t seq,
                         uint8_t *scratch, int32_t scratch_len);

/*
 * Copy whole records from the cursor into buf, stopping at limit_seq,
 * at the end of the written log, or when the next record would not
 * fit. Returns the bytes copied in out_len, 0 when there is nothing to
 * send, and advances the cursor past what it copied.
 *
 * ERR_INVALID when retention has taken the records the cursor stands
 * on, which is the same answer wal_cursor_seek gives for the same
 * position. Saying nothing instead would be indistinguishable from
 * having caught up, and a reader left behind by retention would wait
 * on a log that will never reach it again.
 *
 * Only whole records are copied. A caller streaming these to a replica
 * can hand on what it gets without having to say where the record
 * boundaries are.
 */
result_t wal_cursor_read(wal_t *w, err_t *e, wal_cursor_t *c,
                         uint64_t limit_seq, uint8_t *buf, int32_t buf_len,
                         int32_t *out_len);

result_t wal_cursor_close(wal_cursor_t *c, err_t *e);

/*
 * Append a record that already carries its sequence, as a replica does
 * with what the leader sent it.
 *
 * The sequence has to be the one this log would assign anyway. A
 * replica that has drifted from the leader by even one record cannot
 * be repaired by writing what it was sent, so the mismatch is refused
 * rather than papered over.
 */
result_t wal_append_at(wal_t *w, err_t *e, wal_rec_t *r,
                       const uint8_t *payload, uint8_t *scratch,
                       int32_t scratch_len);

/* Where each key lives in this log. */
const wal_index_t *wal_index(const wal_t *w);

result_t wal_close(wal_t *w, err_t *e);

#endif /* MSGSRVD_WAL_H */

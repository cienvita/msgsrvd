#ifndef MSGSRVD_WAL_H
#define MSGSRVD_WAL_H

#include "core/types.h"
#include "core/err.h"
#include "wal/segment.h"

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
 */
result_t wal_open(wal_t *w, err_t *e, const char *dir_path,
                  int64_t seg_capacity, uint8_t *scratch, int32_t scratch_len,
                  wal_open_t *out);

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

/*
 * Delete whole segments that end before keep_from. The active segment
 * is never deleted, and neither is one holding any sequence at or
 * above keep_from, so retention can only remove what the caller has
 * said it no longer needs.
 */
result_t wal_retain(wal_t *w, err_t *e, uint64_t keep_from,
                    int32_t *removed_out);

result_t wal_close(wal_t *w, err_t *e);

#endif /* MSGSRVD_WAL_H */

#ifndef MSGSRVD_WAL_INDEX_H
#define MSGSRVD_WAL_INDEX_H

#include "core/types.h"
#include "wal/record.h"

/*
 * Where each partition key lives in the log.
 *
 * One sequence carries every key together, so answering "every record
 * for this key" means walking the log and dropping most of what is
 * read. This bounds that walk at both ends: the first sequence a key
 * appears at and the last. A reader starts at the first instead of at
 * sequence 1, stops at the last instead of at the end of the log, and
 * a key that is not there at all is answered without reading anything.
 *
 * What it deliberately does not hold is a position per record. That
 * would be the thing that stops a reader touching records belonging to
 * other keys, and it cannot be a fixed size: it grows with the log.
 * So a key whose records are spread evenly through the log still costs
 * a full walk, and the fix for that is not a bigger index, it is
 * separate storage per key. See docs/streams.md for what that would
 * cost.
 *
 * Sparse waypoints between first and last were tried and removed. A
 * waypoint is a place a record happens to be, not a boundary, so a
 * reader wanting everything from some sequence onwards cannot skip to
 * one: there may be records of that key in between.
 *
 * The table is an accelerator and never an authority. A key it has no
 * room for is answered with "no idea", and the caller walks the log as
 * it would have anyway. Being unhelpful is the only way it can be
 * wrong.
 *
 * Nothing writes it to disk. It is derived from the log, and the
 * recovery scan that rebuilds it is already reading every record.
 */

/* Keys tracked. Beyond this, later keys are simply not known. */
#define WAL_INDEX_KEYS  1024

typedef struct {
    uint64_t    key;
    uint64_t    first;      /* first sequence this key appears at */
    uint64_t    last;       /* most recent */
    uint64_t    count;      /* records with this key */
} wal_index_key_t;

typedef struct {
    wal_index_key_t keys[WAL_INDEX_KEYS];
    int32_t         count;
    int32_t         _pad;
    uint64_t        unknown;    /* records for keys there was no room for */
} wal_index_t;

void wal_index_init(wal_index_t *ix);

/* Account for a record, from a recovery scan or from an append. */
void wal_index_add(wal_index_t *ix, const wal_rec_t *r);

/*
 * The range a reader after this key has to cover, given the earliest
 * sequence it cares about. FALSE when the key is unknown, which means
 * the caller learns nothing and reads what it meant to.
 *
 * out_from is never earlier than `from` and never later than the first
 * record of the key at or after it, so acting on this can save a walk
 * and cannot skip a record.
 */
bool_t wal_index_range(const wal_index_t *ix, uint64_t key, uint64_t from,
                       uint64_t *out_from, uint64_t *out_last);

/*
 * TRUE when the table has room for every key the log holds, which
 * makes "this key is not in the table" mean "this key is not in the
 * log". That is the difference between a reader having to walk the
 * whole log to find nothing and being told so at once.
 */
bool_t wal_index_complete(const wal_index_t *ix);

/* Walking the table, for reporting what a log holds. */
int32_t wal_index_count(const wal_index_t *ix);
const wal_index_key_t *wal_index_at(const wal_index_t *ix, int32_t i);

#endif /* MSGSRVD_WAL_INDEX_H */

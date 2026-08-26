#ifndef MSGSRVD_WAL_SEGMENT_H
#define MSGSRVD_WAL_SEGMENT_H

#include "core/types.h"
#include "core/err.h"
#include "wal/record.h"

/*
 * A WAL segment: one preallocated file holding a contiguous run of
 * records, named for the first sequence it may contain.
 *
 * Preallocation is the point. A record appended into space that is
 * already allocated changes no file metadata, so fdatasync has only
 * the data to flush and not an extent-tree update as well. On the two
 * origins, whose fsync is measured in milliseconds, that is the
 * difference worth having; the flush to the device itself is not
 * something any of this can avoid.
 *
 * Writes are buffered, followed by fdatasync. The design calls for
 * O_DIRECT eventually, which needs every write to be 512-byte aligned
 * in offset, length and memory address. Records are 8-byte aligned, so
 * O_DIRECT only becomes possible once appends are staged through a
 * block-aligned batch buffer. That buffer belongs to the event loop,
 * which does not exist yet, so this layer stays buffered until it
 * does.
 *
 * Segment names are the base sequence in fixed-width decimal plus
 * ".seg", so a directory listing sorts into WAL order without parsing.
 */

#define WAL_SEG_DIGITS   20                     /* fits any uint64 */
#define WAL_SEG_NAME_MAX (WAL_SEG_DIGITS + 5)   /* digits + ".seg" + NUL */

typedef struct {
    int32_t     fd;
    int32_t     dir_fd;         /* not owned; borrowed for the create fsync */
    uint64_t    base_seq;       /* sequence of the first record here */
    uint64_t    next_seq;       /* sequence the next append will be given */
    uint64_t    durable_seq;    /* highest sequence a completed sync covers */
    int64_t     capacity;       /* preallocated bytes */
    int64_t     write_off;      /* append point */
    int64_t     synced_off;     /* bytes a completed sync covers */
} wal_seg_t;

/* What a recovery scan found. */
typedef struct {
    uint64_t    records;    /* records accepted */
    uint64_t    last_seq;   /* last good sequence, base_seq - 1 if none */
    int64_t     end_off;    /* append point after recovery */
    bool_t      torn;       /* a partial or corrupt record was dropped */
    uint8_t     _pad[7];
} wal_seg_scan_t;

/* Build a segment file name for a base sequence. out holds at least
 * WAL_SEG_NAME_MAX bytes. */
void wal_seg_name(uint64_t base_seq, char *out);

/*
 * Create a segment and preallocate it. Fails if one already exists,
 * since silently reusing a file would mix two lives of the WAL
 * together.
 *
 * The directory is fsync'd as well as the file: without that the file
 * can survive a crash while its name does not, which is the same as
 * losing it.
 */
result_t wal_seg_create(wal_seg_t *s, err_t *e, int32_t dir_fd,
                        uint64_t base_seq, int64_t capacity);

/*
 * Open an existing segment. The caller states the capacity it expects
 * rather than the file being asked, because the scan has to be bounded
 * by something trustworthy; a file shorter than that simply ends the
 * scan early.
 *
 * State is left as if the segment were empty. Call wal_seg_recover
 * before appending.
 */
result_t wal_seg_open(wal_seg_t *s, err_t *e, int32_t dir_fd,
                      uint64_t base_seq, int64_t capacity);

/*
 * Scan from the start, accepting records until one fails, and leave
 * the segment ready to append at the end of the last good one.
 *
 * A record is accepted when its length is in range, it fits inside the
 * segment, its checksum matches, and its sequence is exactly one past
 * the last. That last test is what keeps a stale record from an
 * earlier life of these bytes from being mistaken for a current one:
 * the checksum alone would pass, because the record was written
 * correctly, just not now.
 *
 * A torn tail is dropped rather than erased. The bytes stay where they
 * are and the next append overwrites them, which is safe because the
 * scan stops on the same checksum and sequence tests either way.
 * Erasing it would mean a write and a flush on the startup path to
 * remove data that is already unreachable.
 *
 * scratch holds one block of the scan and must be able to fit the
 * largest record present, or the scan fails rather than skipping it.
 */
result_t wal_seg_recover(wal_seg_t *s, err_t *e, uint8_t *scratch,
                         int32_t scratch_len, wal_seg_scan_t *out);

/* TRUE if a record with this payload still fits in the segment. */
bool_t wal_seg_fits(const wal_seg_t *s, int32_t payload_len);

/*
 * Append one record. The segment assigns the sequence and writes it
 * into r, so the caller learns what it got. Nothing is durable until
 * wal_seg_sync returns.
 *
 * Returns ERR_STORAGE with no bytes written if the record does not fit;
 * that is the caller's cue to roll over to a new segment.
 */
result_t wal_seg_append(wal_seg_t *s, err_t *e, wal_rec_t *r,
                        const uint8_t *payload, uint8_t *scratch,
                        int32_t scratch_len);

/*
 * Flush everything appended so far and advance durable_seq.
 *
 * fdatasync rather than fsync: the file's size and extents were fixed
 * at creation, so there is no metadata to flush with it. A sync with
 * nothing outstanding does no syscall at all, since on this path the
 * flush is the expensive operation and skipping an idle one is worth
 * the branch.
 */
result_t wal_seg_sync(wal_seg_t *s, err_t *e);

/* Close the segment. The directory fd is not touched. */
result_t wal_seg_close(wal_seg_t *s, err_t *e);

#endif /* MSGSRVD_WAL_SEGMENT_H */

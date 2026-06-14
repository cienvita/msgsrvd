#ifndef MSGSRVD_WAL_LOG_H
#define MSGSRVD_WAL_LOG_H

#include "core/types.h"
#include "core/err.h"
#include "store/wal.h"
#include "io/uring.h"

/*
 * WAL log: durable append over segment files.
 *
 * Builds on the record codec (store/wal.h) and segment naming
 * (store/wal_seg.h). Records are appended to the active segment via
 * io_uring; when the next record would not fit, the log rolls to a new
 * segment named by the sequence it starts at.
 *
 * This first cut is synchronous: each append submits its write (and, on
 * request, an fdatasync) and waits for the completion before returning.
 * Group commit and async submission come with the event loop.
 *
 * On open the log recovers: it finds the highest existing segment,
 * replays its records validating each crc, stops at the first torn or
 * zero record, truncates that tail, and resumes the append offset and
 * next sequence from there. An empty directory starts a fresh log at
 * sequence 0.
 */

#define WAL_DIR_MAX 256

typedef struct {
    uring_t    *ring;           /* borrowed ring for I/O */
    uint8_t    *scratch;        /* caller-owned staging buffer for encode */
    int32_t     scratch_cap;
    int32_t     seg_size;       /* segment capacity in bytes */
    int32_t     active_fd;      /* fd of the active segment */
    int32_t     active_off;     /* append offset within the active segment */
    uint64_t    active_base;    /* base wal_seq of the active segment */
    uint64_t    next_seq;       /* next wal_seq to assign */
    int32_t     dir_len;
    uint8_t     _pad[4];
    char        dir[WAL_DIR_MAX];
} wal_log_t;

/*
 * Open a WAL log under dir, creating the directory if needed. seg_size
 * is the per-segment capacity and must hold at least one record. ring is
 * borrowed for the lifetime of the log. scratch is a staging buffer the
 * encode step and the recovery scan write into; it must be at least as
 * large as the largest record. Recovers existing segments if present.
 */
result_t wal_log_open(wal_log_t *w, err_t *e, const char *dir,
                      int32_t seg_size, uring_t *ring,
                      uint8_t *scratch, int32_t scratch_cap);

/*
 * Append one record. meta supplies partition_key, client_seq,
 * record_type, and flags; magic, len, wal_seq, and crc are set here. When
 * sync is TRUE the segment is fdatasync'd before returning. The assigned
 * wal_seq is written to *out_seq.
 */
result_t wal_log_append(wal_log_t *w, err_t *e, const wal_rec_t *meta,
                        const uint8_t *payload, int32_t payload_len,
                        bool_t sync, uint64_t *out_seq);

/* Close the active segment. */
void wal_log_close(wal_log_t *w);

#endif /* MSGSRVD_WAL_LOG_H */

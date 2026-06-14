#include "store/wal_log.h"
#include "store/wal_seg.h"
#include "core/mem.h"
#include "sys/os.h"

/*
 * Build the path of the segment with the given base sequence into out.
 * out must hold WAL_DIR_MAX + 1 + WAL_SEG_NAME_LEN + 1 bytes.
 */
static void seg_path(const wal_log_t *w, uint64_t base, char *out)
{
    mem_copy((uint8_t *)out, (const uint8_t *)w->dir, w->dir_len);
    out[w->dir_len] = '/';
    wal_seg_name(base, out + w->dir_len + 1);
}

/* Open (creating if needed) and preallocate the segment for base. */
static result_t open_segment(wal_log_t *w, err_t *e, uint64_t base,
                             int32_t *fd_out)
{
    char     path[WAL_DIR_MAX + 1 + WAL_SEG_NAME_LEN + 1];
    int32_t  fd;
    result_t r;

    seg_path(w, base, path);

    r = os_open(e, path, O_RDWR | O_CREAT, 0644, &fd);
    if (!result_ok(r))
        return r;

    /*
     * Preallocate so appends never extend-on-write. Filesystems without
     * fallocate (tmpfs, some network mounts) report EOPNOTSUPP; fall back
     * to sizing the file with ftruncate, which leaves it sparse but keeps
     * writes within a fixed length.
     */
    r = os_fallocate(e, fd, 0, 0, (uint64_t)w->seg_size);
    if (!result_ok(r)) {
        if (r.detail != EOPNOTSUPP) {
            os_close(e, fd);
            return r;
        }
        err_init(e);
        r = os_ftruncate(e, fd, (uint64_t)w->seg_size);
        if (!result_ok(r)) {
            os_close(e, fd);
            return r;
        }
    }

    *fd_out = fd;
    return RESULT_OK;
}

/* Roll to a fresh segment starting at the next sequence. */
static result_t roll_segment(wal_log_t *w, err_t *e)
{
    int32_t  fd;
    result_t r;

    r = open_segment(w, e, w->next_seq, &fd);
    if (!result_ok(r))
        return r;

    r = os_close(e, w->active_fd);
    if (!result_ok(r)) {
        os_close(e, fd);
        return r;
    }

    w->active_fd = fd;
    w->active_base = w->next_seq;
    w->active_off = 0;
    return RESULT_OK;
}

result_t wal_log_open(wal_log_t *w, err_t *e, const char *dir,
                      int32_t seg_size, uring_t *ring,
                      uint8_t *scratch, int32_t scratch_cap)
{
    int32_t  dlen = 0;
    int32_t  fd;
    result_t r;

    while (dir[dlen] != '\0')
        dlen++;
    if (dlen == 0 || dlen >= WAL_DIR_MAX) {
        ERR_PUSH_INT(e, ERR_INVALID, dlen);
        return RESULT_ERR(ERR_INVALID, dlen);
    }
    if (seg_size < WAL_REC_HDR_SIZE || scratch_cap < WAL_REC_HDR_SIZE) {
        ERR_PUSH_INT(e, ERR_INVALID, seg_size);
        return RESULT_ERR(ERR_INVALID, seg_size);
    }

    mem_zero((uint8_t *)w, (int32_t)sizeof(*w));
    w->ring = ring;
    w->scratch = scratch;
    w->scratch_cap = scratch_cap;
    w->seg_size = seg_size;
    mem_copy((uint8_t *)w->dir, (const uint8_t *)dir, dlen);
    w->dir_len = dlen;
    w->next_seq = 0;
    w->active_base = 0;
    w->active_off = 0;

    /* Create the directory; an existing one is fine. */
    r = os_mkdir(e, dir, 0755);
    if (!result_ok(r)) {
        if (r.detail != EEXIST)
            return r;
        err_init(e);
    }

    r = open_segment(w, e, 0, &fd);
    if (!result_ok(r))
        return r;
    w->active_fd = fd;

    return RESULT_OK;
}

/* Submit one SQE-producing op and wait for its completion. */
static result_t submit_one(wal_log_t *w, err_t *e, io_uring_sqe_t *sqe,
                           int32_t want_res)
{
    io_uring_cqe_t cqe;
    result_t       r;

    (void)sqe;  /* already linked into the ring by uring_get_sqe */

    r = uring_wait_cqe(w->ring, e, 1, &cqe);
    if (!result_ok(r))
        return r;
    if (cqe.res != want_res) {
        ERR_PUSH_INT(e, ERR_STORAGE, cqe.res);
        return RESULT_ERR(ERR_STORAGE, cqe.res);
    }
    return RESULT_OK;
}

result_t wal_log_append(wal_log_t *w, err_t *e, const wal_rec_t *meta,
                        const uint8_t *payload, int32_t payload_len,
                        bool_t sync, uint64_t *out_seq)
{
    int32_t         total = wal_rec_size(payload_len);
    wal_rec_t       rec;
    io_uring_sqe_t *sqe;
    result_t        r;

    if (payload_len < 0 || total > w->scratch_cap) {
        ERR_PUSH_INT(e, ERR_INVALID, total);
        return RESULT_ERR(ERR_INVALID, total);
    }
    if (total > w->seg_size) {
        ERR_PUSH_INT(e, ERR_INVALID, total);
        return RESULT_ERR(ERR_INVALID, total);
    }

    /* Roll if this record will not fit in the active segment. */
    if (w->active_off + total > w->seg_size) {
        r = roll_segment(w, e);
        if (!result_ok(r))
            return r;
    }

    rec = *meta;
    rec.wal_seq = w->next_seq;
    if (wal_encode_rec(w->scratch, w->scratch_cap, &rec,
                       payload, payload_len) != total) {
        ERR_PUSH(e, ERR_INVALID);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    sqe = uring_get_sqe(w->ring);
    if (!sqe) {
        ERR_PUSH(e, ERR_AGAIN);
        return RESULT_ERR(ERR_AGAIN, 0);
    }
    uring_prep_write(sqe, w->active_fd, w->scratch, (uint32_t)total,
                     (uint64_t)w->active_off, w->next_seq);
    r = submit_one(w, e, sqe, total);
    if (!result_ok(r))
        return r;

    if (sync) {
        sqe = uring_get_sqe(w->ring);
        if (!sqe) {
            ERR_PUSH(e, ERR_AGAIN);
            return RESULT_ERR(ERR_AGAIN, 0);
        }
        uring_prep_fsync(sqe, w->active_fd, IORING_FSYNC_DATASYNC,
                         w->next_seq);
        r = submit_one(w, e, sqe, 0);
        if (!result_ok(r))
            return r;
    }

    *out_seq = w->next_seq;
    w->active_off += total;
    w->next_seq++;
    return RESULT_OK;
}

void wal_log_close(wal_log_t *w)
{
    err_t e;

    if (w->active_fd >= 0) {
        err_init(&e);
        os_close(&e, w->active_fd);
        w->active_fd = -1;
    }
}

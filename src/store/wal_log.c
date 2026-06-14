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

/*
 * Size a segment to seg_size. Preallocate so appends never
 * extend-on-write; filesystems without fallocate (tmpfs, some network
 * mounts) report EOPNOTSUPP, so fall back to ftruncate, which leaves the
 * file sparse but keeps writes within a fixed length.
 */
static result_t prealloc(wal_log_t *w, err_t *e, int32_t fd)
{
    result_t r = os_fallocate(e, fd, 0, 0, (uint64_t)w->seg_size);
    if (!result_ok(r)) {
        if (r.detail != EOPNOTSUPP)
            return r;
        err_init(e);
        r = os_ftruncate(e, fd, (uint64_t)w->seg_size);
        if (!result_ok(r))
            return r;
    }
    return RESULT_OK;
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

    r = prealloc(w, e, fd);
    if (!result_ok(r)) {
        os_close(e, fd);
        return r;
    }

    *fd_out = fd;
    return RESULT_OK;
}

/* Read len bytes at off into buf via io_uring. Writes bytes read to *got. */
static result_t read_at(wal_log_t *w, err_t *e, int32_t fd, uint8_t *buf,
                        int32_t len, int64_t off, int32_t *got)
{
    io_uring_sqe_t *sqe;
    io_uring_cqe_t  cqe;
    result_t        r;

    sqe = uring_get_sqe(w->ring);
    if (!sqe) {
        ERR_PUSH(e, ERR_AGAIN);
        return RESULT_ERR(ERR_AGAIN, 0);
    }
    uring_prep_read(sqe, fd, buf, (uint32_t)len, (uint64_t)off, 0);
    r = uring_wait_cqe(w->ring, e, 1, &cqe);
    if (!result_ok(r))
        return r;
    if (cqe.res < 0) {
        ERR_PUSH_INT(e, ERR_STORAGE, cqe.res);
        return RESULT_ERR(ERR_STORAGE, cqe.res);
    }
    *got = cqe.res;
    return RESULT_OK;
}

/*
 * Scan the directory for segment files and report the highest base
 * sequence. *found is FALSE when the directory holds no segments.
 */
static result_t scan_dir_max_base(wal_log_t *w, err_t *e,
                                  uint64_t *max_base, bool_t *found)
{
    uint64_t dbuf[128];     /* 1 KiB dirent buffer, 8-byte aligned */
    int32_t  dfd;
    long     n;
    result_t r;

    *found = FALSE;
    *max_base = 0;

    r = os_open(e, w->dir, O_RDONLY | O_DIRECTORY, 0, &dfd);
    if (!result_ok(r))
        return r;

    for (;;) {
        int32_t bpos = 0;

        r = os_getdents64(e, dfd, dbuf, (int32_t)sizeof(dbuf), &n);
        if (!result_ok(r)) {
            os_close(e, dfd);
            return r;
        }
        if (n == 0)
            break;

        while (bpos < (int32_t)n) {
            linux_dirent64_t *ent =
                (linux_dirent64_t *)((uint8_t *)dbuf + bpos);
            uint64_t base;

            if (wal_seg_parse(ent->d_name, &base)) {
                if (!*found || base > *max_base) {
                    *max_base = base;
                    *found = TRUE;
                }
            }
            bpos += ent->d_reclen;
        }
    }

    return os_close(e, dfd);
}

/*
 * Recover the highest segment: replay its records validating each crc,
 * stop at the first torn or zero record, truncate that tail, and resume
 * the append offset and next sequence from there.
 */
static result_t recover_segment(wal_log_t *w, err_t *e, uint64_t base)
{
    char     path[WAL_DIR_MAX + 1 + WAL_SEG_NAME_LEN + 1];
    int32_t  fd;
    int32_t  off = 0;
    uint64_t next = base;
    result_t r;

    seg_path(w, base, path);
    r = os_open(e, path, O_RDWR, 0, &fd);
    if (!result_ok(r))
        return r;

    for (;;) {
        int32_t        got = 0;
        uint32_t       magic;
        uint32_t       plen;
        int32_t        total;
        wal_rec_t      rec;
        const uint8_t  *pl;

        if (off + WAL_REC_HDR_SIZE > w->seg_size)
            break;

        r = read_at(w, e, fd, w->scratch, WAL_REC_HDR_SIZE, off, &got);
        if (!result_ok(r)) {
            os_close(e, fd);
            return r;
        }
        if (got < WAL_REC_HDR_SIZE)
            break;

        mem_copy((uint8_t *)&magic, w->scratch, 4);
        if (magic != WAL_REC_MAGIC)
            break;
        mem_copy((uint8_t *)&plen, w->scratch + 4, 4);
        if (plen > (uint32_t)WAL_REC_MAX_LEN)
            break;

        total = wal_rec_size((int32_t)plen);
        if (total > w->scratch_cap || off + total > w->seg_size)
            break;

        r = read_at(w, e, fd, w->scratch + WAL_REC_HDR_SIZE,
                    total - WAL_REC_HDR_SIZE, off + WAL_REC_HDR_SIZE, &got);
        if (!result_ok(r)) {
            os_close(e, fd);
            return r;
        }
        if (got < total - WAL_REC_HDR_SIZE)
            break;

        if (wal_decode_rec(w->scratch, total, &rec, &pl) != total)
            break;

        off += total;
        next = rec.wal_seq + 1;
    }

    /* Drop the torn tail, then restore the segment to full size. */
    r = os_ftruncate(e, fd, (uint64_t)off);
    if (!result_ok(r)) {
        os_close(e, fd);
        return r;
    }
    r = prealloc(w, e, fd);
    if (!result_ok(r)) {
        os_close(e, fd);
        return r;
    }

    w->active_fd = fd;
    w->active_base = base;
    w->active_off = off;
    w->next_seq = next;
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

    /* If segments already exist, recover from the highest one. */
    {
        uint64_t max_base;
        bool_t   found;

        r = scan_dir_max_base(w, e, &max_base, &found);
        if (!result_ok(r))
            return r;
        if (found)
            return recover_segment(w, e, max_base);
    }

    /* Fresh log: start at sequence 0. */
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

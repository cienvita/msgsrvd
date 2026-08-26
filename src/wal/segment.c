#include "wal/segment.h"
#include "core/mem.h"
#include "sys/os.h"

void wal_seg_name(uint64_t base_seq, char *out)
{
    int32_t i;

    for (i = WAL_SEG_DIGITS - 1; i >= 0; i--) {
        out[i] = (char)('0' + (int32_t)(base_seq % 10));
        base_seq /= 10;
    }
    out[WAL_SEG_DIGITS + 0] = '.';
    out[WAL_SEG_DIGITS + 1] = 's';
    out[WAL_SEG_DIGITS + 2] = 'e';
    out[WAL_SEG_DIGITS + 3] = 'g';
    out[WAL_SEG_DIGITS + 4] = '\0';
}

/* Fresh segment state: nothing written, nothing durable. */
static void seg_reset(wal_seg_t *s, int32_t fd, int32_t dir_fd,
                      uint64_t base_seq, int64_t capacity)
{
    s->fd = fd;
    s->dir_fd = dir_fd;
    s->base_seq = base_seq;
    s->next_seq = base_seq;
    s->durable_seq = base_seq - 1;
    s->capacity = capacity;
    s->write_off = 0;
    s->synced_off = 0;
}

result_t wal_seg_create(wal_seg_t *s, err_t *e, int32_t dir_fd,
                        uint64_t base_seq, int64_t capacity)
{
    char     name[WAL_SEG_NAME_MAX];
    int32_t  fd = -1;
    result_t r;

    if (base_seq == 0 || capacity < WAL_REC_HEADER_SIZE) {
        ERR_PUSH_INT(e, ERR_INVALID, (int64_t)capacity);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    wal_seg_name(base_seq, name);

    r = os_openat(e, dir_fd, name,
                  O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, MODE_0600, &fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        return r;
    }

    /* Allocate the whole segment up front so appends never extend it. */
    r = os_fallocate(e, fd, 0, 0, (uint64_t)capacity);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        os_close(e, fd);
        return r;
    }

    r = os_fsync(e, fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        os_close(e, fd);
        return r;
    }

    /*
     * The file's contents are durable at this point but its name is
     * not. Without this the segment can come back from a crash
     * unreferenced, which is indistinguishable from never having been
     * created.
     */
    r = os_fsync(e, dir_fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        os_close(e, fd);
        return r;
    }

    seg_reset(s, fd, dir_fd, base_seq, capacity);
    return RESULT_OK;
}

result_t wal_seg_open(wal_seg_t *s, err_t *e, int32_t dir_fd,
                      uint64_t base_seq, int64_t capacity)
{
    char     name[WAL_SEG_NAME_MAX];
    int32_t  fd = -1;
    result_t r;

    if (base_seq == 0 || capacity < WAL_REC_HEADER_SIZE) {
        ERR_PUSH_INT(e, ERR_INVALID, (int64_t)capacity);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    wal_seg_name(base_seq, name);

    r = os_openat(e, dir_fd, name, O_RDWR | O_CLOEXEC, 0, &fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        return r;
    }

    seg_reset(s, fd, dir_fd, base_seq, capacity);
    return RESULT_OK;
}

result_t wal_seg_recover(wal_seg_t *s, err_t *e, uint8_t *scratch,
                         int32_t scratch_len, wal_seg_scan_t *out)
{
    int64_t  off = 0;
    uint64_t expected = s->base_seq;
    uint64_t records = 0;
    bool_t   torn = FALSE;
    bool_t   done = FALSE;

    if (!scratch || scratch_len < WAL_REC_HEADER_SIZE || !out) {
        ERR_PUSH_INT(e, ERR_INVALID, scratch_len);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    while (off < s->capacity) {
        int32_t  want = scratch_len;
        int32_t  n = 0;
        int32_t  pos = 0;
        result_t r;

        if ((int64_t)want > s->capacity - off)
            want = (int32_t)(s->capacity - off);

        r = os_pread_full(e, s->fd, scratch, want, off, &n);
        if (!result_ok(r)) {
            ERR_PUSH_OFFSET(e, ERR_STORAGE, (uint64_t)off);
            return r;
        }

        /* Too little left to hold a header, so nothing more is there. */
        if (n < WAL_REC_HEADER_SIZE)
            break;

        while (pos + WAL_REC_HEADER_SIZE <= n) {
            wal_rec_t rec;
            int32_t   size;

            wal_rec_decode(scratch + pos, n - pos, &rec);

            /* Preallocated space reads as zeros: a clean end. */
            if (wal_rec_is_end(&rec)) {
                done = TRUE;
                break;
            }

            size = wal_rec_size((int32_t)rec.len);
            if (size == 0 || off + pos + size > s->capacity) {
                torn = TRUE;
                done = TRUE;
                break;
            }

            if (pos + size > n) {
                /*
                 * The record runs past this block. Re-read starting at
                 * the record, unless it is already at the front, in
                 * which case scratch is simply too small to hold it and
                 * skipping it would be worse than stopping.
                 */
                if (pos == 0) {
                    ERR_PUSH_INT(e, ERR_NOMEM, size);
                    return RESULT_ERR(ERR_NOMEM, size);
                }
                break;
            }

            if (!wal_rec_verify(scratch + pos, n - pos, &rec) ||
                rec.seq != expected) {
                torn = TRUE;
                done = TRUE;
                break;
            }

            expected++;
            records++;
            pos += size;
        }

        off += pos;

        if (done)
            break;

        /* A refill that consumed nothing would spin. */
        if (pos == 0)
            break;
    }

    out->records = records;
    out->last_seq = expected - 1;
    out->end_off = off;
    out->torn = torn;
    mem_zero(out->_pad, (int32_t)sizeof(out->_pad));

    /*
     * What is on disk is what is durable: it was read back from the
     * file, so no flush of ours is outstanding against it.
     */
    s->next_seq = expected;
    s->durable_seq = expected - 1;
    s->write_off = off;
    s->synced_off = off;

    return RESULT_OK;
}

bool_t wal_seg_fits(const wal_seg_t *s, int32_t payload_len)
{
    int32_t size = wal_rec_size(payload_len);

    if (size == 0)
        return FALSE;
    return (s->write_off + size <= s->capacity) ? TRUE : FALSE;
}

result_t wal_seg_append(wal_seg_t *s, err_t *e, wal_rec_t *r,
                        const uint8_t *payload, uint8_t *scratch,
                        int32_t scratch_len)
{
    int32_t  total;
    result_t res;

    if (!r || !scratch) {
        ERR_PUSH(e, ERR_INVALID);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    /* The segment owns sequence assignment, not the caller. */
    r->seq = s->next_seq;

    total = wal_rec_encode(scratch, scratch_len, r, payload);
    if (total == 0) {
        ERR_PUSH_INT(e, ERR_NOMEM, (int64_t)r->len);
        return RESULT_ERR(ERR_NOMEM, 0);
    }

    if (s->write_off + total > s->capacity) {
        ERR_PUSH_OFFSET(e, ERR_STORAGE, (uint64_t)s->write_off);
        return RESULT_ERR(ERR_STORAGE, 0);
    }

    res = os_pwrite_full(e, s->fd, scratch, total, s->write_off);
    if (!result_ok(res)) {
        ERR_PUSH_OFFSET(e, ERR_STORAGE, (uint64_t)s->write_off);
        return res;
    }

    s->write_off += total;
    s->next_seq++;
    return RESULT_OK;
}

result_t wal_seg_sync(wal_seg_t *s, err_t *e)
{
    result_t r;

    if (s->synced_off == s->write_off)
        return RESULT_OK;

    r = os_fdatasync(e, s->fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        return r;
    }

    s->synced_off = s->write_off;
    s->durable_seq = s->next_seq - 1;
    return RESULT_OK;
}

result_t wal_seg_close(wal_seg_t *s, err_t *e)
{
    result_t r;

    if (s->fd < 0)
        return RESULT_OK;

    r = os_close(e, s->fd);
    s->fd = -1;
    return r;
}

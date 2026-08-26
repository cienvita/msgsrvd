#include "wal/wal.h"
#include "core/mem.h"
#include "sys/os.h"

/*
 * Parse a segment file name. Accepts exactly the form wal_seg_name
 * produces, fixed-width digits followed by ".seg", and rejects
 * anything else so that unrelated files in the directory are ignored
 * rather than guessed at.
 */
static bool_t seg_name_parse(const char *name, int32_t max, uint64_t *out)
{
    uint64_t v = 0;
    int32_t  i;

    if (max < WAL_SEG_NAME_MAX - 1)
        return FALSE;

    for (i = 0; i < WAL_SEG_DIGITS; i++) {
        if (name[i] < '0' || name[i] > '9')
            return FALSE;
        /* A name wide enough to overflow is not one we wrote. */
        if (v > (uint64_t)0xFFFFFFFFFFFFFFFFULL / 10)
            return FALSE;
        v = v * 10 + (uint64_t)(name[i] - '0');
    }

    if (name[WAL_SEG_DIGITS + 0] != '.' || name[WAL_SEG_DIGITS + 1] != 's' ||
        name[WAL_SEG_DIGITS + 2] != 'e' || name[WAL_SEG_DIGITS + 3] != 'g' ||
        name[WAL_SEG_DIGITS + 4] != '\0')
        return FALSE;

    if (v == 0)
        return FALSE;   /* sequences start at 1 */

    *out = v;
    return TRUE;
}

/* Insert into the ascending segment table. FALSE if it is full. */
static bool_t seg_table_insert(wal_t *w, uint64_t base)
{
    int32_t i;
    int32_t j;

    if (w->seg_count >= WAL_MAX_SEGMENTS)
        return FALSE;

    for (i = 0; i < w->seg_count; i++) {
        if (w->seg_base[i] == base)
            return TRUE;        /* already known */
        if (w->seg_base[i] > base)
            break;
    }

    for (j = w->seg_count; j > i; j--)
        w->seg_base[j] = w->seg_base[j - 1];

    w->seg_base[i] = base;
    w->seg_count++;
    return TRUE;
}

/* Read the directory and fill the segment table. */
static result_t seg_table_load(wal_t *w, err_t *e, uint8_t *scratch,
                               int32_t scratch_len)
{
    w->seg_count = 0;

    for (;;) {
        int32_t  n = 0;
        int32_t  off = 0;
        result_t r = os_getdents64(e, w->dir_fd, scratch, scratch_len, &n);

        if (!result_ok(r)) {
            ERR_PUSH(e, ERR_STORAGE);
            return r;
        }
        if (n == 0)
            break;

        while (off < n) {
            uint16_t reclen = 0;
            uint64_t base = 0;
            int32_t  name_max;

            mem_copy((uint8_t *)&reclen, scratch + off + DIRENT64_RECLEN_OFF,
                     (int32_t)sizeof(reclen));

            /* A zero or oversized record length would not advance. */
            if (reclen < DIRENT64_NAME_OFF || off + (int32_t)reclen > n) {
                ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)reclen);
                return RESULT_ERR(ERR_STORAGE, 0);
            }

            name_max = (int32_t)reclen - DIRENT64_NAME_OFF;
            if (seg_name_parse((const char *)(scratch + off +
                                              DIRENT64_NAME_OFF),
                               name_max, &base)) {
                if (!seg_table_insert(w, base)) {
                    ERR_PUSH_INT(e, ERR_NOMEM, (int64_t)w->seg_count);
                    return RESULT_ERR(ERR_NOMEM, w->seg_count);
                }
            }

            off += (int32_t)reclen;
        }
    }

    return RESULT_OK;
}

result_t wal_open(wal_t *w, err_t *e, const char *dir_path,
                  int64_t seg_capacity, uint8_t *scratch, int32_t scratch_len,
                  wal_open_t *out)
{
    result_t r;
    int32_t  i;
    uint64_t expected;
    uint64_t records = 0;

    if (!w || !out || seg_capacity < WAL_REC_HEADER_SIZE ||
        !scratch || scratch_len < WAL_REC_HEADER_SIZE) {
        ERR_PUSH_INT(e, ERR_INVALID, (int64_t)seg_capacity);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    w->dir_fd = -1;
    w->_pad = 0;
    w->_pad2 = 0;
    w->seg_capacity = seg_capacity;
    w->seg_count = 0;
    w->active.fd = -1;

    out->segments = 0;
    out->created = FALSE;
    out->torn = FALSE;
    out->first_seq = 0;
    out->last_seq = 0;
    out->records = 0;
    mem_zero(out->_pad, (int32_t)sizeof(out->_pad));

    r = os_openat(e, AT_FDCWD, dir_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC,
                  0, &w->dir_fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        return r;
    }

    r = seg_table_load(w, e, scratch, scratch_len);
    if (!result_ok(r)) {
        os_close(e, w->dir_fd);
        w->dir_fd = -1;
        return r;
    }

    /* An empty directory becomes a log starting at sequence 1. */
    if (w->seg_count == 0) {
        r = wal_seg_create(&w->active, e, w->dir_fd, 1, seg_capacity);
        if (!result_ok(r)) {
            os_close(e, w->dir_fd);
            w->dir_fd = -1;
            return r;
        }
        w->seg_base[0] = 1;
        w->seg_count = 1;

        out->segments = 1;
        out->created = TRUE;
        out->first_seq = 1;
        out->last_seq = 0;
        return RESULT_OK;
    }

    expected = w->seg_base[0];

    for (i = 0; i < w->seg_count; i++) {
        wal_seg_t      seg;
        wal_seg_scan_t scan;
        bool_t         last = (i == w->seg_count - 1) ? TRUE : FALSE;

        /*
         * A segment must begin exactly where the previous one ended.
         * A gap means records that were acknowledged are missing, and
         * an overlap means two segments claim the same sequences;
         * neither can be repaired by carrying on.
         */
        if (w->seg_base[i] != expected) {
            ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)w->seg_base[i]);
            os_close(e, w->dir_fd);
            w->dir_fd = -1;
            return RESULT_ERR(ERR_STORAGE, 0);
        }

        r = wal_seg_open(&seg, e, w->dir_fd, w->seg_base[i], seg_capacity);
        if (!result_ok(r)) {
            os_close(e, w->dir_fd);
            w->dir_fd = -1;
            return r;
        }

        r = wal_seg_recover(&seg, e, scratch, scratch_len, &scan);
        if (!result_ok(r)) {
            wal_seg_close(&seg, e);
            os_close(e, w->dir_fd);
            w->dir_fd = -1;
            return r;
        }

        records += scan.records;
        expected = scan.last_seq + 1;

        if (!last) {
            /*
             * Damage anywhere but the tail is not a crash artefact.
             * Refusing to open keeps the acknowledged records that
             * follow it recoverable from a replica.
             */
            if (scan.torn) {
                ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)w->seg_base[i]);
                wal_seg_close(&seg, e);
                os_close(e, w->dir_fd);
                w->dir_fd = -1;
                return RESULT_ERR(ERR_STORAGE, 0);
            }
            wal_seg_close(&seg, e);
        } else {
            w->active = seg;
            out->torn = scan.torn;
        }
    }

    out->segments = w->seg_count;
    out->first_seq = w->seg_base[0];
    out->last_seq = expected - 1;
    out->records = records;
    return RESULT_OK;
}

result_t wal_append(wal_t *w, err_t *e, wal_rec_t *r, const uint8_t *payload,
                    uint8_t *scratch, int32_t scratch_len)
{
    result_t res;
    int32_t  size;

    if (!w || !r) {
        ERR_PUSH(e, ERR_INVALID);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    size = wal_rec_size((int32_t)r->len);
    if (size == 0 || (int64_t)size > w->seg_capacity) {
        /* No rollover can make room for this, so say so plainly. */
        ERR_PUSH_INT(e, ERR_INVALID, (int64_t)r->len);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    if (!wal_seg_fits(&w->active, (int32_t)r->len)) {
        uint64_t next_base;

        if (w->seg_count >= WAL_MAX_SEGMENTS) {
            ERR_PUSH_INT(e, ERR_NOMEM, (int64_t)w->seg_count);
            return RESULT_ERR(ERR_NOMEM, w->seg_count);
        }

        /*
         * Flush before rolling over. The new segment's name asserts
         * that the previous one ended at that sequence, so everything
         * it consumed has to be on disk before the claim is made.
         */
        res = wal_sync(w, e);
        if (!result_ok(res))
            return res;

        next_base = w->active.durable_seq + 1;

        res = wal_seg_close(&w->active, e);
        if (!result_ok(res))
            return res;

        res = wal_seg_create(&w->active, e, w->dir_fd, next_base,
                             w->seg_capacity);
        if (!result_ok(res))
            return res;

        w->seg_base[w->seg_count] = next_base;
        w->seg_count++;
    }

    return wal_seg_append(&w->active, e, r, payload, scratch, scratch_len);
}

result_t wal_sync(wal_t *w, err_t *e)
{
    return wal_seg_sync(&w->active, e);
}

uint64_t wal_durable_seq(const wal_t *w)
{
    return w->active.durable_seq;
}

uint64_t wal_next_seq(const wal_t *w)
{
    return w->active.next_seq;
}

uint64_t wal_first_seq(const wal_t *w)
{
    return (w->seg_count > 0) ? w->seg_base[0] : 0;
}

result_t wal_retain(wal_t *w, err_t *e, uint64_t keep_from, int32_t *removed_out)
{
    int32_t  drop = 0;
    int32_t  i;
    result_t r;

    if (removed_out)
        *removed_out = 0;

    /*
     * A segment can go only when the one after it starts at or before
     * keep_from, which is what proves the whole of it is below the
     * line. The last segment is active and is never considered.
     */
    while (drop + 1 < w->seg_count && w->seg_base[drop + 1] <= keep_from)
        drop++;

    if (drop == 0)
        return RESULT_OK;

    for (i = 0; i < drop; i++) {
        char name[WAL_SEG_NAME_MAX];

        wal_seg_name(w->seg_base[i], name);
        r = os_unlinkat(e, w->dir_fd, name, 0);
        if (!result_ok(r)) {
            /*
             * Keep the table consistent with the directory: entries
             * already unlinked are dropped, the rest stay.
             */
            int32_t k;
            for (k = 0; k + i < w->seg_count; k++)
                w->seg_base[k] = w->seg_base[k + i];
            w->seg_count -= i;
            ERR_PUSH(e, ERR_STORAGE);
            if (removed_out)
                *removed_out = i;
            return r;
        }
    }

    for (i = 0; i + drop < w->seg_count; i++)
        w->seg_base[i] = w->seg_base[i + drop];
    w->seg_count -= drop;

    /* Make the removals durable so a crash does not resurrect them. */
    r = os_fsync(e, w->dir_fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        return r;
    }

    if (removed_out)
        *removed_out = drop;
    return RESULT_OK;
}

result_t wal_close(wal_t *w, err_t *e)
{
    result_t r = RESULT_OK;

    if (w->active.fd >= 0)
        r = wal_seg_close(&w->active, e);

    if (w->dir_fd >= 0) {
        os_close(e, w->dir_fd);
        w->dir_fd = -1;
    }

    return r;
}

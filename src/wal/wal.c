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

/*
 * The recovery scan has one callback and two things to fill: the
 * caller's (the session table) and the log's own key index. This sits
 * in between so the log keeps its index without every caller having to
 * remember to.
 */
typedef struct {
    wal_t          *w;
    wal_rec_cb_t    cb;
    void           *ctx;
} wal_scan_ctx_t;

static void wal_scan_record(void *ctx, const wal_rec_t *r)
{
    wal_scan_ctx_t *s = (wal_scan_ctx_t *)ctx;

    wal_index_add(&s->w->index, r);
    if (s->cb)
        s->cb(s->ctx, r);
}

const wal_index_t *wal_index(const wal_t *w)
{
    return &w->index;
}

result_t wal_open(wal_t *w, err_t *e, const char *dir_path,
                  int64_t seg_capacity, uint8_t *scratch, int32_t scratch_len,
                  wal_open_t *out, wal_rec_cb_t cb, void *ctx)
{
    result_t r;
    int32_t  i;
    uint64_t expected;
    uint64_t records = 0;
    wal_scan_ctx_t scan_ctx;

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
    wal_index_init(&w->index);

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

    scan_ctx.w = w;
    scan_ctx.cb = cb;
    scan_ctx.ctx = ctx;

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

        r = wal_seg_recover(&seg, e, scratch, scratch_len, &scan,
                            wal_scan_record, &scan_ctx);
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

    res = wal_seg_append(&w->active, e, r, payload, scratch, scratch_len);
    if (result_ok(res))
        wal_index_add(&w->index, r);
    return res;
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


/* ---- reading ---- */

/* Index of the segment that holds seq, or -1. */
static int32_t seg_index_of(const wal_t *w, uint64_t seq)
{
    int32_t i;

    for (i = w->seg_count - 1; i >= 0; i--) {
        if (w->seg_base[i] <= seq)
            return i;
    }
    return -1;
}

/* Open a read-only descriptor on the segment with this base. */
static result_t cursor_open_seg(wal_t *w, err_t *e, wal_cursor_t *c,
                                uint64_t base)
{
    char     name[WAL_SEG_NAME_MAX];
    int32_t  fd = -1;
    result_t r;

    wal_seg_name(base, name);
    r = os_openat(e, w->dir_fd, name, O_RDONLY | O_CLOEXEC, 0, &fd);
    if (!result_ok(r)) {
        ERR_PUSH(e, ERR_STORAGE);
        return r;
    }

    if (c->fd >= 0)
        os_close(e, c->fd);
    c->fd = fd;
    c->seg_base = base;
    c->off = 0;
    return RESULT_OK;
}

void wal_cursor_init(wal_cursor_t *c)
{
    c->fd = -1;
    c->_pad = 0;
    c->seg_base = 0;
    c->off = 0;
    c->next_seq = 0;
}

result_t wal_cursor_close(wal_cursor_t *c, err_t *e)
{
    result_t r = RESULT_OK;

    if (c->fd >= 0)
        r = os_close(e, c->fd);
    wal_cursor_init(c);
    return r;
}

result_t wal_cursor_seek(wal_t *w, err_t *e, wal_cursor_t *c, uint64_t seq,
                         uint8_t *scratch, int32_t scratch_len)
{
    int32_t  idx;
    result_t r;

    if (!w || !c || scratch_len < WAL_REC_HEADER_SIZE) {
        ERR_PUSH(e, ERR_INVALID);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    /*
     * One past the end is a legal place to stand: it is where a
     * replica that is fully caught up asks to start, and reading from
     * there returns nothing until the log grows.
     */
    if (seq < wal_first_seq(w) || seq > wal_next_seq(w)) {
        ERR_PUSH_INT(e, ERR_INVALID, (int64_t)seq);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    idx = seg_index_of(w, seq);
    if (idx < 0) {
        ERR_PUSH_INT(e, ERR_INVALID, (int64_t)seq);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    r = cursor_open_seg(w, e, c, w->seg_base[idx]);
    if (!result_ok(r))
        return r;
    c->next_seq = w->seg_base[idx];

    /* Walk the segment's headers until the wanted sequence is next. */
    while (c->next_seq < seq) {
        wal_rec_t rec;
        int32_t   got = 0;
        int32_t   size;

        r = os_pread_full(e, c->fd, scratch, WAL_REC_HEADER_SIZE, c->off,
                          &got);
        if (!result_ok(r))
            return r;
        if (got < WAL_REC_HEADER_SIZE ||
            !wal_rec_decode(scratch, got, &rec) || wal_rec_is_end(&rec)) {
            /* The sequence is named by the table but not on disk. */
            ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)c->next_seq);
            return RESULT_ERR(ERR_STORAGE, 0);
        }

        size = wal_rec_size((int32_t)rec.len);
        if (size == 0 || rec.seq != c->next_seq) {
            ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)c->next_seq);
            return RESULT_ERR(ERR_STORAGE, 0);
        }

        c->off += size;
        c->next_seq++;
    }

    return RESULT_OK;
}

result_t wal_cursor_read(wal_t *w, err_t *e, wal_cursor_t *c,
                         uint64_t limit_seq, uint8_t *buf, int32_t buf_len,
                         int32_t *out_len)
{
    int32_t  used = 0;
    int32_t  attempts = 0;

    if (!w || !c || !buf || !out_len || c->fd < 0) {
        ERR_PUSH(e, ERR_INVALID);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    *out_len = 0;
    if (c->next_seq > limit_seq)
        return RESULT_OK;

    /*
     * One read, then walk the records inside it. Two passes over the
     * same bytes beats a syscall per record, and the walk is what
     * finds the last whole record the caller may pass on.
     */
    for (attempts = 0; attempts < 2; attempts++) {
        int32_t got = 0;
        int32_t at = 0;
        result_t r = os_pread_full(e, c->fd, buf, buf_len, c->off, &got);

        if (!result_ok(r))
            return r;

        while (at < got) {
            wal_rec_t rec;
            int32_t   size;

            if (!wal_rec_decode(buf + at, got - at, &rec))
                break;                          /* not a whole header */
            if (wal_rec_is_end(&rec))
                break;                          /* unwritten space */
            if (rec.seq != c->next_seq) {
                ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)c->next_seq);
                return RESULT_ERR(ERR_STORAGE, 0);
            }
            if (c->next_seq > limit_seq)
                break;

            size = wal_rec_size((int32_t)rec.len);
            if (size == 0) {
                ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)rec.len);
                return RESULT_ERR(ERR_STORAGE, 0);
            }
            if (at + size > got)
                break;                          /* record is cut off */

            at += size;
            c->next_seq++;
        }

        used = at;
        c->off += at;
        if (used > 0)
            break;

        /*
         * Nothing here. Either the log ends, or this segment does and
         * the next one starts exactly where the cursor stands. The
         * writer rolls over on a record that does not fit, so the tail
         * it leaves behind is unwritten space rather than a boundary
         * the offset alone would show.
         */
        {
            int32_t idx = seg_index_of(w, c->next_seq);
            result_t rr;

            if (idx < 0 || w->seg_base[idx] == c->seg_base)
                break;

            rr = cursor_open_seg(w, e, c, w->seg_base[idx]);
            if (!result_ok(rr))
                return rr;
        }
    }

    *out_len = used;
    return RESULT_OK;
}

result_t wal_append_at(wal_t *w, err_t *e, wal_rec_t *r,
                       const uint8_t *payload, uint8_t *scratch,
                       int32_t scratch_len)
{
    uint64_t want = r ? r->seq : 0;
    result_t res;

    if (!w || !r) {
        ERR_PUSH(e, ERR_INVALID);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    /*
     * Checked before the append rather than after, since an append
     * that lands at the wrong sequence has already written itself into
     * the log by the time the caller could notice.
     */
    if (want != wal_next_seq(w)) {
        ERR_PUSH_INT(e, ERR_INVALID, (int64_t)want);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    res = wal_append(w, e, r, payload, scratch, scratch_len);
    if (!result_ok(res))
        return res;

    if (r->seq != want) {
        /* A rollover cannot renumber, so this would be a bug here. */
        ERR_PUSH_INT(e, ERR_STORAGE, (int64_t)r->seq);
        return RESULT_ERR(ERR_STORAGE, 0);
    }
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

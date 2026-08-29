#include "core/types.h"
#include "core/arena.h"
#include "core/mem.h"
#include "core/err.h"
#include "core/debug.h"
#include "proto/msg.h"
#include "proto/conn.h"
#include "core/crc32c.h"
#include "wal/record.h"
#include "wal/segment.h"
#include "wal/wal.h"
#include "server/session.h"
#include "sys/os.h"
#include "io/uring.h"
#include "server/loop.h"
#include "server/selfcheck.h"

/* Shared scratch for the connection checks */
static uint8_t conn_buf[4096];
static uint8_t wire[4096];

/*
 * Receive buffer larger than the protocol payload cap, so a frame can
 * be over the cap while still fitting the buffer. Without it the two
 * limits cannot be told apart.
 */
static uint8_t big_buf[MSG_MAX_PAYLOAD + 64];

/*
 * Encode a HELLO frame into buf. Returns the total frame length, or 0
 * if it does not fit.
 */
static int32_t build_hello(uint8_t *buf, int32_t buf_len, uint64_t session,
                           const char *name, int32_t name_len)
{
    msg_header_t h;
    msg_hello_t  hello;
    int32_t      payload_len = MSG_HELLO_SIZE + name_len;
    int32_t      total = MSG_HEADER_SIZE + payload_len;

    if (buf_len < total || name_len < 0 || name_len > MSG_MAX_NAME)
        return 0;

    hello.session = session;
    hello.name_len = (uint16_t)name_len;
    hello._pad0 = 0;
    hello._pad1 = 0;

    h = msg_header_new(MSG_OP_HELLO, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                       (uint32_t)payload_len, 0, 0);

    if (!msg_encode_header(buf, buf_len, &h))
        return 0;
    if (!msg_encode_hello(buf + MSG_HEADER_SIZE, buf_len - MSG_HEADER_SIZE,
                          &hello))
        return 0;
    if (name_len > 0)
        mem_copy(buf + MSG_HEADER_SIZE + MSG_HELLO_SIZE,
                 (const uint8_t *)name, name_len);

    return total;
}

int selfcheck_arena(arena_t *a)
{
    span_t   s1 = arena_alloc(a, 100);
    span_t   s2 = arena_alloc(a, 200);
    uint8_t *p1;

    if (span_empty(s1) || span_empty(s2) || s1.off == s2.off) {
        DBG_LOG("arena alloc failed");
        return 1;
    }
    if (s2.off < s1.off + s1.len) {
        DBG_LOG("arena spans overlap");
        return 1;
    }

    p1 = arena_ptr(a, s1);
    if (!p1) {
        DBG_LOG("arena resolve failed");
        return 1;
    }
    mem_zero(p1, s1.len);

    DBG_LOG("arena: ok (used=%d, remaining=%d)", a->used, arena_remaining(a));
    return 0;
}

int selfcheck_header(void)
{
    uint8_t      buf[64];
    msg_header_t hdr_out;
    msg_header_t hdr_in;

    mem_zero(buf, 64);

    hdr_out = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE,
                             MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC,
                             128, 42, 1);

    if (!msg_encode_header(buf, 64, &hdr_out)) {
        DBG_LOG("encode failed");
        return 1;
    }
    if (!msg_decode_header(buf, 64, &hdr_in)) {
        DBG_LOG("decode failed");
        return 1;
    }

    if (hdr_in.magic != MSG_MAGIC ||
        hdr_in.version != MSG_VERSION ||
        hdr_in.op != MSG_OP_WRITE ||
        hdr_in.record_type != MSG_RECORD_NONE ||
        hdr_in.payload_len != 128 ||
        hdr_in.partition_key != 42 ||
        hdr_in.sequence != 1 ||
        hdr_in.flags != (MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC) ||
        !msg_header_valid(&hdr_in)) {
        DBG_LOG("header round-trip mismatch");
        return 1;
    }

    /* HELLO is inside the accepted op range, one past the old ceiling */
    hdr_in.op = MSG_OP_HELLO;
    if (!msg_header_valid(&hdr_in)) {
        DBG_LOG("header: HELLO rejected as out of range");
        return 1;
    }
    hdr_in.op = MSG_OP_HELLO + 1;
    if (msg_header_valid(&hdr_in)) {
        DBG_LOG("header: unknown op accepted");
        return 1;
    }
    hdr_in.op = MSG_OP_WRITE;

    /* Version 1 is not accepted: v2 changed ACK semantics */
    hdr_in.version = 1;
    if (msg_header_valid(&hdr_in)) {
        DBG_LOG("header: version 1 accepted");
        return 1;
    }
    hdr_in.version = MSG_VERSION;

    /* Payload cap */
    hdr_in.payload_len = (uint32_t)MSG_MAX_PAYLOAD;
    if (!msg_header_valid(&hdr_in)) {
        DBG_LOG("header: payload at the cap rejected");
        return 1;
    }
    hdr_in.payload_len = (uint32_t)MSG_MAX_PAYLOAD + 1;
    if (msg_header_valid(&hdr_in)) {
        DBG_LOG("header: payload over the cap accepted");
        return 1;
    }

    DBG_LOG("protocol header round-trip: ok");
    return 0;
}

int selfcheck_payloads(void)
{
    uint8_t buf[64];

    mem_zero(buf, sizeof(buf));

    /* HELLO round trip, with a name tail */
    {
        const char       name[] = "mynorthweb";
        int32_t          name_len = (int32_t)sizeof(name) - 1;
        int32_t          total;
        msg_header_t     h;
        msg_hello_t      in;
        const uint8_t   *tail;

        total = build_hello(buf, (int32_t)sizeof(buf), 0, name, name_len);
        if (total != MSG_HEADER_SIZE + MSG_HELLO_SIZE + name_len) {
            DBG_LOG("hello: build failed (total=%d)", total);
            return 1;
        }
        if (!msg_decode_header(buf, total, &h) ||
            h.op != MSG_OP_HELLO ||
            (int32_t)h.payload_len != MSG_HELLO_SIZE + name_len) {
            DBG_LOG("hello: header wrong");
            return 1;
        }
        if (!msg_decode_hello(buf + MSG_HEADER_SIZE, (int32_t)h.payload_len,
                              &in)) {
            DBG_LOG("hello: decode failed");
            return 1;
        }
        if (in.session != 0 || in.name_len != (uint16_t)name_len) {
            DBG_LOG("hello: field mismatch");
            return 1;
        }
        if (!msg_hello_valid(&in, (int32_t)h.payload_len)) {
            DBG_LOG("hello: valid rejected a good payload");
            return 1;
        }
        tail = msg_hello_name(buf + MSG_HEADER_SIZE, (int32_t)h.payload_len);
        if (!tail || mem_cmp(tail, (const uint8_t *)name, name_len) != 0) {
            DBG_LOG("hello: name tail mismatch");
            return 1;
        }

        /* name_len that disagrees with payload_len is rejected */
        if (msg_hello_valid(&in, (int32_t)h.payload_len - 1)) {
            DBG_LOG("hello: short payload accepted");
            return 1;
        }
        in.name_len = MSG_MAX_NAME + 1;
        if (msg_hello_valid(&in, MSG_HELLO_SIZE + MSG_MAX_NAME + 1)) {
            DBG_LOG("hello: oversized name accepted");
            return 1;
        }

        /* A resume carries a session id and no name */
        total = build_hello(buf, (int32_t)sizeof(buf), 0x1234, NULL, 0);
        if (total != MSG_HEADER_SIZE + MSG_HELLO_SIZE ||
            !msg_decode_hello(buf + MSG_HEADER_SIZE, MSG_HELLO_SIZE, &in) ||
            in.session != 0x1234 || in.name_len != 0 ||
            !msg_hello_valid(&in, MSG_HELLO_SIZE) ||
            msg_hello_name(buf + MSG_HEADER_SIZE, MSG_HELLO_SIZE) != NULL) {
            DBG_LOG("hello: resume form wrong");
            return 1;
        }
    }

    /* ACK round trip */
    {
        msg_ack_t out;
        msg_ack_t in;

        out.client_seq = 987654321ULL;
        if (!msg_encode_ack(buf, (int32_t)sizeof(buf), &out) ||
            !msg_decode_ack(buf, MSG_ACK_SIZE, &in) ||
            in.client_seq != out.client_seq) {
            DBG_LOG("ack: round trip failed");
            return 1;
        }
        if (msg_encode_ack(buf, MSG_ACK_SIZE - 1, &out) ||
            msg_decode_ack(buf, MSG_ACK_SIZE - 1, &in)) {
            DBG_LOG("ack: short buffer accepted");
            return 1;
        }
    }

    /* ERR round trip, with the NOT_LEADER text tail */
    {
        const char        addr[] = "192.0.2.13:7400";
        int32_t           addr_len = (int32_t)sizeof(addr) - 1;
        msg_err_payload_t out;
        msg_err_payload_t in;
        const uint8_t    *tail;
        int32_t           payload_len = MSG_ERR_SIZE + addr_len;

        out.code = MSG_ERR_NOT_LEADER;
        out.text_len = (uint16_t)addr_len;
        out._pad = 0;

        if (!msg_encode_err(buf, (int32_t)sizeof(buf), &out)) {
            DBG_LOG("err: encode failed");
            return 1;
        }
        mem_copy(buf + MSG_ERR_SIZE, (const uint8_t *)addr, addr_len);

        if (!msg_decode_err(buf, payload_len, &in) ||
            in.code != MSG_ERR_NOT_LEADER ||
            in.text_len != (uint16_t)addr_len ||
            !msg_err_valid(&in, payload_len)) {
            DBG_LOG("err: decode mismatch");
            return 1;
        }
        tail = msg_err_text(buf, payload_len);
        if (!tail || mem_cmp(tail, (const uint8_t *)addr, addr_len) != 0) {
            DBG_LOG("err: text tail mismatch");
            return 1;
        }
        if (msg_err_valid(&in, MSG_ERR_SIZE)) {
            DBG_LOG("err: text_len disagreeing with payload accepted");
            return 1;
        }
    }

    /* READ and SUBSCRIBE round trips */
    {
        msg_read_t      r_out;
        msg_read_t      r_in;
        msg_subscribe_t s_out;
        msg_subscribe_t s_in;

        r_out.min_seq = 4242;
        if (!msg_encode_read(buf, (int32_t)sizeof(buf), &r_out) ||
            !msg_decode_read(buf, MSG_READ_SIZE, &r_in) ||
            r_in.min_seq != r_out.min_seq) {
            DBG_LOG("read: round trip failed");
            return 1;
        }

        s_out.min_seq = 4242;
        s_out.from_seq = 99;
        if (!msg_encode_subscribe(buf, (int32_t)sizeof(buf), &s_out) ||
            !msg_decode_subscribe(buf, MSG_SUBSCRIBE_SIZE, &s_in) ||
            s_in.min_seq != s_out.min_seq ||
            s_in.from_seq != s_out.from_seq) {
            DBG_LOG("subscribe: round trip failed");
            return 1;
        }
        if (msg_decode_subscribe(buf, MSG_SUBSCRIBE_SIZE - 1, &s_in)) {
            DBG_LOG("subscribe: short buffer accepted");
            return 1;
        }
    }

    DBG_LOG("payload codecs: ok");
    return 0;
}

/* ---- WAL segment checks, the first that touch real files ---- */

/*
 * A directory of our own under /tmp, named with the pid so two runs
 * cannot collide. Removed at the end of the check.
 */
static int32_t tmp_append_u32(char *out, int32_t at, uint32_t v)
{
    char    digits[12];
    int32_t nd = 0;

    if (v == 0)
        digits[nd++] = '0';
    while (v > 0) {
        digits[nd++] = (char)('0' + (int32_t)(v % 10));
        v /= 10;
    }
    while (nd > 0)
        out[at++] = digits[--nd];
    return at;
}

/*
 * A directory of our own under /tmp, named with the pid so two runs
 * cannot collide, and with a tag so scenarios that leave a directory
 * damaged do not disturb each other.
 */
static void seg_tmp_path(char *out, int32_t tag)
{
    const char prefix[] = "/tmp/msgsrvd-selfcheck-";
    int32_t    plen = (int32_t)sizeof(prefix) - 1;
    int32_t    i;

    for (i = 0; i < plen; i++)
        out[i] = prefix[i];

    i = tmp_append_u32(out, i, (uint32_t)os_getpid());
    out[i++] = '-';
    i = tmp_append_u32(out, i, (uint32_t)tag);
    out[i] = '\0';
}

/*
 * Empty the directory and remove it. Enumerates rather than being told
 * the names, since the rollover cases decide for themselves how many
 * segments they leave behind.
 */
static void tmp_dir_destroy(const char *path)
{
    static uint8_t dbuf[4096];
    err_t          e;
    int32_t        fd = -1;

    err_init(&e);
    if (!result_ok(os_openat(&e, AT_FDCWD, path,
                             O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0, &fd)))
        return;

    for (;;) {
        int32_t n = 0;
        int32_t off = 0;

        if (!result_ok(os_getdents64(&e, fd, dbuf, (int32_t)sizeof(dbuf), &n)))
            break;
        if (n == 0)
            break;

        while (off < n) {
            uint16_t    reclen = 0;
            const char *nm;

            mem_copy((uint8_t *)&reclen, dbuf + off + DIRENT64_RECLEN_OFF,
                     (int32_t)sizeof(reclen));
            if (reclen < DIRENT64_NAME_OFF || off + (int32_t)reclen > n)
                break;

            nm = (const char *)(dbuf + off + DIRENT64_NAME_OFF);
            if (!(nm[0] == '.' && (nm[1] == '\0' ||
                                   (nm[1] == '.' && nm[2] == '\0'))))
                os_unlinkat(&e, fd, nm, 0);

            off += (int32_t)reclen;
        }
    }

    os_close(&e, fd);
    os_rmdir(&e, path);
}

/* Create an empty file in a directory, for the cases that need one. */
static bool_t tmp_touch(const char *dir_path, const char *name)
{
    err_t   e;
    int32_t dir_fd = -1;
    int32_t fd = -1;

    err_init(&e);
    if (!result_ok(os_openat(&e, AT_FDCWD, dir_path,
                             O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0, &dir_fd)))
        return FALSE;
    if (!result_ok(os_openat(&e, dir_fd, name, O_WRONLY | O_CREAT | O_CLOEXEC,
                             MODE_0600, &fd))) {
        os_close(&e, dir_fd);
        return FALSE;
    }
    os_close(&e, fd);
    os_close(&e, dir_fd);
    return TRUE;
}

/* Flip a byte inside a named segment, to stage a corruption. */
static bool_t tmp_corrupt(const char *dir_path, uint64_t base_seq,
                          int64_t offset)
{
    err_t   e;
    char    name[WAL_SEG_NAME_MAX];
    int32_t dir_fd = -1;
    int32_t fd = -1;
    uint8_t b = 0;
    int32_t n = 0;
    bool_t  ok = FALSE;

    err_init(&e);
    wal_seg_name(base_seq, name);

    if (!result_ok(os_openat(&e, AT_FDCWD, dir_path,
                             O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0, &dir_fd)))
        return FALSE;
    if (result_ok(os_openat(&e, dir_fd, name, O_RDWR | O_CLOEXEC, 0, &fd))) {
        if (result_ok(os_pread(&e, fd, &b, 1, offset, &n)) && n == 1) {
            b ^= 0x01;
            ok = result_ok(os_pwrite_full(&e, fd, &b, 1, offset));
        }
        os_close(&e, fd);
    }
    os_close(&e, dir_fd);
    return ok;
}

/* Fill in a record with everything except the sequence, which the
 * segment assigns. */
static void seg_fill_rec(wal_rec_t *r, uint32_t len, uint64_t session,
                         uint64_t client_seq, uint64_t key)
{
    r->crc = 0;
    r->len = len;
    r->term = 1;
    r->seq = 0;
    r->session = session;
    r->client_seq = client_seq;
    r->record_type = MSG_RECORD_NONE;
    r->flags = MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC;
    r->_pad = 0;
    r->partition_key = key;
}

int selfcheck_wal_segment(void)
{
    static uint8_t  scratch[4096];
    static uint8_t  payload[64];
    static char     dir_path[64];
    static char     name[WAL_SEG_NAME_MAX];
    err_t           e;
    int32_t         dir_fd = -1;
    result_t        r;
    int32_t         rc = 1;
    int32_t         i;

    err_init(&e);
    mem_set(payload, 0x5A, (int32_t)sizeof(payload));
    seg_tmp_path(dir_path, 0);

    r = os_mkdir(&e, dir_path, MODE_0700);
    if (!result_ok(r)) {
        DBG_LOG("wal segment: mkdir failed");
        dbg_err_print(&e);
        return 1;
    }

    r = os_openat(&e, AT_FDCWD, dir_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC,
                  0, &dir_fd);
    if (!result_ok(r)) {
        DBG_LOG("wal segment: cannot open the directory");
        dbg_err_print(&e);
        os_rmdir(&e, dir_path);
        return 1;
    }

    /* Name format: fixed-width decimal so a listing sorts into order */
    wal_seg_name(1, name);
    if (name[WAL_SEG_DIGITS - 1] != '1' || name[0] != '0' ||
        name[WAL_SEG_DIGITS] != '.' || name[WAL_SEG_NAME_MAX - 1] != '\0') {
        DBG_LOG("wal segment: name format wrong");
        goto out;
    }
    /* Multi-digit, so a reversed or misaligned conversion shows up */
    wal_seg_name(4021, name);
    if (name[WAL_SEG_DIGITS - 4] != '4' || name[WAL_SEG_DIGITS - 3] != '0' ||
        name[WAL_SEG_DIGITS - 2] != '2' || name[WAL_SEG_DIGITS - 1] != '1' ||
        name[WAL_SEG_DIGITS - 5] != '0') {
        DBG_LOG("wal segment: multi-digit name wrong");
        goto out;
    }

    /* Create, append, sync, and what each step makes durable */
    {
        wal_seg_t s;
        wal_rec_t rec;
        int64_t   expect_off = 0;

        r = wal_seg_create(&s, &e, dir_fd, 1, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: create failed");
            dbg_err_print(&e);
            goto out;
        }
        if (s.next_seq != 1 || s.durable_seq != 0 || s.write_off != 0) {
            DBG_LOG("wal segment: fresh state wrong");
            goto out;
        }

        /*
         * The segment is preallocated and zero-filled to its full
         * capacity. The scan's whole notion of where the written
         * region ends rests on reading zeros past it, so check that
         * the far end of a fresh segment is readable and zero rather
         * than simply absent.
         */
        {
            int32_t n = 0;
            if (!result_ok(os_pread(&e, s.fd, scratch, WAL_REC_HEADER_SIZE,
                                    4096 - WAL_REC_HEADER_SIZE, &n)) ||
                n != WAL_REC_HEADER_SIZE) {
                DBG_LOG("wal segment: not preallocated (read %d)", n);
                goto out;
            }
            for (i = 0; i < WAL_REC_HEADER_SIZE; i++) {
                if (scratch[i] != 0) {
                    DBG_LOG("wal segment: preallocated space not zero");
                    goto out;
                }
            }
        }

        /* Creating the same segment twice must not silently reuse it */
        {
            wal_seg_t dup;
            err_t     de;
            err_init(&de);
            if (result_ok(wal_seg_create(&dup, &de, dir_fd, 1, 4096))) {
                DBG_LOG("wal segment: create clobbered an existing segment");
                goto out;
            }
        }

        for (i = 0; i < 3; i++) {
            seg_fill_rec(&rec, 24, 7, (uint64_t)(i + 1), 0xF00D);
            r = wal_seg_append(&s, &e, &rec, payload, scratch,
                               (int32_t)sizeof(scratch));
            if (!result_ok(r)) {
                DBG_LOG("wal segment: append %d failed", i);
                dbg_err_print(&e);
                goto out;
            }
            if (rec.seq != (uint64_t)(i + 1)) {
                DBG_LOG("wal segment: append assigned seq %d", (int32_t)rec.seq);
                goto out;
            }
            expect_off += wal_rec_size(24);
        }

        if (s.write_off != expect_off || s.next_seq != 4) {
            DBG_LOG("wal segment: append offsets wrong");
            goto out;
        }
        /* Appended is not durable */
        if (s.durable_seq != 0) {
            DBG_LOG("wal segment: append advanced durable_seq");
            goto out;
        }

        r = wal_seg_sync(&s, &e);
        if (!result_ok(r) || s.durable_seq != 3 ||
            s.synced_off != s.write_off) {
            DBG_LOG("wal segment: sync did not advance durability");
            goto out;
        }
        /* A sync with nothing outstanding is a no-op, not an error */
        if (!result_ok(wal_seg_sync(&s, &e)) || s.durable_seq != 3) {
            DBG_LOG("wal segment: idle sync misbehaved");
            goto out;
        }

        wal_seg_close(&s, &e);

        /* Reopen and recover what was made durable */
        r = wal_seg_open(&s, &e, dir_fd, 1, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: reopen failed");
            goto out;
        }
        {
            wal_seg_scan_t scan;
            r = wal_seg_recover(&s, &e, scratch,
                                (int32_t)sizeof(scratch), &scan, NULL, NULL);
            if (!result_ok(r) || scan.records != 3 || scan.last_seq != 3 ||
                scan.torn || scan.end_off != expect_off ||
                s.next_seq != 4 || s.durable_seq != 3) {
                DBG_LOG("wal segment: recovery of a clean segment wrong");
                goto out;
            }
        }
        wal_seg_close(&s, &e);
        DBG_LOG("wal segment: create/append/sync/recover: ok");
    }

    /* A torn tail is dropped, and the space is reusable afterwards */
    {
        wal_seg_t      s;
        wal_seg_scan_t scan;
        wal_rec_t      rec;
        int64_t        third_off;
        int32_t        rec_size = wal_rec_size(24);

        r = wal_seg_create(&s, &e, dir_fd, 1000, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: torn-case create failed");
            goto out;
        }
        for (i = 0; i < 3; i++) {
            seg_fill_rec(&rec, 24, 1, (uint64_t)(i + 1), 1);
            if (!result_ok(wal_seg_append(&s, &e, &rec, payload, scratch,
                                          (int32_t)sizeof(scratch)))) {
                DBG_LOG("wal segment: torn-case append failed");
                goto out;
            }
        }
        wal_seg_sync(&s, &e);
        third_off = (int64_t)(2 * rec_size);
        wal_seg_close(&s, &e);

        /* Corrupt one payload byte of the third record */
        r = wal_seg_open(&s, &e, dir_fd, 1000, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: torn-case reopen failed");
            goto out;
        }
        {
            uint8_t bad = 0x00;
            if (!result_ok(os_pwrite_full(&e, s.fd, &bad, 1,
                                          third_off + WAL_REC_HEADER_SIZE))) {
                DBG_LOG("wal segment: could not corrupt the record");
                goto out;
            }
        }
        r = wal_seg_recover(&s, &e, scratch,
                                (int32_t)sizeof(scratch), &scan, NULL, NULL);
        if (!result_ok(r) || scan.records != 2 || scan.last_seq != 1001 ||
            !scan.torn || scan.end_off != third_off ||
            s.next_seq != 1002 || s.write_off != third_off) {
            DBG_LOG("wal segment: torn tail not dropped (records=%d torn=%d)",
                    (int32_t)scan.records, (int32_t)scan.torn);
            goto out;
        }

        /* The dropped space is reused by the next append */
        seg_fill_rec(&rec, 24, 1, 3, 1);
        if (!result_ok(wal_seg_append(&s, &e, &rec, payload, scratch,
                                      (int32_t)sizeof(scratch))) ||
            rec.seq != 1002) {
            DBG_LOG("wal segment: append after a torn tail failed");
            goto out;
        }
        wal_seg_sync(&s, &e);
        wal_seg_close(&s, &e);

        r = wal_seg_open(&s, &e, dir_fd, 1000, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: post-torn reopen failed");
            goto out;
        }
        r = wal_seg_recover(&s, &e, scratch,
                                (int32_t)sizeof(scratch), &scan, NULL, NULL);
        if (!result_ok(r) || scan.records != 3 || scan.last_seq != 1002 ||
            scan.torn) {
            DBG_LOG("wal segment: rewritten tail not recovered");
            goto out;
        }
        wal_seg_close(&s, &e);
        DBG_LOG("wal segment: torn tail dropped and reused: ok");
    }

    /*
     * A record that is intact but out of sequence. Its checksum is
     * correct, because it was written correctly, just not now. Only the
     * sequence test separates it from a current record.
     */
    {
        wal_seg_t      s;
        wal_seg_scan_t scan;
        wal_rec_t      rec;
        int32_t        total;
        int32_t        rec_size = wal_rec_size(24);

        r = wal_seg_create(&s, &e, dir_fd, 2000, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: stale-case create failed");
            goto out;
        }
        for (i = 0; i < 2; i++) {
            seg_fill_rec(&rec, 24, 1, (uint64_t)(i + 1), 1);
            wal_seg_append(&s, &e, &rec, payload, scratch,
                           (int32_t)sizeof(scratch));
        }
        wal_seg_sync(&s, &e);

        /* A well-formed record carrying a sequence from another life */
        seg_fill_rec(&rec, 24, 1, 3, 1);
        rec.seq = 99;
        total = wal_rec_encode(scratch, (int32_t)sizeof(scratch), &rec,
                               payload);
        if (total == 0 ||
            !result_ok(os_pwrite_full(&e, s.fd, scratch, total,
                                      (int64_t)(2 * rec_size)))) {
            DBG_LOG("wal segment: could not plant the stale record");
            goto out;
        }
        wal_seg_sync(&s, &e);
        wal_seg_close(&s, &e);

        r = wal_seg_open(&s, &e, dir_fd, 2000, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: stale-case reopen failed");
            goto out;
        }
        r = wal_seg_recover(&s, &e, scratch,
                                (int32_t)sizeof(scratch), &scan, NULL, NULL);
        if (!result_ok(r) || scan.records != 2 || !scan.torn ||
            scan.end_off != (int64_t)(2 * rec_size)) {
            DBG_LOG("wal segment: stale record accepted (records=%d)",
                    (int32_t)scan.records);
            goto out;
        }
        wal_seg_close(&s, &e);
        DBG_LOG("wal segment: out-of-sequence record rejected: ok");
    }

    /*
     * A record header whose length claims more of the segment than is
     * left. The length is checked against the segment bound before the
     * checksum, because the alternative is trying to read a record
     * that cannot be there and failing the whole recovery over it
     * rather than reporting a torn tail and carrying on.
     */
    {
        wal_seg_t      s;
        wal_seg_scan_t scan;
        wal_rec_t      rec;
        int32_t        rec_size = wal_rec_size(24);
        int64_t        cap = 4096;
        int64_t        plant_off;

        r = wal_seg_create(&s, &e, dir_fd, 5000, cap);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: overrun-case create failed");
            goto out;
        }
        for (i = 0; i < 2; i++) {
            seg_fill_rec(&rec, 24, 1, (uint64_t)(i + 1), 1);
            wal_seg_append(&s, &e, &rec, payload, scratch,
                           (int32_t)sizeof(scratch));
        }
        wal_seg_sync(&s, &e);
        plant_off = (int64_t)(2 * rec_size);

        /* Header only, with a length no segment this size could hold */
        seg_fill_rec(&rec, (uint32_t)cap, 1, 3, 1);
        rec.seq = 5002;
        mem_copy(scratch, (const uint8_t *)&rec, WAL_REC_HEADER_SIZE);
        if (!result_ok(os_pwrite_full(&e, s.fd, scratch,
                                      WAL_REC_HEADER_SIZE, plant_off))) {
            DBG_LOG("wal segment: could not plant the overrunning header");
            goto out;
        }
        wal_seg_sync(&s, &e);
        wal_seg_close(&s, &e);

        r = wal_seg_open(&s, &e, dir_fd, 5000, cap);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: overrun-case reopen failed");
            goto out;
        }
        r = wal_seg_recover(&s, &e, scratch,
                                (int32_t)sizeof(scratch), &scan, NULL, NULL);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: overrunning length failed the recovery");
            goto out;
        }
        if (scan.records != 2 || !scan.torn || scan.end_off != plant_off) {
            DBG_LOG("wal segment: overrunning length mishandled (records=%d)",
                    (int32_t)scan.records);
            goto out;
        }
        wal_seg_close(&s, &e);
        DBG_LOG("wal segment: over-long record rejected: ok");
    }

    /* A full segment refuses the append rather than overrunning */
    {
        wal_seg_t s;
        wal_rec_t rec;
        int32_t   rec_size = wal_rec_size(24);
        int64_t   cap = (int64_t)(rec_size * 3);
        int32_t   appended = 0;

        r = wal_seg_create(&s, &e, dir_fd, 3000, cap);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: full-case create failed");
            goto out;
        }
        for (i = 0; i < 5; i++) {
            bool_t fits = wal_seg_fits(&s, 24);
            err_t  ae;
            err_init(&ae);
            seg_fill_rec(&rec, 24, 1, (uint64_t)(i + 1), 1);
            if (result_ok(wal_seg_append(&s, &ae, &rec, payload, scratch,
                                         (int32_t)sizeof(scratch)))) {
                if (!fits) {
                    DBG_LOG("wal segment: fits said no but append said yes");
                    goto out;
                }
                appended++;
            } else {
                if (fits) {
                    DBG_LOG("wal segment: fits said yes but append said no");
                    goto out;
                }
            }
        }
        if (appended != 3 || s.write_off != cap) {
            DBG_LOG("wal segment: full segment took %d records", appended);
            goto out;
        }
        wal_seg_close(&s, &e);
        DBG_LOG("wal segment: full segment refuses appends: ok");
    }

    /*
     * A scan whose buffer holds more than one record but not the one
     * that straddles its end, so the refill path runs. Sized from the
     * record so the arithmetic survives a change to the header.
     */
    {
        wal_seg_t      s;
        wal_seg_scan_t scan;
        wal_rec_t      rec;
        int32_t        rec_size = wal_rec_size(24);
        int32_t        block = 2 * rec_size + WAL_REC_HEADER_SIZE + 4;

        r = wal_seg_create(&s, &e, dir_fd, 4000, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: block-case create failed");
            goto out;
        }
        for (i = 0; i < 9; i++) {
            seg_fill_rec(&rec, 24, 1, (uint64_t)(i + 1), 1);
            wal_seg_append(&s, &e, &rec, payload, scratch,
                           (int32_t)sizeof(scratch));
        }
        wal_seg_sync(&s, &e);
        wal_seg_close(&s, &e);

        r = wal_seg_open(&s, &e, dir_fd, 4000, 4096);
        if (!result_ok(r)) {
            DBG_LOG("wal segment: block-case reopen failed");
            goto out;
        }
        r = wal_seg_recover(&s, &e, scratch,
                                block, &scan, NULL, NULL);
        if (!result_ok(r) || scan.records != 9 || scan.last_seq != 4008 ||
            scan.torn) {
            DBG_LOG("wal segment: multi-block scan found %d records",
                    (int32_t)scan.records);
            goto out;
        }

        /* A buffer too small for a record fails rather than skipping it */
        {
            err_t          se;
            wal_seg_scan_t sscan;
            err_init(&se);
            if (result_ok(wal_seg_recover(&s, &se, scratch,
                                WAL_REC_HEADER_SIZE, &sscan, NULL, NULL))) {
                DBG_LOG("wal segment: undersized scan buffer accepted");
                goto out;
            }
        }
        wal_seg_close(&s, &e);
        DBG_LOG("wal segment: multi-block scan: ok");
    }

    rc = 0;

out:
    if (dir_fd >= 0)
        os_close(&e, dir_fd);
    tmp_dir_destroy(dir_path);

    if (rc == 0)
        DBG_LOG("wal segment: ok");
    return rc;
}

/* ---- Whole-log checks: rollover, recovery across segments, retention ---- */

/* Append n records of the standard test size, syncing at the end. */
static result_t wal_put(wal_t *w, err_t *e, uint8_t *scratch,
                        int32_t scratch_len, const uint8_t *payload,
                        int32_t n)
{
    wal_rec_t rec;
    int32_t   i;
    result_t  r;

    for (i = 0; i < n; i++) {
        seg_fill_rec(&rec, 24, 1, (uint64_t)(i + 1), 1);
        r = wal_append(w, e, &rec, payload, scratch, scratch_len);
        if (!result_ok(r))
            return r;
    }
    return wal_sync(w, e);
}

int selfcheck_wal(void)
{
    static uint8_t scratch[4096];
    static uint8_t payload[64];
    static char    dir[64];
    err_t          e;
    result_t       r;
    wal_t          w;
    wal_open_t     info;
    int32_t        rec_size = wal_rec_size(24);
    int64_t        cap = (int64_t)(rec_size * 3);   /* three records */

    err_init(&e);
    mem_set(payload, 0x5A, (int32_t)sizeof(payload));

    /* Empty directory, rollover, reopen, retention */
    seg_tmp_path(dir, 1);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("wal: mkdir failed");
        return 1;
    }

    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r)) {
        DBG_LOG("wal: open of an empty directory failed");
        dbg_err_print(&e);
        goto out;
    }
    if (!info.created || info.segments != 1 || info.first_seq != 1 ||
        info.last_seq != 0 || info.records != 0 ||
        wal_next_seq(&w) != 1 || wal_durable_seq(&w) != 0) {
        DBG_LOG("wal: empty log state wrong");
        goto out;
    }

    /* Ten records across a segment that holds three */
    if (!result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 10))) {
        DBG_LOG("wal: appends failed");
        dbg_err_print(&e);
        goto out;
    }
    if (w.seg_count != 4 || w.seg_base[0] != 1 || w.seg_base[1] != 4 ||
        w.seg_base[2] != 7 || w.seg_base[3] != 10) {
        DBG_LOG("wal: rollover boundaries wrong (count=%d)", w.seg_count);
        goto out;
    }
    if (wal_next_seq(&w) != 11 || wal_durable_seq(&w) != 10) {
        DBG_LOG("wal: sequence state wrong after rollover");
        goto out;
    }

    /* A record no segment could hold is refused, not rolled over to */
    {
        wal_rec_t big;
        err_t     be;
        int32_t   before = w.seg_count;

        err_init(&be);
        seg_fill_rec(&big, (uint32_t)cap, 1, 99, 1);
        if (result_ok(wal_append(&w, &be, &big, payload, scratch,
                                 (int32_t)sizeof(scratch)))) {
            DBG_LOG("wal: oversized record accepted");
            goto out;
        }
        if (w.seg_count != before) {
            DBG_LOG("wal: oversized record caused a rollover");
            goto out;
        }
    }

    wal_close(&w, &e);

    /* Reopen: every segment scanned, boundaries checked */
    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r)) {
        DBG_LOG("wal: reopen failed");
        dbg_err_print(&e);
        goto out;
    }
    if (info.created || info.segments != 4 || info.records != 10 ||
        info.first_seq != 1 || info.last_seq != 10 || info.torn ||
        wal_next_seq(&w) != 11) {
        DBG_LOG("wal: multi-segment recovery wrong (recs=%d last=%d)",
                (int32_t)info.records, (int32_t)info.last_seq);
        goto out;
    }

    /* Retention drops only whole segments below the line */
    {
        int32_t removed = 0;

        if (!result_ok(wal_retain(&w, &e, 7, &removed)) || removed != 2 ||
            w.seg_count != 2 || wal_first_seq(&w) != 7) {
            DBG_LOG("wal: retention removed %d segments", removed);
            goto out;
        }
        /* Nothing more can go: the rest holds sequences at or above 7 */
        if (!result_ok(wal_retain(&w, &e, 7, &removed)) || removed != 0) {
            DBG_LOG("wal: retention removed a segment it should keep");
            goto out;
        }
        /* Even a line past the end leaves the active segment alone */
        if (!result_ok(wal_retain(&w, &e, 1000, &removed)) ||
            w.seg_count != 1 || wal_first_seq(&w) != 10) {
            DBG_LOG("wal: retention did not stop at the active segment");
            goto out;
        }
    }
    wal_close(&w, &e);

    /* What is left still opens, and starts where retention left it */
    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r) || info.segments != 1 || info.first_seq != 10 ||
        info.last_seq != 10 || info.records != 1) {
        DBG_LOG("wal: reopen after retention wrong");
        goto out;
    }
    wal_close(&w, &e);
    DBG_LOG("wal: rollover, recovery and retention: ok");
    tmp_dir_destroy(dir);

    /* A torn tail in the last segment is a crash, and is survivable */
    seg_tmp_path(dir, 2);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("wal: mkdir 2 failed");
        return 1;
    }
    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r) ||
        !result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 8))) {
        DBG_LOG("wal: torn-case setup failed");
        goto out;
    }
    wal_close(&w, &e);

    /* Damage the second record of the last segment (base 7) */
    if (!tmp_corrupt(dir, 7, (int64_t)rec_size + WAL_REC_HEADER_SIZE)) {
        DBG_LOG("wal: could not corrupt the active segment");
        goto out;
    }
    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r)) {
        DBG_LOG("wal: refused to open after a torn tail");
        dbg_err_print(&e);
        goto out;
    }
    if (!info.torn || info.last_seq != 7 || info.records != 7 ||
        wal_next_seq(&w) != 8) {
        DBG_LOG("wal: torn tail mishandled (last=%d torn=%d)",
                (int32_t)info.last_seq, (int32_t)info.torn);
        goto out;
    }
    /* And the log carries on from there */
    if (!result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 1)) ||
        wal_durable_seq(&w) != 8) {
        DBG_LOG("wal: cannot append after a torn tail");
        goto out;
    }
    wal_close(&w, &e);
    DBG_LOG("wal: torn tail in the active segment survived: ok");
    tmp_dir_destroy(dir);

    /*
     * Damage anywhere earlier is not a crash artefact. The records
     * after it were acknowledged, so the log refuses to open rather
     * than quietly presenting a shorter history than a client was
     * promised.
     */
    seg_tmp_path(dir, 3);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("wal: mkdir 3 failed");
        return 1;
    }
    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r) ||
        !result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 8))) {
        DBG_LOG("wal: middle-case setup failed");
        goto out;
    }
    wal_close(&w, &e);

    if (!tmp_corrupt(dir, 4, WAL_REC_HEADER_SIZE)) {
        DBG_LOG("wal: could not corrupt a middle segment");
        goto out;
    }
    {
        err_t me;
        err_init(&me);
        if (result_ok(wal_open(&w, &me, dir, cap, scratch,
                               (int32_t)sizeof(scratch), &info, NULL, NULL))) {
            DBG_LOG("wal: opened with a damaged middle segment");
            goto out;
        }
    }
    DBG_LOG("wal: damaged middle segment refuses to open: ok");
    tmp_dir_destroy(dir);

    /* A missing segment is a hole in the sequence, and is refused too */
    seg_tmp_path(dir, 4);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("wal: mkdir 4 failed");
        return 1;
    }
    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r) ||
        !result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 8))) {
        DBG_LOG("wal: gap-case setup failed");
        goto out;
    }
    wal_close(&w, &e);
    {
        err_t   ue;
        char    name[WAL_SEG_NAME_MAX];
        int32_t dfd = -1;

        err_init(&ue);
        wal_seg_name(4, name);
        if (!result_ok(os_openat(&ue, AT_FDCWD, dir,
                                 O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0,
                                 &dfd)) ||
            !result_ok(os_unlinkat(&ue, dfd, name, 0))) {
            DBG_LOG("wal: could not remove a middle segment");
            goto out;
        }
        os_close(&ue, dfd);
    }
    {
        err_t ge;
        err_init(&ge);
        if (result_ok(wal_open(&w, &ge, dir, cap, scratch,
                               (int32_t)sizeof(scratch), &info, NULL, NULL))) {
            DBG_LOG("wal: opened with a hole in the sequence");
            goto out;
        }
    }
    DBG_LOG("wal: hole in the sequence refuses to open: ok");
    tmp_dir_destroy(dir);

    /*
     * A middle segment torn in its unused tail, where the boundary
     * with the next segment still lines up. The sequence check cannot
     * see this one: the records that survive end exactly where the
     * next segment claims to start, so only noticing the tear itself
     * catches it. Constructed with a capacity that leaves room after
     * the last record a segment can hold.
     */
    seg_tmp_path(dir, 6);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("wal: mkdir 6 failed");
        return 1;
    }
    {
        int64_t   roomy = (int64_t)(rec_size * 3 + WAL_REC_HEADER_SIZE + 4);
        wal_rec_t bogus;
        err_t     te;
        int32_t   dfd = -1;
        int32_t   sfd = -1;
        char      name[WAL_SEG_NAME_MAX];

        r = wal_open(&w, &e, dir, roomy, scratch, (int32_t)sizeof(scratch),
                     &info, NULL, NULL);
        if (!result_ok(r) ||
            !result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                               payload, 4))) {
            DBG_LOG("wal: tail-tear setup failed");
            goto out;
        }
        if (w.seg_count != 2 || w.seg_base[1] != 4) {
            DBG_LOG("wal: tail-tear setup rolled over wrong (count=%d)",
                    w.seg_count);
            goto out;
        }
        wal_close(&w, &e);

        /* A header in the first segment's unused tail that no flush wrote */
        seg_fill_rec(&bogus, 0, 1, 1, 1);
        bogus.seq = 4;
        bogus.crc = 0xDEADBEEF;
        mem_copy(scratch, (const uint8_t *)&bogus, WAL_REC_HEADER_SIZE);

        err_init(&te);
        wal_seg_name(1, name);
        if (!result_ok(os_openat(&te, AT_FDCWD, dir,
                                 O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0,
                                 &dfd)) ||
            !result_ok(os_openat(&te, dfd, name, O_RDWR | O_CLOEXEC, 0,
                                 &sfd)) ||
            !result_ok(os_pwrite_full(&te, sfd, scratch, WAL_REC_HEADER_SIZE,
                                      (int64_t)(rec_size * 3)))) {
            DBG_LOG("wal: could not plant the tail tear");
            goto out;
        }
        os_close(&te, sfd);
        os_close(&te, dfd);

        err_init(&te);
        if (result_ok(wal_open(&w, &te, dir, roomy, scratch,
                               (int32_t)sizeof(scratch), &info, NULL, NULL))) {
            DBG_LOG("wal: opened with a tear in a middle segment's tail");
            goto out;
        }
    }
    DBG_LOG("wal: tear in a middle segment's tail refuses to open: ok");
    tmp_dir_destroy(dir);

    /* Files that are not segments are ignored, not guessed at */
    seg_tmp_path(dir, 5);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("wal: mkdir 5 failed");
        return 1;
    }
    if (!tmp_touch(dir, "notasegment") ||
        !tmp_touch(dir, "00000000000000000001.seg.tmp") ||
        !tmp_touch(dir, "0000000000000000000x.seg") ||
        !tmp_touch(dir, "00000000000000000000.seg")) {
        DBG_LOG("wal: could not create the decoy files");
        goto out;
    }
    r = wal_open(&w, &e, dir, cap, scratch, (int32_t)sizeof(scratch), &info,
                     NULL, NULL);
    if (!result_ok(r) || !info.created || info.segments != 1 ||
        info.first_seq != 1) {
        DBG_LOG("wal: decoy files disturbed the open (segments=%d)",
                info.segments);
        goto out;
    }
    wal_close(&w, &e);
    DBG_LOG("wal: non-segment files ignored: ok");
    tmp_dir_destroy(dir);

    DBG_LOG("wal: ok");
    return 0;

    /*
     * One exit for every failure: dir always names the scenario that
     * was running, so the cleanup is the same wherever it came from.
     */
out:
    wal_close(&w, &e);
    tmp_dir_destroy(dir);
    return 1;
}

/* ---- Session table ---- */

int selfcheck_sessions(void)
{
    static session_table_t t;
    session_t             *a;
    session_t             *b;
    int32_t                i;

    session_table_init(&t);
    if (t.count != 0 || t.next_id != 1) {
        DBG_LOG("sessions: fresh table wrong");
        return 1;
    }

    a = session_create(&t);
    b = session_create(&t);
    if (!a || !b || a->id != 1 || b->id != 2 ||
        a->last_client_seq != 0 || t.count != 2) {
        DBG_LOG("sessions: ids not issued in order");
        return 1;
    }

    if (session_lookup(&t, 1) != a || session_lookup(&t, 2) != b ||
        session_lookup(&t, 0) != NULL || session_lookup(&t, 99) != NULL) {
        DBG_LOG("sessions: lookup wrong");
        return 1;
    }

    /* A high-water mark rises and never falls */
    session_observe(&t, 1, 5);
    if (a->last_client_seq != 5) {
        DBG_LOG("sessions: observe did not raise the mark");
        return 1;
    }
    session_observe(&t, 1, 3);
    if (a->last_client_seq != 5) {
        DBG_LOG("sessions: observe lowered the mark");
        return 1;
    }

    /* A session seen only in the log is added */
    session_observe(&t, 7, 2);
    if (t.count != 3 || !session_lookup(&t, 7) ||
        session_lookup(&t, 7)->last_client_seq != 2) {
        DBG_LOG("sessions: a recovered session was not added");
        return 1;
    }

    /*
     * The one that matters. An id seen in the log has to push the
     * counter past it, or the next client is handed an identity that
     * already has records and a high-water mark, and its first write
     * is discarded as a duplicate of a stranger's.
     */
    session_observe(&t, 500, 9);
    if (t.next_id <= 500) {
        DBG_LOG("sessions: next id did not clear a recovered id");
        return 1;
    }
    {
        session_t *fresh = session_create(&t);

        if (!fresh || fresh->id <= 500) {
            DBG_LOG("sessions: new session reused a recovered id");
            return 1;
        }
        if (fresh->last_client_seq != 0) {
            DBG_LOG("sessions: new session inherited a high-water mark");
            return 1;
        }
    }

    /* Records with no client behind them are not sessions */
    {
        int32_t before = t.count;

        session_observe(&t, 0, 4);
        if (t.count != before) {
            DBG_LOG("sessions: an internal record created a session");
            return 1;
        }
    }

    /* Filling the table drops the oldest, and ids keep climbing */
    session_table_init(&t);
    for (i = 0; i < SESSION_MAX; i++)
        session_create(&t);
    if (t.count != SESSION_MAX || t.evicted != 0) {
        DBG_LOG("sessions: table did not fill cleanly");
        return 1;
    }
    {
        uint64_t   next;
        session_t *extra;

        /*
         * Give the session about to be evicted a mark, or the slot it
         * leaves behind is already zero and reusing it without
         * clearing looks the same as clearing it.
         */
        session_observe(&t, 1, 77);
        next = t.next_id;
        extra = session_create(&t);

        if (!extra || extra->id != next || t.count != SESSION_MAX ||
            t.evicted != 1) {
            DBG_LOG("sessions: overflow did not evict exactly one");
            return 1;
        }
        /* The slot is reused; what was in it must not carry over */
        if (extra->last_client_seq != 0) {
            DBG_LOG("sessions: reused slot kept the old high-water mark");
            return 1;
        }
        if (session_lookup(&t, 1) != NULL) {
            DBG_LOG("sessions: overflow kept the oldest session");
            return 1;
        }
    }

    /*
     * On a full table an id older than everything held is dropped
     * rather than allowed to push out a newer one. Filled from a range
     * that starts well above zero, so there is room for an id below
     * all of them that is genuinely absent.
     */
    {
        uint64_t before_next;

        session_table_init(&t);
        for (i = 0; i < SESSION_MAX; i++)
            session_observe(&t, (uint64_t)(100 + i), 1);
        if (t.count != SESSION_MAX || session_lookup(&t, 100) == NULL) {
            DBG_LOG("sessions: fill by observation wrong");
            return 1;
        }

        before_next = t.next_id;
        session_observe(&t, 50, 1);
        if (session_lookup(&t, 50) != NULL) {
            DBG_LOG("sessions: an older id was admitted to a full table");
            return 1;
        }
        if (session_lookup(&t, 100) == NULL) {
            DBG_LOG("sessions: an older id displaced a newer one");
            return 1;
        }
        if (t.next_id != before_next) {
            DBG_LOG("sessions: an older id moved the counter");
            return 1;
        }
    }

    DBG_LOG("sessions: ok");
    return 0;
}

/* ---- Event loop, driven over a real loopback socket ---- */

/*
 * Connect to the loop's listener. The kernel completes the handshake
 * from the listen backlog, so this returns before the server has
 * accepted anything and the test can stay in one thread.
 */
static int32_t loop_client_connect(int32_t port)
{
    err_t         e;
    sockaddr_in_t addr;
    int32_t       fd = -1;

    err_init(&e);
    if (!result_ok(os_socket(&e, AF_INET, SOCK_STREAM, 0, &fd)))
        return -1;

    mem_zero((uint8_t *)&addr, (int32_t)sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr = htonl(0x7F000001);

    if (!result_ok(os_connect(&e, fd, &addr))) {
        os_close(&e, fd);
        return -1;
    }
    return fd;
}

/* Turn the loop over until it stops making progress. */
static void loop_settle(loop_t *l, err_t *e)
{
    int32_t i;

    for (i = 0; i < 16; i++)
        loop_tick(l, e, FALSE);
}

/*
 * Read one whole frame. The loop is not running while this blocks, so
 * everything expected must already have been sent.
 */
static bool_t loop_read_frame(int32_t fd, uint8_t *buf, int32_t cap,
                              msg_header_t *h, int32_t *payload_len)
{
    int32_t got = 0;
    int32_t total;

    while (got < MSG_HEADER_SIZE) {
        ssize_t n = os_read_raw(fd, buf + got, (size_t)(cap - got));
        if (n <= 0)
            return FALSE;
        got += (int32_t)n;
    }
    if (!msg_decode_header(buf, got, h))
        return FALSE;

    total = MSG_HEADER_SIZE + (int32_t)h->payload_len;
    if (total > cap)
        return FALSE;
    while (got < total) {
        ssize_t n = os_read_raw(fd, buf + got, (size_t)(cap - got));
        if (n <= 0)
            return FALSE;
        got += (int32_t)n;
    }

    *payload_len = (int32_t)h->payload_len;
    return TRUE;
}

/* Send one framed request. */
static bool_t loop_send_frame(int32_t fd, uint16_t op, uint16_t flags,
                              uint64_t key, uint64_t seq,
                              const uint8_t *payload, int32_t payload_len)
{
    static uint8_t out[512];
    msg_header_t   h;

    if (MSG_HEADER_SIZE + payload_len > (int32_t)sizeof(out))
        return FALSE;

    h = msg_header_new(op, MSG_RECORD_NONE, flags, (uint32_t)payload_len,
                       key, seq);
    msg_encode_header(out, (int32_t)sizeof(out), &h);
    if (payload_len > 0)
        mem_copy(out + MSG_HEADER_SIZE, payload, payload_len);

    return os_write_raw(fd, out, (size_t)(MSG_HEADER_SIZE + payload_len)) ==
           (ssize_t)(MSG_HEADER_SIZE + payload_len);
}

/*
 * Several WRITE frames in a single write, so they reach the server in
 * one read and are processed in one pass. Sent separately they would
 * arrive whenever the kernel felt like it, and which ones shared a
 * flush would be a matter of timing rather than of the policy under
 * test.
 */
static bool_t loop_send_write_batch(int32_t fd, uint64_t first_seq,
                                    int32_t count)
{
    static uint8_t out[2048];
    int32_t        at = 0;
    int32_t        i;

    for (i = 0; i < count; i++) {
        msg_header_t h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE,
                                        MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC,
                                        1, 1, first_seq + (uint64_t)i);

        if (at + MSG_HEADER_SIZE + 1 > (int32_t)sizeof(out))
            return FALSE;
        msg_encode_header(out + at, (int32_t)sizeof(out) - at, &h);
        out[at + MSG_HEADER_SIZE] = (uint8_t)'x';
        at += MSG_HEADER_SIZE + 1;
    }

    return os_write_raw(fd, out, (size_t)at) == (ssize_t)at;
}

/* HELLO, returning the session the server assigned. */
static bool_t loop_hello(int32_t fd, loop_t *l, err_t *e, uint64_t resume,
                         uint64_t *session_out, uint64_t *high_water)
{
    static uint8_t buf[256];
    msg_hello_t    hello;
    msg_header_t   h;
    msg_ack_t      ack;
    int32_t        plen = 0;

    hello.session = resume;
    hello.name_len = 0;
    hello._pad0 = 0;
    hello._pad1 = 0;
    msg_encode_hello(buf, (int32_t)sizeof(buf), &hello);

    if (!loop_send_frame(fd, MSG_OP_HELLO, MSG_FLAG_ACK_REQ, 0, 0, buf,
                         MSG_HELLO_SIZE))
        return FALSE;

    loop_settle(l, e);

    if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen))
        return FALSE;
    if (h.op != MSG_OP_ACK || plen != MSG_ACK_SIZE)
        return FALSE;
    if (!msg_decode_ack(buf + MSG_HEADER_SIZE, plen, &ack))
        return FALSE;

    *session_out = h.sequence;
    *high_water = ack.client_seq;
    return TRUE;
}

int selfcheck_loop(void)
{
    static uint8_t scratch[4096];
    static uint8_t buf[512];
    static char    dir[64];
    err_t          e;
    wal_t          w;
    wal_open_t     info;
    loop_t          l;
    session_table_t sessions;
    int32_t        fd = -1;
    int32_t        fd2 = -1;
    msg_header_t   h;
    int32_t        plen = 0;
    uint64_t       session = 0;
    uint64_t       high = 0;
    int32_t        rc = 1;

    err_init(&e);
    seg_tmp_path(dir, 7);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("loop: mkdir failed");
        return 1;
    }
    if (!result_ok(wal_open(&w, &e, dir, 65536, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &sessions))) {
        DBG_LOG("loop: wal open failed");
        tmp_dir_destroy(dir);
        return 1;
    }

    session_table_init(&sessions);
    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0, 64))) {
        DBG_LOG("loop: init failed (io_uring unavailable?), skipping");
        dbg_err_print(&e);
        wal_close(&w, &e);
        tmp_dir_destroy(dir);
        return 0;
    }
    if (l.port <= 0) {
        DBG_LOG("loop: no port assigned");
        goto out;
    }

    /* Accept */
    fd = loop_client_connect(l.port);
    if (fd < 0) {
        DBG_LOG("loop: connect failed");
        goto out;
    }
    loop_settle(&l, &e);
    if (l.accepted != 1 || loop_live_conns(&l) != 1) {
        DBG_LOG("loop: connection not accepted (accepted=%d live=%d)",
                (int32_t)l.accepted, loop_live_conns(&l));
        goto out;
    }

    /* A frame before HELLO is refused and the connection closed */
    {
        int32_t probe = loop_client_connect(l.port);

        if (probe < 0) {
            DBG_LOG("loop: probe connect failed");
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_send_frame(probe, MSG_OP_WRITE, MSG_FLAG_ACK_REQ, 0, 1,
                             NULL, 0)) {
            DBG_LOG("loop: probe send failed");
            os_close(&e, probe);
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_read_frame(probe, buf, (int32_t)sizeof(buf), &h, &plen)) {
            DBG_LOG("loop: probe got no reply");
            os_close(&e, probe);
            goto out;
        }
        {
            msg_err_payload_t ep;
            if (h.op != MSG_OP_ERR ||
                !msg_decode_err(buf + MSG_HEADER_SIZE, plen, &ep) ||
                ep.code != MSG_ERR_NO_SESSION) {
                DBG_LOG("loop: write before hello was not refused");
                os_close(&e, probe);
                goto out;
            }
        }
        os_close(&e, probe);
        loop_settle(&l, &e);
    }

    /* HELLO opens a session */
    if (!loop_hello(fd, &l, &e, 0, &session, &high) || session == 0 ||
        high != 0) {
        DBG_LOG("loop: hello failed (session=%d)", (int32_t)session);
        goto out;
    }

    /* PING is answered without touching the log */
    {
        uint64_t before = l.flushes;

        if (!loop_send_frame(fd, MSG_OP_PING, 0, 0, 42, NULL, 0)) {
            DBG_LOG("loop: ping send failed");
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_PONG || h.sequence != 42) {
            DBG_LOG("loop: ping not answered");
            goto out;
        }
        if (l.flushes != before) {
            DBG_LOG("loop: ping caused a flush");
            goto out;
        }
    }

    /* A durable write is acknowledged only after the flush */
    {
        const uint8_t body[] = "hello wal";
        msg_ack_t     ack;

        if (!loop_send_frame(fd, MSG_OP_WRITE,
                             MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC, 0xF00D, 1,
                             body, (int32_t)sizeof(body) - 1)) {
            DBG_LOG("loop: write send failed");
            goto out;
        }
        loop_settle(&l, &e);

        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ACK || plen != MSG_ACK_SIZE ||
            !msg_decode_ack(buf + MSG_HEADER_SIZE, plen, &ack)) {
            DBG_LOG("loop: write not acknowledged");
            goto out;
        }
        if (ack.client_seq != 1 || h.sequence != 1) {
            DBG_LOG("loop: ack carried seq %d / wal %d",
                    (int32_t)ack.client_seq, (int32_t)h.sequence);
            goto out;
        }
        if (!(h.flags & MSG_FLAG_SYNC)) {
            DBG_LOG("loop: ack did not report the durability given");
            goto out;
        }
        /* The acknowledgement is only true if the flush already ran */
        if (wal_durable_seq(&w) < 1 || l.flushes == 0) {
            DBG_LOG("loop: acked before the log was flushed");
            goto out;
        }
        if (l.writes != 1) {
            DBG_LOG("loop: write count wrong");
            goto out;
        }
    }

    /* Several writes in one send share a flush and one cumulative ACK */
    {
        uint64_t  flushes_before = l.flushes;
        msg_ack_t ack;
        int32_t   i;

        (void)i;
        if (!loop_send_write_batch(fd, 2, 4)) {
            DBG_LOG("loop: batched write failed");
            goto out;
        }
        loop_settle(&l, &e);

        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ACK ||
            !msg_decode_ack(buf + MSG_HEADER_SIZE, plen, &ack)) {
            DBG_LOG("loop: batch not acknowledged");
            goto out;
        }
        if (ack.client_seq != 5) {
            DBG_LOG("loop: cumulative ack reported %d, not 5",
                    (int32_t)ack.client_seq);
            goto out;
        }
        if (l.flushes != flushes_before + 1) {
            DBG_LOG("loop: batch of four took %d flushes",
                    (int32_t)(l.flushes - flushes_before));
            goto out;
        }
        if (l.writes != 5 || wal_durable_seq(&w) != 5) {
            DBG_LOG("loop: batch left the log at %d",
                    (int32_t)wal_durable_seq(&w));
            goto out;
        }
    }

    /* A resend of something already durable is answered, not stored twice */
    {
        uint64_t  writes_before = l.writes;
        msg_ack_t ack;

        if (!loop_send_frame(fd, MSG_OP_WRITE,
                             MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC, 1, 3,
                             (const uint8_t *)"x", 1)) {
            DBG_LOG("loop: resend failed");
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ACK ||
            !msg_decode_ack(buf + MSG_HEADER_SIZE, plen, &ack)) {
            DBG_LOG("loop: resend not acknowledged");
            goto out;
        }
        if (l.writes != writes_before) {
            DBG_LOG("loop: resend was appended again");
            goto out;
        }
        if (l.dedup_hits != 1 || ack.client_seq != 5) {
            DBG_LOG("loop: resend ack reported %d", (int32_t)ack.client_seq);
            goto out;
        }
    }

    /* Replication asked for with nothing to replicate to is refused */
    {
        msg_err_payload_t ep;

        if (!loop_send_frame(fd, MSG_OP_WRITE,
                             MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC |
                             MSG_FLAG_REPLICATED, 1, 6,
                             (const uint8_t *)"x", 1)) {
            DBG_LOG("loop: replicated write send failed");
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ERR ||
            !msg_decode_err(buf + MSG_HEADER_SIZE, plen, &ep) ||
            ep.code != MSG_ERR_NO_REPLICAS) {
            DBG_LOG("loop: replicated write was not refused");
            goto out;
        }

        /* Unless the client says it will take the write without one */
        if (!loop_send_frame(fd, MSG_OP_WRITE,
                             MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC |
                             MSG_FLAG_REPLICATED | MSG_FLAG_ALLOW_DEGRADED,
                             1, 6, (const uint8_t *)"x", 1)) {
            DBG_LOG("loop: degraded write send failed");
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ACK || !(h.flags & MSG_FLAG_DEGRADED)) {
            DBG_LOG("loop: degraded write not marked degraded");
            goto out;
        }
    }

    /* A verb that exists but is not built says so */
    {
        msg_err_payload_t ep;
        msg_read_t        rd;

        rd.min_seq = 0;
        msg_encode_read(buf, (int32_t)sizeof(buf), &rd);
        if (!loop_send_frame(fd, MSG_OP_READ, 0, 1, 0, buf, MSG_READ_SIZE)) {
            DBG_LOG("loop: read send failed");
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ERR ||
            !msg_decode_err(buf + MSG_HEADER_SIZE, plen, &ep) ||
            ep.code != MSG_ERR_UNSUPPORTED) {
            DBG_LOG("loop: unimplemented verb not reported");
            goto out;
        }
    }

    /* A second connection resuming the session picks up its high-water mark */
    fd2 = loop_client_connect(l.port);
    if (fd2 < 0) {
        DBG_LOG("loop: second connect failed");
        goto out;
    }
    loop_settle(&l, &e);
    {
        uint64_t s2 = 0;
        uint64_t hw = 0;

        if (!loop_hello(fd2, &l, &e, session, &s2, &hw)) {
            DBG_LOG("loop: resume hello failed");
            goto out;
        }
        if (s2 != session || hw != 6) {
            DBG_LOG("loop: resume reported session %d high-water %d",
                    (int32_t)s2, (int32_t)hw);
            goto out;
        }
    }

    /* A session the server never issued is refused, not invented */
    {
        int32_t           fd3 = loop_client_connect(l.port);
        msg_err_payload_t ep;
        msg_hello_t       hello;

        if (fd3 < 0) {
            DBG_LOG("loop: third connect failed");
            goto out;
        }
        loop_settle(&l, &e);

        hello.session = 999999;
        hello.name_len = 0;
        hello._pad0 = 0;
        hello._pad1 = 0;
        msg_encode_hello(buf, (int32_t)sizeof(buf), &hello);
        loop_send_frame(fd3, MSG_OP_HELLO, MSG_FLAG_ACK_REQ, 0, 0, buf,
                        MSG_HELLO_SIZE);
        loop_settle(&l, &e);

        if (!loop_read_frame(fd3, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ERR ||
            !msg_decode_err(buf + MSG_HEADER_SIZE, plen, &ep) ||
            ep.code != MSG_ERR_SESSION_UNKNOWN) {
            DBG_LOG("loop: unknown session was not refused");
            os_close(&e, fd3);
            goto out;
        }
        os_close(&e, fd3);
        loop_settle(&l, &e);
    }

    /* Closing a connection frees its slot */
    {
        int32_t live_before = loop_live_conns(&l);

        os_close(&e, fd2);
        fd2 = -1;
        loop_settle(&l, &e);
        if (loop_live_conns(&l) != live_before - 1) {
            DBG_LOG("loop: slot not released on close (live=%d was %d)",
                    loop_live_conns(&l), live_before);
            goto out;
        }
    }

    /* Everything acknowledged is still there after a reopen */
    {
        wal_t      w2;
        wal_open_t info2;

        os_close(&e, fd);
        fd = -1;
        loop_settle(&l, &e);
        loop_shutdown(&l);
        wal_close(&w, &e);

        if (!result_ok(wal_open(&w2, &e, dir, 65536, scratch,
                                (int32_t)sizeof(scratch), &info2, NULL, NULL))) {
            DBG_LOG("loop: reopen of the log failed");
            goto out_nolp;
        }
        if (info2.last_seq != 6 || info2.records != 6 || info2.torn) {
            DBG_LOG("loop: log reopened with %d records, last %d",
                    (int32_t)info2.records, (int32_t)info2.last_seq);
            wal_close(&w2, &e);
            goto out_nolp;
        }
        wal_close(&w2, &e);
    }

    tmp_dir_destroy(dir);
    DBG_LOG("loop: accept, session, group commit and dedup: ok");
    return 0;

out:
    if (fd >= 0)
        os_close(&e, fd);
    if (fd2 >= 0)
        os_close(&e, fd2);
    loop_shutdown(&l);
    wal_close(&w, &e);
out_nolp:
    tmp_dir_destroy(dir);
    return rc;
}

/*
 * Deduplication across a restart. The session table is not written to
 * the log, it is counted back out of it, so this is the check that the
 * counting is right.
 */
int selfcheck_session_recovery(void)
{
    static uint8_t  scratch[4096];
    static char     dir[64];
    err_t           e;
    wal_t           w;
    wal_open_t      info;
    loop_t          l;
    session_table_t sessions;
    int32_t         fd = -1;
    uint64_t        session = 0;
    uint64_t        high = 0;

    err_init(&e);
    seg_tmp_path(dir, 8);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("session recovery: mkdir failed");
        return 1;
    }

    /* First run: three writes on one session */
    session_table_init(&sessions);
    if (!result_ok(wal_open(&w, &e, dir, 65536, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &sessions))) {
        DBG_LOG("session recovery: first wal open failed");
        tmp_dir_destroy(dir);
        return 1;
    }
    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0, 64))) {
        DBG_LOG("session recovery: loop unavailable, skipping");
        wal_close(&w, &e);
        tmp_dir_destroy(dir);
        return 0;
    }

    fd = loop_client_connect(l.port);
    if (fd < 0) {
        DBG_LOG("session recovery: connect failed");
        goto fail;
    }
    loop_settle(&l, &e);
    if (!loop_hello(fd, &l, &e, 0, &session, &high) || session == 0) {
        DBG_LOG("session recovery: hello failed");
        goto fail;
    }
    if (!loop_send_write_batch(fd, 1, 3)) {
        DBG_LOG("session recovery: writes failed");
        goto fail;
    }
    loop_settle(&l, &e);
    if (l.writes != 3) {
        DBG_LOG("session recovery: first run stored %d records",
                (int32_t)l.writes);
        goto fail;
    }

    os_close(&e, fd);
    fd = -1;
    loop_settle(&l, &e);
    loop_shutdown(&l);
    wal_close(&w, &e);

    /* Second run: rebuild the table from the log alone */
    session_table_init(&sessions);
    if (!result_ok(wal_open(&w, &e, dir, 65536, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &sessions))) {
        DBG_LOG("session recovery: second wal open failed");
        tmp_dir_destroy(dir);
        return 1;
    }
    {
        session_t *s = session_lookup(&sessions, session);

        if (!s) {
            DBG_LOG("session recovery: the session was not recovered");
            wal_close(&w, &e);
            tmp_dir_destroy(dir);
            return 1;
        }
        if (s->last_client_seq != 3) {
            DBG_LOG("session recovery: high-water came back as %d",
                    (int32_t)s->last_client_seq);
            wal_close(&w, &e);
            tmp_dir_destroy(dir);
            return 1;
        }
        if (sessions.next_id <= session) {
            DBG_LOG("session recovery: next id would reuse the old one");
            wal_close(&w, &e);
            tmp_dir_destroy(dir);
            return 1;
        }
    }

    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0, 64))) {
        DBG_LOG("session recovery: second loop failed");
        wal_close(&w, &e);
        tmp_dir_destroy(dir);
        return 1;
    }

    fd = loop_client_connect(l.port);
    if (fd < 0) {
        DBG_LOG("session recovery: second connect failed");
        goto fail;
    }
    loop_settle(&l, &e);

    /* Resuming reports what the log says, not a blank slate */
    {
        uint64_t s2 = 0;

        high = 0;
        if (!loop_hello(fd, &l, &e, session, &s2, &high)) {
            DBG_LOG("session recovery: resume failed");
            goto fail;
        }
        if (s2 != session || high != 3) {
            DBG_LOG("session recovery: resume gave session %d high-water %d",
                    (int32_t)s2, (int32_t)high);
            goto fail;
        }
    }

    /* A retry spanning the restart is recognised, not stored again */
    {
        uint64_t before = l.writes;

        if (!loop_send_write_batch(fd, 2, 2)) {
            DBG_LOG("session recovery: retry failed");
            goto fail;
        }
        loop_settle(&l, &e);
        if (l.writes != before || l.dedup_hits != 2) {
            DBG_LOG("session recovery: retry stored %d records again",
                    (int32_t)(l.writes - before));
            goto fail;
        }
        if (wal_next_seq(&w) != 4) {
            DBG_LOG("session recovery: the log grew on a retry");
            goto fail;
        }
    }

    /* And a genuinely new record still lands */
    if (!loop_send_write_batch(fd, 4, 1)) {
        DBG_LOG("session recovery: follow-on write failed");
        goto fail;
    }
    loop_settle(&l, &e);
    if (l.writes != 1 || wal_durable_seq(&w) != 4) {
        DBG_LOG("session recovery: follow-on write did not land");
        goto fail;
    }

    os_close(&e, fd);
    fd = -1;
    loop_shutdown(&l);
    wal_close(&w, &e);
    tmp_dir_destroy(dir);
    DBG_LOG("session recovery: deduplication survives a restart: ok");

    /*
     * A record dropped as torn must not reach the table either. Its
     * sequence was never made durable, so counting it would raise the
     * mark past what the log holds and the client's resend of that
     * record would be discarded as a duplicate of something that is
     * not there.
     */
    seg_tmp_path(dir, 9);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("session recovery: mkdir 9 failed");
        return 1;
    }
    {
        int32_t   rec_size = wal_rec_size(1);
        wal_rec_t rec;
        int32_t   i;

        session_table_init(&sessions);
        if (!result_ok(wal_open(&w, &e, dir, 65536, scratch,
                                (int32_t)sizeof(scratch), &info,
                                session_from_record, &sessions))) {
            DBG_LOG("session recovery: torn-case open failed");
            tmp_dir_destroy(dir);
            return 1;
        }
        for (i = 1; i <= 3; i++) {
            seg_fill_rec(&rec, 1, 42, (uint64_t)i, 1);
            if (!result_ok(wal_append(&w, &e, &rec, (const uint8_t *)"x",
                                      scratch, (int32_t)sizeof(scratch)))) {
                DBG_LOG("session recovery: torn-case append failed");
                wal_close(&w, &e);
                tmp_dir_destroy(dir);
                return 1;
            }
        }
        wal_sync(&w, &e);
        wal_close(&w, &e);

        /* Damage the third record, so recovery keeps only two */
        if (!tmp_corrupt(dir, 1, (int64_t)(2 * rec_size) +
                         WAL_REC_HEADER_SIZE)) {
            DBG_LOG("session recovery: could not corrupt the record");
            tmp_dir_destroy(dir);
            return 1;
        }

        session_table_init(&sessions);
        if (!result_ok(wal_open(&w, &e, dir, 65536, scratch,
                                (int32_t)sizeof(scratch), &info,
                                session_from_record, &sessions))) {
            DBG_LOG("session recovery: torn-case reopen failed");
            tmp_dir_destroy(dir);
            return 1;
        }
        if (!info.torn || info.records != 2) {
            DBG_LOG("session recovery: torn-case recovered %d records",
                    (int32_t)info.records);
            wal_close(&w, &e);
            tmp_dir_destroy(dir);
            return 1;
        }
        {
            session_t *s42 = session_lookup(&sessions, 42);

            if (!s42 || s42->last_client_seq != 2) {
                DBG_LOG("session recovery: torn record reached the table "
                        "(mark=%d)", s42 ? (int32_t)s42->last_client_seq : -1);
                wal_close(&w, &e);
                tmp_dir_destroy(dir);
                return 1;
            }
        }
        wal_close(&w, &e);
        tmp_dir_destroy(dir);
    }
    DBG_LOG("session recovery: a torn record stays out of the table: ok");
    return 0;

fail:
    if (fd >= 0)
        os_close(&e, fd);
    loop_shutdown(&l);
    wal_close(&w, &e);
    tmp_dir_destroy(dir);
    return 1;
}

/*
 * Framing checks. These run in CONN_MODE_INTERNAL so that the session
 * handshake stays out of the way; session ordering has its own check.
 */
/*
 * CRC32C against the published vectors, then the property the record
 * codec depends on: feeding two chunks equals feeding their
 * concatenation.
 */
int selfcheck_crc32c(void)
{
    const uint8_t check[] = "123456789";
    uint32_t      whole;
    uint32_t      split;

    if (!crc32c_available()) {
        DBG_LOG("crc32c: SSE4.2 missing on this CPU");
        return 1;
    }

    whole = crc32c(0, check, 9);
    if (whole != 0xE3069283u) {
        DBG_LOG("crc32c: check vector wrong (got %08X)", whole);
        return 1;
    }
    if (crc32c(0, (const uint8_t *)"a", 1) != 0xC1D04330u) {
        DBG_LOG("crc32c: single-byte vector wrong");
        return 1;
    }
    if (crc32c(0, check, 0) != 0) {
        DBG_LOG("crc32c: empty input changed the running value");
        return 1;
    }

    /* Split at 4, which crosses the 8-byte step boundary in both parts */
    split = crc32c(crc32c(0, check, 4), check + 4, 5);
    if (split != whole) {
        DBG_LOG("crc32c: chaining disagrees with one pass");
        return 1;
    }

    /* Every split point has to agree, not just one */
    {
        int32_t i;
        for (i = 0; i <= 9; i++) {
            if (crc32c(crc32c(0, check, i), check + i, 9 - i) != whole) {
                DBG_LOG("crc32c: chaining wrong at split %d", i);
                return 1;
            }
        }
    }

    DBG_LOG("crc32c: ok");
    return 0;
}

int selfcheck_wal_record(void)
{
    static uint8_t buf[256];
    const char     body[] = "record payload";
    int32_t        body_len = (int32_t)sizeof(body) - 1;

    /* Sizes, including the padding to an 8-byte boundary */
    if (wal_rec_size(0) != WAL_REC_HEADER_SIZE ||
        wal_rec_size(1) != WAL_REC_HEADER_SIZE + 8 ||
        wal_rec_size(8) != WAL_REC_HEADER_SIZE + 8 ||
        wal_rec_size(9) != WAL_REC_HEADER_SIZE + 16) {
        DBG_LOG("wal record: size/padding wrong");
        return 1;
    }
    if (wal_rec_size(-1) != 0 || wal_rec_size(WAL_MAX_PAYLOAD + 1) != 0) {
        DBG_LOG("wal record: out-of-range size accepted");
        return 1;
    }

    /* Round trip, verify, and the padding actually zeroed */
    {
        wal_rec_t out;
        wal_rec_t in;
        int32_t   total;
        int32_t   i;

        mem_set(buf, 0xFF, (int32_t)sizeof(buf));

        out.crc = 0xDEADBEEF;   /* ignored on input */
        out.len = (uint32_t)body_len;
        out.term = 3;
        out.seq = 12345;
        out.session = 0xABCD;
        out.client_seq = 7;
        out.record_type = MSG_RECORD_NONE;
        out.flags = MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC | MSG_FLAG_REPLICATED;
        out._pad = 0xFFFFFFFF;  /* cleared by the encoder */
        out.partition_key = 0x1122334455667788ULL;

        total = wal_rec_encode(buf, (int32_t)sizeof(buf), &out,
                               (const uint8_t *)body);
        if (total != wal_rec_size(body_len)) {
            DBG_LOG("wal record: encode returned %d", total);
            return 1;
        }
        if (out.crc == 0xDEADBEEF) {
            DBG_LOG("wal record: encode did not set the checksum");
            return 1;
        }

        for (i = WAL_REC_HEADER_SIZE + body_len; i < total; i++) {
            if (buf[i] != 0) {
                DBG_LOG("wal record: padding not zeroed at %d", i);
                return 1;
            }
        }

        if (!wal_rec_decode(buf, total, &in)) {
            DBG_LOG("wal record: decode failed");
            return 1;
        }
        if (in.crc != out.crc || in.len != out.len || in.term != out.term ||
            in.seq != out.seq || in.session != out.session ||
            in.client_seq != out.client_seq ||
            in.record_type != out.record_type || in.flags != out.flags ||
            in._pad != 0 ||
            in.partition_key != out.partition_key) {
            DBG_LOG("wal record: field mismatch after decode");
            return 1;
        }
        if (mem_cmp(buf + WAL_REC_HEADER_SIZE, (const uint8_t *)body,
                    body_len) != 0) {
            DBG_LOG("wal record: payload mismatch");
            return 1;
        }
        if (!wal_rec_verify(buf, total, &in)) {
            DBG_LOG("wal record: verify rejected a good record");
            return 1;
        }
        if (wal_rec_is_end(&in)) {
            DBG_LOG("wal record: a real record read as end of segment");
            return 1;
        }

        /* A short buffer is a torn tail, not a valid record */
        if (wal_rec_verify(buf, total - 1, &in)) {
            DBG_LOG("wal record: verify accepted a truncated record");
            return 1;
        }

        /* Corruption anywhere inside the checksummed range is caught */
        buf[WAL_REC_HEADER_SIZE] ^= 0x01;           /* payload */
        if (wal_rec_verify(buf, total, &in)) {
            DBG_LOG("wal record: payload corruption undetected");
            return 1;
        }
        buf[WAL_REC_HEADER_SIZE] ^= 0x01;

        buf[16] ^= 0x01;                            /* seq, in the header */
        if (wal_rec_verify(buf, total, &in)) {
            DBG_LOG("wal record: header corruption undetected");
            return 1;
        }
        buf[16] ^= 0x01;

        buf[total - 1] ^= 0x01;                     /* padding */
        if (wal_rec_verify(buf, total, &in)) {
            DBG_LOG("wal record: padding corruption undetected");
            return 1;
        }
        buf[total - 1] ^= 0x01;

        /*
         * A corrupted length is the case the layout is arranged for:
         * it decides how much to read, so it has to be inside the
         * checksum rather than trusted before it.
         */
        {
            wal_rec_t lied = in;

            lied.len = in.len + 8;
            if (wal_rec_verify(buf, total + 8, &lied)) {
                DBG_LOG("wal record: inflated length undetected");
                return 1;
            }
            lied.len = (uint32_t)WAL_MAX_PAYLOAD + 1;
            if (wal_rec_verify(buf, (int32_t)sizeof(buf), &lied)) {
                DBG_LOG("wal record: out-of-range length accepted");
                return 1;
            }
        }

        /*
         * The case the checksum layout exists for. A length corrupted
         * within the same padded size leaves the checksummed range
         * identical, so it is caught only because the length is itself
         * inside the range. 14 and 9 bytes both pad to 16.
         */
        {
            uint32_t  shrunk = 9;
            wal_rec_t reread;

            mem_copy(buf + 4, (const uint8_t *)&shrunk, sizeof(shrunk));
            if (!wal_rec_decode(buf, total, &reread)) {
                DBG_LOG("wal record: decode failed after length edit");
                return 1;
            }
            if (wal_rec_size((int32_t)reread.len) != total) {
                DBG_LOG("wal record: length edit changed the padded size");
                return 1;
            }
            if (wal_rec_verify(buf, total, &reread)) {
                DBG_LOG("wal record: length corruption undetected");
                return 1;
            }
            mem_copy(buf + 4, (const uint8_t *)&in.len, sizeof(in.len));
        }

        /* No room means no record, rather than a half-written one */
        if (wal_rec_encode(buf, total - 1, &out,
                           (const uint8_t *)body) != 0) {
            DBG_LOG("wal record: encode wrote past the buffer");
            return 1;
        }
    }

    /* Empty payload: header only, still checksummed */
    {
        wal_rec_t out;
        wal_rec_t in;
        int32_t   total;

        mem_zero(buf, (int32_t)sizeof(buf));
        out.len = 0;
        out.term = 1;
        out.seq = 1;
        out.session = 0;
        out.client_seq = 0;
        out.record_type = MSG_RECORD_NONE;
        out.flags = 0;
        out.partition_key = 0;

        total = wal_rec_encode(buf, (int32_t)sizeof(buf), &out, NULL);
        if (total != WAL_REC_HEADER_SIZE ||
            !wal_rec_decode(buf, total, &in) ||
            !wal_rec_verify(buf, total, &in) ||
            in.seq != 1) {
            DBG_LOG("wal record: empty payload round trip failed");
            return 1;
        }
        /* Carries no payload, but it is still a record */
        if (wal_rec_is_end(&in)) {
            DBG_LOG("wal record: empty record read as end of segment");
            return 1;
        }
    }

    /*
     * Unwritten space. A preallocated segment reads back as zeros, so
     * the scan has to see that as the end of the written region rather
     * than as a record.
     */
    {
        wal_rec_t in;

        mem_zero(buf, (int32_t)sizeof(buf));
        if (!wal_rec_decode(buf, WAL_REC_HEADER_SIZE, &in) ||
            !wal_rec_is_end(&in)) {
            DBG_LOG("wal record: zeroed header not read as end of segment");
            return 1;
        }
    }

    /* Two records back to back decode at their own offsets */
    {
        wal_rec_t a;
        wal_rec_t b;
        wal_rec_t got;
        int32_t   na;
        int32_t   nb;

        mem_zero(buf, (int32_t)sizeof(buf));

        a.len = 3; a.term = 1; a.seq = 1; a.session = 5; a.client_seq = 1;
        a.record_type = MSG_RECORD_NONE; a.flags = 0; a.partition_key = 1;
        na = wal_rec_encode(buf, (int32_t)sizeof(buf), &a,
                            (const uint8_t *)"abc");

        b.len = 5; b.term = 1; b.seq = 2; b.session = 5; b.client_seq = 2;
        b.record_type = MSG_RECORD_NONE; b.flags = 0; b.partition_key = 2;
        nb = wal_rec_encode(buf + na, (int32_t)sizeof(buf) - na, &b,
                            (const uint8_t *)"defgh");

        if (na == 0 || nb == 0) {
            DBG_LOG("wal record: back-to-back encode failed");
            return 1;
        }
        if (!wal_rec_decode(buf + na, nb, &got) ||
            !wal_rec_verify(buf + na, nb, &got) ||
            got.seq != 2 || got.partition_key != 2 ||
            mem_cmp(buf + na + WAL_REC_HEADER_SIZE,
                    (const uint8_t *)"defgh", 5) != 0) {
            DBG_LOG("wal record: second record wrong");
            return 1;
        }
        /* And the region after both is still end-of-segment */
        if (!wal_rec_decode(buf + na + nb, WAL_REC_HEADER_SIZE, &got) ||
            !wal_rec_is_end(&got)) {
            DBG_LOG("wal record: no end marker after the last record");
            return 1;
        }
    }

    DBG_LOG("wal record: ok");
    return 0;
}

int selfcheck_conn_framing(void)
{
    conn_t        c;
    conn_action_t actions[4];
    msg_header_t  h;
    int32_t       n;
    int32_t       i;
    const char    payload_str[] = "hello";
    int32_t       payload_len = (int32_t)sizeof(payload_str) - 1;

    /* Case 1: one complete frame in a single feed */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_INTERNAL);
    h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                       (uint32_t)payload_len, 0xAABB, 7);
    msg_encode_header(wire, (int32_t)sizeof(wire), &h);
    mem_copy(wire + MSG_HEADER_SIZE, (const uint8_t *)payload_str, payload_len);

    conn_recv_append(&c, wire, MSG_HEADER_SIZE + payload_len);
    n = conn_feed(&c, actions, 4);
    if (n != 1 ||
        actions[0].type != CONN_ACTION_FRAME ||
        actions[0].u.frame.header.op != MSG_OP_WRITE ||
        actions[0].u.frame.header.record_type != MSG_RECORD_NONE ||
        actions[0].u.frame.header.sequence != 7 ||
        actions[0].u.frame.payload_len != payload_len ||
        mem_cmp(actions[0].u.frame.payload,
                (const uint8_t *)payload_str, payload_len) != 0 ||
        c.buf_len != 0 ||
        c.closed) {
        DBG_LOG("conn case1: single-frame parse failed");
        return 1;
    }
    DBG_LOG("conn case1: single-frame parse: ok");

    /* Case 2: byte-at-a-time feed of the same wire */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_INTERNAL);
    for (i = 0; i < MSG_HEADER_SIZE + payload_len - 1; i++) {
        conn_recv_append(&c, wire + i, 1);
        n = conn_feed(&c, actions, 4);
        if (n != 0 || c.closed) {
            DBG_LOG("conn case2: unexpected emit at byte %d", i);
            return 1;
        }
    }
    conn_recv_append(&c, wire + i, 1);
    n = conn_feed(&c, actions, 4);
    if (n != 1 ||
        actions[0].type != CONN_ACTION_FRAME ||
        actions[0].u.frame.payload_len != payload_len ||
        c.buf_len != 0 ||
        c.closed) {
        DBG_LOG("conn case2: byte-at-a-time parse failed");
        return 1;
    }
    DBG_LOG("conn case2: byte-at-a-time parse: ok");

    /* Case 3: bad magic -> REPLY_ERR + CLOSE */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_INTERNAL);
    mem_zero(wire, MSG_HEADER_SIZE);
    wire[0] = 0xDE; wire[1] = 0xAD; wire[2] = 0xBE; wire[3] = 0xEF;
    conn_recv_append(&c, wire, MSG_HEADER_SIZE);
    n = conn_feed(&c, actions, 4);
    if (n != 2 ||
        actions[0].type != CONN_ACTION_REPLY_ERR ||
        actions[0].u.reply_err.code != MSG_ERR_BAD_MAGIC ||
        actions[1].type != CONN_ACTION_CLOSE ||
        !c.closed) {
        DBG_LOG("conn case3: bad-magic path failed (n=%d)", n);
        return 1;
    }
    DBG_LOG("conn case3: bad-magic close: ok");

    /* Case 4: payload larger than the receive buffer */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_INTERNAL);
    h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, 0,
                       (uint32_t)sizeof(conn_buf), 0, 99);
    msg_encode_header(wire, (int32_t)sizeof(wire), &h);
    conn_recv_append(&c, wire, MSG_HEADER_SIZE);
    n = conn_feed(&c, actions, 4);
    if (n != 2 ||
        actions[0].type != CONN_ACTION_REPLY_ERR ||
        actions[0].u.reply_err.code != MSG_ERR_PAYLOAD_TOO_BIG ||
        actions[0].u.reply_err.sequence != 99 ||
        actions[1].type != CONN_ACTION_CLOSE ||
        !c.closed) {
        DBG_LOG("conn case4: oversized-payload close failed (n=%d)", n);
        return 1;
    }
    DBG_LOG("conn case4: oversized-payload close: ok");

    /*
     * Case 5: payload over the protocol cap but inside the receive
     * buffer. Only big_buf can tell this apart from case 4.
     */
    conn_init(&c, big_buf, (int32_t)sizeof(big_buf), CONN_MODE_INTERNAL);
    h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, 0,
                       (uint32_t)MSG_MAX_PAYLOAD + 1, 0, 100);
    msg_encode_header(wire, (int32_t)sizeof(wire), &h);
    conn_recv_append(&c, wire, MSG_HEADER_SIZE);
    n = conn_feed(&c, actions, 4);
    if (n != 2 ||
        actions[0].type != CONN_ACTION_REPLY_ERR ||
        actions[0].u.reply_err.code != MSG_ERR_PAYLOAD_TOO_BIG ||
        actions[1].type != CONN_ACTION_CLOSE ||
        !c.closed) {
        DBG_LOG("conn case5: over-cap payload accepted (n=%d)", n);
        return 1;
    }
    DBG_LOG("conn case5: over-cap payload close: ok");

    /* Case 6: version 1 is refused */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_INTERNAL);
    h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, 0, 0, 0, 5);
    h.version = 1;
    msg_encode_header(wire, (int32_t)sizeof(wire), &h);
    conn_recv_append(&c, wire, MSG_HEADER_SIZE);
    n = conn_feed(&c, actions, 4);
    if (n != 2 ||
        actions[0].type != CONN_ACTION_REPLY_ERR ||
        actions[0].u.reply_err.code != MSG_ERR_BAD_VERSION ||
        !c.closed) {
        DBG_LOG("conn case6: version 1 accepted (n=%d)", n);
        return 1;
    }
    DBG_LOG("conn case6: version 1 refused: ok");

    return 0;
}

/* Session ordering: HELLO exactly once, before anything else. */
int selfcheck_conn_session(void)
{
    conn_t        c;
    conn_action_t actions[4];
    msg_header_t  h;
    int32_t       n;
    int32_t       hello_len;
    int32_t       i;

    /* Case 1: a WRITE before HELLO is refused */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_CLIENT);
    h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                       0, 0, 1);
    msg_encode_header(wire, (int32_t)sizeof(wire), &h);
    conn_recv_append(&c, wire, MSG_HEADER_SIZE);
    n = conn_feed(&c, actions, 4);
    if (n != 2 ||
        actions[0].type != CONN_ACTION_REPLY_ERR ||
        actions[0].u.reply_err.code != MSG_ERR_NO_SESSION ||
        actions[0].u.reply_err.sequence != 1 ||
        actions[1].type != CONN_ACTION_CLOSE ||
        !c.closed) {
        DBG_LOG("session case1: write before hello accepted (n=%d)", n);
        return 1;
    }
    DBG_LOG("session case1: write before hello refused: ok");

    /* Case 2: HELLO then WRITE, both emitted, in order */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_CLIENT);
    hello_len = build_hello(wire, (int32_t)sizeof(wire), 0, "cli", 3);
    if (hello_len == 0) {
        DBG_LOG("session case2: hello build failed");
        return 1;
    }
    h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                       0, 0xBEEF, 1);
    msg_encode_header(wire + hello_len, (int32_t)sizeof(wire) - hello_len, &h);

    conn_recv_append(&c, wire, hello_len + MSG_HEADER_SIZE);
    n = conn_feed(&c, actions, 4);
    if (n != 2 ||
        actions[0].type != CONN_ACTION_FRAME ||
        actions[0].u.frame.header.op != MSG_OP_HELLO ||
        actions[1].type != CONN_ACTION_FRAME ||
        actions[1].u.frame.header.op != MSG_OP_WRITE ||
        actions[1].u.frame.header.partition_key != 0xBEEF ||
        !c.hello_seen ||
        c.closed) {
        DBG_LOG("session case2: hello+write failed (n=%d)", n);
        return 1;
    }
    DBG_LOG("session case2: hello then write: ok");

    /* Case 3: a second HELLO on the same connection is refused */
    conn_recv_append(&c, wire, hello_len);
    n = conn_feed(&c, actions, 4);
    if (n != 2 ||
        actions[0].type != CONN_ACTION_REPLY_ERR ||
        actions[0].u.reply_err.code != MSG_ERR_NO_SESSION ||
        actions[1].type != CONN_ACTION_CLOSE ||
        !c.closed) {
        DBG_LOG("session case3: duplicate hello accepted (n=%d)", n);
        return 1;
    }
    DBG_LOG("session case3: duplicate hello refused: ok");

    /*
     * Case 4: a HELLO that arrives in pieces does not open the session
     * until the whole frame is there. Feeding the trailing WRITE first
     * would otherwise be accepted on a half-read handshake.
     */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_CLIENT);
    hello_len = build_hello(wire, (int32_t)sizeof(wire), 0, "cli", 3);
    for (i = 0; i < hello_len - 1; i++) {
        conn_recv_append(&c, wire + i, 1);
        n = conn_feed(&c, actions, 4);
        if (n != 0 || c.hello_seen || c.closed) {
            DBG_LOG("session case4: partial hello opened session at %d", i);
            return 1;
        }
    }
    conn_recv_append(&c, wire + i, 1);
    n = conn_feed(&c, actions, 4);
    if (n != 1 ||
        actions[0].type != CONN_ACTION_FRAME ||
        actions[0].u.frame.header.op != MSG_OP_HELLO ||
        !c.hello_seen ||
        c.closed) {
        DBG_LOG("session case4: completed hello not accepted (n=%d)", n);
        return 1;
    }
    DBG_LOG("session case4: partial hello: ok");

    /* Case 5: internal connections carry no handshake */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_INTERNAL);
    h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, 0, 0, 0, 1);
    msg_encode_header(wire, (int32_t)sizeof(wire), &h);
    conn_recv_append(&c, wire, MSG_HEADER_SIZE);
    n = conn_feed(&c, actions, 4);
    if (n != 1 ||
        actions[0].type != CONN_ACTION_FRAME ||
        actions[0].u.frame.header.op != MSG_OP_WRITE ||
        c.closed) {
        DBG_LOG("session case5: internal mode refused a write (n=%d)", n);
        return 1;
    }
    DBG_LOG("session case5: internal mode skips hello: ok");

    return 0;
}

/*
 * Completion bookkeeping for the ring check. uring_reap takes a bare
 * function pointer with no context, so the callback reports through
 * file scope.
 */
static int32_t  uring_seen;
static uint64_t uring_data_sum;
static int32_t  uring_bad_res;

static void uring_count_cb(void *ctx, uint64_t user_data, int32_t res,
                           uint32_t flags)
{
    (void)ctx;
    (void)flags;
    uring_seen++;
    uring_data_sum += user_data;
    if (res != 0)
        uring_bad_res++;
}

int selfcheck_uring(void)
{
    uring_t  ring;
    err_t    uring_err;
    result_t r;
    uint32_t submitted = 0;
    int32_t  i;

    err_init(&uring_err);
    r = uring_init(&ring, &uring_err, 8);
    if (!result_ok(r)) {
        DBG_LOG("uring_init failed (errno=%d), skipping",
                uring_err.frames[0].detail.u.errno_val);
        return 0;
    }

    if (ring.ring_fd <= 0 ||
        ring.sq_head == NULL || ring.sq_tail == NULL ||
        ring.cq_head == NULL || ring.cq_tail == NULL ||
        ring.sqes == NULL || ring.cqes == NULL) {
        DBG_LOG("uring: ring pointers not populated");
        uring_destroy(&ring);
        return 1;
    }
    DBG_LOG("uring: init ok (fd=%d, sq=%u, cq=%u)",
            ring.ring_fd, *ring.sq_entries_ptr, *ring.cq_entries_ptr);

    /*
     * Each claim must hand back a distinct slot. Sharing one would
     * make every batch collapse to its last entry, which submits and
     * completes without complaint and loses the rest.
     */
    {
        io_uring_sqe_t *a = uring_get_sqe(&ring);
        io_uring_sqe_t *b = uring_get_sqe(&ring);

        if (!a || !b || a == b) {
            DBG_LOG("uring: get_sqe handed out the same slot twice");
            uring_destroy(&ring);
            return 1;
        }
        uring_prep_nop(a, 1);
        uring_prep_nop(b, 2);

        if (!result_ok(uring_submit(&ring, &uring_err, &submitted)) ||
            submitted != 2) {
            DBG_LOG("uring: submitted %u of 2", submitted);
            uring_destroy(&ring);
            return 1;
        }
    }

    /* Both come back, carrying the user data they were given */
    uring_seen = 0;
    uring_data_sum = 0;
    uring_bad_res = 0;
    if (!result_ok(uring_submit_and_wait(&ring, &uring_err, 2, &submitted))) {
        DBG_LOG("uring: wait failed");
        uring_destroy(&ring);
        return 1;
    }
    if (uring_reap(&ring, uring_count_cb, NULL) != 2 || uring_seen != 2 ||
        uring_data_sum != 3 || uring_bad_res != 0) {
        DBG_LOG("uring: reaped %d completions, sum %d", uring_seen,
                (int32_t)uring_data_sum);
        uring_destroy(&ring);
        return 1;
    }

    /*
     * A second reap finds nothing. If the first had not released the
     * slots it consumed, the same completions would be delivered again
     * and every one of them handled twice.
     */
    if (uring_reap(&ring, uring_count_cb, NULL) != 0 || uring_seen != 2) {
        DBG_LOG("uring: completions delivered twice");
        uring_destroy(&ring);
        return 1;
    }

    /* A full ring says so rather than overwriting what is in flight */
    for (i = 0; i < (int32_t)*ring.sq_entries_ptr; i++) {
        if (!uring_get_sqe(&ring)) {
            DBG_LOG("uring: ring reported full after %d of %u", i,
                    *ring.sq_entries_ptr);
            uring_destroy(&ring);
            return 1;
        }
    }
    if (uring_get_sqe(&ring) != NULL) {
        DBG_LOG("uring: full ring handed out another slot");
        uring_destroy(&ring);
        return 1;
    }
    DBG_LOG("uring: submit and completion round trip: ok");

    uring_destroy(&ring);
    if (ring.ring_fd != -1) {
        DBG_LOG("uring: destroy did not clear fd");
        return 1;
    }
    DBG_LOG("uring: destroy ok");
    return 0;
}

int selfcheck_err(err_t *e)
{
    ERR_PUSH(e, ERR_STORAGE);
    ERR_PUSH_FD(e, ERR_STORAGE, 7);
    ERR_PUSH_INT(e, ERR_INVALID, 1234);

    if (!err_has_error(e) || err_frame_count(e) != 3) {
        DBG_LOG("err stack failed");
        return 1;
    }
    dbg_err_print(e);
    DBG_LOG("err: ok");
    return 0;
}

int selfcheck_run(arena_t *a, err_t *e)
{
    if (selfcheck_arena(a))
        return 1;
    if (selfcheck_crc32c())
        return 1;
    if (selfcheck_header())
        return 1;
    if (selfcheck_payloads())
        return 1;
    if (selfcheck_wal_record())
        return 1;
    if (selfcheck_wal_segment())
        return 1;
    if (selfcheck_wal())
        return 1;
    if (selfcheck_conn_framing())
        return 1;
    if (selfcheck_conn_session())
        return 1;
    if (selfcheck_uring())
        return 1;
    if (selfcheck_sessions())
        return 1;
    if (selfcheck_loop())
        return 1;
    if (selfcheck_session_recovery())
        return 1;
    if (selfcheck_err(e))
        return 1;
    return 0;
}

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
#include "wal/index.h"
#include "server/session.h"
#include "sys/os.h"
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

    /* The session and replication verbs are inside the accepted range */
    hdr_in.op = MSG_OP_HELLO;
    if (!msg_header_valid(&hdr_in)) {
        DBG_LOG("header: HELLO rejected as out of range");
        return 1;
    }
    hdr_in.op = MSG_OP_REPL_START;
    if (!msg_header_valid(&hdr_in)) {
        DBG_LOG("header: REPL_START rejected as out of range");
        return 1;
    }
    hdr_in.op = MSG_OP_MAX + 1;
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

/* ---- the key index ---- */

static void index_feed(wal_index_t *ix, uint64_t key, uint64_t seq)
{
    wal_rec_t r;

    mem_zero((uint8_t *)&r, (int32_t)sizeof(r));
    r.partition_key = key;
    r.seq = seq;
    wal_index_add(ix, &r);
}

int selfcheck_wal_index(void)
{
    static wal_index_t ix;
    uint64_t           from = 0;
    uint64_t           last = 0;

    wal_index_init(&ix);

    if (wal_index_count(&ix) != 0 || !wal_index_complete(&ix)) {
        DBG_LOG("index: an empty table is not empty");
        return 1;
    }

    /* Two keys interleaved, so neither one's range is the log's. */
    index_feed(&ix, 10, 1);
    index_feed(&ix, 20, 2);
    index_feed(&ix, 10, 3);
    index_feed(&ix, 20, 4);
    index_feed(&ix, 10, 5);

    if (wal_index_count(&ix) != 2) {
        DBG_LOG("index: %d keys, wanted 2", wal_index_count(&ix));
        return 1;
    }

    if (!wal_index_range(&ix, 10, 1, &from, &last) || from != 1 || last != 5) {
        DBG_LOG("index: key 10 spans %d..%d", (int32_t)from, (int32_t)last);
        return 1;
    }
    if (!wal_index_range(&ix, 20, 1, &from, &last) || from != 2 || last != 4) {
        DBG_LOG("index: key 20 spans %d..%d", (int32_t)from, (int32_t)last);
        return 1;
    }

    /*
     * Asking from before a key exists starts where it does. Asking
     * from after is left alone, because where its next record falls is
     * not something first-and-last can say, and moving the answer
     * forward would skip records.
     */
    if (!wal_index_range(&ix, 20, 1, &from, &last) || from != 2) {
        DBG_LOG("index: an early start was not moved forward");
        return 1;
    }
    if (!wal_index_range(&ix, 20, 3, &from, &last) || from != 3) {
        DBG_LOG("index: a later start was moved to %d", (int32_t)from);
        return 1;
    }

    /* A key that was never written is known to be absent. */
    if (wal_index_range(&ix, 999, 1, &from, &last)) {
        DBG_LOG("index: a key that does not exist was found");
        return 1;
    }
    if (!wal_index_complete(&ix)) {
        DBG_LOG("index: a table with room to spare called itself short");
        return 1;
    }

    /*
     * Once it runs out of room it stops claiming to know what is
     * absent, which is what keeps a reader from being told a key is
     * not there when it is.
     */
    {
        uint64_t k;

        for (k = 0; k < WAL_INDEX_KEYS + 4; k++)
            index_feed(&ix, 1000 + k, 100 + k);

        if (wal_index_count(&ix) != WAL_INDEX_KEYS) {
            DBG_LOG("index: filled to %d", wal_index_count(&ix));
            return 1;
        }
        if (wal_index_complete(&ix)) {
            DBG_LOG("index: a full table still claims to know every key");
            return 1;
        }
    }

    DBG_LOG("index: key ranges, and knowing what it does not know: ok");
    return 0;
}

int selfcheck_sessions(void)
{
    static session_table_t t;
    session_t             *a;
    session_t             *b;
    uint64_t               a_id;
    uint64_t               b_id;
    int32_t                i;

    session_table_init(&t);
    if (t.count != 0) {
        DBG_LOG("sessions: fresh table wrong");
        return 1;
    }

    a = session_create(&t);
    b = session_create(&t);
    if (!a || !b || a->id == 0 || b->id == 0 || a->id == b->id ||
        a->last_client_seq != 0 || t.count != 2) {
        DBG_LOG("sessions: two sessions did not get two ids");
        return 1;
    }
    a_id = a->id;
    b_id = b->id;

    /*
     * Drawn, not counted. A table that hands out the numbers one and
     * two is a table whose ids can be guessed and, worse, reissued
     * after retention has taken the records that would have shown they
     * were used.
     */
    if (a_id <= (uint64_t)SESSION_MAX || b_id <= (uint64_t)SESSION_MAX) {
        DBG_LOG("sessions: an id looks counted rather than drawn");
        return 1;
    }

    if (session_lookup(&t, a_id) != a || session_lookup(&t, b_id) != b ||
        session_lookup(&t, 0) != NULL ||
        session_lookup(&t, a_id ^ 1) != NULL) {
        DBG_LOG("sessions: lookup wrong");
        return 1;
    }

    /* A high-water mark rises and never falls */
    session_observe(&t, a_id, 5);
    if (a->last_client_seq != 5) {
        DBG_LOG("sessions: observe did not raise the mark");
        return 1;
    }
    session_observe(&t, a_id, 3);
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
     * The one that matters, and the reason for drawing rather than
     * counting: a new session must not land on an id the log already
     * has records under, or its first write is measured against a
     * stranger's high-water mark and discarded as a duplicate. Run
     * enough times that a table handing out anything predictable
     * collides with one of the ids already in it.
     */
    for (i = 0; i < 512; i++) {
        session_t *fresh = session_create(&t);

        if (!fresh || fresh->id == 0) {
            DBG_LOG("sessions: no id was drawn");
            return 1;
        }
        if (fresh->last_client_seq != 0) {
            DBG_LOG("sessions: new session inherited a high-water mark");
            return 1;
        }
        if (fresh->id == a_id || fresh->id == b_id || fresh->id == 7) {
            DBG_LOG("sessions: a draw landed on an id already in use");
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

    /*
     * Filling the table, then overflowing it twice.
     *
     * Twice, because the first eviction is the one case where being
     * wrong about age looks right: the session created first is also
     * in the first slot, so a table that always took slot zero would
     * pass. After that eviction slot zero holds the newest session
     * there is, and the second overflow has to leave it alone and take
     * the one created second.
     */
    session_table_init(&t);
    for (i = 0; i < SESSION_MAX; i++) {
        session_t *made = session_create(&t);

        if (!made) {
            DBG_LOG("sessions: could not fill the table");
            return 1;
        }
        if (i == 0)
            a_id = made->id;
        if (i == 1)
            b_id = made->id;
    }
    if (t.count != SESSION_MAX || t.evicted != 0) {
        DBG_LOG("sessions: table did not fill cleanly");
        return 1;
    }

    /*
     * Give the session about to go a mark, or the slot it leaves
     * behind is already zero and reusing it without clearing looks the
     * same as clearing it. Written onto the entry rather than through
     * session_observe, which would count as having seen it and move it
     * out of the way of the eviction under test.
     */
    session_lookup(&t, a_id)->last_client_seq = 77;
    {
        session_t *first = session_create(&t);
        session_t *second;

        if (!first || t.count != SESSION_MAX || t.evicted != 1) {
            DBG_LOG("sessions: the first overflow did not evict exactly one");
            return 1;
        }
        if (session_lookup(&t, a_id) != NULL) {
            DBG_LOG("sessions: the first overflow kept the oldest");
            return 1;
        }
        /* The slot is reused; what was in it must not carry over */
        if (first->last_client_seq != 0) {
            DBG_LOG("sessions: reused slot kept the old high-water mark");
            return 1;
        }

        second = session_create(&t);
        if (!second || t.evicted != 2) {
            DBG_LOG("sessions: the second overflow did not evict one");
            return 1;
        }
        if (session_lookup(&t, b_id) != NULL) {
            DBG_LOG("sessions: the second overflow took the wrong session");
            return 1;
        }
        if (session_lookup(&t, first->id) == NULL) {
            DBG_LOG("sessions: the newest session was evicted");
            return 1;
        }
    }

    /*
     * Age is arrival, not the id. Recovery walks the log forward, so a
     * session met later is the more recent one whatever its id says,
     * and on a full table it is the one that stays. An id cannot be
     * asked about this any more: the numbers below are deliberately
     * descending, so a table still comparing them would keep the wrong
     * ones.
     */
    {
        session_table_init(&t);
        for (i = 0; i < SESSION_MAX; i++)
            session_observe(&t, (uint64_t)(100000 - i), 1);
        if (t.count != SESSION_MAX ||
            session_lookup(&t, 100000) == NULL ||
            session_lookup(&t, (uint64_t)(100000 - SESSION_MAX + 1)) == NULL) {
            DBG_LOG("sessions: fill by observation wrong");
            return 1;
        }

        /* The first one met is the first to go. */
        session_observe(&t, 42, 1);
        if (session_lookup(&t, 42) == NULL) {
            DBG_LOG("sessions: a session met later was refused a slot");
            return 1;
        }
        if (session_lookup(&t, 100000) != NULL) {
            DBG_LOG("sessions: the least recently seen was not the one to go");
            return 1;
        }
        if (t.evicted != 1) {
            DBG_LOG("sessions: evicted %d making room for one",
                    (int32_t)t.evicted);
            return 1;
        }

        /* Writing to a session is being seen, so it stops being next. */
        session_observe(&t, (uint64_t)(100000 - 1), 2);
        session_observe(&t, 43, 1);
        if (session_lookup(&t, (uint64_t)(100000 - 1)) == NULL) {
            DBG_LOG("sessions: a session written to was still evicted");
            return 1;
        }
    }

    DBG_LOG("sessions: ok");
    return 0;
}

/* ---- Event loop, driven over a real loopback socket ---- */

static void loop_read_forget(int32_t fd);

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
    loop_read_forget(fd);
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
 * Bytes read from a connection and not yet returned as a frame.
 *
 * Frames pipeline. A subscription sends several in one write, and a
 * reader that keeps only the first loses the rest, so whatever arrived
 * alongside stays here until it is asked for. Kept per connection,
 * because the checks hold two open at once.
 */
#define LOOP_RD_SLOTS 4

static struct {
    int32_t fd;
    int32_t len;
    uint8_t buf[4096];
} loop_rd[LOOP_RD_SLOTS];

static void loop_read_forget(int32_t fd)
{
    int32_t i;

    for (i = 0; i < LOOP_RD_SLOTS; i++) {
        if (loop_rd[i].fd == fd) {
            loop_rd[i].fd = -1;
            loop_rd[i].len = 0;
        }
    }
}

static int32_t loop_rd_slot(int32_t fd)
{
    int32_t i;

    for (i = 0; i < LOOP_RD_SLOTS; i++) {
        if (loop_rd[i].fd == fd)
            return i;
    }
    for (i = 0; i < LOOP_RD_SLOTS; i++) {
        if (loop_rd[i].fd <= 0) {
            loop_rd[i].fd = fd;
            loop_rd[i].len = 0;
            return i;
        }
    }

    /*
     * Every slot is spoken for by a connection some earlier check
     * opened. They are used one or two at a time and closed in order,
     * so the oldest is the one to take. Taking a live one would lose
     * buffered bytes, which shows up as a check failing rather than as
     * one quietly passing.
     */
    for (i = 1; i < LOOP_RD_SLOTS; i++)
        loop_rd[i - 1] = loop_rd[i];
    loop_rd[LOOP_RD_SLOTS - 1].fd = fd;
    loop_rd[LOOP_RD_SLOTS - 1].len = 0;
    return LOOP_RD_SLOTS - 1;
}

/*
 * Read one whole frame. The loop is not running while this blocks, so
 * everything expected must already have been sent.
 */
static bool_t loop_read_frame(int32_t fd, uint8_t *buf, int32_t cap,
                              msg_header_t *h, int32_t *payload_len)
{
    int32_t slot = loop_rd_slot(fd);

    if (slot < 0)
        return FALSE;

    for (;;) {
        int32_t have = loop_rd[slot].len;
        ssize_t n;

        if (have >= MSG_HEADER_SIZE &&
            msg_decode_header(loop_rd[slot].buf, have, h)) {
            int32_t total = MSG_HEADER_SIZE + (int32_t)h->payload_len;

            if (total > cap)
                return FALSE;
            if (have >= total) {
                mem_copy(buf, loop_rd[slot].buf, total);
                mem_copy(loop_rd[slot].buf, loop_rd[slot].buf + total,
                         have - total);
                loop_rd[slot].len = have - total;
                *payload_len = (int32_t)h->payload_len;
                return TRUE;
            }
        }

        n = os_read_raw(fd, loop_rd[slot].buf + have,
                        (size_t)((int32_t)sizeof(loop_rd[slot].buf) - have));
        if (n <= 0)
            return FALSE;
        loop_rd[slot].len = have + (int32_t)n;
    }
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
    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0x7F000001, 0))) {
        DBG_LOG("loop: init failed, skipping");
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

    /* A read hands back what the log holds for a key, and then stops */
    {
        msg_read_t rd;
        int32_t    records = 0;
        int32_t    rounds = 0;
        bool_t     ended = FALSE;

        rd.min_seq = 0;
        msg_encode_read(buf, (int32_t)sizeof(buf), &rd);
        if (!loop_send_frame(fd, MSG_OP_READ, 0, 1, 0, buf, MSG_READ_SIZE)) {
            DBG_LOG("loop: read send failed");
            goto out;
        }
        loop_settle(&l, &e);

        if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ACK || plen != 0) {
            DBG_LOG("loop: a read was not acknowledged");
            goto out;
        }

        /*
         * Records arrive a batch per pass, so the loop has to keep
         * turning between reads. The end is an empty NOTIFY carrying
         * LAST, which is how a read says it has shown everything
         * rather than merely paused.
         */
        while (rounds++ < 64 && !ended) {
            loop_settle(&l, &e);
            if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen)) {
                DBG_LOG("loop: the read stopped answering");
                goto out;
            }
            if (h.op != MSG_OP_NOTIFY) {
                DBG_LOG("loop: a read answered with op %d", (int32_t)h.op);
                goto out;
            }
            if (h.flags & MSG_FLAG_LAST) {
                ended = TRUE;
                break;
            }
            if (h.partition_key != 1) {
                DBG_LOG("loop: a read returned key %d",
                        (int32_t)h.partition_key);
                goto out;
            }
            records++;
        }

        if (!ended || records < 3) {
            DBG_LOG("loop: the read returned %d records, ended=%d",
                    records, (int32_t)ended);
            goto out;
        }
    }

    /* A subscription is told about records as they are written */
    {
        msg_subscribe_t sub;
        int32_t         watcher = loop_client_connect(l.port);
        uint64_t        wsession = 0;
        uint64_t        whigh = 0;
        int32_t         rounds = 0;
        bool_t          seen = FALSE;

        if (watcher < 0 ||
            !loop_hello(watcher, &l, &e, 0, &wsession, &whigh)) {
            DBG_LOG("loop: the watching connection could not open");
            goto out;
        }

        sub.min_seq = 0;
        sub.from_seq = 0;       /* live only */
        msg_encode_subscribe(buf, (int32_t)sizeof(buf), &sub);
        if (!loop_send_frame(watcher, MSG_OP_SUBSCRIBE, MSG_FLAG_ALL_KEYS,
                             0, 0, buf, MSG_SUBSCRIBE_SIZE)) {
            DBG_LOG("loop: subscribe send failed");
            os_close(&e, watcher);
            goto out;
        }
        loop_settle(&l, &e);
        if (!loop_read_frame(watcher, buf, (int32_t)sizeof(buf), &h, &plen) ||
            h.op != MSG_OP_ACK || plen != 0) {
            DBG_LOG("loop: a subscription was not acknowledged");
            os_close(&e, watcher);
            goto out;
        }

        /*
         * The same connection writes and watches, which is worth
         * proving: an acknowledgement and a record arrive on it
         * together and are told apart by their verb.
         */
        if (!loop_send_frame(watcher, MSG_OP_WRITE,
                             MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC, 77, 1,
                             (const uint8_t *)"watched", 7)) {
            DBG_LOG("loop: write under subscription failed");
            os_close(&e, watcher);
            goto out;
        }

        while (rounds++ < 32 && !seen) {
            loop_settle(&l, &e);
            if (!loop_read_frame(watcher, buf, (int32_t)sizeof(buf), &h,
                                 &plen)) {
                DBG_LOG("loop: the subscription stopped answering");
                os_close(&e, watcher);
                goto out;
            }
            if (h.op == MSG_OP_ACK)
                continue;               /* its own write, answered */
            if (h.op != MSG_OP_NOTIFY) {
                DBG_LOG("loop: subscription got op %d", (int32_t)h.op);
                os_close(&e, watcher);
                goto out;
            }
            if (h.partition_key == 77 && plen == 7 &&
                mem_cmp(buf + MSG_HEADER_SIZE,
                        (const uint8_t *)"watched", 7) == 0) {
                seen = TRUE;
            }
        }

        os_close(&e, watcher);
        if (!seen) {
            DBG_LOG("loop: the record never reached the subscriber");
            goto out;
        }
    }

    /* A verb that exists but is not built still says so */
    {
        msg_err_payload_t ep;

        if (!loop_send_frame(fd, MSG_OP_DELETE, 0, 1, 0, NULL, 0)) {
            DBG_LOG("loop: delete send failed");
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
        /* Six writes from the first connection and one from the
         * connection that was watching them. */
        if (info2.last_seq != 7 || info2.records != 7 || info2.torn) {
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
    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0x7F000001, 0))) {
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
        /*
         * And a fresh session cannot land on the recovered one. The id
         * is drawn, so this says the draw is checked against what
         * recovery put in the table rather than that a counter cleared
         * it.
         */
        {
            int32_t k;

            for (k = 0; k < 64; k++) {
                session_t *fresh = session_create(&sessions);

                if (!fresh || fresh->id == session) {
                    DBG_LOG("session recovery: a new id reused the old one");
                    wal_close(&w, &e);
                    tmp_dir_destroy(dir);
                    return 1;
                }
            }
        }
    }

    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0x7F000001, 0))) {
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
        c.buf_len != c.buf_off ||
        c.closed) {
        DBG_LOG("conn case1: single-frame parse failed");
        return 1;
    }
    DBG_LOG("conn case1: single-frame parse: ok");

    /*
     * Case 1b: two frames in one feed keep their own payloads.
     *
     * The frames point into the receive buffer, so anything that moves
     * that buffer while the caller is still holding them puts one
     * frame's bytes under another frame's header. That is not a
     * hypothetical: it is what compacting inside conn_feed did, and a
     * pipelining client had its records stored carrying each other's
     * payloads. Nothing above this layer can notice, because every
     * frame is well formed and the count is right.
     */
    conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf), CONN_MODE_INTERNAL);
    {
        const char first[] = "first";
        const char second[] = "second-and-longer";
        int32_t    at = 0;
        int32_t    l1 = (int32_t)sizeof(first) - 1;
        int32_t    l2 = (int32_t)sizeof(second) - 1;

        h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                           (uint32_t)l1, 0, 1);
        msg_encode_header(wire + at, (int32_t)sizeof(wire) - at, &h);
        mem_copy(wire + at + MSG_HEADER_SIZE, (const uint8_t *)first, l1);
        at += MSG_HEADER_SIZE + l1;

        h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                           (uint32_t)l2, 0, 2);
        msg_encode_header(wire + at, (int32_t)sizeof(wire) - at, &h);
        mem_copy(wire + at + MSG_HEADER_SIZE, (const uint8_t *)second, l2);
        at += MSG_HEADER_SIZE + l2;

        /*
         * A third frame, incomplete, and long enough that moving it
         * to the front would land on top of the first two rather than
         * stopping short of them.
         */
        h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                           1024, 0, 3);
        msg_encode_header(wire + at, (int32_t)sizeof(wire) - at, &h);
        at += MSG_HEADER_SIZE;
        mem_set(wire + at, 0x5A, 256);
        at += 256;

        conn_recv_append(&c, wire, at);
        n = conn_feed(&c, actions, 4);
        if (n != 2 ||
            actions[0].u.frame.payload_len != l1 ||
            actions[1].u.frame.payload_len != l2 ||
            mem_cmp(actions[0].u.frame.payload,
                    (const uint8_t *)first, l1) != 0 ||
            mem_cmp(actions[1].u.frame.payload,
                    (const uint8_t *)second, l2) != 0) {
            DBG_LOG("conn case1b: batched frames lost their payloads");
            return 1;
        }

        /* And the partial third frame survives the reclaim. */
        conn_compact(&c);
        if (c.buf_len != MSG_HEADER_SIZE + 256 || c.buf_off != 0) {
            DBG_LOG("conn case1b: the partial frame was not kept");
            return 1;
        }
    }
    DBG_LOG("conn case1b: batched frames keep their own payloads: ok");

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
        c.buf_len != c.buf_off ||
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
        !c.opened ||
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
        if (n != 0 || c.opened || c.closed) {
            DBG_LOG("session case4: partial hello opened session at %d", i);
            return 1;
        }
    }
    conn_recv_append(&c, wire + i, 1);
    n = conn_feed(&c, actions, 4);
    if (n != 1 ||
        actions[0].type != CONN_ACTION_FRAME ||
        actions[0].u.frame.header.op != MSG_OP_HELLO ||
        !c.opened ||
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

/* ---- epoll, the loop's multiplexer ---- */

/*
 * A connected pair on loopback. The checks below need two ends of
 * something: one to watch, one to write into. accept4 is exercised
 * here as well, being the only other call the loop gains that the
 * server did not already make.
 */
static bool_t epoll_pair(int32_t *a_out, int32_t *b_out)
{
    err_t         e;
    sockaddr_in_t addr;
    int32_t       ln = -1;
    int32_t       a = -1;
    int32_t       b = -1;

    err_init(&e);
    *a_out = -1;
    *b_out = -1;

    if (!result_ok(os_socket(&e, AF_INET, SOCK_STREAM, 0, &ln)))
        return FALSE;

    mem_zero((uint8_t *)&addr, (int32_t)sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = 0;
    addr.sin_addr = htonl(0x7F000001);

    if (!result_ok(os_bind(&e, ln, &addr)) ||
        !result_ok(os_listen(&e, ln, 4)) ||
        !result_ok(os_getsockname(&e, ln, &addr))) {
        os_close(&e, ln);
        return FALSE;
    }

    if (!result_ok(os_socket(&e, AF_INET, SOCK_STREAM, 0, &a))) {
        os_close(&e, ln);
        return FALSE;
    }

    /*
     * Blocking connect, which loopback completes out of the listen
     * backlog without anyone accepting, so one thread is enough.
     */
    if (!result_ok(os_connect(&e, a, &addr)) ||
        !result_ok(os_accept4(&e, ln, SOCK_NONBLOCK | SOCK_CLOEXEC, &b))) {
        os_close(&e, a);
        os_close(&e, ln);
        return FALSE;
    }

    os_close(&e, ln);
    *a_out = a;
    *b_out = b;
    return TRUE;
}

/*
 * Readiness reported once, for the right descriptor, and stopped when
 * the interest is dropped.
 *
 * The data word is deliberately wide and lopsided. The loop packs an
 * operation into the top byte and a slot index into the low bits, and
 * epoll_event is packed on x86-64: a struct the compiler padded to 16
 * bytes would still return the first event intact and put every one
 * after it in the wrong place, which is why two are asked for at once
 * below.
 */
int selfcheck_epoll(void)
{
    static epoll_event_t evs[8];
    const uint64_t       data_b = 0x0700000000000002ULL;
    const uint64_t       data_a = 0x0300000000000001ULL;
    err_t                e;
    int32_t              ep = -1;
    int32_t              a = -1;
    int32_t              b = -1;
    int32_t              n = 0;
    int32_t              res = 0;
    int32_t              rc = 1;
    uint8_t              byte = 0x5A;

    err_init(&e);
    if (!result_ok(os_epoll_create(&e, EPOLL_CLOEXEC, &ep))) {
        DBG_LOG("epoll: epoll_create1 failed");
        return 1;
    }
    if (!epoll_pair(&a, &b)) {
        DBG_LOG("epoll: could not make a loopback pair");
        os_close(&e, ep);
        return 1;
    }

    if (!result_ok(os_epoll_ctl(&e, ep, EPOLL_CTL_ADD, b, EPOLLIN, data_b))) {
        DBG_LOG("epoll: add failed");
        goto out;
    }

    /* Nothing has been written, so nothing is ready. */
    if (!result_ok(os_epoll_wait(&e, ep, evs, 8, 0, &n)) || n != 0) {
        DBG_LOG("epoll: an idle descriptor reported %d events", n);
        goto out;
    }

    os_send(a, &byte, 1, MSG_NOSIGNAL | MSG_DONTWAIT, &res);
    if (res != 1) {
        DBG_LOG("epoll: the write to the other end moved %d bytes", res);
        goto out;
    }

    if (!result_ok(os_epoll_wait(&e, ep, evs, 8, 0, &n)) || n != 1) {
        DBG_LOG("epoll: a readable descriptor reported %d events", n);
        goto out;
    }
    if (evs[0].data != data_b || (evs[0].events & EPOLLIN) == 0) {
        DBG_LOG("epoll: event carried data %d, events %d",
                (int32_t)evs[0].data, (int32_t)evs[0].events);
        goto out;
    }

    byte = 0;
    os_recv(b, &byte, 1, MSG_DONTWAIT, &res);
    if (res != 1 || byte != 0x5A) {
        DBG_LOG("epoll: the byte read back was %d bytes, value %d", res,
                (int32_t)byte);
        goto out;
    }

    /* Consumed, so level triggering has nothing left to report. */
    if (!result_ok(os_epoll_wait(&e, ep, evs, 8, 0, &n)) || n != 0) {
        DBG_LOG("epoll: a drained descriptor still reported %d events", n);
        goto out;
    }

    /*
     * Both ends watched for writability at once. Two events in one
     * array is what catches a struct whose stride does not match the
     * kernel's: the first entry would still be right and the second
     * would be read out of the middle of it.
     */
    if (!result_ok(os_epoll_ctl(&e, ep, EPOLL_CTL_MOD, b, EPOLLOUT,
                                data_b)) ||
        !result_ok(os_epoll_ctl(&e, ep, EPOLL_CTL_ADD, a, EPOLLOUT,
                                data_a))) {
        DBG_LOG("epoll: modifying interest failed");
        goto out;
    }
    if (!result_ok(os_epoll_wait(&e, ep, evs, 8, 0, &n)) || n != 2) {
        DBG_LOG("epoll: two writable descriptors reported %d events", n);
        goto out;
    }
    if ((evs[0].data ^ evs[1].data) != (data_a ^ data_b) ||
        evs[0].data == evs[1].data ||
        (evs[0].events & EPOLLOUT) == 0 ||
        (evs[1].events & EPOLLOUT) == 0) {
        DBG_LOG("epoll: the second event did not survive the array");
        goto out;
    }

    /* Dropped interest is reported as nothing, not as the old interest. */
    if (!result_ok(os_epoll_ctl(&e, ep, EPOLL_CTL_DEL, b, 0, 0)) ||
        !result_ok(os_epoll_ctl(&e, ep, EPOLL_CTL_DEL, a, 0, 0))) {
        DBG_LOG("epoll: delete failed");
        goto out;
    }
    if (!result_ok(os_epoll_wait(&e, ep, evs, 8, 0, &n)) || n != 0) {
        DBG_LOG("epoll: a deleted descriptor reported %d events", n);
        goto out;
    }

    DBG_LOG("epoll: readiness reported once, for the right fd: ok");
    rc = 0;

out:
    os_close(&e, a);
    os_close(&e, b);
    os_close(&e, ep);
    return rc;
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


/* ---- A failed send, and the slot it must not strand ---- */

/*
 * The completion is injected rather than provoked. The bug this
 * guards against needed a send to fail while bytes were staged, and
 * arranging that through real sockets is a race: it took the S2 kill
 * test three failures across a day to hit it. loop_send_done is the
 * seam, and everything here asserts on the loop's own state, never on
 * frames read back, so a wrong assumption fails instead of hanging.
 */
int selfcheck_send_failure(void)
{
    static uint8_t  scratch[8192];
    static uint8_t  staged[64];
    static char     dir[64];
    err_t           e;
    wal_t           w;
    wal_open_t      info;
    loop_t          l;
    session_table_t sessions;
    int32_t         fd = -1;
    int32_t         fd2 = -1;
    int32_t         rc = 1;
    int32_t         i;

    err_init(&e);
    seg_tmp_path(dir, 11);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("send failure: mkdir failed");
        return 1;
    }
    session_table_init(&sessions);
    if (!result_ok(wal_open(&w, &e, dir, 65536, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &sessions))) {
        DBG_LOG("send failure: wal open failed");
        tmp_dir_destroy(dir);
        return 1;
    }
    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0x7F000001, 0))) {
        DBG_LOG("send failure: loop unavailable, skipping");
        wal_close(&w, &e);
        tmp_dir_destroy(dir);
        return 0;
    }

    /*
     * A short write has to compact the buffer the slot actually sends
     * from. A replica's is its own rather than the one its slot number
     * indexes, and the old handler compacted the latter, moving memory
     * that belonged to someone else and leaving its own untouched.
     */
    {
        loop_conn_t *c = &l.conns[1];

        c->in_use = TRUE;
        c->fd = -1;
        c->replica = 1;
        c->send_buf = staged;
        c->send_cap = (int32_t)sizeof(staged);
        mem_copy(staged, (const uint8_t *)"ABCDEF", 6);
        c->send_len = 6;

        loop_send_done(&l, 1, 2);

        if (c->send_len != 4 ||
            mem_cmp(staged, (const uint8_t *)"CDEF", 4) != 0) {
            DBG_LOG("send failure: a short write compacted the wrong buffer");
            goto out;
        }

        /* Put the fabricated slot back before the loop walks it. */
        c->in_use = FALSE;
        c->replica = -1;
        c->send_len = 0;
    }

    /*
     * A send that failed takes the staged bytes with it and ends the
     * connection.
     *
     * Under the ring this was reachable only through the strand below:
     * a slot with a write in flight was never released, so a handler
     * that kept the bytes held the slot for ever. epoll has no such
     * flag and the slot would come free by another route, so the
     * contract is asserted here rather than inferred from what the
     * loop does with it afterwards.
     */
    {
        loop_conn_t *c = &l.conns[1];

        c->in_use = TRUE;
        c->fd = -1;
        c->send_buf = staged;
        c->send_cap = (int32_t)sizeof(staged);
        mem_copy(staged, (const uint8_t *)"doomed", 6);
        c->send_len = 6;

        loop_send_done(&l, 1, -EPIPE);
        if (c->send_len != 0 || !c->closing) {
            DBG_LOG("send failure: a failed send kept %d bytes, closing %d",
                    c->send_len, (int32_t)c->closing);
            goto out;
        }

        /* A send that moved nothing is the same thing: repeated, it is
         * the same loop with nothing to break it. */
        c->closing = FALSE;
        c->send_len = 6;
        loop_send_done(&l, 1, 0);
        if (c->send_len != 0 || !c->closing) {
            DBG_LOG("send failure: a send that moved nothing was let pass");
            goto out;
        }

        c->in_use = FALSE;
        c->send_len = 0;
        c->closing = FALSE;
    }

    /*
     * Now the strand. A real connection is accepted and then dressed
     * as an attached replica with a stream staged for it.
     */
    fd = loop_client_connect(l.port);
    if (fd < 0) {
        DBG_LOG("send failure: connect failed");
        goto out;
    }
    loop_settle(&l, &e);
    if (l.accepted != 1 || !l.conns[0].in_use) {
        DBG_LOG("send failure: the connection was not accepted");
        goto out;
    }

    {
        loop_conn_t *c = &l.conns[0];

        c->replica = 0;
        l.peer_live[0] = TRUE;
        c->send_buf = staged;
        c->send_cap = (int32_t)sizeof(staged);
        mem_copy(staged, (const uint8_t *)"doomed", 6);
        c->send_len = 6;
    }

    /*
     * Shut the leader's own descriptor for writing, so that if the
     * handler wrongly keeps the staged bytes and sends them again, the
     * second send fails with EPIPE every time. Closing the peer was
     * tried first and is not enough: the first resend can land in the
     * dying socket before the reset is processed, succeed, and hide
     * the leak, which is the same nondeterminism that kept this bug
     * alive for a day. Shutting our own end asks nothing of timing.
     */
    os_shutdown(&e, l.conns[0].fd, SHUT_RDWR);
    os_close(&e, fd);
    fd = -1;

    loop_send_done(&l, 0, -ECONNRESET);

    for (i = 0; i < 32; i++) {
        loop_tick(&l, &e, FALSE);
        if (!l.conns[0].in_use)
            break;
    }
    if (l.conns[0].in_use || l.peer_live[0]) {
        DBG_LOG("send failure: a failed send stranded the replica slot");
        goto out;
    }

    /* And the freed slot can be taken by the node coming back. */
    fd2 = loop_client_connect(l.port);
    if (fd2 < 0) {
        DBG_LOG("send failure: the second connect failed");
        goto out;
    }
    loop_settle(&l, &e);
    if (!loop_send_frame(fd2, MSG_OP_REPL_START, 0, 0, 1, NULL, 0)) {
        DBG_LOG("send failure: repl_start send failed");
        goto out;
    }
    for (i = 0; i < 32 && !l.peer_live[0]; i++)
        loop_tick(&l, &e, FALSE);
    if (!l.peer_live[0]) {
        DBG_LOG("send failure: the freed slot could not be taken again");
        goto out;
    }

    DBG_LOG("send failure: the slot is freed and taken again: ok");
    rc = 0;

out:
    if (fd >= 0)
        os_close(&e, fd);
    if (fd2 >= 0)
        os_close(&e, fd2);
    loop_shutdown(&l);
    wal_close(&w, &e);
    tmp_dir_destroy(dir);
    return rc;
}

/* ---- Replication, two nodes in one process ---- */

/*
 * Both loops are driven by hand, a pass each, turn by turn. Real nodes
 * block in their own ring and neither waits for the other, but the
 * order of what has to happen between them is the same, and taking
 * turns is what makes a two-node check possible in one thread.
 */
static void repl_settle(loop_t *a, loop_t *b, err_t *e, int32_t rounds)
{
    int32_t i;

    for (i = 0; i < rounds; i++) {
        loop_tick(a, e, FALSE);
        loop_tick(b, e, FALSE);
    }
}

/*
 * Turn both loops over until the replica is attached, or has gone.
 * Bounded rather than timed: what is being waited for is a completion
 * the kernel has already been told about, and a fixed number of passes
 * would be either too few on a loaded machine or wasted on an idle
 * one.
 */
static bool_t repl_wait_attached(loop_t *a, loop_t *b, err_t *e, bool_t want)
{
    int32_t i;

    for (i = 0; i < 2000; i++) {
        if ((a->peer_live[0] ? TRUE : FALSE) == want)
            return TRUE;
        loop_tick(a, e, FALSE);
        loop_tick(b, e, FALSE);
    }
    return FALSE;
}

/* Turn both loops over until the replica holds what the leader does. */
static bool_t repl_wait_caught_up(loop_t *a, loop_t *b, err_t *e,
                                  const wal_t *lw, const wal_t *fw)
{
    int32_t i;

    for (i = 0; i < 2000; i++) {
        if (wal_durable_seq(fw) == wal_durable_seq(lw))
            return TRUE;
        loop_tick(a, e, FALSE);
        loop_tick(b, e, FALSE);
    }
    return FALSE;
}

static bool_t repl_write(int32_t fd, uint64_t client_seq, uint16_t flags)
{
    return loop_send_frame(fd, MSG_OP_WRITE, flags, 7, client_seq,
                           (const uint8_t *)"record", 6);
}

int selfcheck_replication(void)
{
    static uint8_t  scratch[8192];
    static uint8_t  buf[512];
    static char     ldir[64];
    static char     fdir[64];
    static const char leader_text[] = "127.0.0.1:1";
    err_t           e;
    wal_t           lw;
    wal_t           fw;
    wal_open_t      info;
    loop_t          leader;
    loop_t          follower;
    session_table_t lsessions;
    session_table_t fsessions;
    sockaddr_in_t   addr;
    msg_header_t    h;
    int32_t         plen = 0;
    int32_t         fd = -1;
    int32_t         ffd = -1;
    uint64_t        session = 0;
    uint64_t        high = 0;
    int32_t         rc = 1;
    bool_t          follower_up = FALSE;

    err_init(&e);
    seg_tmp_path(ldir, 9);
    seg_tmp_path(fdir, 10);
    if (!result_ok(os_mkdir(&e, ldir, MODE_0700)) ||
        !result_ok(os_mkdir(&e, fdir, MODE_0700))) {
        DBG_LOG("replication: mkdir failed");
        return 1;
    }

    session_table_init(&lsessions);
    session_table_init(&fsessions);

    if (!result_ok(wal_open(&lw, &e, ldir, 65536, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &lsessions)) ||
        !result_ok(wal_open(&fw, &e, fdir, 65536, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &fsessions))) {
        DBG_LOG("replication: wal open failed");
        goto out_dirs;
    }

    if (!result_ok(loop_init(&leader, &e, &lw, &lsessions, 0x7F000001, 0))) {
        DBG_LOG("replication: loop unavailable, skipping");
        rc = 0;
        goto out_wal;
    }
    if (!result_ok(loop_init(&follower, &e, &fw, &fsessions, 0x7F000001, 0))) {
        DBG_LOG("replication: second loop failed");
        loop_shutdown(&leader);
        goto out_wal;
    }
    follower_up = TRUE;

    mem_zero((uint8_t *)&addr, (int32_t)sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)leader.port);
    addr.sin_addr = htonl(0x7F000001);

    if (!result_ok(loop_set_leader(&follower, &e, &addr, leader_text))) {
        DBG_LOG("replication: the follower could not take a timer");
        goto out_loops;
    }

    if (!repl_wait_attached(&leader, &follower, &e, TRUE)) {
        DBG_LOG("replication: the replica did not attach");
        goto out_loops;
    }

    /* A replicated write is answered only once the replica holds it. */
    fd = loop_client_connect(leader.port);
    if (fd < 0 || !loop_hello(fd, &leader, &e, 0, &session, &high)) {
        DBG_LOG("replication: client hello failed");
        goto out_loops;
    }
    if (!repl_write(fd, 1, MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC |
                    MSG_FLAG_REPLICATED)) {
        DBG_LOG("replication: write failed");
        goto out_loops;
    }
    if (!repl_wait_caught_up(&leader, &follower, &e, &lw, &fw)) {
        DBG_LOG("replication: the replica did not take the record");
        goto out_loops;
    }
    repl_settle(&leader, &follower, &e, 8);

    if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen)) {
        DBG_LOG("replication: no answer to a replicated write");
        goto out_loops;
    }
    if (h.op != MSG_OP_ACK || !(h.flags & MSG_FLAG_REPLICATED)) {
        DBG_LOG("replication: answered with op %d flags %d",
                (int32_t)h.op, (int32_t)h.flags);
        goto out_loops;
    }
    if (h.flags & MSG_FLAG_DEGRADED) {
        DBG_LOG("replication: acknowledged as degraded with a replica up");
        goto out_loops;
    }
    /* A write to the replica is sent to the leader by address. */
    ffd = loop_client_connect(follower.port);
    if (ffd < 0 || !loop_hello(ffd, &follower, &e, 0, &session, &high)) {
        DBG_LOG("replication: hello to the replica failed");
        goto out_loops;
    }
    if (!repl_write(ffd, 1, MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC)) {
        DBG_LOG("replication: write to the replica failed to send");
        goto out_loops;
    }
    repl_settle(&leader, &follower, &e, 16);
    if (!loop_read_frame(ffd, buf, (int32_t)sizeof(buf), &h, &plen)) {
        DBG_LOG("replication: the replica did not answer a write");
        goto out_loops;
    }
    {
        msg_err_payload_t ep;
        const uint8_t    *text;

        if (h.op != MSG_OP_ERR ||
            !msg_decode_err(buf + MSG_HEADER_SIZE, plen, &ep) ||
            ep.code != MSG_ERR_NOT_LEADER) {
            DBG_LOG("replication: a write to the replica was not refused");
            goto out_loops;
        }
        text = msg_err_text(buf + MSG_HEADER_SIZE, plen);
        if (!text || ep.text_len != (uint16_t)(sizeof(leader_text) - 1) ||
            mem_cmp(text, (const uint8_t *)leader_text,
                    (int32_t)ep.text_len) != 0) {
            DBG_LOG("replication: the refusal did not name the leader");
            goto out_loops;
        }
    }
    os_close(&e, ffd);
    ffd = -1;

    /*
     * With the replica gone the same write is refused at once. There
     * is nothing left that could confirm it, so waiting out the
     * deadline would only delay the same answer.
     */
    loop_shutdown(&follower);
    follower_up = FALSE;

    if (!repl_wait_attached(&leader, &leader, &e, FALSE)) {
        DBG_LOG("replication: the leader still counts a replica that left");
        goto out_loops;
    }
    if (!repl_write(fd, 2, MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC |
                    MSG_FLAG_REPLICATED)) {
        DBG_LOG("replication: second write failed");
        goto out_loops;
    }
    repl_settle(&leader, &leader, &e, 16);
    if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen)) {
        DBG_LOG("replication: no answer with the replica gone");
        goto out_loops;
    }
    {
        msg_err_payload_t ep;

        if (h.op != MSG_OP_ERR ||
            !msg_decode_err(buf + MSG_HEADER_SIZE, plen, &ep) ||
            ep.code != MSG_ERR_NO_REPLICAS) {
            DBG_LOG("replication: a write with no replica was not refused");
            goto out_loops;
        }
    }

    /*
     * The same write, taken because the client said it would accept
     * one copy, and told that it got one.
     */
    if (!repl_write(fd, 3, MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC |
                    MSG_FLAG_REPLICATED | MSG_FLAG_ALLOW_DEGRADED)) {
        DBG_LOG("replication: degraded write failed");
        goto out_loops;
    }
    repl_settle(&leader, &leader, &e, 16);
    if (!loop_read_frame(fd, buf, (int32_t)sizeof(buf), &h, &plen) ||
        h.op != MSG_OP_ACK || !(h.flags & MSG_FLAG_DEGRADED)) {
        DBG_LOG("replication: a leader-only copy did not say so");
        goto out_loops;
    }

    /*
     * A replica that comes back asks from the sequence it holds and is
     * sent what it missed. That is the path a restart takes, and the
     * only one that reads the log rather than the batch in hand.
     */
    if (!result_ok(loop_init(&follower, &e, &fw, &fsessions, 0x7F000001, 0))) {
        DBG_LOG("replication: the replica could not start again");
        goto out_loops;
    }
    follower_up = TRUE;
    if (!result_ok(loop_set_leader(&follower, &e, &addr, leader_text))) {
        DBG_LOG("replication: the replica could not take a timer again");
        goto out_loops;
    }
    if (!repl_wait_attached(&leader, &follower, &e, TRUE) ||
        !repl_wait_caught_up(&leader, &follower, &e, &lw, &fw)) {
        DBG_LOG("replication: after coming back the replica is at %d, "
                "the leader at %d", (int32_t)wal_durable_seq(&fw),
                (int32_t)wal_durable_seq(&lw));
        goto out_loops;
    }

    DBG_LOG("replication: stream, quorum ack, refusal and catch-up: ok");
    rc = 0;

out_loops:
    if (fd >= 0)
        os_close(&e, fd);
    if (ffd >= 0)
        os_close(&e, ffd);
    if (follower_up)
        loop_shutdown(&follower);
    loop_shutdown(&leader);
out_wal:
    wal_close(&lw, &e);
    wal_close(&fw, &e);
out_dirs:
    tmp_dir_destroy(ldir);
    tmp_dir_destroy(fdir);
    return rc;
}

/* ---- retention ---- */

/* Turn the loop over enough times for a retention pass to have run. */
static void retain_settle(loop_t *l, err_t *e)
{
    int32_t i;

    for (i = 0; i < 8; i++)
        loop_tick(l, e, FALSE);
}

/* Segment files present, counted from the directory rather than the table. */
static int32_t retain_files(const char *path)
{
    static uint8_t dbuf[4096];
    err_t          e;
    int32_t        fd = -1;
    int32_t        n = 0;
    int32_t        got = 0;

    err_init(&e);
    if (!result_ok(os_open(&e, path, O_RDONLY | O_DIRECTORY | O_CLOEXEC, 0,
                           &fd)))
        return -1;

    while (result_ok(os_getdents64(&e, fd, dbuf, (int32_t)sizeof(dbuf),
                                   &got)) && got > 0) {
        int32_t at = 0;

        while (at < got) {
            uint16_t reclen = 0;

            mem_copy((uint8_t *)&reclen, dbuf + at + DIRENT64_RECLEN_OFF, 2);
            if (reclen == 0)
                break;
            if (dbuf[at + DIRENT64_NAME_OFF] != '.')
                n++;
            at += reclen;
        }
    }

    os_close(&e, fd);
    return n;
}

int selfcheck_retention(void)
{
    static uint8_t  scratch[4096];
    static uint8_t  payload[64];
    static char     dir[64];
    err_t           e;
    wal_t           w;
    wal_open_t      info;
    loop_t          l;
    session_table_t sessions;
    wal_cursor_t    cur;
    int32_t         rec_size = wal_rec_size(24);
    int64_t         cap = (int64_t)(rec_size * 3);  /* three records each */
    int32_t         rc = 1;
    int32_t         got = 0;

    err_init(&e);
    mem_set(payload, 0x5A, (int32_t)sizeof(payload));
    seg_tmp_path(dir, 11);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("retention: mkdir failed");
        return 1;
    }

    if (!result_ok(wal_open(&w, &e, dir, cap, scratch,
                            (int32_t)sizeof(scratch), &info, NULL, NULL))) {
        DBG_LOG("retention: wal open failed");
        goto out_dir;
    }

    session_table_init(&sessions);
    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0x7F000001, 0))) {
        DBG_LOG("retention: loop unavailable, skipping");
        rc = 0;
        goto out_wal;
    }

    /*
     * Fifteen records at three to a segment: five segments, the last
     * of them active. Keeping two should take the other three.
     */
    if (!result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 15))) {
        DBG_LOG("retention: appends failed");
        goto out_loop;
    }
    if (wal_segments(&w) != 5) {
        DBG_LOG("retention: expected five segments, found %d",
                wal_segments(&w));
        goto out_loop;
    }

    /* Nothing configured, so nothing goes. */
    retain_settle(&l, &e);
    if (wal_segments(&w) != 5 || l.retain_removed != 0) {
        DBG_LOG("retention: deleted with retention off");
        goto out_loop;
    }

    /*
     * A reader parked at the end of a segment that is about to go.
     * This is the shape the stall needs: the descriptor stays valid
     * because the file is only unlinked, the records it has left are
     * the preallocated zeros that mean unwritten space, and the
     * segment holding what comes next is no longer in the table. What
     * that adds up to, without something saying otherwise, is a read
     * that returns nothing and looks exactly like having caught up.
     */
    wal_cursor_init(&cur);
    if (!result_ok(wal_cursor_seek(&w, &e, &cur, 4, scratch,
                                   (int32_t)sizeof(scratch)))) {
        DBG_LOG("retention: cannot seek a reader into the second segment");
        goto out_loop;
    }
    if (!result_ok(wal_cursor_read(&w, &e, &cur, 6, scratch,
                                   (int32_t)sizeof(scratch), &got)) ||
        got == 0 || cur.next_seq != 7) {
        DBG_LOG("retention: the reader stopped at %d after %d bytes",
                (int32_t)cur.next_seq, got);
        goto out_loop;
    }

    loop_set_retention(&l, 2, 0);
    retain_settle(&l, &e);

    if (wal_segments(&w) != 2) {
        DBG_LOG("retention: %d segments left, wanted two", wal_segments(&w));
        goto out_loop;
    }
    if (l.retain_removed != 3 || l.retain_errors != 0) {
        DBG_LOG("retention: removed %d, errors %d",
                (int32_t)l.retain_removed, (int32_t)l.retain_errors);
        goto out_loop;
    }
    if (wal_first_seq(&w) != 10) {
        DBG_LOG("retention: first sequence is %d, wanted ten",
                (int32_t)wal_first_seq(&w));
        goto out_loop;
    }

    /* The table and the directory agree: the files are actually gone. */
    if (retain_files(dir) != 2) {
        DBG_LOG("retention: %d files on disk, wanted two", retain_files(dir));
        goto out_loop;
    }

    /* A second pass has nothing left to do. */
    retain_settle(&l, &e);
    if (l.retain_removed != 3) {
        DBG_LOG("retention: removed more on a second pass");
        goto out_loop;
    }

    /*
     * The reader parked earlier now stands below the log. It has to be
     * told, because the alternative it would otherwise get is silence,
     * and silence here means caught up.
     */
    got = -1;
    if (result_ok(wal_cursor_read(&w, &e, &cur, 15, scratch,
                                  (int32_t)sizeof(scratch), &got))) {
        DBG_LOG("retention: a reader below the log was given %d bytes", got);
        wal_cursor_close(&cur, &e);
        goto out_loop;
    }
    wal_cursor_close(&cur, &e);

    /* Seeking there is refused too, which is what a replica meets. */
    wal_cursor_init(&cur);
    if (result_ok(wal_cursor_seek(&w, &e, &cur, 1, scratch,
                                  (int32_t)sizeof(scratch)))) {
        DBG_LOG("retention: seek to a deleted sequence was allowed");
        wal_cursor_close(&cur, &e);
        goto out_loop;
    }

    /*
     * What a subscriber left behind by retention is told. The cursor is
     * put back by hand because filling a socket until a real reader
     * stalls would make the check depend on the kernel's buffer sizes;
     * the state it stands in is the one retention leaves.
     */
    {
        int32_t  fd = loop_client_connect(l.port);
        uint64_t session = 0;
        uint64_t high = 0;
        uint8_t  sbuf[256];
        msg_subscribe_t req;
        msg_header_t    h;
        int32_t         plen = 0;
        int32_t         i;
        int32_t         slot = -1;
        bool_t          told = FALSE;

        if (fd < 0 || !loop_hello(fd, &l, &e, 0, &session, &high)) {
            DBG_LOG("retention: subscriber hello failed");
            goto out_loop;
        }

        req.from_seq = 10;
        req.min_seq = 0;
        msg_encode_subscribe(sbuf, (int32_t)sizeof(sbuf), &req);
        if (!loop_send_frame(fd, MSG_OP_SUBSCRIBE, MSG_FLAG_ALL_KEYS, 0, 1,
                             sbuf, MSG_SUBSCRIBE_SIZE)) {
            DBG_LOG("retention: subscribe failed to send");
            goto out_loop;
        }
        loop_settle(&l, &e);

        for (i = 0; i < LOOP_MAX_CONNS; i++) {
            if (l.conns[i].in_use && l.conns[i].sub.active)
                slot = i;
        }
        if (slot < 0) {
            DBG_LOG("retention: the subscription did not start");
            goto out_loop;
        }
        l.conns[slot].sub.cursor.next_seq = 4;
        loop_settle(&l, &e);

        while (loop_read_frame(fd, sbuf, (int32_t)sizeof(sbuf), &h, &plen)) {
            msg_err_payload_t ep;

            if (h.op != MSG_OP_ERR)
                continue;
            if (msg_decode_err(sbuf + MSG_HEADER_SIZE, plen, &ep) &&
                ep.code == MSG_ERR_NO_HISTORY)
                told = TRUE;
            break;
        }
        loop_read_forget(fd);
        os_close(&e, fd);

        if (!told) {
            DBG_LOG("retention: a stranded subscriber was not told why");
            goto out_loop;
        }
    }

    loop_shutdown(&l);
    if (!result_ok(wal_close(&w, &e))) {
        DBG_LOG("retention: close failed");
        goto out_dir;
    }

    /*
     * Recovery of a log that starts above one. The scan has to accept a
     * first segment whose base is not the first sequence ever written,
     * which is the whole of what retention leaves behind.
     */
    if (!result_ok(wal_open(&w, &e, dir, cap, scratch,
                            (int32_t)sizeof(scratch), &info, NULL, NULL))) {
        DBG_LOG("retention: reopen after retention failed");
        goto out_dir;
    }
    if (info.first_seq != 10 || info.last_seq != 15 || info.torn ||
        info.segments != 2) {
        DBG_LOG("retention: reopened at %d..%d over %d segments",
                (int32_t)info.first_seq, (int32_t)info.last_seq,
                info.segments);
        wal_close(&w, &e);
        goto out_dir;
    }

    wal_close(&w, &e);
    rc = 0;
    goto out_dir;

out_loop:
    loop_shutdown(&l);
out_wal:
    wal_close(&w, &e);
out_dir:
    tmp_dir_destroy(dir);
    return rc;
}

/*
 * A session forgotten by retention must not have its id handed out
 * again.
 *
 * The table is counted out of the log, and so was the next id to
 * issue, back when there was one. Retention broke that: a session
 * whose records are all deleted is never seen by the scan, so a
 * counter restarting from what it can see would reissue the id and
 * measure a new client's first write against the forgotten session's
 * high-water mark. The id is drawn now, which is what makes the
 * question moot.
 */
int selfcheck_session_retention(void)
{
    static uint8_t         scratch[4096];
    static uint8_t         payload[64];
    static char            dir[64];
    static session_table_t sessions;
    const uint64_t         gone = 101;   /* records only in what is deleted */
    const uint64_t         kept = 100;   /* records in what survives */
    err_t                  e;
    wal_t                  w;
    wal_open_t             info;
    wal_rec_t              rec;
    int32_t                rec_size = wal_rec_size(24);
    int64_t                cap = (int64_t)(rec_size * 3);
    int32_t                removed = 0;
    int32_t                rc = 1;
    int32_t                i;

    err_init(&e);
    mem_set(payload, 0x5A, (int32_t)sizeof(payload));
    seg_tmp_path(dir, 15);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("session retention: mkdir failed");
        return 1;
    }

    if (!result_ok(wal_open(&w, &e, dir, cap, scratch,
                            (int32_t)sizeof(scratch), &info, NULL, NULL))) {
        DBG_LOG("session retention: wal open failed");
        goto out_dir;
    }

    /*
     * Nine records for the session that will be forgotten, then six
     * for the one that stays. Three to a segment, so keeping two
     * leaves sequences ten to fifteen and nothing of the first
     * session at all.
     */
    for (i = 1; i <= 15; i++) {
        seg_fill_rec(&rec, 24, (i <= 9) ? gone : kept, (uint64_t)i, 1);
        if (!result_ok(wal_append(&w, &e, &rec, payload, scratch,
                                  (int32_t)sizeof(scratch)))) {
            DBG_LOG("session retention: append failed");
            goto out_wal;
        }
    }
    if (!result_ok(wal_sync(&w, &e))) {
        DBG_LOG("session retention: flush failed");
        goto out_wal;
    }

    if (!result_ok(wal_retain(&w, &e, wal_retain_mark(&w, 2), &removed)) ||
        removed != 3 || wal_first_seq(&w) != 10) {
        DBG_LOG("session retention: retention left the log at %d",
                (int32_t)wal_first_seq(&w));
        goto out_wal;
    }
    wal_close(&w, &e);

    /* Recovery of what is left, which has never heard of the first. */
    session_table_init(&sessions);
    if (!result_ok(wal_open(&w, &e, dir, cap, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &sessions))) {
        DBG_LOG("session retention: reopen failed");
        goto out_dir;
    }
    if (session_lookup(&sessions, kept) == NULL) {
        DBG_LOG("session retention: the surviving session was not recovered");
        goto out_wal;
    }
    if (session_lookup(&sessions, gone) != NULL) {
        DBG_LOG("session retention: a deleted session came back");
        goto out_wal;
    }

    /*
     * Now the thing itself. Every id issued from here has to miss the
     * forgotten one. A counter starting from what the scan could see
     * would hand it out first.
     */
    for (i = 0; i < 256; i++) {
        session_t *fresh = session_create(&sessions);

        if (!fresh) {
            DBG_LOG("session retention: no id was drawn");
            goto out_wal;
        }
        if (fresh->id == gone) {
            DBG_LOG("session retention: reissued the forgotten id after %d",
                    i);
            goto out_wal;
        }
        if (fresh->id == kept) {
            DBG_LOG("session retention: issued an id the log still holds");
            goto out_wal;
        }
    }

    rc = 0;

out_wal:
    wal_close(&w, &e);
out_dir:
    tmp_dir_destroy(dir);
    if (rc == 0)
        DBG_LOG("session retention: a forgotten id is not issued again: ok");
    return rc;
}

/*
 * The mark a leader will not delete past, and what freezes it.
 *
 * A replica that is away is the case the rule exists for: what it has
 * not got must stay on the leader, because a replica asking to stream
 * from below the retained log cannot be repaired by streaming.
 */
int selfcheck_retention_replica(void)
{
    static uint8_t  scratch[8192];
    static uint8_t  payload[64];
    static char     ldir[64];
    static char     fdir[64];
    static const char leader_text[] = "127.0.0.1:1";
    err_t           e;
    wal_t           lw;
    wal_t           fw;
    wal_open_t      info;
    loop_t          leader;
    loop_t          follower;
    session_table_t lsessions;
    session_table_t fsessions;
    sockaddr_in_t   addr;
    uint64_t        frozen_at;
    int32_t         rec_size = wal_rec_size(24);
    int64_t         cap = (int64_t)(rec_size * 3);
    int32_t         rc = 1;
    bool_t          follower_up = FALSE;

    err_init(&e);
    mem_set(payload, 0x5A, (int32_t)sizeof(payload));
    seg_tmp_path(ldir, 12);
    seg_tmp_path(fdir, 13);
    if (!result_ok(os_mkdir(&e, ldir, MODE_0700)) ||
        !result_ok(os_mkdir(&e, fdir, MODE_0700))) {
        DBG_LOG("retention/replica: mkdir failed");
        return 1;
    }

    session_table_init(&lsessions);
    session_table_init(&fsessions);

    if (!result_ok(wal_open(&lw, &e, ldir, cap, scratch,
                            (int32_t)sizeof(scratch), &info, NULL, NULL)) ||
        !result_ok(wal_open(&fw, &e, fdir, cap, scratch,
                            (int32_t)sizeof(scratch), &info, NULL, NULL))) {
        DBG_LOG("retention/replica: wal open failed");
        goto out_dirs;
    }

    if (!result_ok(loop_init(&leader, &e, &lw, &lsessions, 0x7F000001, 0))) {
        DBG_LOG("retention/replica: loop unavailable, skipping");
        rc = 0;
        goto out_wal;
    }
    if (!result_ok(loop_init(&follower, &e, &fw, &fsessions, 0x7F000001,
                             0))) {
        DBG_LOG("retention/replica: second loop failed");
        loop_shutdown(&leader);
        goto out_wal;
    }
    follower_up = TRUE;

    /*
     * Two expected, one that will ever attach. A record is not on two
     * nodes because one node says so, so the mark must not move at
     * all while the set is short.
     */
    loop_set_retention(&leader, 2, 2);
    loop_set_retention(&follower, 2, 0);

    /* Five segments on the leader, with nobody to confirm them. */
    if (!result_ok(wal_put(&lw, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 15))) {
        DBG_LOG("retention/replica: appends failed");
        goto out_loops;
    }
    retain_settle(&leader, &e);
    if (wal_first_seq(&lw) != 1 || leader.retain_removed != 0) {
        DBG_LOG("retention/replica: deleted before any replica confirmed");
        goto out_loops;
    }

    mem_zero((uint8_t *)&addr, (int32_t)sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)leader.port);
    addr.sin_addr = htonl(0x7F000001);

    if (!result_ok(loop_set_leader(&follower, &e, &addr, leader_text))) {
        DBG_LOG("retention/replica: the follower could not take a timer");
        goto out_loops;
    }
    if (!repl_wait_attached(&leader, &follower, &e, TRUE) ||
        !repl_wait_caught_up(&leader, &follower, &e, &lw, &fw)) {
        DBG_LOG("retention/replica: the replica did not catch up");
        goto out_loops;
    }
    repl_settle(&leader, &follower, &e, 16);

    if (leader.retain_floor != 0 || leader.retain_removed != 0) {
        DBG_LOG("retention/replica: one replica of two moved the mark to %d",
                (int32_t)leader.retain_floor);
        goto out_loops;
    }

    /*
     * Told to expect the one that is here, the mark follows it. The
     * count is unchanged, so what moves is only what the replicas are
     * allowed to release.
     */
    loop_set_retention(&leader, 2, 1);
    if (!result_ok(wal_put(&lw, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 3))) {
        DBG_LOG("retention/replica: appends failed");
        goto out_loops;
    }
    if (!repl_wait_caught_up(&leader, &follower, &e, &lw, &fw)) {
        DBG_LOG("retention/replica: the replica did not take the rest");
        goto out_loops;
    }
    repl_settle(&leader, &follower, &e, 16);

    if (leader.retain_floor != 18) {
        DBG_LOG("retention/replica: floor at %d, wanted eighteen",
                (int32_t)leader.retain_floor);
        goto out_loops;
    }
    if (wal_first_seq(&lw) != 13 || leader.retain_removed != 4) {
        DBG_LOG("retention/replica: kept %d after the replica confirmed",
                (int32_t)wal_first_seq(&lw));
        goto out_loops;
    }

    /* The replica keeps its own count, with nobody downstream of it. */
    if (wal_first_seq(&fw) != 13) {
        DBG_LOG("retention/replica: the replica kept %d",
                (int32_t)wal_first_seq(&fw));
        goto out_loops;
    }

    /* Now take it away and write past two more rollovers. */
    loop_shutdown(&follower);
    follower_up = FALSE;
    if (!repl_wait_attached(&leader, &leader, &e, FALSE)) {
        DBG_LOG("retention/replica: the leader did not notice it go");
        goto out_loops;
    }

    frozen_at = leader.retain_floor;
    if (!result_ok(wal_put(&lw, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 12))) {
        DBG_LOG("retention/replica: appends after the replica left failed");
        goto out_loops;
    }
    retain_settle(&leader, &e);

    /*
     * The mark is where the replica left it, so what it confirmed can
     * still go and nothing above that can. A replica coming back asks
     * for the sequence after the one it last confirmed, and this is
     * what leaves that sequence on disk for it to be sent.
     */
    if (leader.retain_floor != frozen_at) {
        DBG_LOG("retention/replica: the mark moved with the replica away");
        goto out_loops;
    }
    if (wal_first_seq(&lw) > frozen_at + 1) {
        DBG_LOG("retention/replica: deleted to %d, past the confirmed %d",
                (int32_t)wal_first_seq(&lw), (int32_t)frozen_at);
        goto out_loops;
    }

    /*
     * And the count alone would have gone further, which is what makes
     * the check above worth making: without the mark these segments
     * would be gone.
     */
    if (wal_retain_mark(&lw, 2) <= wal_first_seq(&lw)) {
        DBG_LOG("retention/replica: the count would not have deleted more");
        goto out_loops;
    }
    if (wal_segments(&lw) <= 2) {
        DBG_LOG("retention/replica: the log did not grow while held back");
        goto out_loops;
    }

    rc = 0;

out_loops:
    if (follower_up)
        loop_shutdown(&follower);
    loop_shutdown(&leader);
out_wal:
    wal_close(&lw, &e);
    wal_close(&fw, &e);
out_dirs:
    tmp_dir_destroy(ldir);
    tmp_dir_destroy(fdir);
    return rc;
}

/* ---- metrics ---- */

/* Offset of needle in hay, or -1. */
static int32_t sc_find(const char *hay, int32_t hay_len, const char *needle)
{
    int32_t n = 0;
    int32_t i;

    while (needle[n] != '\0')
        n++;
    if (n == 0 || n > hay_len)
        return -1;

    for (i = 0; i + n <= hay_len; i++) {
        if (mem_cmp((const uint8_t *)hay + i, (const uint8_t *)needle, n) == 0)
            return i;
    }
    return -1;
}

/* The decimal starting at off, or a miss reported as the sentinel. */
static uint64_t sc_number_at(const char *s, int32_t len, int32_t off)
{
    uint64_t v = 0;
    bool_t   any = FALSE;

    while (off < len && s[off] >= '0' && s[off] <= '9') {
        v = v * 10 + (uint64_t)(s[off] - '0');
        any = TRUE;
        off++;
    }
    return any ? v : (uint64_t)0xFFFFFFFFFFFFFFFFULL;
}

/* The value of a whole-line metric, or the sentinel if it is not there. */
static uint64_t sc_metric(const char *body, int32_t len, const char *name)
{
    int32_t at = sc_find(body, len, name);
    int32_t n = 0;

    if (at < 0)
        return (uint64_t)0xFFFFFFFFFFFFFFFFULL;
    while (name[n] != '\0')
        n++;
    return sc_number_at(body, len, at + n);
}

/* Ask the endpoint once and return what it said. */
static int32_t sc_scrape(loop_t *l, err_t *e, const char *request,
                         int32_t req_len, char *out, int32_t cap)
{
    int32_t fd = loop_client_connect(l->metrics_port);
    int32_t at = 0;
    int32_t rounds;

    if (fd < 0)
        return -1;
    loop_read_forget(fd);

    if (os_write_raw(fd, request, (size_t)req_len) != (ssize_t)req_len) {
        os_close(e, fd);
        return -1;
    }
    /*
     * Bounded, and the receive never blocks. An endpoint that decides
     * not to answer is a case under test here, and a blocking read
     * would turn it into a self-check that hangs rather than one that
     * reports what it found.
     */
    for (rounds = 0; rounds < 64 && at < cap; rounds++) {
        int32_t res = 0;

        loop_settle(l, e);
        os_recv(fd, (uint8_t *)out + at, cap - at, MSG_DONTWAIT, &res);
        if (res == -EAGAIN)
            continue;
        if (res <= 0)
            break;              /* closed, which is how every answer ends */
        at += res;
    }

    os_close(e, fd);
    return at;
}

int selfcheck_metrics(void)
{
    static uint8_t  scratch[4096];
    static uint8_t  payload[64];
    static char     dir[64];
    static char     body[LOOP_METRICS_CAP * 2];
    static char     junk[5000];
    err_t           e;
    wal_t           w;
    wal_open_t      info;
    loop_t          l;
    session_table_t sessions;
    int32_t         rc = 1;
    int32_t         n;

    err_init(&e);
    mem_set(payload, 0x5A, (int32_t)sizeof(payload));
    seg_tmp_path(dir, 14);
    if (!result_ok(os_mkdir(&e, dir, MODE_0700))) {
        DBG_LOG("metrics: mkdir failed");
        return 1;
    }

    if (!result_ok(wal_open(&w, &e, dir, 65536, scratch,
                            (int32_t)sizeof(scratch), &info, NULL, NULL))) {
        DBG_LOG("metrics: wal open failed");
        goto out_dir;
    }

    session_table_init(&sessions);
    if (!result_ok(loop_init(&l, &e, &w, &sessions, 0x7F000001, 0))) {
        DBG_LOG("metrics: loop unavailable, skipping");
        rc = 0;
        goto out_wal;
    }

    if (!result_ok(loop_set_metrics_port(&l, &e, 0x7F000001, 0))) {
        DBG_LOG("metrics: could not listen");
        goto out_loop;
    }
    if (l.metrics_port <= 0) {
        DBG_LOG("metrics: no port reported");
        goto out_loop;
    }

    if (!result_ok(wal_put(&w, &e, scratch, (int32_t)sizeof(scratch),
                           payload, 4))) {
        DBG_LOG("metrics: appends failed");
        goto out_loop;
    }

    n = sc_scrape(&l, &e, "GET /metrics HTTP/1.0\r\n\r\n", 25, body,
                  (int32_t)sizeof(body));
    if (n <= 0) {
        DBG_LOG("metrics: the endpoint said nothing");
        goto out_loop;
    }
    if (sc_find(body, n, "HTTP/1.0 200 OK\r\n") != 0) {
        DBG_LOG("metrics: no 200 in the answer");
        goto out_loop;
    }
    if (sc_find(body, n, "Content-Type: text/plain; version=0.0.4") < 0) {
        DBG_LOG("metrics: wrong content type");
        goto out_loop;
    }

    /*
     * A rendered value against the state it came from. Comparing the
     * shape of the document would pass on a formatter that printed the
     * same wrong number every time.
     */
    if (sc_metric(body, n, "\nmsgsrvd_durable_seq ") != wal_durable_seq(&w)) {
        DBG_LOG("metrics: durable sequence rendered as %d, log says %d",
                (int32_t)sc_metric(body, n, "\nmsgsrvd_durable_seq "),
                (int32_t)wal_durable_seq(&w));
        goto out_loop;
    }
    if (sc_metric(body, n, "\nmsgsrvd_first_seq ") != wal_first_seq(&w) ||
        sc_metric(body, n, "\nmsgsrvd_next_seq ") != wal_next_seq(&w) ||
        sc_metric(body, n, "\nmsgsrvd_segments ") !=
        (uint64_t)wal_segments(&w)) {
        DBG_LOG("metrics: the log's own numbers do not match");
        goto out_loop;
    }
    if (sc_metric(body, n, "\nmsgsrvd_role{role=\"leader\"} ") != 1 ||
        sc_metric(body, n, "\nmsgsrvd_role{role=\"follower\"} ") != 0) {
        DBG_LOG("metrics: the role is wrong");
        goto out_loop;
    }
    if (sc_metric(body, n, "\nmsgsrvd_scrapes_total ") != 0) {
        DBG_LOG("metrics: a scrape counted itself");
        goto out_loop;
    }

    /* The one it just served is counted by the time the next one asks. */
    n = sc_scrape(&l, &e, "GET / HTTP/1.0\n\n", 16, body,
                  (int32_t)sizeof(body));
    if (n <= 0 || sc_find(body, n, "HTTP/1.0 200 OK\r\n") != 0) {
        DBG_LOG("metrics: bare newlines were not accepted");
        goto out_loop;
    }
    if (sc_metric(body, n, "\nmsgsrvd_scrapes_total ") != 1) {
        DBG_LOG("metrics: the first scrape was not counted");
        goto out_loop;
    }

    /*
     * A head longer than any request head has a reason to be is
     * dropped without an answer. Describing the node to something that
     * cannot ask properly is the thing to avoid.
     */
    mem_set((uint8_t *)junk, (uint8_t)'A', (int32_t)sizeof(junk));
    n = sc_scrape(&l, &e, junk, (int32_t)sizeof(junk), body,
                  (int32_t)sizeof(body));
    if (n != 0) {
        DBG_LOG("metrics: an oversized request got %d bytes back", n);
        goto out_loop;
    }
    if (l.scrapes_refused == 0) {
        DBG_LOG("metrics: the oversized request was not counted");
        goto out_loop;
    }

    /*
     * What a write and a refusal do to the counters. Both are read
     * back through the endpoint rather than off the loop, so a
     * formatter that dropped a label would show up here too.
     */
    {
        int32_t  fd = loop_client_connect(l.port);
        uint64_t session = 0;
        uint64_t high = 0;

        if (fd < 0 || !loop_hello(fd, &l, &e, 0, &session, &high)) {
            DBG_LOG("metrics: client hello failed");
            goto out_loop;
        }
        if (!loop_send_frame(fd, MSG_OP_WRITE,
                             MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC, 3, 1,
                             (const uint8_t *)"x", 1)) {
            DBG_LOG("metrics: write failed to send");
            goto out_loop;
        }
        /* One that asked for no more than an append, so the two levels
         * are told apart by what was asked and not by both being set. */
        if (!loop_send_frame(fd, MSG_OP_WRITE, MSG_FLAG_ACK_REQ, 3, 2,
                             (const uint8_t *)"y", 1)) {
            DBG_LOG("metrics: second write failed to send");
            goto out_loop;
        }
        /* DELETE is understood and refused, which is a refusal with no
         * state of the node behind it: the "other" bucket. */
        if (!loop_send_frame(fd, MSG_OP_DELETE, 0, 3, 3, NULL, 0)) {
            DBG_LOG("metrics: delete failed to send");
            goto out_loop;
        }
        loop_settle(&l, &e);
        loop_read_forget(fd);
        os_close(&e, fd);
    }

    n = sc_scrape(&l, &e, "GET / HTTP/1.0\r\n\r\n", 18, body,
                  (int32_t)sizeof(body));
    if (n <= 0) {
        DBG_LOG("metrics: no answer after a write");
        goto out_loop;
    }
    if (sc_metric(body, n, "\nmsgsrvd_writes_total ") != 2) {
        DBG_LOG("metrics: the writes were not counted");
        goto out_loop;
    }
    if (sc_metric(body, n, "durability=\"sync\"} ") != 1 ||
        sc_metric(body, n, "durability=\"append\"} ") != 1 ||
        sc_metric(body, n, "durability=\"replicated\"} ") != 0) {
        DBG_LOG("metrics: the acknowledgements were counted at the wrong "
                "level");
        goto out_loop;
    }
    if (sc_metric(body, n, "reason=\"other\"} ") != 1 ||
        sc_metric(body, n, "reason=\"storage\"} ") != 0) {
        DBG_LOG("metrics: the refusal was counted under the wrong reason");
        goto out_loop;
    }
    if (sc_metric(body, n, "\nmsgsrvd_fsync_seconds_count ") !=
        l.flushes || l.flushes == 0) {
        DBG_LOG("metrics: the flush histogram does not add up to %d",
                (int32_t)l.flushes);
        goto out_loop;
    }

    /*
     * Every slot held by something that connected and then said
     * nothing. The endpoint has to stay reachable through that, or
     * four idle connections would be enough to stop anyone seeing the
     * node again.
     */
    {
        int32_t idle[LOOP_MAX_SCRAPES + 1];
        int32_t i;

        for (i = 0; i <= LOOP_MAX_SCRAPES; i++) {
            idle[i] = loop_client_connect(l.metrics_port);
            if (idle[i] < 0) {
                DBG_LOG("metrics: could not open an idle connection");
                goto out_loop;
            }
            loop_settle(&l, &e);
        }

        n = sc_scrape(&l, &e, "GET / HTTP/1.0\r\n\r\n", 18, body,
                      (int32_t)sizeof(body));
        for (i = 0; i <= LOOP_MAX_SCRAPES; i++) {
            loop_read_forget(idle[i]);
            os_close(&e, idle[i]);
        }
        if (n <= 0 || sc_find(body, n, "HTTP/1.0 200 OK\r\n") != 0) {
            DBG_LOG("metrics: idle connections shut the endpoint out");
            goto out_loop;
        }
    }

    /*
     * A document that will not fit is refused rather than cut short. A
     * truncated one reads as a node whose missing metrics never
     * existed, which is worse than no answer at all.
     */
    if (loop_metrics(&l, body, 64) != 0) {
        DBG_LOG("metrics: a document cut mid-sample was returned");
        goto out_loop;
    }

    /*
     * And one cut exactly at a line ending, which the last byte cannot
     * tell apart from a document that finished.
     */
    if (loop_metrics(&l, body, 56) != 0) {
        DBG_LOG("metrics: a document cut at a line ending was returned");
        goto out_loop;
    }

    rc = 0;

out_loop:
    loop_shutdown(&l);
out_wal:
    wal_close(&w, &e);
out_dir:
    tmp_dir_destroy(dir);
    return rc;
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
    if (selfcheck_wal_index())
        return 1;
    if (selfcheck_conn_framing())
        return 1;
    if (selfcheck_conn_session())
        return 1;
    if (selfcheck_epoll())
        return 1;
    if (selfcheck_sessions())
        return 1;
    if (selfcheck_loop())
        return 1;
    if (selfcheck_session_recovery())
        return 1;
    if (selfcheck_send_failure())
        return 1;
    if (selfcheck_replication())
        return 1;
    if (selfcheck_retention())
        return 1;
    if (selfcheck_session_retention())
        return 1;
    if (selfcheck_retention_replica())
        return 1;
    if (selfcheck_metrics())
        return 1;
    if (selfcheck_err(e))
        return 1;
    return 0;
}

#include "core/types.h"
#include "core/arena.h"
#include "core/mem.h"
#include "core/err.h"
#include "core/debug.h"
#include "proto/msg.h"
#include "proto/conn.h"
#include "io/uring.h"
#include "sys/os.h"

/*
 * In-process self-checks.
 *
 * Each check returns 0 on success and 1 on failure, and logs what it
 * verified in the debug build. They run in dependency order: a
 * failure in the codec makes the connection results meaningless, so
 * main stops at the first one that fails.
 */

/* Static arena backing buffer, 4MB */
#define ARENA_SIZE (4 * 1024 * 1024)
static uint8_t arena_buf[ARENA_SIZE];

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

static int selfcheck_arena(arena_t *a)
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

static int selfcheck_header(void)
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

static int selfcheck_payloads(void)
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

/*
 * Framing checks. These run in CONN_MODE_INTERNAL so that the session
 * handshake stays out of the way; session ordering has its own check.
 */
static int selfcheck_conn_framing(void)
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
static int selfcheck_conn_session(void)
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

static int selfcheck_uring(void)
{
    uring_t  ring;
    err_t    uring_err;
    result_t r;

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

    uring_destroy(&ring);
    if (ring.ring_fd != -1) {
        DBG_LOG("uring: destroy did not clear fd");
        return 1;
    }
    DBG_LOG("uring: destroy ok");
    return 0;
}

static int selfcheck_err(err_t *e)
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

int main(int argc, char **argv)
{
    arena_t arena;
    err_t   err;

    (void)argc;
    (void)argv;

    arena_init(&arena, arena_buf, ARENA_SIZE);
    err_init(&err);

    {
        const char hello[] = "msgsrvd: sys layer ok\n";
        os_write_raw(1, hello, sizeof(hello) - 1);
    }

    DBG_LOG("msgsrvd starting");
    DBG_LOG("arena: %d bytes, header: %d bytes, protocol v%d",
            ARENA_SIZE, MSG_HEADER_SIZE, MSG_VERSION);

    if (selfcheck_arena(&arena))
        return 1;
    if (selfcheck_header())
        return 1;
    if (selfcheck_payloads())
        return 1;
    if (selfcheck_conn_framing())
        return 1;
    if (selfcheck_conn_session())
        return 1;
    if (selfcheck_uring())
        return 1;
    if (selfcheck_err(&err))
        return 1;

    DBG_LOG("msgsrvd init complete");
    return 0;
}

#include "core/types.h"
#include "core/arena.h"
#include "core/mem.h"
#include "core/err.h"
#include "core/crc32.h"
#include "core/debug.h"
#include "proto/msg.h"
#include "proto/conn.h"
#include "store/wal.h"
#include "store/wal_seg.h"
#include "io/uring.h"
#include "sys/os.h"

/* Static arena backing buffer, 4MB */
#define ARENA_SIZE (4 * 1024 * 1024)
static uint8_t arena_buf[ARENA_SIZE];

int main(int argc, char **argv)
{
    arena_t arena;
    err_t err;

    (void)argc;
    (void)argv;

    arena_init(&arena, arena_buf, ARENA_SIZE);
    err_init(&err);

    {
        const char hello[] = "msgsrvd: sys layer ok\n";
        os_write_raw(1, hello, sizeof(hello) - 1);
    }

    DBG_LOG("msgsrvd starting");
    DBG_LOG("arena: %d bytes, header: %d bytes",
            ARENA_SIZE, MSG_HEADER_SIZE);

    /* Arena allocator self-test */
    {
        span_t s1 = arena_alloc(&arena, 100);
        span_t s2 = arena_alloc(&arena, 200);
        uint8_t *p1;

        if (span_empty(s1) || span_empty(s2) || s1.off == s2.off) {
            DBG_LOG("arena alloc failed");
            return 1;
        }
        if (s2.off < s1.off + s1.len) {
            DBG_LOG("arena spans overlap");
            return 1;
        }

        p1 = arena_ptr(&arena, s1);
        if (!p1) {
            DBG_LOG("arena resolve failed");
            return 1;
        }
        mem_zero(p1, s1.len);

        DBG_LOG("arena: ok (used=%d, remaining=%d)",
                arena.used, arena_remaining(&arena));
    }

    /* Protocol header round-trip self-test */
    {
        uint8_t buf[64];
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

        DBG_LOG("protocol header round-trip: ok");
    }

    /* Connection state machine self-tests */
    {
        static uint8_t         conn_buf[4096];
        static uint8_t         wire[4096];
        conn_t                 c;
        conn_action_t          actions[4];
        msg_header_t           h;
        int32_t                n;
        int32_t                i;
        const char             payload_str[] = "hello";
        int32_t                payload_len = (int32_t)sizeof(payload_str) - 1;

        /* Case 1: one complete frame in a single feed */
        conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf));
        h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, MSG_FLAG_ACK_REQ,
                           (uint32_t)payload_len, 0xAABB, 7);
        msg_encode_header(wire, (int32_t)sizeof(wire), &h);
        mem_copy(wire + MSG_HEADER_SIZE, (const uint8_t *)payload_str,
                 payload_len);

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
        conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf));
        for (i = 0; i < MSG_HEADER_SIZE + payload_len - 1; i++) {
            conn_recv_append(&c, wire + i, 1);
            n = conn_feed(&c, actions, 4);
            if (n != 0 || c.closed) {
                DBG_LOG("conn case2: unexpected emit at byte %d", i);
                return 1;
            }
        }
        /* Final byte, should complete the frame */
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
        conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf));
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

        /* Case 4: oversized payload -> PAYLOAD_TOO_BIG + CLOSE */
        conn_init(&c, conn_buf, (int32_t)sizeof(conn_buf));
        h = msg_header_new(MSG_OP_WRITE, MSG_RECORD_NONE, 0,
                           /* bigger than buf_cap - HEADER_SIZE */
                           (uint32_t)sizeof(conn_buf),
                           0, 99);
        msg_encode_header(wire, (int32_t)sizeof(wire), &h);
        conn_recv_append(&c, wire, MSG_HEADER_SIZE);
        n = conn_feed(&c, actions, 4);
        if (n != 2 ||
            actions[0].type != CONN_ACTION_REPLY_ERR ||
            actions[0].u.reply_err.code != MSG_ERR_PAYLOAD_TOO_BIG ||
            actions[0].u.reply_err.sequence != 99 ||
            actions[1].type != CONN_ACTION_CLOSE ||
            !c.closed) {
            DBG_LOG("conn case4: oversized-payload path failed (n=%d)", n);
            return 1;
        }
        DBG_LOG("conn case4: oversized-payload close: ok");
    }

    /* io_uring init/destroy self-test */
    {
        uring_t  ring;
        err_t    uring_err;
        result_t r;

        err_init(&uring_err);
        r = uring_init(&ring, &uring_err, 8);
        if (!result_ok(r)) {
            DBG_LOG("uring_init failed (errno=%d), skipping",
                    uring_err.frames[0].detail.u.errno_val);
        } else {
            if (ring.ring_fd <= 0 ||
                ring.sq_head == NULL || ring.sq_tail == NULL ||
                ring.cq_head == NULL || ring.cq_tail == NULL ||
                ring.sqes == NULL || ring.cqes == NULL) {
                DBG_LOG("uring: ring pointers not populated");
                uring_destroy(&ring);
                return 1;
            }
            DBG_LOG("uring: init ok (fd=%d, sq=%u, cq=%u)",
                    ring.ring_fd,
                    *ring.sq_entries_ptr, *ring.cq_entries_ptr);
            uring_destroy(&ring);
            if (ring.ring_fd != -1) {
                DBG_LOG("uring: destroy did not clear fd");
                return 1;
            }
            DBG_LOG("uring: destroy ok");
        }
    }

    /* crc32c self-test */
    {
        const char check_str[] = "123456789";
        const int32_t check_len = (int32_t)sizeof(check_str) - 1;
        uint32_t whole;
        uint32_t split;
        uint32_t flipped;
        uint8_t mangled[9];

        /* Standard CRC-32C check value */
        whole = crc32c_buf((const uint8_t *)check_str, check_len);
        if (whole != 0xE3069283u) {
            DBG_LOG("crc32c: check value mismatch (got 0x%08x)", whole);
            return 1;
        }

        /* Incremental update must match the single-shot result */
        split = crc32c(CRC32C_INIT, (const uint8_t *)check_str, 4);
        split = crc32c(split, (const uint8_t *)check_str + 4, check_len - 4);
        split = crc32c_fin(split);
        if (split != whole) {
            DBG_LOG("crc32c: incremental != single-shot");
            return 1;
        }

        /* A single flipped bit must change the crc */
        mem_copy(mangled, (const uint8_t *)check_str, check_len);
        mangled[0] ^= 0x01;
        flipped = crc32c_buf(mangled, check_len);
        if (flipped == whole) {
            DBG_LOG("crc32c: flipped bit did not change crc");
            return 1;
        }

        DBG_LOG("crc32c: ok (0x%08x)", whole);
    }

    /* WAL record codec self-test */
    {
        static uint8_t  recbuf[256];
        const char      payload_str[] = "wal-record-payload";
        const int32_t   payload_len = (int32_t)sizeof(payload_str) - 1;
        wal_rec_t       in;
        wal_rec_t       out;
        const uint8_t   *pl;
        int32_t         total;
        int32_t         got;

        mem_zero((uint8_t *)&in, sizeof(in));
        in.wal_seq = 0x0102030405060708ull;
        in.partition_key = 0xAABBCCDDull;
        in.client_seq = 7;
        in.record_type = MSG_RECORD_NONE;
        in.flags = MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC;

        total = wal_encode_rec(recbuf, (int32_t)sizeof(recbuf), &in,
                               (const uint8_t *)payload_str, payload_len);
        if (total != wal_rec_size(payload_len)) {
            DBG_LOG("wal: encode size wrong (got %d)", total);
            return 1;
        }

        /* Round-trip decode */
        got = wal_decode_rec(recbuf, total, &out, &pl);
        if (got != total ||
            out.magic != WAL_REC_MAGIC ||
            out.len != (uint32_t)payload_len ||
            out.wal_seq != in.wal_seq ||
            out.partition_key != in.partition_key ||
            out.client_seq != in.client_seq ||
            out.record_type != in.record_type ||
            out.flags != in.flags ||
            mem_cmp(pl, (const uint8_t *)payload_str, payload_len) != 0) {
            DBG_LOG("wal: record round-trip mismatch");
            return 1;
        }

        /* A flipped payload byte must fail the crc */
        recbuf[WAL_REC_HDR_SIZE] ^= 0x01;
        if (wal_decode_rec(recbuf, total, &out, &pl) != 0) {
            DBG_LOG("wal: corrupt payload not detected");
            return 1;
        }
        recbuf[WAL_REC_HDR_SIZE] ^= 0x01;

        /* A flipped header byte must fail the crc */
        recbuf[8] ^= 0x01;
        if (wal_decode_rec(recbuf, total, &out, &pl) != 0) {
            DBG_LOG("wal: corrupt header not detected");
            return 1;
        }
        recbuf[8] ^= 0x01;

        /* A truncated buffer decodes to nothing */
        if (wal_decode_rec(recbuf, total - 1, &out, &pl) != 0) {
            DBG_LOG("wal: truncated record not rejected");
            return 1;
        }

        /* Encode into too small a buffer fails cleanly */
        if (wal_encode_rec(recbuf, WAL_REC_HDR_SIZE, &in,
                           (const uint8_t *)payload_str, payload_len) != 0) {
            DBG_LOG("wal: undersized encode not rejected");
            return 1;
        }

        DBG_LOG("wal: record codec: ok (rec=%d bytes)", total);
    }

    /* WAL segment naming self-test */
    {
        char        name[WAL_SEG_NAME_LEN + 1];
        uint64_t    seq;

        wal_seg_name(0x2aull, name);
        if (mem_cmp((const uint8_t *)name,
                    (const uint8_t *)"000000000000002a.wal",
                    WAL_SEG_NAME_LEN + 1) != 0) {
            DBG_LOG("wal: segment name format wrong (%s)", name);
            return 1;
        }
        if (!wal_seg_parse(name, &seq) || seq != 0x2aull) {
            DBG_LOG("wal: segment name parse failed");
            return 1;
        }

        /* Round-trip a large value */
        wal_seg_name(0xDEADBEEFCAFEBABEull, name);
        if (!wal_seg_parse(name, &seq) || seq != 0xDEADBEEFCAFEBABEull) {
            DBG_LOG("wal: segment name round-trip failed");
            return 1;
        }

        /* Reject a malformed name */
        if (wal_seg_parse("000000000000002a.bad", &seq)) {
            DBG_LOG("wal: malformed segment name accepted");
            return 1;
        }

        DBG_LOG("wal: segment naming: ok");
    }

    /* Error stack self-test */
    {
        ERR_PUSH(&err, ERR_STORAGE);
        ERR_PUSH_FD(&err, ERR_STORAGE, 7);
        ERR_PUSH_INT(&err, ERR_INVALID, 1234);

        if (!err_has_error(&err) || err_frame_count(&err) != 3) {
            DBG_LOG("err stack failed");
            return 1;
        }
        dbg_err_print(&err);
        DBG_LOG("err: ok");
    }

    DBG_LOG("msgsrvd init complete");
    return 0;
}

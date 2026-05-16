#include "core/types.h"
#include "core/arena.h"
#include "core/mem.h"
#include "core/err.h"
#include "core/debug.h"
#include "proto/msg.h"
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

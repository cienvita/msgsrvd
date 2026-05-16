#include "core/types.h"
#include "core/arena.h"
#include "core/mem.h"
#include "core/err.h"
#include "core/debug.h"

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

    DBG_LOG("msgsrvd starting");
    DBG_LOG("arena: %d bytes", ARENA_SIZE);

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

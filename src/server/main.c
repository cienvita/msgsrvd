#include "core/types.h"
#include "core/arena.h"
#include "core/err.h"
#include "core/debug.h"
#include "proto/msg.h"
#include "server/selfcheck.h"
#include "sys/os.h"

/* Static arena backing buffer, 4MB */
#define ARENA_SIZE (4 * 1024 * 1024)
static uint8_t arena_buf[ARENA_SIZE];

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

    if (selfcheck_run(&arena, &err))
        return 1;

    DBG_LOG("msgsrvd init complete");
    return 0;
}

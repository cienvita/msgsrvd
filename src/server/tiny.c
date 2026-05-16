#include "core/types.h"
#include "sys/os.h"

/*
 * Minimal freestanding entry point. Hello world only.
 * No libc, no CRT, no main(), no error helpers.
 */

void _start(void)
{
    static const char msg[] = "hello world\n";
    os_write_raw(1, msg, sizeof(msg) - 1);
    os_exit(0);
}

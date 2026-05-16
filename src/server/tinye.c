#include "core/types.h"
#include "core/err.h"
#include "sys/os.h"

/*
 * Freestanding entry point with error stack demo.
 * No libc, no CRT, no main(), no debug helpers.
 * Formats err_t frames via os_write_raw.
 */

static int32_t str_len(const char *s)
{
    int32_t n = 0;
    while (s[n]) n++;
    return n;
}

static void write_str(const char *s)
{
    os_write_raw(1, s, (size_t)str_len(s));
}

/* Decimal int64 -> stdout, no libc */
static void write_i64(int64_t v)
{
    char buf[24];
    int32_t i = (int32_t)sizeof(buf);
    uint64_t u;
    bool_t neg = FALSE;

    if (v < 0) {
        neg = TRUE;
        u = (uint64_t)(-(v + 1)) + 1; /* INT64_MIN-safe negation */
    } else {
        u = (uint64_t)v;
    }

    if (u == 0) {
        buf[--i] = '0';
    } else {
        while (u) {
            buf[--i] = (char)('0' + (u % 10));
            u /= 10;
        }
    }
    if (neg) buf[--i] = '-';
    os_write_raw(1, buf + i, (size_t)((int32_t)sizeof(buf) - i));
}

static void print_err(const err_t *e)
{
    int32_t count = err_frame_count(e);
    int32_t i;

    if (!err_has_error(e)) {
        write_str("[err] (none)\n");
        return;
    }

    write_str("[err] ");
    write_i64(count);
    write_str(" frames:\n");

    for (i = 0; i < count; i++) {
        const err_frame_t *f = err_frame_at(e, i);
        if (!f) continue;
        write_str("  [");
        write_i64(i);
        write_str("] ");
        write_str(f->file);
        write_str(":");
        write_i64(f->line);
        write_str(" ");
        write_str(f->func);
        write_str("() code=");
        write_i64(f->code);
        switch (f->detail.tag) {
        case ERR_DTL_ERRNO:
            write_str(" errno=");
            write_i64(f->detail.u.errno_val);
            break;
        case ERR_DTL_FD:
            write_str(" fd=");
            write_i64(f->detail.u.fd);
            break;
        case ERR_DTL_INT:
            write_str(" val=");
            write_i64(f->detail.u.ival);
            break;
        default:
            break;
        }
        write_str("\n");
    }
}

void _start(void)
{
    static const char msg[] = "hello world\n";
    err_t err;

    os_write_raw(1, msg, sizeof(msg) - 1);

    err_init(&err);
    ERR_PUSH_ERRNO(&err, ERR_SYSCALL, 28);   /* simulated ENOSPC */
    ERR_PUSH_FD(&err, ERR_STORAGE, 7);
    ERR_PUSH_INT(&err, ERR_INVALID, 1234);
    print_err(&err);

    os_exit(0);
}

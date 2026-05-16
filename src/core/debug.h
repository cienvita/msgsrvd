#ifndef MSGSRVD_DEBUG_H
#define MSGSRVD_DEBUG_H

#include "core/types.h"
#include "core/err.h"

/*
 * Debug output, only available in MSGSRVD_DEBUG builds.
 * Uses libc printf. Never called from functional code paths.
 */

#ifdef MSGSRVD_DEBUG

/* Forward-declare printf to avoid including stdio.h everywhere */
int printf(const char *fmt, ...);

static inline void dbg_err_print(const err_t *e)
{
    int32_t i;
    int32_t count = err_frame_count(e);

    if (!err_has_error(e)) {
        printf("[err] (none)\n");
        return;
    }

    printf("[err] %d frames (%d dropped):\n", count, e->dropped);
    for (i = 0; i < count; i++) {
        const err_frame_t *f = err_frame_at(e, i);
        if (!f) continue;

        printf("  [%d] %s:%u %s() code=%d",
               i, f->file, f->line, f->func, f->code);

        switch (f->detail.tag) {
        case ERR_DTL_ERRNO:
            printf(" errno=%d", f->detail.u.errno_val);
            break;
        case ERR_DTL_FD:
            printf(" fd=%d", f->detail.u.fd);
            break;
        case ERR_DTL_OFFSET:
            printf(" offset=%llu", (unsigned long long)f->detail.u.offset);
            break;
        case ERR_DTL_SPAN:
            printf(" span(off=%d,len=%d)", f->detail.u.span.off,
                   f->detail.u.span.len);
            break;
        case ERR_DTL_INT:
            printf(" val=%lld", (long long)f->detail.u.ival);
            break;
        default:
            break;
        }
        printf("\n");
    }
}

#define DBG_LOG(...) do { printf("[dbg] " __VA_ARGS__); printf("\n"); } while (0)

#else

#define DBG_LOG(...) ((void)0)
static inline void dbg_err_print(const err_t *e) { (void)e; }

#endif /* MSGSRVD_DEBUG */

#endif /* MSGSRVD_DEBUG_H */

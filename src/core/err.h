#ifndef MSGSRVD_ERR_H
#define MSGSRVD_ERR_H

#include "types.h"

/*
 * Structured error context, carried through the call stack.
 * Fixed size, zero allocations.
 *
 * Stores top frames (most recent callers) and bottom frames
 * (error origin). Middle frames are dropped when full.
 * Frame count is compile-time configurable.
 *
 * Each frame carries structured detail (fd, errno, offset, span)
 * rather than text. Human-readable formatting is done only in the
 * debug build's logging layer.
 *
 * Convention: only used at module boundaries and I/O paths.
 * Pure leaf functions (span ops, arena ops) use sentinel returns.
 */

#ifndef ERR_MAX_FRAMES
#define ERR_MAX_FRAMES 8
#endif

STATIC_ASSERT(ERR_MAX_FRAMES >= 4, err_frames_minimum);
STATIC_ASSERT(ERR_MAX_FRAMES % 2 == 0, err_frames_even);

/* Detail tags, generic context categories, not error types */
enum {
    ERR_DTL_NONE    = 0,    /* no additional detail */
    ERR_DTL_ERRNO   = 1,    /* kernel errno value */
    ERR_DTL_FD      = 2,    /* file descriptor involved */
    ERR_DTL_OFFSET  = 3,    /* file/stream offset */
    ERR_DTL_SPAN    = 4,    /* reference to data in a known buffer */
    ERR_DTL_INT     = 5,    /* generic integer context (length, count, etc) */
};

typedef struct {
    int32_t     tag;
    union {
        int32_t     errno_val;
        int32_t     fd;
        uint64_t    offset;
        span_t      span;       /* index into a contextually known buffer */
        int64_t     ival;
    } u;
} err_detail_t;

#define ERR_DTL_EMPTY   ((err_detail_t){ERR_DTL_NONE, .u = {.errno_val = 0}})

typedef struct {
    const char     *file;       /* __FILE__, static string literal */
    const char     *func;       /* __func__, static string literal */
    uint32_t        line;       /* __LINE__ */
    int32_t         code;       /* error code at this frame */
    err_detail_t    detail;     /* structured context */
} err_frame_t;

typedef struct {
    err_frame_t frames[ERR_MAX_FRAMES];
    int32_t     top_count;      /* frames filled from top (index 0..) */
    int32_t     bottom_count;   /* frames filled from bottom (index max-1..) */
    int32_t     dropped;        /* middle frames dropped */
} err_t;

static inline void err_init(err_t *e)
{
    int32_t i;
    e->top_count = 0;
    e->bottom_count = 0;
    e->dropped = 0;
    for (i = 0; i < ERR_MAX_FRAMES; i++) {
        e->frames[i].file = NULL;
        e->frames[i].func = NULL;
        e->frames[i].line = 0;
        e->frames[i].code = 0;
        e->frames[i].detail.tag = ERR_DTL_NONE;
    }
}

/*
 * Push a frame onto the error stack.
 * First call is the origin (stored at bottom).
 * Subsequent calls are callers (stored at top, then bottom fills).
 * When full, most recent top frame is overwritten.
 */
static inline void err_push_frame(err_t *e, const char *file, const char *func,
                                  uint32_t line, int32_t code,
                                  err_detail_t detail)
{
    int32_t half = ERR_MAX_FRAMES / 2;
    int32_t idx;
    err_frame_t f;

    f.file = file;
    f.func = func;
    f.line = line;
    f.code = code;
    f.detail = detail;

    /* First push is always the origin, bottom slot */
    if (e->top_count == 0 && e->bottom_count == 0) {
        e->frames[ERR_MAX_FRAMES - 1] = f;
        e->bottom_count = 1;
        return;
    }

    /* Fill bottom half (frames closest to origin) */
    if (e->bottom_count < half) {
        idx = ERR_MAX_FRAMES - 1 - e->bottom_count;
        e->frames[idx] = f;
        e->bottom_count++;
        return;
    }

    /* Fill top half (frames closest to caller) */
    if (e->top_count < half) {
        e->frames[e->top_count] = f;
        e->top_count++;
        return;
    }

    /* Full: overwrite the most recent top frame, count the drop */
    e->frames[half - 1] = f;
    e->dropped++;
}

/* Push with no detail */
#define ERR_PUSH(e, code) \
    err_push_frame((e), __FILE__, __func__, (uint32_t)__LINE__, \
                   (code), ERR_DTL_EMPTY)

/* Push with errno detail */
#define ERR_PUSH_ERRNO(e, code, eno) \
    err_push_frame((e), __FILE__, __func__, (uint32_t)__LINE__, \
                   (code), (err_detail_t){ERR_DTL_ERRNO, .u = {.errno_val = (eno)}})

/* Push with fd detail */
#define ERR_PUSH_FD(e, code, fildes) \
    err_push_frame((e), __FILE__, __func__, (uint32_t)__LINE__, \
                   (code), (err_detail_t){ERR_DTL_FD, .u = {.fd = (fildes)}})

/* Push with offset detail */
#define ERR_PUSH_OFFSET(e, code, off) \
    err_push_frame((e), __FILE__, __func__, (uint32_t)__LINE__, \
                   (code), (err_detail_t){ERR_DTL_OFFSET, .u = {.offset = (off)}})

/* Push with span detail */
#define ERR_PUSH_SPAN(e, code, s) \
    err_push_frame((e), __FILE__, __func__, (uint32_t)__LINE__, \
                   (code), (err_detail_t){ERR_DTL_SPAN, .u = {.span = (s)}})

/* Push with generic int detail */
#define ERR_PUSH_INT(e, code, val) \
    err_push_frame((e), __FILE__, __func__, (uint32_t)__LINE__, \
                   (code), (err_detail_t){ERR_DTL_INT, .u = {.ival = (val)}})

/* Check if error has any frames */
static inline bool_t err_has_error(const err_t *e)
{
    return (e->top_count > 0 || e->bottom_count > 0) ? TRUE : FALSE;
}

/* Total frames stored */
static inline int32_t err_frame_count(const err_t *e)
{
    return e->top_count + e->bottom_count;
}

/*
 * Iterate frames in logical order: top (most recent) to bottom (origin).
 * idx 0 = most recent caller
 * idx frame_count-1 = error origin
 */
static inline const err_frame_t *err_frame_at(const err_t *e, int32_t idx)
{
    if (idx < 0)
        return NULL;
    if (idx < e->top_count)
        return &e->frames[idx];
    idx -= e->top_count;
    if (idx < e->bottom_count)
        return &e->frames[ERR_MAX_FRAMES - e->bottom_count + idx];
    return NULL;
}

#endif /* MSGSRVD_ERR_H */

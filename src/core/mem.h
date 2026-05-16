#ifndef MSGSRVD_MEM_H
#define MSGSRVD_MEM_H

#include "types.h"

/*
 * Memory operations without libc.
 * These are intentionally simple byte-at-a-time implementations.
 * The compiler will auto-vectorize in release builds (-O2).
 */

static inline void mem_copy(uint8_t *dst, const uint8_t *src, int32_t len)
{
    int32_t i;
    for (i = 0; i < len; i++)
        dst[i] = src[i];
}

static inline void mem_set(uint8_t *dst, uint8_t val, int32_t len)
{
    int32_t i;
    for (i = 0; i < len; i++)
        dst[i] = val;
}

static inline void mem_zero(uint8_t *dst, int32_t len)
{
    mem_set(dst, 0, len);
}

static inline int32_t mem_cmp(const uint8_t *a, const uint8_t *b, int32_t len)
{
    int32_t i;
    for (i = 0; i < len; i++) {
        if (a[i] != b[i])
            return a[i] < b[i] ? -1 : 1;
    }
    return 0;
}

/* Compare two spans within their respective bases */
static inline int32_t span_cmp(const uint8_t *base_a, int32_t base_a_len,
                               span_t a,
                               const uint8_t *base_b, int32_t base_b_len,
                               span_t b)
{
    const uint8_t *pa = span_cptr(base_a, base_a_len, a);
    const uint8_t *pb = span_cptr(base_b, base_b_len, b);
    int32_t min_len;
    int32_t r;

    if (!pa || !pb)
        return pa ? 1 : (pb ? -1 : 0);

    min_len = a.len < b.len ? a.len : b.len;
    r = mem_cmp(pa, pb, min_len);
    if (r != 0)
        return r;
    return a.len < b.len ? -1 : (a.len > b.len ? 1 : 0);
}

/* Copy span contents between bases */
static inline bool_t span_copy(uint8_t *dst_base, int32_t dst_base_len,
                               span_t dst,
                               const uint8_t *src_base, int32_t src_base_len,
                               span_t src)
{
    uint8_t *dp;
    const uint8_t *sp;

    if (dst.len < src.len)
        return FALSE;

    dp = span_ptr(dst_base, dst_base_len, dst);
    sp = span_cptr(src_base, src_base_len, src);
    if (!dp || !sp)
        return FALSE;

    mem_copy(dp, sp, src.len);
    return TRUE;
}

#endif /* MSGSRVD_MEM_H */

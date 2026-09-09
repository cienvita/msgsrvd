#ifndef MSGSRVD_FMT_H
#define MSGSRVD_FMT_H

#include "core/types.h"
#include "core/mem.h"

/*
 * Text out of numbers, without libc.
 *
 * Every one of these appends to a caller-owned buffer and returns the
 * new length, or the length unchanged when what was asked for would
 * not fit. A buffer that fills therefore stops growing rather than
 * overflowing, and a caller building a whole document checks for
 * truncation once at the end instead of after every field.
 *
 * The one thing that costs is that a truncated document is a valid
 * shorter one, so the caller has to compare the length it got against
 * the room it had and refuse to send a document that hit the ceiling.
 */

/* Append s. */
static inline int32_t fmt_str(char *buf, int32_t cap, int32_t len,
                              const char *s)
{
    int32_t n = 0;

    if (!buf || !s || len < 0 || len > cap)
        return len;

    while (s[n] != '\0')
        n++;
    if (n > cap - len)
        return len;

    mem_copy((uint8_t *)buf + len, (const uint8_t *)s, n);
    return len + n;
}

/* Append v in decimal. */
static inline int32_t fmt_u64(char *buf, int32_t cap, int32_t len, uint64_t v)
{
    char    tmp[20];
    int32_t n = 0;

    if (!buf || len < 0 || len > cap)
        return len;

    do {
        tmp[n++] = (char)('0' + (int32_t)(v % 10));
        v /= 10;
    } while (v > 0);

    if (n > cap - len)
        return len;

    while (n > 0)
        buf[len++] = tmp[--n];
    return len;
}

/*
 * Append ns as seconds, with the point nine digits in.
 *
 * Prometheus counts seconds and this is a nanosecond clock, so the
 * conversion is where the point is written rather than any arithmetic
 * that could lose a digit. No floating point is involved at any stage.
 */
static inline int32_t fmt_ns_seconds(char *buf, int32_t cap, int32_t len,
                                     uint64_t ns)
{
    uint64_t frac = ns % 1000000000ULL;
    int32_t  end;
    int32_t  i;

    len = fmt_u64(buf, cap, len, ns / 1000000000ULL);
    len = fmt_str(buf, cap, len, ".");
    if (len < 0 || len > cap || cap - len < 9)
        return len;

    /* Written backwards so a short fraction keeps its leading zeros. */
    end = len + 9;
    for (i = 1; i <= 9; i++) {
        buf[end - i] = (char)('0' + (int32_t)(frac % 10));
        frac /= 10;
    }
    return end;
}

#endif /* MSGSRVD_FMT_H */

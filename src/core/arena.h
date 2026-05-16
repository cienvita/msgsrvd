#ifndef MSGSRVD_ARENA_H
#define MSGSRVD_ARENA_H

#include "types.h"

/*
 * Fixed-size arena allocator.
 * No libc malloc. The backing buffer is provided externally
 * (static array, mmap region, etc).
 * All allocations return index-based spans into the arena.
 * No individual free, reset the entire arena at once.
 */

typedef struct {
    uint8_t    *base;       /* backing buffer (only raw pointer we hold) */
    int32_t     capacity;   /* total size of backing buffer */
    int32_t     used;       /* current watermark */
} arena_t;

/* Initialize arena over an existing buffer */
static inline void arena_init(arena_t *a, uint8_t *buf, int32_t capacity)
{
    a->base = buf;
    a->capacity = capacity;
    a->used = 0;
}

/* Allocate a span from the arena. Returns SPAN_NULL on exhaustion.
 * Aligns to 8 bytes for safe struct access. */
static inline span_t arena_alloc(arena_t *a, int32_t size)
{
    int32_t aligned;
    int32_t off;

    if (size <= 0)
        return SPAN_NULL;

    /* Round up to 8-byte alignment */
    aligned = (size + 7) & ~7;

    if (a->used + aligned > a->capacity)
        return SPAN_NULL;

    off = a->used;
    a->used += aligned;
    return SPAN(off, size);
}

/* Allocate unaligned (for raw byte buffers where alignment doesn't matter) */
static inline span_t arena_alloc_raw(arena_t *a, int32_t size)
{
    int32_t off;

    if (size <= 0 || a->used + size > a->capacity)
        return SPAN_NULL;

    off = a->used;
    a->used += size;
    return SPAN(off, size);
}

/* Reset arena, all previous spans become invalid */
static inline void arena_reset(arena_t *a)
{
    a->used = 0;
}

/* Remaining capacity */
static inline int32_t arena_remaining(const arena_t *a)
{
    return a->capacity - a->used;
}

/* Resolve a span to a pointer within this arena */
static inline uint8_t *arena_ptr(arena_t *a, span_t s)
{
    return span_ptr(a->base, a->capacity, s);
}

static inline const uint8_t *arena_cptr(const arena_t *a, span_t s)
{
    return span_cptr(a->base, a->capacity, s);
}

#endif /* MSGSRVD_ARENA_H */

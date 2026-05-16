#ifndef MSGSRVD_TYPES_H
#define MSGSRVD_TYPES_H

/*
 * Core types for msgsrvd.
 * No libc. Fixed-width types via compiler builtins.
 * All data references are index-based spans, not pointers.
 */

/* Fixed-width types without stdint.h */
typedef signed char             int8_t;
typedef unsigned char           uint8_t;
typedef signed short            int16_t;
typedef unsigned short          uint16_t;
typedef signed int              int32_t;
typedef unsigned int            uint32_t;
typedef signed long long        int64_t;
typedef unsigned long long      uint64_t;
typedef unsigned long           uintptr_t;
typedef long                    intptr_t;
typedef unsigned long           size_t;
typedef long                    ssize_t;

/* Boolean */
typedef uint8_t                 bool_t;
#define TRUE  ((bool_t)1)
#define FALSE ((bool_t)0)

/* Null */
#define NULL ((void *)0)

/*
 * Index-based byte span.
 * References a region within a known base buffer as (offset, length).
 * Resolve to a pointer only at point of use via span_ptr().
 * Serialization-safe, relocatable, wire-safe for replication.
 */
typedef struct {
    int32_t     off;
    int32_t     len;
} span_t;

#define SPAN(o, l)      ((span_t){(int32_t)(o), (int32_t)(l)})
#define SPAN_NULL       ((span_t){0, 0})

/* Resolve span to pointer with bounds check. Returns NULL on out-of-bounds. */
static inline uint8_t *span_ptr(uint8_t *base, int32_t base_len, span_t s)
{
    if (s.off < 0 || s.len < 0 || s.off + s.len > base_len)
        return NULL;
    return base + s.off;
}

/* Const resolve */
static inline const uint8_t *span_cptr(const uint8_t *base, int32_t base_len,
                                       span_t s)
{
    if (s.off < 0 || s.len < 0 || s.off + s.len > base_len)
        return NULL;
    return base + s.off;
}

/* Check if span is empty */
static inline bool_t span_empty(span_t s)
{
    return s.len <= 0 ? TRUE : FALSE;
}

/* Sub-span: offset relative to parent span's start */
static inline span_t span_sub(span_t parent, int32_t off, int32_t len)
{
    if (off < 0 || len < 0 || off + len > parent.len)
        return SPAN_NULL;
    return SPAN(parent.off + off, len);
}

/*
 * Result type for operations that can fail.
 * code: 0 = success, negative = error category
 * detail: errno or application-specific context
 */
typedef struct {
    int32_t     code;
    int32_t     detail;
} result_t;

#define RESULT_OK           ((result_t){0, 0})
#define RESULT_ERR(c, d)    ((result_t){(c), (d)})

static inline bool_t result_ok(result_t r) { return r.code == 0 ? TRUE : FALSE; }

/* Error codes */
enum {
    ERR_NONE        =  0,
    ERR_SYSCALL     = -1,   /* check detail for errno */
    ERR_NOMEM       = -2,   /* arena exhausted */
    ERR_OVERFLOW    = -3,   /* buffer/integer overflow */
    ERR_INVALID     = -4,   /* invalid argument or state */
    ERR_EOF         = -5,   /* end of stream / connection closed */
    ERR_PROTOCOL    = -6,   /* wire protocol violation */
    ERR_STORAGE     = -7,   /* storage layer error */
    ERR_AGAIN       = -8,   /* try again (non-blocking) */
};

/* Compile-time assertions without libc */
#define STATIC_ASSERT(cond, msg) \
    typedef char static_assert_##msg[(cond) ? 1 : -1]

STATIC_ASSERT(sizeof(uint8_t)  == 1, uint8_t_size);
STATIC_ASSERT(sizeof(uint16_t) == 2, uint16_t_size);
STATIC_ASSERT(sizeof(uint32_t) == 4, uint32_t_size);
STATIC_ASSERT(sizeof(uint64_t) == 8, uint64_t_size);
STATIC_ASSERT(sizeof(size_t)   == 8, size_t_64bit);
STATIC_ASSERT(sizeof(uintptr_t) == sizeof(void *), uintptr_holds_pointer);
STATIC_ASSERT(sizeof(intptr_t)  == sizeof(void *), intptr_holds_pointer);
STATIC_ASSERT(sizeof(span_t)   == 8, span_t_compact);

#endif /* MSGSRVD_TYPES_H */

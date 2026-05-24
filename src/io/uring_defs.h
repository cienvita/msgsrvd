#ifndef MSGSRVD_URING_DEFS_H
#define MSGSRVD_URING_DEFS_H

#include "core/types.h"

/*
 * io_uring kernel structures and constants.
 * Defined here to avoid linux/io_uring.h dependency.
 * These must match the kernel ABI exactly.
 */

/* io_uring_setup flags */
#define IORING_SETUP_SQPOLL    (1U << 1)
#define IORING_SETUP_SQ_AFF    (1U << 2)
#define IORING_SETUP_CQSIZE    (1U << 3)

/* io_uring_enter flags */
#define IORING_ENTER_GETEVENTS (1U << 0)
#define IORING_ENTER_SQ_WAKEUP (1U << 1)

/* SQE opcode */
enum {
    IORING_OP_NOP           = 0,
    IORING_OP_READV         = 1,
    IORING_OP_WRITEV        = 2,
    IORING_OP_FSYNC         = 3,
    IORING_OP_READ_FIXED    = 4,
    IORING_OP_WRITE_FIXED   = 5,
    IORING_OP_ACCEPT        = 13,
    IORING_OP_RECV          = 27,
    IORING_OP_SEND          = 26,
    IORING_OP_CLOSE         = 19,
    IORING_OP_READ          = 22,
    IORING_OP_WRITE         = 23,
};

/* SQE flags */
#define IOSQE_FIXED_FILE    (1U << 0)
#define IOSQE_IO_DRAIN      (1U << 1)
#define IOSQE_IO_LINK       (1U << 2)

/* FSYNC flags */
#define IORING_FSYNC_DATASYNC (1U << 0)

/* Offsets for mmap of SQ ring */
#define IORING_OFF_SQ_RING  0ULL
#define IORING_OFF_CQ_RING  0x8000000ULL
#define IORING_OFF_SQES     0x10000000ULL

/* Submission Queue Entry, 64 bytes */
typedef struct {
    uint8_t     opcode;
    uint8_t     flags;
    uint16_t    ioprio;
    int32_t     fd;
    uint64_t    off;        /* also: addr2 */
    uint64_t    addr;       /* also: splice_off_in */
    uint32_t    len;
    uint32_t    op_flags;   /* rw_flags, fsync_flags, accept_flags, msg_flags */
    uint64_t    user_data;
    uint16_t    buf_index;  /* also: buf_group */
    uint16_t    __pad1;
    uint32_t    __pad2;
    uint64_t    __pad3[2];
} io_uring_sqe_t;

STATIC_ASSERT(sizeof(io_uring_sqe_t) == 64, sqe_size);

/* Completion Queue Entry, 16 bytes */
typedef struct {
    uint64_t    user_data;
    int32_t     res;
    uint32_t    flags;
} io_uring_cqe_t;

STATIC_ASSERT(sizeof(io_uring_cqe_t) == 16, cqe_size);

/* io_uring_params, passed to io_uring_setup */
typedef struct {
    uint32_t    sq_entries;
    uint32_t    cq_entries;
    uint32_t    flags;
    uint32_t    sq_thread_cpu;
    uint32_t    sq_thread_idle;
    uint32_t    features;
    uint32_t    wq_fd;
    uint32_t    resv[3];
    /* SQ ring offsets */
    struct {
        uint32_t head;
        uint32_t tail;
        uint32_t ring_mask;
        uint32_t ring_entries;
        uint32_t flags;
        uint32_t dropped;
        uint32_t array;
        uint32_t resv1;
        uint64_t resv2;
    } sq_off;
    /* CQ ring offsets */
    struct {
        uint32_t head;
        uint32_t tail;
        uint32_t ring_mask;
        uint32_t ring_entries;
        uint32_t overflow;
        uint32_t cqes;
        uint32_t flags;
        uint32_t resv1;
        uint64_t resv2;
    } cq_off;
} io_uring_params_t;

#endif /* MSGSRVD_URING_DEFS_H */

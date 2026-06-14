#ifndef MSGSRVD_URING_H
#define MSGSRVD_URING_H

#include "core/types.h"
#include "core/err.h"
#include "io/uring_defs.h"

/*
 * io_uring ring management.
 * Setup, SQE submission, CQE harvesting, event loop.
 */

/* Ring state, holds mmap'd pointers into the kernel-shared rings */
typedef struct {
    /* SQ ring */
    uint32_t       *sq_head;
    uint32_t       *sq_tail;
    uint32_t       *sq_mask;
    uint32_t       *sq_entries_ptr;
    uint32_t       *sq_flags;
    uint32_t       *sq_array;
    io_uring_sqe_t *sqes;

    /* CQ ring */
    uint32_t       *cq_head;
    uint32_t       *cq_tail;
    uint32_t       *cq_mask;
    uint32_t       *cq_entries_ptr;
    io_uring_cqe_t *cqes;

    /* Ring fd and sizes */
    int32_t         ring_fd;
    uint32_t        sq_ring_sz;
    uint32_t        cq_ring_sz;

    /* Mapped base addresses (for munmap) */
    void           *sq_ring_ptr;
    void           *cq_ring_ptr;
    void           *sqes_ptr;
    size_t          sqes_sz;
} uring_t;

/*
 * Completion callback.
 * user_data is whatever was set on the SQE.
 * res is the syscall result (bytes transferred, fd, or negative errno).
 */
typedef void (*uring_cb_t)(uint64_t user_data, int32_t res, uint32_t flags);

/* Initialize the ring. entries must be power of 2. */
result_t uring_init(uring_t *ring, err_t *e, uint32_t entries);

/* Destroy the ring, unmapping all memory. */
void uring_destroy(uring_t *ring);

/* Get the next available SQE. Returns NULL if the SQ is full. */
io_uring_sqe_t *uring_get_sqe(uring_t *ring);

/* Submit all pending SQEs to the kernel. Returns number submitted. */
result_t uring_submit(uring_t *ring, err_t *e, uint32_t *submitted);

/* Submit and wait for at least min_complete completions. */
result_t uring_submit_and_wait(uring_t *ring, err_t *e,
                               uint32_t min_complete, uint32_t *submitted);

/* Process all available CQEs, calling cb for each. Returns count processed. */
int32_t uring_reap(uring_t *ring, uring_cb_t cb);

/*
 * Submit pending SQEs, wait for at least min_complete completions, and
 * copy the oldest CQE into *out, advancing the CQ head past it. For
 * synchronous single-op callers (e.g. the WAL append path) that need the
 * result directly rather than through a reap callback. Returns ERR_AGAIN
 * if no completion is present after the wait.
 */
result_t uring_wait_cqe(uring_t *ring, err_t *e, uint32_t min_complete,
                        io_uring_cqe_t *out);

/* ---- SQE prep helpers ---- */

static inline void uring_prep_accept(io_uring_sqe_t *sqe, int32_t fd,
                                     void *addr, void *addrlen,
                                     uint32_t flags, uint64_t user_data)
{
    sqe->opcode = IORING_OP_ACCEPT;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = (uint64_t)(uintptr_t)addrlen;
    sqe->addr = (uint64_t)(uintptr_t)addr;
    sqe->len = 0;
    sqe->op_flags = flags;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

static inline void uring_prep_recv(io_uring_sqe_t *sqe, int32_t fd,
                                   void *buf, uint32_t len,
                                   uint64_t user_data)
{
    sqe->opcode = IORING_OP_RECV;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = 0;
    sqe->addr = (uint64_t)(uintptr_t)buf;
    sqe->len = len;
    sqe->op_flags = 0;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

static inline void uring_prep_send(io_uring_sqe_t *sqe, int32_t fd,
                                   const void *buf, uint32_t len,
                                   uint64_t user_data)
{
    sqe->opcode = IORING_OP_SEND;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = 0;
    sqe->addr = (uint64_t)(uintptr_t)buf;
    sqe->len = len;
    sqe->op_flags = 0;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

static inline void uring_prep_read(io_uring_sqe_t *sqe, int32_t fd,
                                   void *buf, uint32_t len, uint64_t offset,
                                   uint64_t user_data)
{
    sqe->opcode = IORING_OP_READ;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = offset;
    sqe->addr = (uint64_t)(uintptr_t)buf;
    sqe->len = len;
    sqe->op_flags = 0;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

static inline void uring_prep_write(io_uring_sqe_t *sqe, int32_t fd,
                                    const void *buf, uint32_t len,
                                    uint64_t offset, uint64_t user_data)
{
    sqe->opcode = IORING_OP_WRITE;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = offset;
    sqe->addr = (uint64_t)(uintptr_t)buf;
    sqe->len = len;
    sqe->op_flags = 0;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

static inline void uring_prep_fsync(io_uring_sqe_t *sqe, int32_t fd,
                                    uint32_t fsync_flags, uint64_t user_data)
{
    sqe->opcode = IORING_OP_FSYNC;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = 0;
    sqe->addr = 0;
    sqe->len = 0;
    sqe->op_flags = fsync_flags;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

static inline void uring_prep_nop(io_uring_sqe_t *sqe, uint64_t user_data)
{
    sqe->opcode = IORING_OP_NOP;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = -1;
    sqe->off = 0;
    sqe->addr = 0;
    sqe->len = 0;
    sqe->op_flags = 0;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

static inline void uring_prep_close(io_uring_sqe_t *sqe, int32_t fd,
                                    uint64_t user_data)
{
    sqe->opcode = IORING_OP_CLOSE;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = 0;
    sqe->addr = 0;
    sqe->len = 0;
    sqe->op_flags = 0;
    sqe->user_data = user_data;
    sqe->buf_index = 0;
}

#endif /* MSGSRVD_URING_H */

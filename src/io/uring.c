#include "io/uring.h"
#include "sys/syscall.h"
#include "sys/linux.h"
#include "core/mem.h"

/*
 * Memory barrier for shared ring access.
 * The kernel and userspace share the SQ/CQ rings.
 * We need store/load barriers to ensure ordering.
 */
#define io_barrier() __asm__ volatile("" ::: "memory")

result_t uring_init(uring_t *ring, err_t *e, uint32_t entries)
{
    io_uring_params_t params;
    long fd;
    void *sq_ptr;
    void *cq_ptr;
    void *sqes_ptr;
    long r;
    size_t sq_ring_sz;
    size_t cq_ring_sz;
    size_t sqes_sz;

    mem_zero((uint8_t *)&params, (int32_t)sizeof(params));
    mem_zero((uint8_t *)ring, (int32_t)sizeof(*ring));

    /* io_uring_setup */
    fd = sys_call2(SYS_io_uring_setup, (long)entries, (long)&params);
    if (sys_is_err(fd)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(fd));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(fd));
    }

    ring->ring_fd = (int32_t)fd;

    /* Calculate mmap sizes */
    sq_ring_sz = (size_t)(params.sq_off.array +
                          params.sq_entries * sizeof(uint32_t));
    cq_ring_sz = (size_t)(params.cq_off.cqes +
                          params.cq_entries * sizeof(io_uring_cqe_t));
    sqes_sz = (size_t)(params.sq_entries * sizeof(io_uring_sqe_t));

    /* mmap SQ ring */
    r = sys_call6(SYS_mmap, 0, (long)sq_ring_sz,
                  PROT_READ | PROT_WRITE, MAP_SHARED,
                  (long)ring->ring_fd, (long)IORING_OFF_SQ_RING);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        goto fail_close;
    }
    sq_ptr = (void *)r;

    /* mmap CQ ring */
    r = sys_call6(SYS_mmap, 0, (long)cq_ring_sz,
                  PROT_READ | PROT_WRITE, MAP_SHARED,
                  (long)ring->ring_fd, (long)IORING_OFF_CQ_RING);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        goto fail_unmap_sq;
    }
    cq_ptr = (void *)r;

    /* mmap SQEs */
    r = sys_call6(SYS_mmap, 0, (long)sqes_sz,
                  PROT_READ | PROT_WRITE, MAP_SHARED,
                  (long)ring->ring_fd, (long)IORING_OFF_SQES);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        goto fail_unmap_cq;
    }
    sqes_ptr = (void *)r;

    /* Set up SQ ring pointers */
    ring->sq_head = (uint32_t *)((uint8_t *)sq_ptr + params.sq_off.head);
    ring->sq_tail = (uint32_t *)((uint8_t *)sq_ptr + params.sq_off.tail);
    ring->sq_mask = (uint32_t *)((uint8_t *)sq_ptr + params.sq_off.ring_mask);
    ring->sq_entries_ptr = (uint32_t *)((uint8_t *)sq_ptr + params.sq_off.ring_entries);
    ring->sq_flags = (uint32_t *)((uint8_t *)sq_ptr + params.sq_off.flags);
    ring->sq_array = (uint32_t *)((uint8_t *)sq_ptr + params.sq_off.array);
    ring->sqes = (io_uring_sqe_t *)sqes_ptr;

    /* Set up CQ ring pointers */
    ring->cq_head = (uint32_t *)((uint8_t *)cq_ptr + params.cq_off.head);
    ring->cq_tail = (uint32_t *)((uint8_t *)cq_ptr + params.cq_off.tail);
    ring->cq_mask = (uint32_t *)((uint8_t *)cq_ptr + params.cq_off.ring_mask);
    ring->cq_entries_ptr = (uint32_t *)((uint8_t *)cq_ptr + params.cq_off.ring_entries);
    ring->cqes = (io_uring_cqe_t *)((uint8_t *)cq_ptr + params.cq_off.cqes);

    /* Save mmap info for cleanup */
    ring->sq_ring_ptr = sq_ptr;
    ring->cq_ring_ptr = cq_ptr;
    ring->sqes_ptr = sqes_ptr;
    ring->sq_ring_sz = (uint32_t)sq_ring_sz;
    ring->cq_ring_sz = (uint32_t)cq_ring_sz;
    ring->sqes_sz = sqes_sz;

    return RESULT_OK;

fail_unmap_cq:
    sys_call2(SYS_munmap, (long)cq_ptr, (long)cq_ring_sz);
fail_unmap_sq:
    sys_call2(SYS_munmap, (long)sq_ptr, (long)sq_ring_sz);
fail_close:
    sys_call1(SYS_close, (long)ring->ring_fd);
    ring->ring_fd = -1;
    return RESULT_ERR(ERR_SYSCALL, 0);
}

void uring_destroy(uring_t *ring)
{
    if (ring->ring_fd < 0)
        return;

    sys_call2(SYS_munmap, (long)ring->sqes_ptr, (long)ring->sqes_sz);
    sys_call2(SYS_munmap, (long)ring->cq_ring_ptr, (long)ring->cq_ring_sz);
    sys_call2(SYS_munmap, (long)ring->sq_ring_ptr, (long)ring->sq_ring_sz);
    sys_call1(SYS_close, (long)ring->ring_fd);
    ring->ring_fd = -1;
}

io_uring_sqe_t *uring_get_sqe(uring_t *ring)
{
    uint32_t        head;
    uint32_t        mask = *ring->sq_mask;
    uint32_t        idx;
    io_uring_sqe_t *sqe;

    io_barrier();
    head = *ring->sq_head;

    /*
     * Everything from the kernel's head to our own tail is either in
     * flight or filled and waiting to be published, so that span is
     * what the ring's capacity has to cover.
     */
    if (ring->sqe_tail - head >= *ring->sq_entries_ptr)
        return NULL;

    idx = ring->sqe_tail & mask;
    sqe = &ring->sqes[idx];
    ring->sq_array[idx] = idx;
    ring->sqe_tail++;

    mem_zero((uint8_t *)sqe, (int32_t)sizeof(*sqe));
    return sqe;
}

result_t uring_submit(uring_t *ring, err_t *e, uint32_t *submitted)
{
    uint32_t to_submit = ring->sqe_tail - *ring->sq_tail;
    long     r;

    /*
     * Publish the SQEs only after they are written. On x86-64 stores
     * are not reordered with stores, so keeping the compiler from
     * moving the tail update is the whole of the requirement.
     */
    io_barrier();
    *ring->sq_tail = ring->sqe_tail;
    io_barrier();

    r = sys_call4(SYS_io_uring_enter, (long)ring->ring_fd,
                  (long)to_submit, 0, 0);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }

    if (submitted)
        *submitted = (uint32_t)r;
    return RESULT_OK;
}

result_t uring_submit_and_wait(uring_t *ring, err_t *e,
                               uint32_t min_complete, uint32_t *submitted)
{
    uint32_t to_submit = ring->sqe_tail - *ring->sq_tail;
    long     r;

    io_barrier();
    *ring->sq_tail = ring->sqe_tail;
    io_barrier();

    r = sys_call4(SYS_io_uring_enter, (long)ring->ring_fd,
                  (long)to_submit, (long)min_complete,
                  IORING_ENTER_GETEVENTS);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }

    if (submitted)
        *submitted = (uint32_t)r;
    return RESULT_OK;
}

int32_t uring_reap(uring_t *ring, uring_cb_t cb, void *ctx)
{
    uint32_t head = *ring->cq_head;
    uint32_t tail;
    uint32_t mask = *ring->cq_mask;
    int32_t count = 0;

    io_barrier();
    tail = *ring->cq_tail;

    while (head != tail) {
        io_uring_cqe_t *cqe = &ring->cqes[head & mask];
        cb(ctx, cqe->user_data, cqe->res, cqe->flags);
        head++;
        count++;
    }

    /* Advance head so the kernel can reuse CQE slots */
    *ring->cq_head = head;
    io_barrier();

    return count;
}

#ifndef MSGSRVD_OS_H
#define MSGSRVD_OS_H

#include "core/types.h"
#include "core/err.h"
#include "sys/syscall.h"
#include "sys/linux.h"

/*
 * Typed OS operations built on raw syscalls.
 * These return result_t and populate err_t on failure.
 */

/* ---- File operations ---- */

static inline result_t os_open(err_t *e, const char *path, int32_t flags,
                               int32_t mode, int32_t *fd_out)
{
    long r = sys_call3(SYS_open, (long)path, (long)flags, (long)mode);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    *fd_out = (int32_t)r;
    return RESULT_OK;
}

/*
 * openat against a directory fd. Segment files are named relative to
 * the WAL directory, which is held open for the whole run so that the
 * directory can be fsync'd after a create.
 */
static inline result_t os_openat(err_t *e, int32_t dir_fd, const char *path,
                                 int32_t flags, int32_t mode, int32_t *fd_out)
{
    long r = sys_call4(SYS_openat, (long)dir_fd, (long)path, (long)flags,
                       (long)mode);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    *fd_out = (int32_t)r;
    return RESULT_OK;
}

static inline result_t os_close(err_t *e, int32_t fd)
{
    long r = sys_call1(SYS_close, (long)fd);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline result_t os_fsync(err_t *e, int32_t fd)
{
    long r = sys_call1(SYS_fsync, (long)fd);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline result_t os_fdatasync(err_t *e, int32_t fd)
{
    long r = sys_call1(SYS_fdatasync, (long)fd);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

/*
 * One pread. n_out comes back short at end of file, which is how a
 * scan of a preallocated segment learns it has run off the end.
 */
static inline result_t os_pread(err_t *e, int32_t fd, uint8_t *buf,
                                int32_t count, int64_t offset,
                                int32_t *n_out)
{
    long r;

    if (count < 0 || offset < 0) {
        ERR_PUSH_INT(e, ERR_INVALID, count);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    r = sys_call4(SYS_pread64, (long)fd, (long)buf, (long)count,
                  (long)offset);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    *n_out = (int32_t)r;
    return RESULT_OK;
}

/*
 * Read until count bytes are in hand or the file ends. A pread can
 * come back short for reasons other than end of file, so a single call
 * is not enough to conclude anything about what is there.
 */
static inline result_t os_pread_full(err_t *e, int32_t fd, uint8_t *buf,
                                     int32_t count, int64_t offset,
                                     int32_t *n_out)
{
    int32_t done = 0;

    while (done < count) {
        int32_t  n = 0;
        result_t r = os_pread(e, fd, buf + done, count - done,
                              offset + done, &n);
        if (!result_ok(r))
            return r;
        if (n == 0)
            break;      /* end of file */
        done += n;
    }

    *n_out = done;
    return RESULT_OK;
}

/*
 * Write all count bytes or fail. A short write is not an error the
 * caller can ignore on a WAL: the record would be torn on purpose.
 */
static inline result_t os_pwrite_full(err_t *e, int32_t fd,
                                      const uint8_t *buf, int32_t count,
                                      int64_t offset)
{
    int32_t done = 0;

    if (count < 0 || offset < 0) {
        ERR_PUSH_INT(e, ERR_INVALID, count);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    while (done < count) {
        long r = sys_call4(SYS_pwrite64, (long)fd, (long)(buf + done),
                           (long)(count - done), (long)(offset + done));
        if (sys_is_err(r)) {
            ERR_PUSH_FD(e, ERR_SYSCALL, fd);
            return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
        }
        if (r == 0) {
            /* No progress and no error leaves nothing to retry on. */
            ERR_PUSH_FD(e, ERR_STORAGE, fd);
            return RESULT_ERR(ERR_STORAGE, 0);
        }
        done += (int32_t)r;
    }

    return RESULT_OK;
}

/*
 * One getdents64 call. Returns the bytes of directory records placed
 * in buf, or 0 at the end of the directory. The caller walks the
 * records and calls again until it gets 0.
 */
static inline result_t os_getdents64(err_t *e, int32_t fd, uint8_t *buf,
                                     int32_t count, int32_t *n_out)
{
    long r = sys_call3(SYS_getdents64, (long)fd, (long)buf, (long)count);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    *n_out = (int32_t)r;
    return RESULT_OK;
}

static inline result_t os_ftruncate(err_t *e, int32_t fd, int64_t length)
{
    long r = sys_call2(SYS_ftruncate, (long)fd, (long)length);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline result_t os_mkdir(err_t *e, const char *path, int32_t mode)
{
    long r = sys_call2(SYS_mkdir, (long)path, (long)mode);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline result_t os_rmdir(err_t *e, const char *path)
{
    long r = sys_call1(SYS_rmdir, (long)path);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline result_t os_unlinkat(err_t *e, int32_t dir_fd, const char *path,
                                   int32_t flags)
{
    long r = sys_call3(SYS_unlinkat, (long)dir_fd, (long)path, (long)flags);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline int32_t os_getpid(void)
{
    return (int32_t)sys_call0(SYS_getpid);
}

static inline result_t os_fallocate(err_t *e, int32_t fd, int32_t mode,
                                    uint64_t offset, uint64_t len)
{
    long r = sys_call4(SYS_fallocate, (long)fd, (long)mode,
                       (long)offset, (long)len);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

/* ---- Memory mapping ---- */

static inline result_t os_mmap(err_t *e, void *addr, size_t length,
                               int32_t prot, int32_t flags,
                               int32_t fd, uint64_t offset,
                               void **out)
{
    long r = sys_call6(SYS_mmap, (long)addr, (long)length,
                       (long)prot, (long)flags, (long)fd, (long)offset);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    *out = (void *)r;
    return RESULT_OK;
}

static inline result_t os_munmap(err_t *e, void *addr, size_t length)
{
    long r = sys_call2(SYS_munmap, (long)addr, (long)length);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

/* ---- Socket operations ---- */

static inline result_t os_socket(err_t *e, int32_t domain, int32_t type,
                                 int32_t protocol, int32_t *fd_out)
{
    long r = sys_call3(SYS_socket, (long)domain, (long)type, (long)protocol);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    *fd_out = (int32_t)r;
    return RESULT_OK;
}

static inline result_t os_bind(err_t *e, int32_t fd,
                               const sockaddr_in_t *addr)
{
    long r = sys_call3(SYS_bind, (long)fd, (long)addr,
                       (long)sizeof(sockaddr_in_t));
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline result_t os_listen(err_t *e, int32_t fd, int32_t backlog)
{
    long r = sys_call2(SYS_listen, (long)fd, (long)backlog);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

static inline result_t os_setsockopt(err_t *e, int32_t fd, int32_t level,
                                     int32_t optname, const void *optval,
                                     int32_t optlen)
{
    long r = sys_call5(SYS_setsockopt, (long)fd, (long)level, (long)optname,
                       (long)optval, (long)optlen);
    if (sys_is_err(r)) {
        ERR_PUSH_FD(e, ERR_SYSCALL, fd);
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    return RESULT_OK;
}

/* ---- Write (for debug output) ---- */

static inline ssize_t os_write_raw(int32_t fd, const void *buf, size_t count)
{
    return (ssize_t)sys_call3(SYS_write, (long)fd, (long)buf, (long)count);
}

/* ---- Process ---- */

static inline void os_exit(int32_t code)
{
    sys_call1(SYS_exit, (long)code);
    /* unreachable, but prevents compiler warning */
    for (;;) {}
}

/* ---- eventfd ---- */

static inline result_t os_eventfd(err_t *e, uint32_t initval, int32_t flags,
                                  int32_t *fd_out)
{
    long r = sys_call2(SYS_eventfd2, (long)initval, (long)flags);
    if (sys_is_err(r)) {
        ERR_PUSH_ERRNO(e, ERR_SYSCALL, sys_errno(r));
        return RESULT_ERR(ERR_SYSCALL, sys_errno(r));
    }
    *fd_out = (int32_t)r;
    return RESULT_OK;
}

#endif /* MSGSRVD_OS_H */

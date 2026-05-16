#ifndef MSGSRVD_SYSCALL_H
#define MSGSRVD_SYSCALL_H

#include "core/types.h"

/*
 * Raw Linux x86-64 syscall interface.
 * No libc wrapper, inline asm directly to the kernel.
 */

/* Syscall numbers, Linux x86-64 */
#define SYS_read            0
#define SYS_write           1
#define SYS_open            2
#define SYS_close           3
#define SYS_fstat           5
#define SYS_mmap            9
#define SYS_munmap          11
#define SYS_socket          41
#define SYS_accept          43
#define SYS_bind            49
#define SYS_listen          50
#define SYS_setsockopt      54
#define SYS_exit            60
#define SYS_fsync           74
#define SYS_fdatasync       75
#define SYS_fallocate       285
#define SYS_eventfd2        290
#define SYS_io_uring_setup  425
#define SYS_io_uring_enter  426
#define SYS_io_uring_register 427

/* Raw syscall with 0-6 arguments */
static inline long sys_call0(long n)
{
    long ret;
    __asm__ volatile("syscall"
        : "=a"(ret)
        : "a"(n)
        : "rcx", "r11", "memory");
    return ret;
}

static inline long sys_call1(long n, long a1)
{
    long ret;
    __asm__ volatile("syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1)
        : "rcx", "r11", "memory");
    return ret;
}

static inline long sys_call2(long n, long a1, long a2)
{
    long ret;
    __asm__ volatile("syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2)
        : "rcx", "r11", "memory");
    return ret;
}

static inline long sys_call3(long n, long a1, long a2, long a3)
{
    long ret;
    __asm__ volatile("syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3)
        : "rcx", "r11", "memory");
    return ret;
}

static inline long sys_call4(long n, long a1, long a2, long a3, long a4)
{
    long ret;
    register long r10 __asm__("r10") = a4;
    __asm__ volatile("syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10)
        : "rcx", "r11", "memory");
    return ret;
}

static inline long sys_call5(long n, long a1, long a2, long a3, long a4,
                             long a5)
{
    long ret;
    register long r10 __asm__("r10") = a4;
    register long r8  __asm__("r8")  = a5;
    __asm__ volatile("syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10), "r"(r8)
        : "rcx", "r11", "memory");
    return ret;
}

static inline long sys_call6(long n, long a1, long a2, long a3, long a4,
                             long a5, long a6)
{
    long ret;
    register long r10 __asm__("r10") = a4;
    register long r8  __asm__("r8")  = a5;
    register long r9  __asm__("r9")  = a6;
    __asm__ volatile("syscall"
        : "=a"(ret)
        : "a"(n), "D"(a1), "S"(a2), "d"(a3), "r"(r10), "r"(r8), "r"(r9)
        : "rcx", "r11", "memory");
    return ret;
}

/* Check if return value is an error (-4095 to -1 are errno values) */
static inline bool_t sys_is_err(long ret)
{
    return (ret < 0 && ret >= -4095) ? TRUE : FALSE;
}

static inline int32_t sys_errno(long ret)
{
    return sys_is_err(ret) ? (int32_t)(-ret) : 0;
}

#endif /* MSGSRVD_SYSCALL_H */

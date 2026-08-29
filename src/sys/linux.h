#ifndef MSGSRVD_LINUX_H
#define MSGSRVD_LINUX_H

#include "core/types.h"

/*
 * Linux kernel constants and structures.
 * Defined here to avoid any kernel header dependency.
 */

/* open() flags */
#define O_RDONLY    0x0000
#define O_WRONLY    0x0001
#define O_RDWR      0x0002
#define O_CREAT     0x0040
#define O_EXCL      0x0080
#define O_TRUNC     0x0200
#define O_APPEND    0x0400
#define O_DIRECT    0x4000
#define O_DSYNC     0x1000
#define O_DIRECTORY 0x10000
#define O_CLOEXEC   0x80000

/* openat() special dirfd, and unlinkat() flags */
#define AT_FDCWD            (-100)
#define AT_REMOVEDIR        0x200

/* File mode bits for a file only its owner may read or write */
#define MODE_0600   0600
#define MODE_0700   0700

/* mmap() */
#define PROT_READ   0x1
#define PROT_WRITE  0x2
#define MAP_SHARED  0x01
#define MAP_PRIVATE 0x02
#define MAP_ANONYMOUS 0x20
#define MAP_FAILED  ((void *)-1)

/* socket */
#define AF_INET     2
#define SOCK_STREAM 1
#define SOCK_NONBLOCK 0x800
#define SOCK_CLOEXEC  0x80000
#define SOL_SOCKET  1
#define SO_REUSEADDR 2
#define SO_REUSEPORT 15
#define IPPROTO_TCP 6
#define TCP_NODELAY 1

/* shutdown() directions */
#define SHUT_RD     0
#define SHUT_WR     1
#define SHUT_RDWR   2

/* Common errno values */
#define ENOENT       2
#define EIO          5
#define EBADF        9
#define EAGAIN      11
#define ENOMEM      12
#define EEXIST      17
#define EINVAL      22
#define ENOSPC      28
#define EPIPE       32
#define ECONNRESET  104
#define ETIMEDOUT   110

/*
 * Signals. Delivered through a file descriptor rather than a handler:
 * a handler in a program with no libc needs its own restorer
 * trampoline, and a descriptor drops into the same ring as everything
 * else instead of interrupting it. The signals have to be blocked
 * first, or the default action still fires.
 */
#define SIGINT      2
#define SIGTERM     15

#define SIG_BLOCK   0
#define SIGSET_SIZE 8               /* bytes of sigset the kernel expects */

#define SFD_CLOEXEC 0x80000

/* One signalfd read returns this many bytes of siginfo. */
#define SIGNALFD_SIGINFO_SIZE 128

/* Bit for a signal number in a sigset word. */
#define SIGMASK(n)  ((uint64_t)1 << ((n) - 1))

/* eventfd */
#define EFD_NONBLOCK 0x800
#define EFD_CLOEXEC  0x80000

/*
 * Timers, also delivered through a descriptor so they land in the ring
 * with everything else.
 *
 * The loop needs one only where something has to happen without a
 * client or a peer causing it: a replica that has to be dialled again
 * after its connection dropped, and a write waiting on a second copy
 * that has to stop waiting at some point. A node with no replication
 * configured never arms it and keeps a loop whose only clock is the
 * flush.
 */
#define CLOCK_MONOTONIC 1
#define TFD_CLOEXEC     0x80000

/* One timerfd read returns a u64 count of expirations. */
#define TIMERFD_READ_SIZE 8

typedef struct {
    int64_t tv_sec;
    int64_t tv_nsec;
} timespec_t;

typedef struct {
    timespec_t interval;
    timespec_t value;
} itimerspec_t;

/*
 * getdents64 record. The kernel packs these: the name starts at a
 * fixed byte offset and each record's length is carried inside it, so
 * the layout is walked with explicit offsets rather than a struct
 * whose padding the compiler would choose.
 */
#define DIRENT64_RECLEN_OFF 16      /* uint16, bytes in this record */
#define DIRENT64_TYPE_OFF   18      /* uint8 */
#define DIRENT64_NAME_OFF   19      /* NUL-terminated, fills the record */

#define DT_REG  8                   /* d_type: regular file */

/* sockaddr_in, IPv4 */
typedef struct {
    uint16_t    sin_family;
    uint16_t    sin_port;       /* network byte order */
    uint32_t    sin_addr;       /* network byte order */
    uint8_t     sin_zero[8];
} sockaddr_in_t;

STATIC_ASSERT(sizeof(sockaddr_in_t) == 16, sockaddr_in_size);

/* Byte order helpers, we're little-endian only */
static inline uint16_t bswap16(uint16_t v)
{
    return (uint16_t)((v >> 8) | (v << 8));
}

static inline uint32_t bswap32(uint32_t v)
{
    return ((v >> 24) & 0x000000FF) |
           ((v >>  8) & 0x0000FF00) |
           ((v <<  8) & 0x00FF0000) |
           ((v << 24) & 0xFF000000);
}

/* Network byte order (big-endian) conversion, only needed for socket addrs */
#define htons(x) bswap16(x)
#define htonl(x) bswap32(x)
#define ntohs(x) bswap16(x)
#define ntohl(x) bswap32(x)

#endif /* MSGSRVD_LINUX_H */

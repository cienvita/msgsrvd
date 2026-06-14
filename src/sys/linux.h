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
#define O_TRUNC     0x0200
#define O_APPEND    0x0400
#define O_DIRECT    0x4000
#define O_DSYNC     0x1000
#define O_DIRECTORY 0x10000

/* lseek() whence */
#define SEEK_SET    0
#define SEEK_CUR    1
#define SEEK_END    2

/*
 * getdents64() directory entry. d_name is NUL-terminated and the record
 * is d_reclen bytes; the next entry starts d_reclen bytes on. d_name
 * sits at a fixed offset of 19 bytes from the start.
 */
typedef struct {
    uint64_t    d_ino;
    int64_t     d_off;
    uint16_t    d_reclen;
    uint8_t     d_type;
    char        d_name[];
} linux_dirent64_t;

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

/* Common errno values */
#define EAGAIN      11
#define ENOMEM      12
#define EEXIST      17
#define EINVAL      22
#define ENOSPC      28
#define EPIPE       32
#define EOPNOTSUPP  95
#define ECONNRESET  104
#define ETIMEDOUT   110

/* eventfd */
#define EFD_NONBLOCK 0x800
#define EFD_CLOEXEC  0x80000

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

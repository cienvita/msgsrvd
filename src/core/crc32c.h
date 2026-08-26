#ifndef MSGSRVD_CRC32C_H
#define MSGSRVD_CRC32C_H

#include "core/types.h"

/*
 * CRC32C (Castagnoli, polynomial 0x1EDC6F41), the checksum on every
 * WAL record.
 *
 * Implemented on the SSE4.2 crc32 instruction, which is a hard
 * requirement rather than an optimisation. A batch is checksummed on
 * the commit path, so the cost lands next to the fsync it precedes:
 * on a host whose fsync is 57 us, a megabyte of software CRC would be
 * the dominant term by a wide margin, and a fallback that quietly ran
 * 25x slower would be worse than not starting. crc32c_available()
 * reports the capability so startup can refuse a CPU that lacks it
 * rather than take SIGILL on the first record.
 *
 * SSE4.2 has been present on x86-64 since 2008 and this is an x86-64
 * only program, so the check is a guard against the absurd, not a
 * portability story.
 *
 * Running value: start at 0, feed chunks in order, and the return of
 * each call is the checksum of everything fed so far. Feeding two
 * chunks gives the same answer as feeding their concatenation, which
 * is what lets a record be checksummed over its header and its
 * payload without copying the two together.
 */

bool_t   crc32c_available(void);
uint32_t crc32c(uint32_t crc, const uint8_t *buf, int32_t len);

#endif /* MSGSRVD_CRC32C_H */

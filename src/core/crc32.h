#ifndef MSGSRVD_CRC32_H
#define MSGSRVD_CRC32_H

#include "core/types.h"

/*
 * CRC-32C (Castagnoli, polynomial 0x1EDC6F41, reflected form
 * 0x82F63B78). Used as the WAL record integrity check on append and
 * recovery. Software table-driven, no libc, no dependency on the SSE4.2
 * crc32 instruction.
 *
 * Running form: seed with CRC32C_INIT, feed buffers, then finalize.
 *   uint32_t c = crc32c(CRC32C_INIT, buf, len);
 *   c = crc32c_fin(c);
 * Single-shot:
 *   uint32_t c = crc32c_buf(buf, len);
 *
 * crc32c_buf("123456789", 9) == 0xE3069283 (the standard check value).
 */

#define CRC32C_INIT 0xFFFFFFFFu

/* Update a running CRC over len bytes. Seed with CRC32C_INIT. */
uint32_t crc32c(uint32_t crc, const uint8_t *buf, int32_t len);

/* Finalize a running CRC (the trailing XOR-out). */
static inline uint32_t crc32c_fin(uint32_t crc)
{
    return crc ^ 0xFFFFFFFFu;
}

/* Single-shot: seed, update, finalize. */
static inline uint32_t crc32c_buf(const uint8_t *buf, int32_t len)
{
    return crc32c_fin(crc32c(CRC32C_INIT, buf, len));
}

#endif /* MSGSRVD_CRC32_H */

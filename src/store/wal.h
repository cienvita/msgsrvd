#ifndef MSGSRVD_WAL_H
#define MSGSRVD_WAL_H

#include "core/types.h"
#include "core/mem.h"
#include "core/crc32.h"

/*
 * WAL record framing.
 *
 * Every mutation is one WAL record: a fixed 40-byte prefix followed by
 * the payload, padded up to an 8-byte boundary. The prefix is
 * memcpy-decoded, same idiom as the wire header in proto/msg.h.
 *
 * crc32c covers the prefix (with the crc field excluded) plus the
 * payload, but not the trailing padding. A torn or corrupt record fails
 * the crc check on recovery and terminates the replay scan.
 *
 * This header is pure framing: encode, decode, size. No I/O, no
 * knowledge of segment files. The append and recovery paths build on
 * top of it.
 */

#define WAL_REC_MAGIC    0x57414C52u    /* 'WALR' */
#define WAL_REC_HDR_SIZE 40
#define WAL_REC_CRC_OFF  36             /* byte offset of the crc field */
#define WAL_REC_MAX_LEN  (64 * 1024 * 1024)

/*
 * Record prefix, 40 bytes, naturally aligned, memcpy-decodable.
 *
 * [0..3]   magic           WAL_REC_MAGIC
 * [4..7]   len             payload length following the prefix
 * [8..15]  wal_seq         server-assigned, monotonic, gap-free
 * [16..23] partition_key   copied from the wire header
 * [24..31] client_seq      from the wire header sequence, for dedup
 * [32..33] record_type     MSG_RECORD_*
 * [34..35] flags           durability flags carried from the request
 * [36..39] crc32c          over prefix (crc excluded) + payload
 */
typedef struct {
    uint32_t    magic;
    uint32_t    len;
    uint64_t    wal_seq;
    uint64_t    partition_key;
    uint64_t    client_seq;
    uint16_t    record_type;
    uint16_t    flags;
    uint32_t    crc;
} wal_rec_t;

STATIC_ASSERT(sizeof(wal_rec_t) == WAL_REC_HDR_SIZE, wal_rec_size);

/*
 * On-disk size of a record with the given payload: prefix plus the
 * payload padded up to an 8-byte boundary.
 */
static inline int32_t wal_rec_size(int32_t payload_len)
{
    int32_t padded = (payload_len + 7) & ~7;
    return WAL_REC_HDR_SIZE + padded;
}

/*
 * crc32c over the prefix (crc field excluded) plus the payload.
 * buf points at the start of an encoded record; payload_len is the
 * record's len field. The padding is not covered.
 */
static inline uint32_t wal_rec_crc(const uint8_t *buf, int32_t payload_len)
{
    uint32_t c = crc32c(CRC32C_INIT, buf, WAL_REC_CRC_OFF);
    c = crc32c(c, buf + WAL_REC_HDR_SIZE, payload_len);
    return crc32c_fin(c);
}

/*
 * Encode a record into buf. rec supplies the header fields except magic,
 * len, and crc, which are set here. The tail padding is zeroed. Returns
 * the total record size (including padding), or 0 if buf is too small or
 * payload_len is out of range.
 */
static inline int32_t wal_encode_rec(uint8_t *buf, int32_t buf_len,
                                     const wal_rec_t *rec,
                                     const uint8_t *payload,
                                     int32_t payload_len)
{
    wal_rec_t h;
    uint32_t crc;
    int32_t total;

    if (payload_len < 0 || payload_len > WAL_REC_MAX_LEN)
        return 0;

    total = wal_rec_size(payload_len);
    if (total > buf_len)
        return 0;

    h = *rec;
    h.magic = WAL_REC_MAGIC;
    h.len = (uint32_t)payload_len;
    h.crc = 0;
    mem_copy(buf, (const uint8_t *)&h, WAL_REC_HDR_SIZE);

    if (payload_len > 0)
        mem_copy(buf + WAL_REC_HDR_SIZE, payload, payload_len);
    mem_zero(buf + WAL_REC_HDR_SIZE + payload_len,
             total - WAL_REC_HDR_SIZE - payload_len);

    crc = wal_rec_crc(buf, payload_len);
    mem_copy(buf + WAL_REC_CRC_OFF, (const uint8_t *)&crc, 4);
    return total;
}

/*
 * Decode and validate a record from buf. On success fills *rec_out, sets
 * *payload_out to point into buf at the payload, and returns the total
 * record size (including padding). Returns 0 on a short buffer, bad
 * magic, out-of-range length, or crc mismatch.
 */
static inline int32_t wal_decode_rec(const uint8_t *buf, int32_t buf_len,
                                     wal_rec_t *rec_out,
                                     const uint8_t **payload_out)
{
    wal_rec_t h;
    int32_t total;

    if (buf_len < WAL_REC_HDR_SIZE)
        return 0;

    mem_copy((uint8_t *)&h, buf, WAL_REC_HDR_SIZE);

    if (h.magic != WAL_REC_MAGIC)
        return 0;
    if (h.len > (uint32_t)WAL_REC_MAX_LEN)
        return 0;

    total = wal_rec_size((int32_t)h.len);
    if (total > buf_len)
        return 0;

    if (wal_rec_crc(buf, (int32_t)h.len) != h.crc)
        return 0;

    *rec_out = h;
    *payload_out = buf + WAL_REC_HDR_SIZE;
    return total;
}

#endif /* MSGSRVD_WAL_H */

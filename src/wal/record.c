#include "wal/record.h"
#include "core/crc32c.h"
#include "core/mem.h"

/* Checksummed range starts after the checksum itself. */
#define CRC_OFFSET  4

int32_t wal_rec_size(int32_t payload_len)
{
    int32_t padded;

    if (payload_len < 0 || payload_len > WAL_MAX_PAYLOAD)
        return 0;

    padded = (payload_len + 7) & ~7;
    return WAL_REC_HEADER_SIZE + padded;
}

int32_t wal_rec_encode(uint8_t *buf, int32_t buf_len, wal_rec_t *r,
                       const uint8_t *payload)
{
    int32_t total;
    int32_t len;
    int32_t padded;

    if (!buf || !r)
        return 0;

    /*
     * wal_rec_size is the only bounds gate on a length. A length too
     * large to be real, including one that wraps negative on the cast,
     * comes back as a zero size and stops here.
     */
    len = (int32_t)r->len;
    if (len > 0 && !payload)
        return 0;

    total = wal_rec_size(len);
    if (total == 0 || buf_len < total)
        return 0;

    padded = total - WAL_REC_HEADER_SIZE;

    r->crc = 0;
    r->_pad = 0;
    mem_copy(buf, (const uint8_t *)r, WAL_REC_HEADER_SIZE);

    if (len > 0)
        mem_copy(buf + WAL_REC_HEADER_SIZE, payload, len);

    /*
     * Padding is zeroed rather than left as whatever the buffer held,
     * so that the checksum over it is reproducible and a reader can
     * cover the whole record in one range.
     */
    if (padded > len)
        mem_zero(buf + WAL_REC_HEADER_SIZE + len, padded - len);

    r->crc = crc32c(0, buf + CRC_OFFSET, total - CRC_OFFSET);
    mem_copy(buf, (const uint8_t *)&r->crc, sizeof(r->crc));

    return total;
}

bool_t wal_rec_decode(const uint8_t *buf, int32_t buf_len, wal_rec_t *out)
{
    if (!buf || !out || buf_len < WAL_REC_HEADER_SIZE)
        return FALSE;

    mem_copy((uint8_t *)out, buf, WAL_REC_HEADER_SIZE);
    return TRUE;
}

bool_t wal_rec_is_end(const wal_rec_t *r)
{
    return (r && r->seq == 0) ? TRUE : FALSE;
}

bool_t wal_rec_verify(const uint8_t *buf, int32_t buf_len, const wal_rec_t *r)
{
    int32_t  total;
    uint32_t computed;

    if (!buf || !r)
        return FALSE;

    total = wal_rec_size((int32_t)r->len);
    if (total == 0 || buf_len < total)
        return FALSE;

    computed = crc32c(0, buf + CRC_OFFSET, total - CRC_OFFSET);
    return (computed == r->crc) ? TRUE : FALSE;
}

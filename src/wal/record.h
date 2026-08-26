#ifndef MSGSRVD_WAL_RECORD_H
#define MSGSRVD_WAL_RECORD_H

#include "core/types.h"
#include "proto/msg.h"

/*
 * WAL record format.
 *
 * A record is a 56-byte fixed header, the payload, and zero padding to
 * the next 8-byte boundary. Records are appended back to back inside a
 * preallocated segment file.
 *
 * Layout, little-endian like the wire protocol:
 *
 *   [0..3]   crc            CRC32C over [4..record end), padding included
 *   [4..7]   len            payload bytes, before padding
 *   [8..15]  term           leader term that wrote the record
 *   [16..23] seq            WAL sequence, dense, starts at 1
 *   [24..31] session        client session, 0 for internal records
 *   [32..39] client_seq     client-assigned sequence within the session
 *   [40..41] record_type    MSG_RECORD_*, routes to a projector
 *   [42..43] flags          durability flags the write asked for
 *   [44..47] _pad
 *   [48..55] partition_key
 *   [56..]   payload, then zero padding to an 8-byte boundary
 *
 * The checksum comes first so that len falls inside it. A corrupted
 * length would otherwise decide how many bytes to read before anything
 * had a chance to detect it, and recovery would be trusting a number
 * it could not check. Putting crc at offset 0 leaves one contiguous
 * checksummed range rather than two.
 *
 * Unwritten space is a record with seq 0. Segments are preallocated
 * and therefore read back as zeros, so the end of the written region
 * identifies itself without a marker having to be written, and every
 * real record has seq >= 1. Testing seq rather than a zero checksum
 * avoids the case where an empty record's checksum happens to be zero.
 */

#define WAL_REC_HEADER_SIZE 56

/* A record's payload is a protocol payload, so the two limits agree. */
#define WAL_MAX_PAYLOAD     MSG_MAX_PAYLOAD

/*
 * Largest a record can be on disk. A recovery scan needs a buffer at
 * least this big to be sure it can hold any single record it meets.
 */
#define WAL_REC_MAX_SIZE \
    (WAL_REC_HEADER_SIZE + ((WAL_MAX_PAYLOAD + 7) & ~7))

typedef struct {
    uint32_t    crc;            /* over everything after this field */
    uint32_t    len;            /* payload bytes, before padding */
    uint64_t    term;
    uint64_t    seq;
    uint64_t    session;
    uint64_t    client_seq;
    uint16_t    record_type;
    uint16_t    flags;
    uint32_t    _pad;
    uint64_t    partition_key;
} wal_rec_t;

STATIC_ASSERT(sizeof(wal_rec_t) == WAL_REC_HEADER_SIZE, wal_rec_header_size);

/*
 * On-disk size of a record carrying payload_len bytes, padding
 * included. Returns 0 if payload_len is out of range.
 *
 * This is the only bounds gate on a record length, and the encoder and
 * the verifier both route through it rather than repeating the test. A
 * length read off disk is untrusted, and one large enough to wrap
 * negative on the cast from uint32_t lands in the same rejected range
 * as any other absurd value.
 */
int32_t wal_rec_size(int32_t payload_len);

/*
 * Encode a record into buf: header, payload, then zeroed padding.
 * r->crc is ignored on input and set on output, so the caller gets
 * back the checksum that was written. payload may be NULL when
 * r->len is 0.
 *
 * Returns the bytes written, or 0 if the record does not fit or
 * r->len is out of range.
 */
int32_t wal_rec_encode(uint8_t *buf, int32_t buf_len, wal_rec_t *r,
                       const uint8_t *payload);

/*
 * Decode the fixed header. Does not verify the checksum and does not
 * require the payload to be present, so a scan can read a header and
 * decide how much more it needs. Returns FALSE only if buf is too
 * small to hold a header.
 */
bool_t wal_rec_decode(const uint8_t *buf, int32_t buf_len, wal_rec_t *out);

/* TRUE if this header is unwritten space rather than a record. */
bool_t wal_rec_is_end(const wal_rec_t *r);

/*
 * Recompute the checksum over the record at buf and compare it with
 * the one in r. buf must hold the whole record, and buf_len says how
 * much is there. Returns FALSE on a short buffer, an out-of-range
 * length, or a mismatch, which are the three shapes a torn tail takes.
 */
bool_t wal_rec_verify(const uint8_t *buf, int32_t buf_len,
                      const wal_rec_t *r);

#endif /* MSGSRVD_WAL_RECORD_H */

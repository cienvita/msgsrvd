#ifndef MSGSRVD_MSG_H
#define MSGSRVD_MSG_H

#include "core/types.h"
#include "core/mem.h"

/*
 * Wire protocol definitions.
 *
 * WAL-centric: every mutation is a WAL record. The protocol has a
 * small fixed set of verbs (op) and routes to a projector by
 * record_type. New storage types are added as new record_type values
 * without changing the op set.
 *
 * All integers are little-endian. x86-64 and AArch64 are natively
 * little-endian on all shipping Linux platforms, so there is no
 * byte-order conversion anywhere in the codec.
 *
 * Header is 32 bytes, naturally aligned, zero parsing cost.
 *
 * Codec uses raw pointers + length, transient access only.
 * Stored references to message data use index-based spans.
 */

#define MSG_MAGIC       0x4D534756  /* "MSGV" in little-endian */
#define MSG_VERSION     1
#define MSG_HEADER_SIZE 32

/*
 * Operations, fixed verbs. Flat 1..N numbering. record_type
 * distinguishes per-projector semantics; op stays small and stable.
 */
enum {
    MSG_OP_WRITE        = 1,    /* client -> server: append WAL record */
    MSG_OP_READ         = 2,    /* client -> server: read via projector */
    MSG_OP_DELETE       = 3,    /* client -> server: tombstone via projector */
    MSG_OP_SUBSCRIBE    = 4,    /* client -> server: register subscription */
    MSG_OP_NOTIFY       = 5,    /* server -> client: pushed subscription record */
    MSG_OP_ACK          = 6,    /* server -> client: acknowledgment */
    MSG_OP_ERR          = 7,    /* server -> client: error response */
    MSG_OP_PING         = 8,    /* keepalive request */
    MSG_OP_PONG         = 9     /* keepalive response */
};

/*
 * Record type, 16-bit slot in the header reserved for future routing.
 * Currently only RECORD_NONE is defined; specific record types are not
 * supported yet.
 */
enum {
    MSG_RECORD_NONE     = 0
};

/*
 * Protocol-level error codes. Sent in the payload of MSG_OP_ERR
 * responses. Business-level errors (NOT_FOUND, INSUFFICIENT_REPLICAS,
 * etc.) are handled above the protocol layer and are not listed here.
 */
enum {
    MSG_ERR_OK              = 0,
    MSG_ERR_BAD_MAGIC       = 1,    /* header magic mismatch */
    MSG_ERR_BAD_VERSION     = 2,    /* unsupported protocol version */
    MSG_ERR_BAD_OP          = 3,    /* unknown op code */
    MSG_ERR_PAYLOAD_TOO_BIG = 4     /* payload exceeds receive buffer */
};

/*
 * Header flags.
 *
 * Durability control on WRITE requests:
 *   (none)                                   fire-and-forget
 *   ACK_REQ                                  ACK after WAL append
 *   ACK_REQ | SYNC                           ACK after fsync
 *   ACK_REQ | SYNC | REPLICATED              ACK after replica confirms
 *   + ALLOW_DEGRADED                         accept if replicas unreachable;
 *                                            ACK will carry DEGRADED flag
 *
 * LAST terminates a subscription stream (server -> client).
 */
enum {
    MSG_FLAG_ACK_REQ        = (1 << 0),     /* sender wants an ACK */
    MSG_FLAG_SYNC           = (1 << 1),     /* fsync before ACK */
    MSG_FLAG_REPLICATED     = (1 << 2),     /* replica confirm before ACK */
    MSG_FLAG_ALLOW_DEGRADED = (1 << 3),     /* accept write even if replicas down */
    MSG_FLAG_DEGRADED       = (1 << 4),     /* response: accepted but not replicated */
    MSG_FLAG_LAST           = (1 << 5)      /* final record in a subscribe stream */
};

/*
 * Wire header, 32 bytes, memcpy-decodable, naturally aligned.
 *
 * [0..3]   magic           0x4D534756
 * [4..5]   version         protocol version
 * [6..7]   flags           MSG_FLAG_*
 * [8..9]   op              MSG_OP_*
 * [10..11] record_type     MSG_RECORD_* (0 when not applicable)
 * [12..15] payload_len     bytes following the header
 * [16..23] partition_key   routing/colocation key (hashed for partitioning).
 *                          Not a semantic ID, identity lives in the payload
 *                          and is interpreted by the projector.
 * [24..31] sequence        dual-use field:
 *                            WRITE request: client-assigned monotonic id
 *                                           (used by server for dedup on retry)
 *                            ACK response:  server-assigned WAL sequence number
 *                            other:         0 or context-specific
 */
typedef struct {
    uint32_t    magic;
    uint16_t    version;
    uint16_t    flags;
    uint16_t    op;
    uint16_t    record_type;
    uint32_t    payload_len;
    uint64_t    partition_key;
    uint64_t    sequence;
} msg_header_t;

STATIC_ASSERT(sizeof(msg_header_t) == 32, header_size);

/*
 * Decode header from a buffer.
 * No parsing, direct memcpy from wire to struct.
 * Returns FALSE if buffer too small or magic mismatch.
 */
static inline bool_t msg_decode_header(const uint8_t *buf, int32_t buf_len,
                                       msg_header_t *hdr)
{
    if (buf_len < MSG_HEADER_SIZE)
        return FALSE;

    mem_copy((uint8_t *)hdr, buf, MSG_HEADER_SIZE);

    if (hdr->magic != MSG_MAGIC)
        return FALSE;

    return TRUE;
}

/*
 * Encode header into a buffer.
 * Returns FALSE if not enough space.
 */
static inline bool_t msg_encode_header(uint8_t *buf, int32_t buf_len,
                                       const msg_header_t *hdr)
{
    if (buf_len < MSG_HEADER_SIZE)
        return FALSE;

    mem_copy(buf, (const uint8_t *)hdr, MSG_HEADER_SIZE);
    return TRUE;
}

/* Build a header with common fields pre-filled */
static inline msg_header_t msg_header_new(uint16_t op, uint16_t record_type,
                                          uint16_t flags,
                                          uint32_t payload_len,
                                          uint64_t partition_key,
                                          uint64_t sequence)
{
    msg_header_t h;
    h.magic = MSG_MAGIC;
    h.version = MSG_VERSION;
    h.flags = flags;
    h.op = op;
    h.record_type = record_type;
    h.payload_len = payload_len;
    h.partition_key = partition_key;
    h.sequence = sequence;
    return h;
}

/*
 * Validate a decoded header.
 * Returns TRUE if the header looks well-formed.
 */
static inline bool_t msg_header_valid(const msg_header_t *hdr)
{
    if (hdr->magic != MSG_MAGIC)
        return FALSE;
    if (hdr->version == 0 || hdr->version > MSG_VERSION)
        return FALSE;
    if (hdr->op < MSG_OP_WRITE || hdr->op > MSG_OP_PONG)
        return FALSE;
    /* Reject absurdly large payloads (>64MB) */
    if (hdr->payload_len > (64 * 1024 * 1024))
        return FALSE;
    return TRUE;
}

/*
 * Check if a buffer contains a complete message (header + payload).
 * Returns the total message size, or 0 if incomplete/invalid.
 */
static inline int32_t msg_complete_size(const uint8_t *buf, int32_t buf_len)
{
    msg_header_t hdr;
    int32_t total;

    if (!msg_decode_header(buf, buf_len, &hdr))
        return 0;

    total = MSG_HEADER_SIZE + (int32_t)hdr.payload_len;
    if (total > buf_len)
        return 0;

    return total;
}

/* Pointer to the payload portion of a message buffer */
static inline const uint8_t *msg_payload_ptr(const uint8_t *buf,
                                             int32_t buf_len)
{
    if (buf_len < MSG_HEADER_SIZE)
        return NULL;
    return buf + MSG_HEADER_SIZE;
}

#endif /* MSGSRVD_MSG_H */

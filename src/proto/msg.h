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
 *
 * Sessions. A client connection is bound to a session by HELLO, which
 * must be the first frame on the connection. The session id does not
 * appear on the wire after that, it is connection state. Dedup is
 * keyed on (session, sequence), so resuming a session lets the server
 * discard records it already holds when a client resends after a
 * reconnect.
 */

#define MSG_MAGIC       0x4D534756  /* "MSGV" in little-endian */
#define MSG_VERSION     2
#define MSG_VERSION_MIN 2           /* oldest version this build accepts */
#define MSG_HEADER_SIZE 32

/*
 * Largest payload accepted on any frame. A per-connection receive
 * buffer smaller than this bounds it further. Clients coalesce small
 * records into one frame rather than sending large ones, so this does
 * not need to be generous.
 */
#define MSG_MAX_PAYLOAD (1024 * 1024)

/* Longest client name carried by HELLO */
#define MSG_MAX_NAME     64

/* Longest text carried by an ERR payload */
#define MSG_MAX_ERR_TEXT 64

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
    MSG_OP_PONG         = 9,    /* keepalive response */
    MSG_OP_HELLO        = 10    /* client -> server: open or resume session */
};

#define MSG_OP_MIN MSG_OP_WRITE
#define MSG_OP_MAX MSG_OP_HELLO

/*
 * Record type, 16-bit slot in the header reserved for future routing.
 * Currently only RECORD_NONE is defined; specific record types are not
 * supported yet.
 */
enum {
    MSG_RECORD_NONE     = 0
};

/*
 * Error codes. Sent in the payload of MSG_OP_ERR responses.
 *
 * 1..4 are framing violations: the frame could not be trusted, so the
 * connection is closed after the ERR. 5..8 are routing and session
 * conditions: the frame was well-formed and the connection survives,
 * the client is expected to act on the code and carry on.
 *
 * Business-level errors (NOT_FOUND and the like) belong to the
 * projector above this layer and are not listed here.
 */
enum {
    MSG_ERR_OK              = 0,
    MSG_ERR_BAD_MAGIC       = 1,    /* header magic mismatch */
    MSG_ERR_BAD_VERSION     = 2,    /* unsupported protocol version */
    MSG_ERR_BAD_OP          = 3,    /* unknown op code */
    MSG_ERR_PAYLOAD_TOO_BIG = 4,    /* payload exceeds the cap or the buffer */
    MSG_ERR_NOT_LEADER      = 5,    /* write reached a follower; text = leader */
    MSG_ERR_BEHIND          = 6,    /* min_seq not reached within the bound */
    MSG_ERR_NO_SESSION      = 7,    /* frame before HELLO, or a second HELLO */
    MSG_ERR_SESSION_UNKNOWN = 8,    /* resume of an expired or unknown session */
    MSG_ERR_UNSUPPORTED     = 9,    /* verb understood but not implemented yet */
    MSG_ERR_NO_REPLICAS     = 10    /* replication asked for, none available */
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
 * An ACK echoes the durability flags it satisfies, which is what lets
 * one session carry more than one level without the two high-water
 * marks being confused for each other.
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
 *                            WRITE request: client-assigned sequence within
 *                                           the session, dense, starts at 1
 *                                           (used by server for dedup on retry)
 *                            ACK response:  server-assigned WAL sequence number,
 *                                           or the session id when acking HELLO
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
 * Payload layouts.
 *
 * Each is fixed-size and naturally aligned, decoded by memcpy like the
 * header. Variable-length tails (HELLO's name, ERR's text) follow the
 * fixed part and are bounded by a length field inside it.
 */

/*
 * HELLO request payload.
 *
 * session 0 asks the server to open a new session, any other value
 * asks it to resume that one. The client name is advisory: it reaches
 * server logs and nothing else.
 *
 * The reply is an ACK whose header sequence carries the session id and
 * whose payload carries the highest client sequence the server holds
 * durably for it, which is where the client resumes sending. A resume
 * of a session the server has forgotten is answered with ERR
 * SESSION_UNKNOWN rather than a new id, so a client cannot mistake a
 * fresh session for its old one and skip a resend.
 */
typedef struct {
    uint64_t    session;        /* 0 = open new, else resume */
    uint16_t    name_len;       /* name bytes following this struct */
    uint16_t    _pad0;
    uint32_t    _pad1;
} msg_hello_t;

#define MSG_HELLO_SIZE 16
STATIC_ASSERT(sizeof(msg_hello_t) == MSG_HELLO_SIZE, hello_size);

/*
 * ACK payload, cumulative.
 *
 * client_seq is a high-water mark: every record from this session with
 * sequence <= client_seq is durable to the level named by the ACK
 * header's flags. The server sends one ACK per group commit per
 * session rather than one per record, so a client with a deep pipeline
 * pays one return frame per batch.
 */
typedef struct {
    uint64_t    client_seq;
} msg_ack_t;

#define MSG_ACK_SIZE 8
STATIC_ASSERT(sizeof(msg_ack_t) == MSG_ACK_SIZE, ack_size);

/*
 * ERR payload. text is optional context, currently used only by
 * NOT_LEADER, which carries the leader's address as "host:port".
 */
typedef struct {
    uint16_t    code;           /* MSG_ERR_* */
    uint16_t    text_len;       /* text bytes following this struct */
    uint32_t    _pad;
} msg_err_payload_t;

#define MSG_ERR_SIZE 8
STATIC_ASSERT(sizeof(msg_err_payload_t) == MSG_ERR_SIZE, err_payload_size);

/*
 * READ request payload prefix.
 *
 * min_seq is a read-your-writes barrier: the node serves the read only
 * once its durable sequence has reached min_seq, and answers ERR
 * BEHIND if that has not happened within the node's configured bound.
 * 0 disables the barrier. Clients default it to the WAL sequence from
 * their last ACK, which is what makes a read from a nearby follower
 * consistent with the client's own writes to the leader.
 *
 * A projector-specific selector follows the prefix. No projector
 * defines one yet, so the selector is empty and the read is by
 * partition_key alone.
 */
typedef struct {
    uint64_t    min_seq;
} msg_read_t;

#define MSG_READ_SIZE 8
STATIC_ASSERT(sizeof(msg_read_t) == MSG_READ_SIZE, read_size);

/*
 * SUBSCRIBE request payload.
 *
 * from_seq is where replay starts: the first WAL sequence the client
 * has not processed. 0 means live only, no replay. A reconnecting
 * client passes the sequence after the last record it processed, which
 * is what makes delivery at-least-once rather than at-most-once.
 *
 * min_seq is the same barrier as READ.
 */
typedef struct {
    uint64_t    min_seq;
    uint64_t    from_seq;
} msg_subscribe_t;

#define MSG_SUBSCRIBE_SIZE 16
STATIC_ASSERT(sizeof(msg_subscribe_t) == MSG_SUBSCRIBE_SIZE, subscribe_size);

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
    if (hdr->version < MSG_VERSION_MIN || hdr->version > MSG_VERSION)
        return FALSE;
    if (hdr->op < MSG_OP_MIN || hdr->op > MSG_OP_MAX)
        return FALSE;
    if (hdr->payload_len > (uint32_t)MSG_MAX_PAYLOAD)
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

/*
 * Fixed-size payload codec. Same contract as the header codec: memcpy
 * in and out, bounds checked, no parsing. The typed wrappers below are
 * what callers use; these two carry the bounds check.
 */
static inline bool_t msg_payload_get(const uint8_t *buf, int32_t buf_len,
                                     void *out, int32_t size)
{
    if (!buf || buf_len < size)
        return FALSE;
    mem_copy((uint8_t *)out, buf, size);
    return TRUE;
}

static inline bool_t msg_payload_put(uint8_t *buf, int32_t buf_len,
                                     const void *in, int32_t size)
{
    if (!buf || buf_len < size)
        return FALSE;
    mem_copy(buf, (const uint8_t *)in, size);
    return TRUE;
}

static inline bool_t msg_decode_hello(const uint8_t *buf, int32_t buf_len,
                                      msg_hello_t *out)
{
    return msg_payload_get(buf, buf_len, out, MSG_HELLO_SIZE);
}

static inline bool_t msg_encode_hello(uint8_t *buf, int32_t buf_len,
                                      const msg_hello_t *in)
{
    return msg_payload_put(buf, buf_len, in, MSG_HELLO_SIZE);
}

static inline bool_t msg_decode_ack(const uint8_t *buf, int32_t buf_len,
                                    msg_ack_t *out)
{
    return msg_payload_get(buf, buf_len, out, MSG_ACK_SIZE);
}

static inline bool_t msg_encode_ack(uint8_t *buf, int32_t buf_len,
                                    const msg_ack_t *in)
{
    return msg_payload_put(buf, buf_len, in, MSG_ACK_SIZE);
}

static inline bool_t msg_decode_err(const uint8_t *buf, int32_t buf_len,
                                    msg_err_payload_t *out)
{
    return msg_payload_get(buf, buf_len, out, MSG_ERR_SIZE);
}

static inline bool_t msg_encode_err(uint8_t *buf, int32_t buf_len,
                                    const msg_err_payload_t *in)
{
    return msg_payload_put(buf, buf_len, in, MSG_ERR_SIZE);
}

static inline bool_t msg_decode_read(const uint8_t *buf, int32_t buf_len,
                                     msg_read_t *out)
{
    return msg_payload_get(buf, buf_len, out, MSG_READ_SIZE);
}

static inline bool_t msg_encode_read(uint8_t *buf, int32_t buf_len,
                                     const msg_read_t *in)
{
    return msg_payload_put(buf, buf_len, in, MSG_READ_SIZE);
}

static inline bool_t msg_decode_subscribe(const uint8_t *buf, int32_t buf_len,
                                          msg_subscribe_t *out)
{
    return msg_payload_get(buf, buf_len, out, MSG_SUBSCRIBE_SIZE);
}

static inline bool_t msg_encode_subscribe(uint8_t *buf, int32_t buf_len,
                                          const msg_subscribe_t *in)
{
    return msg_payload_put(buf, buf_len, in, MSG_SUBSCRIBE_SIZE);
}

/*
 * Validate a decoded HELLO against the frame's payload length.
 * name_len has to account for exactly the bytes after the fixed part,
 * so a truncated or padded name is rejected rather than read past.
 */
static inline bool_t msg_hello_valid(const msg_hello_t *h, int32_t payload_len)
{
    if (payload_len < MSG_HELLO_SIZE)
        return FALSE;
    if (h->name_len > MSG_MAX_NAME)
        return FALSE;
    if ((int32_t)h->name_len != payload_len - MSG_HELLO_SIZE)
        return FALSE;
    return TRUE;
}

/* Name bytes of a validated HELLO, or NULL when the name is empty. */
static inline const uint8_t *msg_hello_name(const uint8_t *payload,
                                            int32_t payload_len)
{
    if (!payload || payload_len <= MSG_HELLO_SIZE)
        return NULL;
    return payload + MSG_HELLO_SIZE;
}

/* Same contract as msg_hello_valid, for the ERR text tail. */
static inline bool_t msg_err_valid(const msg_err_payload_t *p,
                                   int32_t payload_len)
{
    if (payload_len < MSG_ERR_SIZE)
        return FALSE;
    if (p->text_len > MSG_MAX_ERR_TEXT)
        return FALSE;
    if ((int32_t)p->text_len != payload_len - MSG_ERR_SIZE)
        return FALSE;
    return TRUE;
}

/* Text bytes of a validated ERR payload, or NULL when there are none. */
static inline const uint8_t *msg_err_text(const uint8_t *payload,
                                          int32_t payload_len)
{
    if (!payload || payload_len <= MSG_ERR_SIZE)
        return NULL;
    return payload + MSG_ERR_SIZE;
}

#endif /* MSGSRVD_MSG_H */

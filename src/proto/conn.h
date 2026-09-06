#ifndef MSGSRVD_CONN_H
#define MSGSRVD_CONN_H

#include "core/types.h"
#include "proto/msg.h"

/*
 * Connection-level protocol state machine.
 *
 * Pure function: takes bytes in, produces parsed frames and control
 * actions out. No I/O, no knowledge of sockets, WAL, or projectors.
 * Higher layers handle dispatch; this layer owns only protocol
 * correctness (framing, magic, version, bounds).
 *
 * Sessions are enforced but not assigned here: a client-mode
 * connection must open with exactly one opening frame, HELLO from a
 * client or REPL_START from a replica, and nothing may precede it.
 * That is an ordering rule this layer can check without knowing what a
 * session is. The id itself comes from the WAL layer above, which sees
 * the HELLO frame like any other.
 *
 * Contract:
 *   1. Caller reads bytes from the socket into conn->buf using
 *      conn_recv_append, then calls conn_feed to drain complete
 *      frames into an action list.
 *   2. Action payloads for CONN_ACTION_FRAME reference conn->buf
 *      directly (zero-copy). They stay valid until conn_compact,
 *      which is the caller saying it is done with them and wants the
 *      room back.
 *   3. When conn_feed emits CONN_ACTION_CLOSE or sets c->closed,
 *      the caller should stop feeding bytes and tear the connection
 *      down. Any remaining unparsed bytes are discarded.
 */

/*
 * Connection mode. Client connections carry the session handshake;
 * node-to-node connections (replication) do not, since they are not
 * sessions and their peer is not a client.
 */
enum {
    CONN_MODE_CLIENT    = 0,    /* HELLO required as the first frame */
    CONN_MODE_INTERNAL  = 1     /* no session handshake */
};

/* Action emitted by conn_feed */
enum {
    CONN_ACTION_FRAME       = 1,    /* complete inbound frame */
    CONN_ACTION_REPLY_ERR   = 2,    /* caller should send an ERR response */
    CONN_ACTION_CLOSE       = 3     /* fatal; caller should close connection */
};

typedef struct {
    uint8_t         type;       /* CONN_ACTION_* */
    uint8_t         _pad[3];
    union {
        struct {
            msg_header_t    header;
            const uint8_t   *payload;       /* points into conn->buf */
            int32_t         payload_len;
        } frame;
        struct {
            uint16_t        code;           /* MSG_ERR_* */
            uint16_t        _pad2;
            uint64_t        sequence;       /* echo from offending frame, or 0 */
        } reply_err;
    } u;
} conn_action_t;

/*
 * Per-connection state.
 *
 * buf is a flat receive buffer. Bytes accumulate at buf[0..buf_len)
 * and conn_feed parses forward from buf_off, leaving the bytes it has
 * parsed where they are. That is what makes a frame's payload pointer
 * safe to use after the call that emitted it: reclaiming the space is
 * a separate step the caller takes when it is finished.
 *
 * Compacting inside conn_feed would move the unparsed tail over the
 * frames just emitted, so a caller handed several frames at once would
 * find the earlier ones carrying the bytes of the later ones.
 */
typedef struct {
    uint8_t         *buf;
    int32_t         buf_cap;
    int32_t         buf_len;
    int32_t         buf_off;    /* parsed up to here, not yet reclaimed */
    bool_t          closed;     /* set on fatal protocol error */
    uint8_t         mode;       /* CONN_MODE_* */
    bool_t          opened;     /* an opening frame has been emitted */
    uint8_t         _pad;
} conn_t;

/*
 * Initialize a connection with a caller-provided receive buffer.
 * buf typically comes from a per-connection arena.
 * mode is CONN_MODE_CLIENT or CONN_MODE_INTERNAL.
 */
void conn_init(conn_t *c, uint8_t *buf, int32_t buf_cap, uint8_t mode);

/*
 * Append received bytes to the connection's receive buffer.
 * Returns number of bytes actually appended (may be less than n if
 * the buffer is full, the caller must drain via conn_feed first).
 */
int32_t conn_recv_append(conn_t *c, const uint8_t *src, int32_t n);

/*
 * Free space at the end of the receive buffer, and where it starts.
 *
 * These exist for a caller that hands the buffer to the kernel instead
 * of copying into it: the recv reads into conn_recv_ptr for
 * conn_recv_space bytes, and conn_recv_commit accounts for what
 * arrived. conn_recv_append is the copying counterpart, for bytes the
 * caller already holds.
 */
int32_t conn_recv_space(const conn_t *c);
uint8_t *conn_recv_ptr(conn_t *c);

/*
 * Account for n bytes written directly into the receive buffer.
 * Returns the number accepted, which is less than n only if the caller
 * has overrun the space it was given.
 */
int32_t conn_recv_commit(conn_t *c, int32_t n);

/*
 * Drive the state machine. Parses as many complete frames as possible
 * from the receive buffer, emitting actions into out[0..out_cap).
 * Returns the number of actions written.
 *
 * If out fills up before the buffer drains, call again after consuming
 * the actions, the state machine resumes from where it stopped.
 *
 * After frames are consumed, the buffer is compacted (any trailing
 * partial frame moves to buf[0]). This invalidates pointers from
 * previously-emitted FRAME actions, consume them before calling
 * conn_feed or conn_recv_append again.
 */
int32_t conn_feed(conn_t *c, conn_action_t *out, int32_t out_cap);

/*
 * Reclaim the space of everything parsed so far, moving any unparsed
 * tail to the front. This is what invalidates the payload pointers of
 * emitted frames, so the caller does it once it has handled them and
 * before it reads more bytes in.
 */
void conn_compact(conn_t *c);

#endif /* MSGSRVD_CONN_H */

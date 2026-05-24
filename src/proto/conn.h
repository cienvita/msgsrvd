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
 * Contract:
 *   1. Caller reads bytes from the socket into conn->buf using
 *      conn_recv_append, then calls conn_feed to drain complete
 *      frames into an action list.
 *   2. Action payloads for CONN_ACTION_FRAME reference conn->buf
 *      directly (zero-copy). These pointers are invalidated by any
 *      subsequent conn_* call, consume actions before the next call.
 *   3. When conn_feed emits CONN_ACTION_CLOSE or sets c->closed,
 *      the caller should stop feeding bytes and tear the connection
 *      down. Any remaining unparsed bytes are discarded.
 */

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
 * until conn_feed consumes complete frames. After consumption, any
 * trailing partial frame is memmoved to the front of buf (compaction),
 * which invalidates pointers from previously-emitted FRAME actions.
 */
typedef struct {
    uint8_t         *buf;
    int32_t         buf_cap;
    int32_t         buf_len;
    bool_t          closed;     /* set on fatal protocol error */
    uint8_t         _pad[3];
} conn_t;

/*
 * Initialize a connection with a caller-provided receive buffer.
 * buf typically comes from a per-connection arena.
 */
void conn_init(conn_t *c, uint8_t *buf, int32_t buf_cap);

/*
 * Append received bytes to the connection's receive buffer.
 * Returns number of bytes actually appended (may be less than n if
 * the buffer is full, the caller must drain via conn_feed first).
 */
int32_t conn_recv_append(conn_t *c, const uint8_t *src, int32_t n);

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

#endif /* MSGSRVD_CONN_H */

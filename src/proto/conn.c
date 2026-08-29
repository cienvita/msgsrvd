#include "proto/conn.h"
#include "core/mem.h"

void conn_init(conn_t *c, uint8_t *buf, int32_t buf_cap, uint8_t mode)
{
    c->buf = buf;
    c->buf_cap = buf_cap;
    c->buf_len = 0;
    c->buf_off = 0;
    c->closed = FALSE;
    c->mode = mode;
    c->opened = FALSE;
    c->_pad = 0;
}

void conn_compact(conn_t *c)
{
    int32_t tail;

    if (c->buf_off <= 0)
        return;

    tail = c->buf_len - c->buf_off;
    if (tail > 0)
        mem_copy(c->buf, c->buf + c->buf_off, tail);
    c->buf_len = tail;
    c->buf_off = 0;
}

int32_t conn_recv_append(conn_t *c, const uint8_t *src, int32_t n)
{
    int32_t space;
    int32_t take;

    if (c->closed || n <= 0)
        return 0;

    conn_compact(c);
    space = c->buf_cap - c->buf_len;
    take = n < space ? n : space;
    if (take <= 0)
        return 0;

    mem_copy(c->buf + c->buf_len, src, take);
    c->buf_len += take;
    return take;
}

int32_t conn_recv_space(const conn_t *c)
{
    return c->buf_cap - c->buf_len;
}

uint8_t *conn_recv_ptr(conn_t *c)
{
    return c->buf + c->buf_len;
}

int32_t conn_recv_commit(conn_t *c, int32_t n)
{
    int32_t space;

    if (c->closed || n <= 0)
        return 0;

    space = c->buf_cap - c->buf_len;
    if (n > space)
        n = space;

    c->buf_len += n;
    return n;
}

/*
 * Emit a protocol error + close pair. Returns the number of actions
 * written (0, 1, or 2 depending on remaining out capacity). Sets
 * c->closed so subsequent calls do nothing.
 */
static int32_t emit_fatal(conn_t *c, conn_action_t *out, int32_t out_cap,
                          uint16_t code, uint64_t echo_seq)
{
    int32_t n = 0;

    c->closed = TRUE;

    if (n < out_cap) {
        out[n].type = CONN_ACTION_REPLY_ERR;
        out[n].u.reply_err.code = code;
        out[n].u.reply_err._pad2 = 0;
        out[n].u.reply_err.sequence = echo_seq;
        n++;
    }
    if (n < out_cap) {
        out[n].type = CONN_ACTION_CLOSE;
        n++;
    }
    return n;
}

int32_t conn_feed(conn_t *c, conn_action_t *out, int32_t out_cap)
{
    int32_t cursor = c->buf_off;
    int32_t n_actions = 0;

    if (c->closed || out_cap <= 0)
        return 0;

    /*
     * Parse frames from buf[cursor..buf_len). Stop on:
     *   - out full
     *   - incomplete frame (need more bytes)
     *   - protocol error (emits fatal actions, sets closed)
     */
    for (;;) {
        msg_header_t hdr;
        int32_t avail = c->buf_len - cursor;
        int32_t total;

        if (n_actions >= out_cap)
            break;

        if (avail < MSG_HEADER_SIZE)
            break;  /* need more bytes for header */

        /* memcpy-decode header */
        mem_copy((uint8_t *)&hdr, c->buf + cursor, MSG_HEADER_SIZE);

        if (hdr.magic != MSG_MAGIC) {
            n_actions += emit_fatal(c, out + n_actions, out_cap - n_actions,
                                    MSG_ERR_BAD_MAGIC, 0);
            cursor = c->buf_len;    /* discard remaining bytes */
            break;
        }

        if (hdr.version < MSG_VERSION_MIN || hdr.version > MSG_VERSION) {
            n_actions += emit_fatal(c, out + n_actions, out_cap - n_actions,
                                    MSG_ERR_BAD_VERSION, hdr.sequence);
            cursor = c->buf_len;
            break;
        }

        if (hdr.op < MSG_OP_MIN || hdr.op > MSG_OP_MAX) {
            n_actions += emit_fatal(c, out + n_actions, out_cap - n_actions,
                                    MSG_ERR_BAD_OP, hdr.sequence);
            cursor = c->buf_len;
            break;
        }

        /*
         * Payload must be within the protocol cap and must fit in this
         * connection's receive buffer, which is usually the smaller of
         * the two. Both produce the same code: the client's remedy is
         * the same either way, send less.
         */
        if (hdr.payload_len > (uint32_t)MSG_MAX_PAYLOAD ||
            hdr.payload_len > (uint32_t)(c->buf_cap - MSG_HEADER_SIZE)) {
            n_actions += emit_fatal(c, out + n_actions, out_cap - n_actions,
                                    MSG_ERR_PAYLOAD_TOO_BIG, hdr.sequence);
            cursor = c->buf_len;
            break;
        }

        /*
         * Ordering: exactly one opening frame, and before anything
         * else. HELLO opens a client connection and REPL_START opens a
         * replica's, and which one it was is the caller's business,
         * not this layer's. Two ways to violate it, a repeated opening
         * frame and a frame that arrives without one. Checked before
         * the frame is complete, since no further bytes can make
         * either legal.
         */
        if (c->mode == CONN_MODE_CLIENT) {
            bool_t opens = (hdr.op == MSG_OP_HELLO ||
                            hdr.op == MSG_OP_REPL_START) ? TRUE : FALSE;

            if ((opens && c->opened) || (!opens && !c->opened)) {
                n_actions += emit_fatal(c, out + n_actions,
                                        out_cap - n_actions,
                                        MSG_ERR_NO_SESSION, hdr.sequence);
                cursor = c->buf_len;
                break;
            }
        }

        total = MSG_HEADER_SIZE + (int32_t)hdr.payload_len;
        if (avail < total)
            break;  /* need more bytes for payload */

        /* Complete frame, emit */
        out[n_actions].type = CONN_ACTION_FRAME;
        out[n_actions].u.frame.header = hdr;
        out[n_actions].u.frame.payload =
            (hdr.payload_len > 0) ? (c->buf + cursor + MSG_HEADER_SIZE) : NULL;
        out[n_actions].u.frame.payload_len = (int32_t)hdr.payload_len;
        n_actions++;
        cursor += total;

        /*
         * Only a frame that was actually emitted opens the
         * connection. A HELLO whose payload has not fully arrived
         * breaks out above and is re-examined on the next call.
         */
        if (hdr.op == MSG_OP_HELLO || hdr.op == MSG_OP_REPL_START)
            c->opened = TRUE;
    }

    /*
     * The parsed bytes stay where they are: the frames just emitted
     * point into them. conn_compact is what takes the space back.
     */
    c->buf_off = cursor;
    return n_actions;
}

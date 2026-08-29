#include "server/loop.h"
#include "core/mem.h"
#include "sys/os.h"

/*
 * Completion identity. The kind is in the top byte and the slot index
 * in the low bits, so a completion says what finished and for whom
 * without any lookup table.
 */
#define OP_ACCEPT   1
#define OP_RECV     2
#define OP_SEND     3
#define OP_SIGNAL   4

#define UD_MAKE(op, slot)   (((uint64_t)(op) << 56) | (uint64_t)(uint32_t)(slot))
#define UD_OP(ud)           ((int32_t)((ud) >> 56))
#define UD_SLOT(ud)         ((int32_t)((ud) & 0xFFFFFFFFULL))

/* Receive and send buffers, one pair per slot. */
static uint8_t recv_buf[LOOP_MAX_CONNS][LOOP_RECV_CAP];
static uint8_t send_buf[LOOP_MAX_CONNS][LOOP_SEND_CAP];

/* Somewhere for the signalfd read to land; the contents are not used. */
static uint8_t signal_buf[SIGNALFD_SIGINFO_SIZE];

/* ---- slots ---- */

static int32_t slot_alloc(loop_t *l)
{
    int32_t i;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        if (!l->conns[i].in_use)
            return i;
    }
    return -1;
}

static void slot_reset(loop_t *l, int32_t i)
{
    loop_conn_t *c = &l->conns[i];

    mem_zero((uint8_t *)c, (int32_t)sizeof(*c));
    c->fd = -1;
}

/*
 * Give up a slot once the kernel holds nothing for it. A slot with an
 * operation still in flight keeps its buffers until that completion
 * arrives, or the kernel would be writing into a slot handed to
 * another connection.
 */
static void slot_release(loop_t *l, int32_t i)
{
    loop_conn_t *c = &l->conns[i];
    err_t        e;

    if (!c->in_use)
        return;
    if (c->recv_pending || c->send_pending)
        return;

    if (c->fd >= 0) {
        err_init(&e);
        os_close(&e, c->fd);
    }
    slot_reset(l, i);
    l->closed++;
}

/* ---- reply staging ---- */

/* Append a frame to the connection's send buffer. FALSE if it is full. */
static bool_t reply(loop_conn_t *c, int32_t slot, uint16_t op, uint16_t flags,
                    uint64_t sequence, const uint8_t *payload,
                    int32_t payload_len)
{
    msg_header_t h;
    uint8_t     *out = send_buf[slot] + c->send_len;
    int32_t      room = LOOP_SEND_CAP - c->send_len;

    if (room < MSG_HEADER_SIZE + payload_len)
        return FALSE;

    h = msg_header_new(op, MSG_RECORD_NONE, flags, (uint32_t)payload_len,
                       0, sequence);
    msg_encode_header(out, room, &h);
    if (payload_len > 0)
        mem_copy(out + MSG_HEADER_SIZE, payload, payload_len);

    c->send_len += MSG_HEADER_SIZE + payload_len;
    return TRUE;
}

static bool_t reply_err(loop_conn_t *c, int32_t slot, uint16_t code,
                        uint64_t sequence)
{
    msg_err_payload_t p;
    uint8_t           buf[MSG_ERR_SIZE];

    p.code = code;
    p.text_len = 0;
    p._pad = 0;
    msg_encode_err(buf, MSG_ERR_SIZE, &p);

    return reply(c, slot, MSG_OP_ERR, 0, sequence, buf, MSG_ERR_SIZE);
}

static bool_t reply_ack(loop_conn_t *c, int32_t slot, uint16_t flags,
                        uint64_t sequence, uint64_t client_seq)
{
    msg_ack_t a;
    uint8_t   buf[MSG_ACK_SIZE];

    a.client_seq = client_seq;
    msg_encode_ack(buf, MSG_ACK_SIZE, &a);

    return reply(c, slot, MSG_OP_ACK, flags, sequence, buf, MSG_ACK_SIZE);
}

/* ---- frame handling ---- */

static void handle_hello(loop_t *l, int32_t slot, const msg_header_t *h,
                         const uint8_t *payload, int32_t payload_len)
{
    loop_conn_t *c = &l->conns[slot];
    msg_hello_t  hello;
    session_t   *s;

    if (!msg_decode_hello(payload, payload_len, &hello) ||
        !msg_hello_valid(&hello, payload_len)) {
        reply_err(c, slot, MSG_ERR_PAYLOAD_TOO_BIG, h->sequence);
        c->closing = TRUE;
        return;
    }

    if (hello.session == 0) {
        s = session_create(l->sessions);
        if (!s) {
            reply_err(c, slot, MSG_ERR_SESSION_UNKNOWN, h->sequence);
            c->closing = TRUE;
            return;
        }
    } else {
        s = session_lookup(l->sessions, hello.session);
        if (!s) {
            /*
             * The client is told its session is gone rather than
             * handed a fresh one, so it cannot mistake a new session
             * for its old one and skip resending what was in flight.
             */
            reply_err(c, slot, MSG_ERR_SESSION_UNKNOWN, h->sequence);
            return;
        }
    }

    c->session = s->id;
    reply_ack(c, slot, MSG_FLAG_ACK_REQ, s->id, s->last_client_seq);
}

static void handle_write(loop_t *l, int32_t slot, const msg_header_t *h,
                         const uint8_t *payload, int32_t payload_len,
                         uint8_t *scratch, int32_t scratch_len)
{
    loop_conn_t *c = &l->conns[slot];
    session_t   *s = session_lookup(l->sessions, c->session);
    wal_rec_t       rec;
    err_t           e;
    uint16_t        ack_flags;

    if (!s) {
        reply_err(c, slot, MSG_ERR_NO_SESSION, h->sequence);
        c->closing = TRUE;
        return;
    }

    /*
     * Replication is not built, so a write that requires it cannot be
     * honoured. Saying so is the only honest answer; accepting it
     * would be a promise of a second copy that does not exist. A
     * client willing to do without says so with ALLOW_DEGRADED, and
     * the acknowledgement it gets back says the same.
     */
    if ((h->flags & MSG_FLAG_REPLICATED) &&
        !(h->flags & MSG_FLAG_ALLOW_DEGRADED)) {
        reply_err(c, slot, MSG_ERR_NO_REPLICAS, h->sequence);
        return;
    }

    ack_flags = (uint16_t)(h->flags &
                           (MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC |
                            MSG_FLAG_REPLICATED));
    if (h->flags & MSG_FLAG_REPLICATED)
        ack_flags |= MSG_FLAG_DEGRADED;

    /*
     * A resend of something already durable is answered, not appended
     * again. This is what the client-assigned sequence is for: a retry
     * after a reconnect repeats whatever it could not confirm.
     */
    if (h->sequence <= s->last_client_seq) {
        l->dedup_hits++;
        if (h->flags & MSG_FLAG_ACK_REQ) {
            c->ack_due = TRUE;
            c->ack_flags = ack_flags;
            c->ack_client_seq = s->last_client_seq;
            c->ack_wal_seq = wal_durable_seq(l->wal);
        }
        return;
    }

    rec.crc = 0;
    rec.len = (uint32_t)payload_len;
    rec.term = 1;
    rec.seq = 0;
    rec.session = c->session;
    rec.client_seq = h->sequence;
    rec.record_type = h->record_type;
    rec.flags = h->flags;
    rec._pad = 0;
    rec.partition_key = h->partition_key;

    err_init(&e);
    if (!result_ok(wal_append(l->wal, &e, &rec, payload, scratch,
                              scratch_len))) {
        reply_err(c, slot, MSG_ERR_PAYLOAD_TOO_BIG, h->sequence);
        return;
    }

    l->writes++;
    l->wal_dirty = TRUE;
    s->last_client_seq = h->sequence;

    if (h->flags & MSG_FLAG_ACK_REQ) {
        c->ack_due = TRUE;
        c->ack_flags = ack_flags;
        c->ack_client_seq = h->sequence;
        c->ack_wal_seq = rec.seq;
    }
}

static void handle_frame(loop_t *l, int32_t slot, const conn_action_t *a,
                         uint8_t *scratch, int32_t scratch_len)
{
    loop_conn_t        *c = &l->conns[slot];
    const msg_header_t *h = &a->u.frame.header;

    switch (h->op) {
    case MSG_OP_HELLO:
        handle_hello(l, slot, h, a->u.frame.payload, a->u.frame.payload_len);
        break;
    case MSG_OP_WRITE:
        handle_write(l, slot, h, a->u.frame.payload, a->u.frame.payload_len,
                     scratch, scratch_len);
        break;
    case MSG_OP_PING:
        reply(c, slot, MSG_OP_PONG, 0, h->sequence, NULL, 0);
        break;
    case MSG_OP_READ:
    case MSG_OP_DELETE:
    case MSG_OP_SUBSCRIBE:
        reply_err(c, slot, MSG_ERR_UNSUPPORTED, h->sequence);
        break;
    default:
        /* A server-to-client verb arriving from a client is nonsense. */
        reply_err(c, slot, MSG_ERR_BAD_OP, h->sequence);
        c->closing = TRUE;
        break;
    }
}

/* ---- completion handling ---- */

typedef struct {
    loop_t  *l;
    uint8_t *scratch;
    int32_t  scratch_len;
} tick_ctx_t;

static void on_accept(loop_t *l, int32_t res)
{
    int32_t slot;
    err_t   e;

    l->accept_pending = FALSE;

    if (res < 0)
        return;

    slot = slot_alloc(l);
    if (slot < 0) {
        /* No room: refuse now rather than hold a connection we cannot serve */
        err_init(&e);
        os_close(&e, res);
        return;
    }

    slot_reset(l, slot);
    l->conns[slot].in_use = TRUE;
    l->conns[slot].fd = res;
    conn_init(&l->conns[slot].conn, recv_buf[slot], LOOP_RECV_CAP,
              CONN_MODE_CLIENT);
    l->accepted++;
}

static void on_recv(tick_ctx_t *tc, int32_t slot, int32_t res)
{
    loop_t        *l = tc->l;
    loop_conn_t   *c = &l->conns[slot];
    conn_action_t  actions[8];
    int32_t        n;
    int32_t        i;

    c->recv_pending = FALSE;

    if (!c->in_use)
        return;

    /* 0 is an orderly close, negative is an error; both end the connection */
    if (res <= 0) {
        c->closing = TRUE;
        return;
    }

    /* io_uring read straight into the buffer, so this only accounts. */
    conn_recv_commit(&c->conn, res);

    for (;;) {
        n = conn_feed(&c->conn, actions, 8);
        if (n == 0)
            break;

        for (i = 0; i < n; i++) {
            switch (actions[i].type) {
            case CONN_ACTION_FRAME:
                handle_frame(l, slot, &actions[i], tc->scratch,
                             tc->scratch_len);
                break;
            case CONN_ACTION_REPLY_ERR:
                reply_err(c, slot, actions[i].u.reply_err.code,
                          actions[i].u.reply_err.sequence);
                break;
            case CONN_ACTION_CLOSE:
                c->closing = TRUE;
                break;
            default:
                break;
            }
        }

        if (c->closing)
            break;
    }
}

static void on_send(loop_t *l, int32_t slot, int32_t res)
{
    loop_conn_t *c = &l->conns[slot];

    c->send_pending = FALSE;

    if (!c->in_use)
        return;

    if (res < 0) {
        c->closing = TRUE;
        return;
    }

    /*
     * A short write leaves the rest staged; the remaining bytes move
     * to the front and go out on the next pass.
     */
    if (res < c->send_len) {
        mem_copy(send_buf[slot], send_buf[slot] + res, c->send_len - res);
        c->send_len -= res;
    } else {
        c->send_len = 0;
    }
}

static void tick_cb(void *ctx, uint64_t user_data, int32_t res, uint32_t flags)
{
    tick_ctx_t *tc = (tick_ctx_t *)ctx;
    int32_t     slot = UD_SLOT(user_data);

    (void)flags;

    switch (UD_OP(user_data)) {
    case OP_ACCEPT:
        on_accept(tc->l, res);
        break;
    case OP_RECV:
        on_recv(tc, slot, res);
        break;
    case OP_SEND:
        on_send(tc->l, slot, res);
        break;
    case OP_SIGNAL:
        tc->l->signal_pending = FALSE;
        /*
         * Stop after this pass rather than here, so a record appended
         * moments ago still gets the flush it is owed.
         */
        if (res > 0)
            tc->l->stop = TRUE;
        break;
    default:
        break;
    }
}

/* ---- submission ---- */

static void arm_accept(loop_t *l)
{
    io_uring_sqe_t *sqe;

    if (l->accept_pending || l->stop)
        return;

    sqe = uring_get_sqe(&l->ring);
    if (!sqe)
        return;

    uring_prep_accept(sqe, l->listen_fd, NULL, NULL, 0, UD_MAKE(OP_ACCEPT, 0));
    l->accept_pending = TRUE;
}

static void arm_signal(loop_t *l)
{
    io_uring_sqe_t *sqe;

    if (l->signal_fd < 0 || l->signal_pending || l->stop)
        return;

    sqe = uring_get_sqe(&l->ring);
    if (!sqe)
        return;

    uring_prep_read(sqe, l->signal_fd, signal_buf,
                    (uint32_t)sizeof(signal_buf), 0, UD_MAKE(OP_SIGNAL, 0));
    l->signal_pending = TRUE;
}

/*
 * Queue whatever each connection is ready for. Sending takes priority
 * over reading, and neither is queued while the other is in flight, so
 * a slot never has two operations pointing at the same buffer. A slot
 * is given up only once nothing is staged for it and nothing is in
 * flight, since the kernel would otherwise be reading into or sending
 * from buffers already handed to another connection.
 */
static void arm_conns(loop_t *l)
{
    int32_t i;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        loop_conn_t    *c = &l->conns[i];
        io_uring_sqe_t *sqe;
        int32_t         space;

        if (!c->in_use)
            continue;

        if (c->recv_pending || c->send_pending)
            continue;

        /*
         * Staged output goes out even on a connection being closed. A
         * protocol error is reported and then the connection ends, so
         * dropping the slot first would close it silently and leave
         * the client to guess.
         */
        if (c->send_len > 0) {
            sqe = uring_get_sqe(&l->ring);
            if (!sqe)
                return;
            uring_prep_send(sqe, c->fd, send_buf[i], (uint32_t)c->send_len,
                            UD_MAKE(OP_SEND, i));
            c->send_pending = TRUE;
            continue;
        }

        if (c->closing) {
            slot_release(l, i);
            continue;
        }

        space = conn_recv_space(&c->conn);
        if (space <= 0) {
            /* Nothing was consumed and there is no room to read more. */
            c->closing = TRUE;
            slot_release(l, i);
            continue;
        }

        sqe = uring_get_sqe(&l->ring);
        if (!sqe)
            return;
        uring_prep_recv(sqe, c->fd, conn_recv_ptr(&c->conn), (uint32_t)space,
                        UD_MAKE(OP_RECV, i));
        c->recv_pending = TRUE;
    }
}

/* ---- public ---- */

result_t loop_init(loop_t *l, err_t *e, wal_t *wal,
                   session_table_t *sessions, uint16_t port,
                   uint32_t ring_entries)
{
    sockaddr_in_t addr;
    result_t      r;
    int32_t       one = 1;
    int32_t       i;

    mem_zero((uint8_t *)l, (int32_t)sizeof(*l));
    l->wal = wal;
    l->sessions = sessions;
    l->listen_fd = -1;
    l->signal_fd = -1;

    for (i = 0; i < LOOP_MAX_CONNS; i++)
        l->conns[i].fd = -1;

    r = uring_init(&l->ring, e, ring_entries);
    if (!result_ok(r))
        return r;

    r = os_socket(e, AF_INET, SOCK_STREAM, 0, &l->listen_fd);
    if (!result_ok(r)) {
        uring_destroy(&l->ring);
        return r;
    }

    os_setsockopt(e, l->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one,
                  (int32_t)sizeof(one));

    mem_zero((uint8_t *)&addr, (int32_t)sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr = htonl(0x7F000001);      /* loopback for now */

    r = os_bind(e, l->listen_fd, &addr);
    if (!result_ok(r))
        goto fail;

    r = os_listen(e, l->listen_fd, 128);
    if (!result_ok(r))
        goto fail;

    r = os_getsockname(e, l->listen_fd, &addr);
    if (!result_ok(r))
        goto fail;
    l->port = (int32_t)ntohs(addr.sin_port);

    return RESULT_OK;

fail:
    os_close(e, l->listen_fd);
    l->listen_fd = -1;
    uring_destroy(&l->ring);
    return r;
}

result_t loop_tick(loop_t *l, err_t *e, bool_t wait)
{
    tick_ctx_t tc;
    static uint8_t scratch[WAL_REC_MAX_SIZE];
    result_t   r;
    uint32_t   submitted = 0;
    int32_t    i;

    tc.l = l;
    tc.scratch = scratch;
    tc.scratch_len = (int32_t)sizeof(scratch);

    arm_accept(l);
    arm_signal(l);
    arm_conns(l);

    if (wait)
        r = uring_submit_and_wait(&l->ring, e, 1, &submitted);
    else
        r = uring_submit(&l->ring, e, &submitted);
    if (!result_ok(r))
        return r;

    l->wal_dirty = FALSE;
    uring_reap(&l->ring, tick_cb, &tc);

    /*
     * One flush for everything this pass appended. The batch is
     * whatever arrived while the previous flush was running, which is
     * the whole of the group-commit policy.
     *
     * Every append is flushed, including one whose sender asked for
     * nothing stronger than an append. Separating those out would save
     * nothing here, since the flush is already happening for the
     * others in the batch, and it means a weaker request is never
     * answered with a weaker guarantee than it actually got.
     */
    if (l->wal_dirty) {
        r = wal_sync(l->wal, e);
        if (!result_ok(r))
            return r;
        l->flushes++;
    }

    /* Only now is an acknowledgement true, so only now is it staged. */
    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        loop_conn_t *c = &l->conns[i];

        if (!c->in_use || !c->ack_due)
            continue;

        reply_ack(c, i, c->ack_flags, c->ack_wal_seq, c->ack_client_seq);
        c->ack_due = FALSE;
    }

    arm_accept(l);
    arm_signal(l);
    arm_conns(l);
    return uring_submit(&l->ring, e, &submitted);
}

void loop_set_signal_fd(loop_t *l, int32_t fd)
{
    l->signal_fd = fd;
    l->signal_pending = FALSE;
}

result_t loop_run(loop_t *l, err_t *e)
{
    while (!l->stop) {
        result_t r = loop_tick(l, e, TRUE);
        if (!result_ok(r))
            return r;
    }
    return RESULT_OK;
}

void loop_shutdown(loop_t *l)
{
    err_t   e;
    int32_t i;

    err_init(&e);
    l->stop = TRUE;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        if (l->conns[i].in_use && l->conns[i].fd >= 0) {
            os_close(&e, l->conns[i].fd);
            slot_reset(l, i);
        }
    }

    if (l->listen_fd >= 0) {
        os_close(&e, l->listen_fd);
        l->listen_fd = -1;
    }
    if (l->signal_fd >= 0) {
        os_close(&e, l->signal_fd);
        l->signal_fd = -1;
    }
    uring_destroy(&l->ring);
}

int32_t loop_live_conns(const loop_t *l)
{
    int32_t i;
    int32_t n = 0;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        if (l->conns[i].in_use)
            n++;
    }
    return n;
}

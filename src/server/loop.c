#include "server/loop.h"
#include "core/mem.h"
#include "core/fmt.h"
#include "sys/os.h"

/*
 * Event identity. The kind of descriptor is in the top byte and the
 * slot index in the low bits, so an event says what is ready and for
 * whom without any lookup table.
 *
 * The kind is the descriptor rather than the operation, because epoll
 * registers interest per descriptor and one report can carry both
 * directions at once. Which direction it is comes from the event bits,
 * not from the word.
 */
#define OP_LISTEN   1
#define OP_CONN     2
#define OP_SIGNAL   3
#define OP_TIMER    4
#define OP_STREAM   5
#define OP_METRICS  6
#define OP_SCRAPE   7

#define UD_MAKE(op, slot)   (((uint64_t)(op) << 56) | (uint64_t)(uint32_t)(slot))
#define UD_OP(ud)           ((int32_t)((ud) >> 56))
#define UD_SLOT(ud)         ((int32_t)((ud) & 0xFFFFFFFFULL))

/* Receive and send buffers, one pair per slot. */
static uint8_t recv_buf[LOOP_MAX_CONNS][LOOP_RECV_CAP];
static uint8_t send_buf[LOOP_MAX_CONNS][LOOP_SEND_CAP];

/*
 * A replica's slot sends from one of these instead of its own, since
 * what it carries is WAL records rather than replies.
 */
static uint8_t repl_send_buf[LOOP_MAX_REPLICAS][LOOP_REPL_CAP];

/* Follower side: the stream from the leader, and the acks going back. */
static uint8_t stream_recv_buf[LOOP_REPL_CAP];
static uint8_t stream_send_buf[MSG_HEADER_SIZE * 2];

/* Somewhere for the signalfd and timerfd reads to land. */
static uint8_t signal_buf[SIGNALFD_SIGINFO_SIZE];
static uint8_t timer_buf[TIMERFD_READ_SIZE];

/*
 * Scrape buffers. The request is read only to find where it ends, and
 * the response is built once and then sent from here across as many
 * passes as the socket needs.
 */
static uint8_t scrape_in[LOOP_MAX_SCRAPES][LOOP_SCRAPE_CAP];
static char    scrape_out[LOOP_MAX_SCRAPES][LOOP_METRICS_CAP];

/* Upper bounds of the flush latency buckets, in nanoseconds. */
static const uint64_t fsync_bound_ns[LOOP_FSYNC_BUCKETS] = {
    500000, 1000000, 2000000, 5000000, 10000000, 20000000, 50000000
};

/* The same bounds as Prometheus wants to read them, in seconds. */
static const char *const fsync_bound_text[LOOP_FSYNC_BUCKETS] = {
    "0.0005", "0.001", "0.002", "0.005", "0.01", "0.02", "0.05"
};

/*
 * Say something to the journal.
 *
 * The loop is otherwise silent, and for the ordinary path that is
 * right: nothing it does per record is worth a line. A replica that
 * the leader will not stream to is the exception. It retries for ever
 * and changes nothing, and without a word from it the only symptom is
 * a log that stops growing, which nobody is watching yet.
 */
static void say(const char *s)
{
    int32_t n = 0;

    while (s[n] != '\0')
        n++;
    os_write_raw(2, s, (size_t)n);
}

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
    c->replica = -1;
    c->send_buf = send_buf[i];
    c->send_cap = LOOP_SEND_CAP;
    wal_cursor_init(&c->sub.cursor);
}

/* Forget a replica the leader was streaming to. */
static void replica_release(loop_t *l, int32_t idx)
{
    err_t e;

    if (idx < 0 || idx >= LOOP_MAX_REPLICAS)
        return;

    err_init(&e);
    wal_cursor_close(&l->peer_cursor[idx], &e);
    l->peer_live[idx] = FALSE;
    l->peer_durable[idx] = 0;
    l->peer_ack_ns[idx] = 0;

    /*
     * Nothing to recompute: the mark only rises, and with a replica
     * gone the set is short, so it stays where it is until the replica
     * is back and has caught up past it.
     */
}

/*
 * Give up a slot. Only ever called from the arm phase, once the events
 * of this pass have all been dispatched: an event array still being
 * walked can name the slot, and handing it to another connection
 * underneath that would misdirect whatever the array had left.
 */
static void slot_release(loop_t *l, int32_t i)
{
    loop_conn_t *c = &l->conns[i];
    err_t        e;

    if (!c->in_use)
        return;

    if (c->replica >= 0)
        replica_release(l, c->replica);

    if (c->sub.cursor.fd >= 0) {
        err_t se;

        err_init(&se);
        wal_cursor_close(&c->sub.cursor, &se);
    }

    if (c->fd >= 0) {
        err_init(&e);
        /*
         * Closing the descriptor drops it from the epoll set on its
         * own. Saying so first costs one call and leaves the intent on
         * the page rather than in the kernel's semantics.
         */
        os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_DEL, c->fd, 0, 0);
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
    uint8_t     *out = c->send_buf + c->send_len;
    int32_t      room = c->send_cap - c->send_len;

    (void)slot;

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

/*
 * A record on its way to a subscriber. Unlike every other reply this
 * one carries the record's own key and type, since a subscriber is
 * being told about a record rather than about its own request.
 */
static bool_t reply_record(loop_conn_t *c, const wal_rec_t *r,
                           const uint8_t *payload)
{
    msg_header_t h;
    uint8_t     *out = c->send_buf + c->send_len;
    int32_t      room = c->send_cap - c->send_len;
    int32_t      len = (int32_t)r->len;

    if (room < MSG_HEADER_SIZE + len)
        return FALSE;

    h = msg_header_new(MSG_OP_NOTIFY, r->record_type, 0, (uint32_t)len,
                       r->partition_key, r->seq);
    msg_encode_header(out, room, &h);
    if (len > 0)
        mem_copy(out + MSG_HEADER_SIZE, payload, len);

    c->send_len += MSG_HEADER_SIZE + len;
    return TRUE;
}

static int32_t text_len(const char *s)
{
    int32_t n = 0;

    while (s && s[n] != '\0')
        n++;
    return n;
}

/*
 * An ERR with optional text. Only NOT_LEADER carries any: the address
 * to go to instead, which is the one error a client can act on without
 * a human.
 */
/*
 * Which bucket a refusal is counted in.
 *
 * Grouped by what an operator would do about it. The four that name a
 * state of the node are worth watching on their own; everything else
 * is a client sending something the server will not take, which is one
 * number until it is large enough to go looking.
 */
static int32_t refused_kind(uint16_t code)
{
    switch (code) {
    case MSG_ERR_NOT_LEADER:    return LOOP_REFUSED_NOT_LEADER;
    case MSG_ERR_NO_REPLICAS:   return LOOP_REFUSED_NO_REPLICAS;
    case MSG_ERR_BEHIND:        return LOOP_REFUSED_BEHIND;
    case MSG_ERR_NO_HISTORY:    return LOOP_REFUSED_NO_HISTORY;
    case MSG_ERR_STORAGE:       return LOOP_REFUSED_STORAGE;
    default:                    return LOOP_REFUSED_OTHER;
    }
}

static bool_t reply_err_text(loop_t *l, loop_conn_t *c, int32_t slot,
                             uint16_t code, uint64_t sequence,
                             const char *text)
{
    msg_err_payload_t p;
    uint8_t           buf[MSG_ERR_SIZE + MSG_MAX_ERR_TEXT];
    int32_t           n = text_len(text);

    if (n > MSG_MAX_ERR_TEXT)
        n = MSG_MAX_ERR_TEXT;

    l->refused[refused_kind(code)]++;

    p.code = code;
    p.text_len = (uint16_t)n;
    p._pad = 0;
    msg_encode_err(buf, MSG_ERR_SIZE, &p);
    if (n > 0)
        mem_copy(buf + MSG_ERR_SIZE, (const uint8_t *)text, n);

    return reply(c, slot, MSG_OP_ERR, 0, sequence, buf, MSG_ERR_SIZE + n);
}

static bool_t reply_err(loop_t *l, loop_conn_t *c, int32_t slot,
                       uint16_t code, uint64_t sequence)
{
    return reply_err_text(l, c, slot, code, sequence, NULL);
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
        reply_err(l, c, slot, MSG_ERR_PAYLOAD_TOO_BIG, h->sequence);
        c->closing = TRUE;
        return;
    }

    if (hello.session == 0) {
        s = session_create(l->sessions);
        if (!s) {
            reply_err(l, c, slot, MSG_ERR_SESSION_UNKNOWN, h->sequence);
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
            reply_err(l, c, slot, MSG_ERR_SESSION_UNKNOWN, h->sequence);
            return;
        }
    }

    c->session = s->id;
    reply_ack(c, slot, MSG_FLAG_ACK_REQ, s->id, s->last_client_seq);
}

/* ---- acknowledgements ---- */

static void ack_clear(loop_ack_t *a)
{
    a->due = FALSE;
    a->degradable = FALSE;
    a->flags = 0;
    a->client_seq = 0;
    a->wal_seq = 0;
    a->deadline = 0;
}

/*
 * Record that an acknowledgement is owed.
 *
 * One slot serves any number of records because the acknowledgement is
 * cumulative: the highest sequence waiting is the only one that has to
 * be remembered. The flags accumulate rather than replace, since every
 * record in the slot is covered by whatever satisfies the strongest of
 * them, and a client waiting on the stronger level would otherwise be
 * left waiting by an acknowledgement that named the weaker one.
 *
 * The deadline belongs to the first record to wait, not the last. A
 * later record extending it would let a steady stream of writes keep a
 * stalled one waiting for ever.
 */
static void ack_stage(loop_ack_t *a, uint16_t flags, bool_t degradable,
                      uint64_t client_seq, uint64_t wal_seq,
                      uint32_t deadline)
{
    if (!a->due) {
        a->due = TRUE;
        a->deadline = deadline;
    }
    a->flags = (uint16_t)(a->flags | flags);
    if (degradable)
        a->degradable = TRUE;
    if (client_seq > a->client_seq)
        a->client_seq = client_seq;
    if (wal_seq > a->wal_seq)
        a->wal_seq = wal_seq;
}

/* Replicas attached and streaming. */
static bool_t repl_live(const loop_t *l)
{
    int32_t i;

    for (i = 0; i < LOOP_MAX_REPLICAS; i++) {
        if (l->peer_live[i])
            return TRUE;
    }
    return FALSE;
}

/*
 * The highest sequence any one replica has made durable.
 *
 * ANY 1 of the followers, so the best of them is what the rule needs:
 * a record is on two nodes as soon as the fastest replica has it, and
 * which one that is changes with whichever flush lands first.
 */
static uint64_t repl_best(const loop_t *l)
{
    uint64_t best = 0;
    int32_t  i;

    for (i = 0; i < LOOP_MAX_REPLICAS; i++) {
        if (l->peer_live[i] && l->peer_durable[i] > best)
            best = l->peer_durable[i];
    }
    return best;
}

/*
 * The sequence retention is allowed to delete up to.
 *
 * Every replica the node expects has to be attached and to have
 * confirmed the mark, so one that is away holds the log where it is
 * rather than letting the leader delete what that replica has not got.
 * A replica asking to stream from below the retained log cannot be
 * repaired by streaming, so the cost of being wrong here is a re-seed
 * by hand and the cost of being slow is disk.
 *
 * The mark only rises. A replica dropping does not un-confirm what it
 * already had; it stops the mark moving on.
 *
 * Marks cannot be remembered per slot instead. Replicas have no
 * identity on the wire and take whichever slot is free, so one coming
 * back into the other's slot would inherit a promise it never made.
 * Requiring the whole set to be present is what makes the mark safe
 * without anyone having a name.
 */
static void retain_floor_update(loop_t *l)
{
    uint64_t lowest = 0;
    int32_t  live = 0;
    int32_t  i;

    if (l->replicas_want <= 0)
        return;

    for (i = 0; i < LOOP_MAX_REPLICAS; i++) {
        if (!l->peer_live[i])
            continue;
        if (live == 0 || l->peer_durable[i] < lowest)
            lowest = l->peer_durable[i];
        live++;
    }

    if (live < l->replicas_want)
        return;
    if (lowest > l->retain_floor)
        l->retain_floor = lowest;
}

/*
 * Route an owed acknowledgement to the slot that can satisfy it.
 *
 * A write that asked only for local durability is answered by the
 * flush this pass is about to do. One that asked for a second copy
 * waits on a replica instead, unless there is no replica to wait for
 * and the writer said it would take one copy, in which case it is
 * answered by the same flush and told the copy is not there.
 */
static void ack_write(loop_t *l, loop_conn_t *c, const msg_header_t *h,
                      uint16_t ack_flags, bool_t wants_repl,
                      bool_t degradable, uint64_t client_seq,
                      uint64_t wal_seq)
{
    (void)h;

    if (!wants_repl) {
        ack_stage(&c->ack_local, ack_flags, FALSE, client_seq, wal_seq, 0);
        return;
    }

    if (!repl_live(l)) {
        ack_stage(&c->ack_local, (uint16_t)(ack_flags | MSG_FLAG_DEGRADED),
                  TRUE, client_seq, wal_seq, 0);
        l->repl_degraded++;
        return;
    }

    ack_stage(&c->ack_repl, ack_flags, degradable, client_seq, wal_seq,
              l->tick + LOOP_REPL_DEADLINE_TICKS);
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
    bool_t          wants_repl;
    bool_t          degradable;

    if (!s) {
        reply_err(l, c, slot, MSG_ERR_NO_SESSION, h->sequence);
        c->closing = TRUE;
        return;
    }

    /*
     * A follower's log is the leader's, arriving over the stream. A
     * record appended here would sit at a sequence the stream is about
     * to claim, so the client is sent to the leader instead.
     */
    if (l->is_follower) {
        reply_err_text(l, c, slot, MSG_ERR_NOT_LEADER, h->sequence,
                       l->leader_text);
        return;
    }

    wants_repl = (h->flags & MSG_FLAG_REPLICATED) ? TRUE : FALSE;
    degradable = (h->flags & MSG_FLAG_ALLOW_DEGRADED) ? TRUE : FALSE;

    /*
     * With no replica attached there is nothing to wait for, so the
     * answer is immediate either way: the write is refused, or it is
     * taken and told plainly that it has one copy. Waiting out the
     * deadline first would only delay the same answer.
     */
    if (wants_repl && !repl_live(l) && !degradable) {
        reply_err(l, c, slot, MSG_ERR_NO_REPLICAS, h->sequence);
        return;
    }

    ack_flags = (uint16_t)(h->flags &
                           (MSG_FLAG_ACK_REQ | MSG_FLAG_SYNC |
                            MSG_FLAG_REPLICATED));

    /*
     * A resend of something already durable is answered, not appended
     * again. This is what the client-assigned sequence is for: a retry
     * after a reconnect repeats whatever it could not confirm.
     */
    if (h->sequence <= s->last_client_seq) {
        l->dedup_hits++;
        if (h->flags & MSG_FLAG_ACK_REQ)
            ack_write(l, c, h, ack_flags, wants_repl, degradable,
                      s->last_client_seq, wal_durable_seq(l->wal));
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
    {
        result_t r = wal_append(l->wal, &e, &rec, payload, scratch,
                                scratch_len);

        if (!result_ok(r)) {
            /*
             * A record too large for an empty segment is the client's
             * to fix. Anything else is the log's own trouble, a full
             * disk above all, and saying so is what points at the node
             * rather than at the sender.
             */
            reply_err(l, c, slot,
                      r.code == ERR_INVALID ? MSG_ERR_PAYLOAD_TOO_BIG
                                            : MSG_ERR_STORAGE,
                      h->sequence);
            return;
        }
    }

    l->writes++;
    l->wal_dirty = TRUE;
    s->last_client_seq = h->sequence;
    session_touch(l->sessions, s);

    if (h->flags & MSG_FLAG_ACK_REQ)
        ack_write(l, c, h, ack_flags, wants_repl, degradable, h->sequence,
                  rec.seq);
}

/* ---- subscriptions ---- */

static void arm_timer_now(loop_t *l);

/*
 * Start following the log.
 *
 * from_seq 0 means live only, which is the sequence the log will next
 * assign; anything else replays from there, and a client that
 * reconnects names the record after the last one it handled.
 *
 * min_seq holds the stream back until this node has flushed that far.
 * That is read-your-writes across nodes: a client that wrote to the
 * leader and subscribes here passes the sequence its acknowledgement
 * named, and is not shown a view older than its own write. The wait is
 * bounded, and running out is an answer rather than a silence.
 */
static void handle_subscribe(loop_t *l, int32_t slot, const msg_header_t *h,
                             const uint8_t *payload, int32_t payload_len,
                             uint8_t *scratch, int32_t scratch_len)
{
    loop_conn_t     *c = &l->conns[slot];
    msg_subscribe_t  req;
    uint64_t         from;
    err_t            e;

    if (!msg_decode_subscribe(payload, payload_len, &req)) {
        reply_err(l, c, slot, MSG_ERR_PAYLOAD_TOO_BIG, h->sequence);
        c->closing = TRUE;
        return;
    }

    if (c->sub.active || c->sub.waiting) {
        /* One stream per connection. A second would need its own
         * cursor and its own place in the send buffer. */
        reply_err(l, c, slot, MSG_ERR_UNSUPPORTED, h->sequence);
        return;
    }

    from = (req.from_seq == 0) ? wal_next_seq(l->wal) : req.from_seq;

    /*
     * A replay asking from further back than the key has ever existed
     * starts where it does exist instead. Only the front of the walk
     * can be skipped this way: where the key's next record falls after
     * that point is not something a first-and-last index knows.
     */
    if (!(h->flags & MSG_FLAG_ALL_KEYS)) {
        uint64_t key_from;
        uint64_t key_last;

        if (wal_index_range(wal_index(l->wal), h->partition_key, from,
                            &key_from, &key_last))
            from = key_from;
    }

    err_init(&e);
    wal_cursor_init(&c->sub.cursor);
    if (!result_ok(wal_cursor_seek(l->wal, &e, &c->sub.cursor, from, scratch,
                                   scratch_len))) {
        /* Older than this node retains, or past the end of its log. */
        reply_err(l, c, slot, MSG_ERR_NO_HISTORY, h->sequence);
        return;
    }

    c->sub.key = h->partition_key;
    c->sub.all_keys = (h->flags & MSG_FLAG_ALL_KEYS) ? TRUE : FALSE;
    c->sub.min_seq = req.min_seq;

    if (req.min_seq > wal_durable_seq(l->wal)) {
        c->sub.waiting = TRUE;
        c->sub.deadline = l->tick + LOOP_SUB_DEADLINE_TICKS;
        arm_timer_now(l);
        return;         /* answered when it catches up, or gives up */
    }

    c->sub.active = TRUE;
    reply(c, slot, MSG_OP_ACK, 0, from, NULL, 0);
}

/*
 * Read what this node holds for a key, and stop.
 *
 * The same machinery as a subscription, bounded at both ends: it
 * starts at the oldest record still on disk and ends at whatever was
 * durable when the request arrived, so a read is a view of the log at
 * a moment rather than a stream that never finishes. The end is marked
 * with an empty NOTIFY carrying LAST, which is what that flag is for.
 */
static void handle_read(loop_t *l, int32_t slot, const msg_header_t *h,
                        const uint8_t *payload, int32_t payload_len,
                        uint8_t *scratch, int32_t scratch_len)
{
    loop_conn_t *c = &l->conns[slot];
    msg_read_t   req;
    uint64_t     from;
    err_t        e;

    if (!msg_decode_read(payload, payload_len, &req)) {
        reply_err(l, c, slot, MSG_ERR_PAYLOAD_TOO_BIG, h->sequence);
        c->closing = TRUE;
        return;
    }

    if (c->sub.active || c->sub.waiting) {
        reply_err(l, c, slot, MSG_ERR_UNSUPPORTED, h->sequence);
        return;
    }

    c->sub.key = h->partition_key;
    c->sub.all_keys = (h->flags & MSG_FLAG_ALL_KEYS) ? TRUE : FALSE;
    c->sub.min_seq = req.min_seq;
    c->sub.once = TRUE;
    c->sub.until = wal_durable_seq(l->wal);
    from = wal_first_seq(l->wal);

    /*
     * What the log knows about where this key lives. It bounds the
     * walk at both ends: nothing of the key exists before its first
     * record or after its last, so the records in between are the only
     * ones worth reading. A key the index has never heard of is read
     * the long way, which is also how a key that genuinely is not
     * there answers with nothing.
     */
    if (!c->sub.all_keys) {
        uint64_t key_from;
        uint64_t key_last;

        if (wal_index_range(wal_index(l->wal), c->sub.key, from, &key_from,
                            &key_last)) {
            from = key_from;
            if (key_last < c->sub.until)
                c->sub.until = key_last;
        } else if (wal_index_complete(wal_index(l->wal))) {
            /*
             * The index has seen every key in this log and has never
             * seen this one, so there is nothing to read. Answered
             * without opening the log at all: the push below finds a
             * cursor already past its end and says so.
             */
            c->sub.until = 0;
            c->sub.cursor.next_seq = 1;
            c->sub.active = TRUE;
            reply(c, slot, MSG_OP_ACK, 0, 0, NULL, 0);
            return;
        }
    }

    err_init(&e);
    wal_cursor_init(&c->sub.cursor);
    if (!result_ok(wal_cursor_seek(l->wal, &e, &c->sub.cursor, from, scratch,
                                   scratch_len))) {
        reply_err(l, c, slot, MSG_ERR_NO_HISTORY, h->sequence);
        return;
    }

    if (req.min_seq > c->sub.until) {
        c->sub.waiting = TRUE;
        c->sub.deadline = l->tick + LOOP_SUB_DEADLINE_TICKS;
        arm_timer_now(l);
        return;
    }

    c->sub.active = TRUE;
    reply(c, slot, MSG_OP_ACK, 0, c->sub.cursor.next_seq, NULL, 0);
}

/*
 * Push what has been flushed since the last pass to every subscriber.
 *
 * The read is bounded by the room left in the connection's send
 * buffer, which is what keeps a slow subscriber from needing the
 * cursor rewound: a record on the wire is smaller than the same record
 * on disk, since the frame header is 32 bytes against the record's 56
 * and the disk copy is padded. Whatever was read therefore fits, and
 * the cursor never moves past what was sent.
 */
static void subs_push(loop_t *l, uint8_t *scratch, int32_t scratch_len)
{
    uint64_t durable = wal_durable_seq(l->wal);
    int32_t  i;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        loop_conn_t *c = &l->conns[i];
        int32_t      room;
        int32_t      got = 0;
        int32_t      at = 0;
        err_t        e;

        if (!c->in_use || c->closing)
            continue;

        /* A stream held for min_seq starts once the node reaches it,
         * and is refused once it is clear it will not in time. */
        if (c->sub.waiting) {
            if (durable >= c->sub.min_seq) {
                c->sub.waiting = FALSE;
                c->sub.active = TRUE;
                /* A read that waited sees the log as it is now, not as
                 * it was when the request arrived and was short. */
                if (c->sub.once)
                    c->sub.until = durable;
                reply(c, i, MSG_OP_ACK, 0, c->sub.cursor.next_seq, NULL, 0);
                l->progress = TRUE;
            } else if (l->tick >= c->sub.deadline) {
                c->sub.waiting = FALSE;
                reply_err(l, c, i, MSG_ERR_BEHIND, c->sub.min_seq);
                err_init(&e);
                wal_cursor_close(&c->sub.cursor, &e);
            }
            continue;
        }

        if (!c->sub.active || c->want_out)
            continue;

        room = c->send_cap - c->send_len;
        if (room < MSG_HEADER_SIZE)
            continue;               /* nothing would fit; try next pass */
        if (room > scratch_len)
            room = scratch_len;

        if (c->sub.once) {
            if (c->sub.cursor.next_seq > c->sub.until) {
                /* Nothing more belongs to this read. */
                reply(c, i, MSG_OP_NOTIFY, MSG_FLAG_LAST, c->sub.until,
                      NULL, 0);
                c->sub.active = FALSE;
                err_init(&e);
                wal_cursor_close(&c->sub.cursor, &e);
                continue;
            }
            if (durable > c->sub.until)
                durable = c->sub.until;
        }

        err_init(&e);
        if (!result_ok(wal_cursor_read(l->wal, &e, &c->sub.cursor, durable,
                                       scratch, room, &got))) {
            /*
             * Retention has taken what this stream was reading, or the
             * log cannot be read at all. Either way the client is told
             * on the connection it was using rather than left to find
             * out by reconnecting and being refused there.
             */
            reply_err(l, c, i, MSG_ERR_NO_HISTORY, c->sub.cursor.next_seq);
            c->closing = TRUE;
            continue;
        }

        /*
         * The cursor moved, so there may be more behind what it took,
         * and the frame that ends a read is itself a further pass. A
         * pass that read nothing sets nothing and the loop goes back
         * to waiting, which is what keeps this from spinning on a
         * record too large for the buffer.
         */
        if (got > 0)
            l->progress = TRUE;

        while (at < got) {
            wal_rec_t rec;
            int32_t   size;

            if (!wal_rec_decode(scratch + at, got - at, &rec))
                break;
            size = wal_rec_size((int32_t)rec.len);
            if (size == 0 || at + size > got)
                break;

            if (c->sub.all_keys || rec.partition_key == c->sub.key) {
                reply_record(c, &rec, scratch + at + WAL_REC_HEADER_SIZE);
                l->notified++;
            }
            at += size;
        }
    }
}

/* ---- replication: the leader's side ---- */

/*
 * A replica has named the sequence it wants. Give it a slot, a cursor
 * into the log, and the larger send buffer its stream needs.
 *
 * Nothing has been staged on this connection: REPL_START is its first
 * frame and it is answered with the stream itself rather than a reply,
 * so moving the buffer here cannot lose bytes.
 */
static void handle_repl_start(loop_t *l, int32_t slot, const msg_header_t *h,
                              uint8_t *scratch, int32_t scratch_len)
{
    loop_conn_t *c = &l->conns[slot];
    err_t        e;
    int32_t      idx = -1;
    int32_t      i;

    if (l->is_follower) {
        /*
         * A follower does not own the log it holds, so it cannot serve
         * a stream from it. Saying where the leader is turns a
         * misconfigured replica into one that finds its way.
         */
        reply_err_text(l, c, slot, MSG_ERR_NOT_LEADER, h->sequence,
                       l->leader_text);
        c->closing = TRUE;
        return;
    }

    for (i = 0; i < LOOP_MAX_REPLICAS; i++) {
        if (!l->peer_live[i]) {
            idx = i;
            break;
        }
    }
    if (idx < 0) {
        reply_err(l, c, slot, MSG_ERR_NO_REPLICAS, h->sequence);
        c->closing = TRUE;
        return;
    }

    err_init(&e);
    wal_cursor_init(&l->peer_cursor[idx]);
    if (!result_ok(wal_cursor_seek(l->wal, &e, &l->peer_cursor[idx],
                                   h->sequence, scratch, scratch_len))) {
        /*
         * Either older than this log retains or past its end. Neither
         * is something a stream can repair: the replica needs a copy
         * of the segments, which is an operator's job.
         */
        reply_err(l, c, slot, MSG_ERR_NO_HISTORY, h->sequence);
        c->closing = TRUE;
        return;
    }

    c->replica = idx;
    c->send_buf = repl_send_buf[idx];
    c->send_cap = LOOP_REPL_CAP;
    c->send_len = 0;
    l->peer_live[idx] = TRUE;
    l->peer_durable[idx] = h->sequence - 1;
    l->peer_ack_ns[idx] = os_now_ns();
    retain_floor_update(l);
    arm_timer_now(l);
}

/*
 * Fill each replica's buffer with whatever it has not been sent.
 *
 * Only durable records go out. A replica that confirmed a record the
 * leader had not yet flushed would let a write count two copies when
 * one of them could still go away.
 */
static void repl_stream(loop_t *l)
{
    uint64_t limit = wal_durable_seq(l->wal);
    int32_t  i;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        loop_conn_t *c = &l->conns[i];
        int32_t      idx = c->replica;
        int32_t      n = 0;
        uint64_t     first;
        err_t        e;

        if (!c->in_use || c->closing || idx < 0)
            continue;
        if (c->send_len > 0)
            continue;               /* the last chunk has not gone yet */

        first = l->peer_cursor[idx].next_seq;
        err_init(&e);
        if (!result_ok(wal_cursor_read(l->wal, &e, &l->peer_cursor[idx],
                                       limit, c->send_buf + MSG_HEADER_SIZE,
                                       c->send_cap - MSG_HEADER_SIZE, &n))) {
            c->closing = TRUE;
            continue;
        }
        if (n == 0)
            continue;
        l->progress = TRUE;

        {
            msg_header_t h = msg_header_new(MSG_OP_REPL_DATA, MSG_RECORD_NONE,
                                            0, (uint32_t)n, 0, first);

            msg_encode_header(c->send_buf, c->send_cap, &h);
            c->send_len = MSG_HEADER_SIZE + n;
        }
        l->repl_records += l->peer_cursor[idx].next_seq - first;
    }
}

/* ---- replication: the follower's side ---- */

/* Stage one header-only frame for the leader. */
static void stream_frame(loop_t *l, uint16_t op, uint64_t sequence)
{
    msg_header_t h;
    int32_t      room = (int32_t)sizeof(stream_send_buf) - l->repl_send_len;

    if (room < MSG_HEADER_SIZE)
        return;

    h = msg_header_new(op, MSG_RECORD_NONE, 0, 0, 0, sequence);
    msg_encode_header(stream_send_buf + l->repl_send_len, room, &h);
    l->repl_send_len += MSG_HEADER_SIZE;
}

/* Give up the stream. Called from the arm phase, like slot_release. */
static void stream_release(loop_t *l)
{
    err_t e;

    if (l->repl_fd < 0 || l->repl_connecting)
        return;

    err_init(&e);
    os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_DEL, l->repl_fd, 0, 0);
    os_close(&e, l->repl_fd);
    l->repl_fd = -1;
    l->repl_closing = FALSE;
    l->repl_want_out = FALSE;
    l->repl_send_len = 0;
    l->repl_next_dial = l->tick + LOOP_REDIAL_TICKS;
}

/*
 * Apply what the leader sent.
 *
 * The checksum and the sequence are both checked before anything is
 * written. The checksum is the leader's, computed over the same fields
 * this node will write, so a record that survives the network intact
 * is stored with the identical bytes; and the sequence has to be the
 * next one this log expects, since a replica that has drifted cannot
 * be repaired by writing what it was sent.
 */
static void repl_apply(loop_t *l, const uint8_t *p, int32_t n,
                       uint8_t *scratch, int32_t scratch_len)
{
    err_t   e;
    int32_t at = 0;

    err_init(&e);

    while (at < n) {
        wal_rec_t rec;
        int32_t   size;

        if (!wal_rec_decode(p + at, n - at, &rec)) {
            l->repl_closing = TRUE;
            return;
        }

        size = wal_rec_size((int32_t)rec.len);
        if (size == 0 || at + size > n ||
            !wal_rec_verify(p + at, n - at, &rec) ||
            rec.seq != wal_next_seq(l->wal)) {
            l->repl_closing = TRUE;
            return;
        }

        if (!result_ok(wal_append_at(l->wal, &e, &rec,
                                     p + at + WAL_REC_HEADER_SIZE, scratch,
                                     scratch_len))) {
            l->repl_closing = TRUE;
            return;
        }

        /*
         * The session table is fed here as well as by recovery, so a
         * follower that is promoted knows what its clients had already
         * made durable and does not hand out an id the log has used.
         */
        session_from_record(l->sessions, &rec);

        /* Streaming again, so a refusal after this is news. */
        l->repl_last_err = 0;
        l->wal_dirty = TRUE;
        l->repl_records++;
        at += size;
    }
}

static void on_stream_frame(loop_t *l, const conn_action_t *a,
                            uint8_t *scratch, int32_t scratch_len)
{
    const msg_header_t *h = &a->u.frame.header;

    switch (h->op) {
    case MSG_OP_REPL_DATA:
        repl_apply(l, a->u.frame.payload, a->u.frame.payload_len, scratch,
                   scratch_len);
        break;
    case MSG_OP_ERR: {
        /*
         * The leader refused the stream. Dialling again is all this
         * node can do about it, so it keeps doing that; saying so once
         * is what turns a silent retry loop into something an operator
         * can act on. Once, not once per attempt: the same refusal
         * every half second would bury the journal.
         */
        uint16_t code = 0;
        msg_err_payload_t p;

        if (msg_decode_err(a->u.frame.payload, a->u.frame.payload_len, &p))
            code = p.code;

        if (code != l->repl_last_err) {
            l->repl_last_err = code;
            switch (code) {
            case MSG_ERR_NO_HISTORY:
                say("msgsrvd: the leader does not hold the sequence this "
                    "node asked for.\nIts log has to be replaced with a "
                    "copy of the leader's; streaming cannot repair it.\n");
                break;
            case MSG_ERR_NOT_LEADER:
                say("msgsrvd: the node named as leader is a replica "
                    "itself.\n");
                break;
            case MSG_ERR_NO_REPLICAS:
                say("msgsrvd: the leader has no room for another "
                    "replica.\n");
                break;
            default:
                say("msgsrvd: the leader refused the replication "
                    "stream.\n");
                break;
            }
        }
        l->repl_closing = TRUE;
        break;
    }
    default:
        l->repl_closing = TRUE;
        break;
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
    case MSG_OP_REPL_START:
        handle_repl_start(l, slot, h, scratch, scratch_len);
        break;
    case MSG_OP_REPL_ACK:
        /*
         * What a replica has made durable. The frame carries nothing
         * else: the leader knows what it sent, and the replica only
         * has to say how far it got.
         */
        if (c->replica >= 0) {
            l->peer_durable[c->replica] = h->sequence;
            l->peer_ack_ns[c->replica] = os_now_ns();
            retain_floor_update(l);
        }
        break;
    case MSG_OP_SUBSCRIBE:
        handle_subscribe(l, slot, h, a->u.frame.payload,
                         a->u.frame.payload_len, scratch, scratch_len);
        break;
    case MSG_OP_READ:
        handle_read(l, slot, h, a->u.frame.payload, a->u.frame.payload_len,
                    scratch, scratch_len);
        break;
    case MSG_OP_DELETE:
        reply_err(l, c, slot, MSG_ERR_UNSUPPORTED, h->sequence);
        break;
    default:
        /* A server-to-client verb arriving from a client is nonsense. */
        reply_err(l, c, slot, MSG_ERR_BAD_OP, h->sequence);
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

    if (res < 0)
        return;

    err_init(&e);

    slot = slot_alloc(l);
    if (slot < 0) {
        /* No room: refuse now rather than hold a connection we cannot serve */
        os_close(&e, res);
        return;
    }

    /*
     * Readable only. A socket with room to write is nearly always
     * writable, so a slot that carried EPOLLOUT from the start would
     * wake the loop every pass with nothing to say.
     */
    if (!result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_ADD, res, EPOLLIN,
                                UD_MAKE(OP_CONN, slot)))) {
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

    if (!c->in_use)
        return;

    /* 0 is an orderly close, negative is an error; both end the connection */
    if (res <= 0) {
        c->closing = TRUE;
        return;
    }

    /* The recv read straight into the buffer, so this only accounts. */
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
                reply_err(l, c, slot, actions[i].u.reply_err.code,
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

void loop_send_done(loop_t *l, int32_t slot, int32_t res)
{
    loop_conn_t *c = &l->conns[slot];

    if (!c->in_use)
        return;

    /*
     * A send that failed takes what was staged with it. Keeping those
     * bytes would have the next pass submit the same doomed write, and
     * a slot with a write in flight is never released, so the
     * connection would sit there for ever holding what it owns. For a
     * replica that is one of the leader's two slots, and the node it
     * belonged to could never rejoin: it would be told there was no
     * room, by a leader keeping room for a peer that had gone.
     *
     * Zero counts as failure for the same reason. A send that moves
     * nothing, repeated, is the same loop.
     */
    if (res <= 0) {
        c->closing = TRUE;
        c->send_len = 0;
        return;
    }

    /*
     * A short write leaves the rest staged; the remaining bytes move
     * to the front and go out on the next pass. The buffer is the
     * slot's own, which for a replica is not the one the slot number
     * indexes: its stream is carried in a larger buffer of its own.
     */
    if (res < c->send_len) {
        mem_copy(c->send_buf, c->send_buf + res, c->send_len - res);
        c->send_len -= res;
    } else {
        c->send_len = 0;
    }
}

static void on_repl_dial(loop_t *l, int32_t res)
{
    err_t e;

    l->repl_connecting = FALSE;
    err_init(&e);

    if (res < 0) {
        if (l->repl_fd >= 0) {
            os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_DEL, l->repl_fd, 0, 0);
            os_close(&e, l->repl_fd);
        }
        l->repl_fd = -1;
        l->repl_next_dial = l->tick + LOOP_REDIAL_TICKS;
        return;
    }

    /*
     * The dial was watched for writability, which is how a connect
     * that was still in progress reports itself. Connected, the stream
     * wants to hear about records arriving instead.
     */
    l->repl_want_out = FALSE;
    if (!result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_MOD, l->repl_fd,
                                EPOLLIN, UD_MAKE(OP_STREAM, 0)))) {
        l->repl_closing = TRUE;
        return;
    }

    conn_init(&l->repl_conn, stream_recv_buf, LOOP_REPL_CAP,
              CONN_MODE_INTERNAL);
    l->repl_send_len = 0;
    l->repl_closing = FALSE;

    /*
     * The first sequence this node does not hold. Asking from the next
     * one rather than from the last durable one is what keeps the
     * stream free of records it would only discard.
     */
    l->repl_acked = wal_durable_seq(l->wal);
    stream_frame(l, MSG_OP_REPL_START, wal_next_seq(l->wal));
}

static void on_repl_recv(loop_t *l, int32_t res, uint8_t *scratch,
                         int32_t scratch_len)
{
    conn_action_t actions[4];
    int32_t       n;
    int32_t       i;

    if (l->repl_fd < 0)
        return;
    if (res <= 0) {
        l->repl_closing = TRUE;
        return;
    }

    conn_recv_commit(&l->repl_conn, res);

    for (;;) {
        n = conn_feed(&l->repl_conn, actions, 4);
        if (n == 0)
            break;

        for (i = 0; i < n; i++) {
            if (actions[i].type == CONN_ACTION_FRAME)
                on_stream_frame(l, &actions[i], scratch, scratch_len);
            else
                l->repl_closing = TRUE;
        }

        if (l->repl_closing)
            break;
    }
}

static void on_repl_send(loop_t *l, int32_t res)
{
    if (l->repl_fd < 0)
        return;
    if (res < 0) {
        l->repl_closing = TRUE;
        return;
    }

    if (res < l->repl_send_len) {
        mem_copy(stream_send_buf, stream_send_buf + res,
                 l->repl_send_len - res);
        l->repl_send_len -= res;
    } else {
        l->repl_send_len = 0;
    }
}

/* ---- interest ---- */

/*
 * Carry EPOLLOUT for a slot, or stop carrying it.
 *
 * A socket with room in its send buffer is writable, which is nearly
 * always, so a slot registered for EPOLLOUT with nothing staged would
 * wake the loop on every pass to be told what it already knew. The
 * interest goes on only when a send could not take everything, and
 * comes off as soon as it has.
 */
static void conn_want_out(loop_t *l, int32_t i, bool_t want)
{
    loop_conn_t *c = &l->conns[i];
    uint32_t     events = EPOLLIN;
    err_t        e;

    if (c->fd < 0 || (c->want_out ? TRUE : FALSE) == want)
        return;

    if (want)
        events = EPOLLIN | EPOLLOUT;

    err_init(&e);
    if (!result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_MOD, c->fd,
                                events, UD_MAKE(OP_CONN, i)))) {
        /*
         * Nothing left to try. Without the change the staged bytes
         * either never go out or wake the loop for ever, and ending
         * the connection at least says so to the client.
         */
        c->closing = TRUE;
        return;
    }
    c->want_out = want;
}

/* The same for the follower's one stream. */
static void stream_want_out(loop_t *l, bool_t want)
{
    uint32_t events = EPOLLIN;
    err_t    e;

    if (l->repl_fd < 0 || (l->repl_want_out ? TRUE : FALSE) == want)
        return;

    if (want)
        events = EPOLLIN | EPOLLOUT;

    err_init(&e);
    if (!result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_MOD, l->repl_fd,
                                events, UD_MAKE(OP_STREAM, 0)))) {
        l->repl_closing = TRUE;
        return;
    }
    l->repl_want_out = want;
}

/* ---- doing what an event says is possible ---- */

/*
 * epoll reports readiness; the ring reported completions. What sits
 * between them is this: each of these takes the syscall the event says
 * will not block, and hands its result to the handler above unchanged.
 * The handlers therefore never learn which multiplexer they are under.
 */

static void do_accept(loop_t *l)
{
    err_t   e;
    int32_t fd = -1;

    if (l->stop)
        return;

    /*
     * One accept per readiness report, the way the ring did one per
     * completion. Draining the backlog here would let a burst of
     * connections put off the flush the records already taken are
     * waiting for.
     */
    err_init(&e);
    if (!result_ok(os_accept4(&e, l->listen_fd, SOCK_NONBLOCK | SOCK_CLOEXEC,
                              &fd)))
        return;

    on_accept(l, fd);
}

/*
 * One receive per connection per pass, for the room the framing layer
 * has. Reading until EAGAIN would let one fast client fill the batch
 * on its own and push out the flush every other connection is waiting
 * for, which is the shape the group commit argument rests on.
 */
static void do_recv(tick_ctx_t *tc, int32_t slot)
{
    loop_conn_t *c = &tc->l->conns[slot];
    int32_t      space;
    int32_t      res = 0;

    if (!c->in_use || c->fd < 0)
        return;

    /*
     * The arm phase reclaimed the parsed bytes and closed the
     * connection if there was no room left, so a slot arriving here
     * with none has already been dealt with.
     */
    space = conn_recv_space(&c->conn);
    if (space <= 0)
        return;

    os_recv(c->fd, conn_recv_ptr(&c->conn), space, MSG_DONTWAIT, &res);

    /*
     * Readiness is not a promise. A wake can be spurious, or an
     * earlier event in the same pass can have taken what this one was
     * reported for, and neither is the connection ending.
     */
    if (res == -EAGAIN)
        return;

    on_recv(tc, slot, res);
}

/* Push what is staged. The caller has already decided there is some. */
static void conn_send(loop_t *l, int32_t i)
{
    loop_conn_t *c = &l->conns[i];
    int32_t      res = 0;

    os_send(c->fd, c->send_buf, c->send_len, MSG_NOSIGNAL | MSG_DONTWAIT,
            &res);

    /*
     * A socket that cannot take anything right now has moved zero
     * bytes, not failed. loop_send_done reads any result at or below
     * zero as the end of the connection, so this one never reaches it.
     */
    if (res == -EAGAIN) {
        conn_want_out(l, i, TRUE);
        return;
    }

    loop_send_done(l, i, res);
    conn_want_out(l, i, c->send_len > 0 ? TRUE : FALSE);
}

static void do_send(loop_t *l, int32_t i)
{
    loop_conn_t *c = &l->conns[i];

    if (!c->in_use || c->fd < 0)
        return;
    if (c->send_len <= 0) {
        conn_want_out(l, i, FALSE);
        return;
    }
    conn_send(l, i);
}

static void do_signal(loop_t *l)
{
    ssize_t n;

    if (l->signal_fd < 0)
        return;

    n = os_read_raw(l->signal_fd, signal_buf, sizeof(signal_buf));

    /*
     * Stop after this pass rather than here, so a record appended
     * moments ago still gets the flush it is owed.
     */
    if (n > 0)
        l->stop = TRUE;
}

static void do_timer(loop_t *l)
{
    ssize_t n;

    if (l->timer_fd < 0)
        return;

    n = os_read_raw(l->timer_fd, timer_buf, sizeof(timer_buf));
    if (n == TIMERFD_READ_SIZE) {
        uint64_t fired = 0;

        mem_copy((uint8_t *)&fired, timer_buf, TIMERFD_READ_SIZE);
        /* Count what expired, not what was noticed: a pass that ran
         * long must not make a deadline shorter than it is. */
        l->tick += (uint32_t)fired;
    }
}

/*
 * A connect left in progress reports itself as writable, and the
 * result is in SO_ERROR: zero for connected, an errno otherwise.
 * Reading it also clears it, so it is asked for exactly once.
 */
static void do_repl_dial(loop_t *l)
{
    err_t   e;
    int32_t so_err = 0;

    err_init(&e);
    if (!result_ok(os_getsockopt_int(&e, l->repl_fd, SOL_SOCKET, SO_ERROR,
                                     &so_err))) {
        on_repl_dial(l, -EIO);
        return;
    }
    on_repl_dial(l, -so_err);
}

static void do_repl_recv(tick_ctx_t *tc)
{
    loop_t *l = tc->l;
    int32_t space;
    int32_t res = 0;

    if (l->repl_fd < 0 || l->repl_closing)
        return;

    space = conn_recv_space(&l->repl_conn);
    if (space <= 0)
        return;

    os_recv(l->repl_fd, conn_recv_ptr(&l->repl_conn), space, MSG_DONTWAIT,
            &res);
    if (res == -EAGAIN)
        return;

    on_repl_recv(l, res, tc->scratch, tc->scratch_len);
}

static void stream_send(loop_t *l)
{
    int32_t res = 0;

    os_send(l->repl_fd, stream_send_buf, l->repl_send_len,
            MSG_NOSIGNAL | MSG_DONTWAIT, &res);

    if (res == -EAGAIN) {
        stream_want_out(l, TRUE);
        return;
    }

    on_repl_send(l, res);
    stream_want_out(l, l->repl_send_len > 0 ? TRUE : FALSE);
}

static void do_repl_send(loop_t *l)
{
    if (l->repl_fd < 0 || l->repl_closing)
        return;
    if (l->repl_send_len <= 0) {
        stream_want_out(l, FALSE);
        return;
    }
    stream_send(l);
}

/* ---- metrics ---- */

/*
 * The endpoint speaks the least HTTP that works.
 *
 * Read until the blank line that ends the request head, ignore every
 * byte of it, write one response, close. There is no routing, no
 * method check and no keep-alive, so the request is never interpreted
 * and the only thing this parser can get wrong is where it stops. That
 * matters: it is the second parser in the process reading from a
 * socket, and the first one is the reason the daemon is treated as the
 * process most likely to have a bug.
 */
static const char metrics_head[] =
    "HTTP/1.0 200 OK\r\n"
    "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
    "Connection: close\r\n"
    "\r\n";

/* One metric family carrying a single unlabelled sample. */
static int32_t m_one(char *b, int32_t cap, int32_t at, const char *name,
                     const char *type, uint64_t v)
{
    at = fmt_str(b, cap, at, "# TYPE ");
    at = fmt_str(b, cap, at, name);
    at = fmt_str(b, cap, at, " ");
    at = fmt_str(b, cap, at, type);
    at = fmt_str(b, cap, at, "\n");
    at = fmt_str(b, cap, at, name);
    at = fmt_str(b, cap, at, " ");
    at = fmt_u64(b, cap, at, v);
    at = fmt_str(b, cap, at, "\n");
    return at;
}

/* A sample of an already-declared family, with one label. */
static int32_t m_label(char *b, int32_t cap, int32_t at, const char *name,
                       const char *label, const char *value, uint64_t v)
{
    at = fmt_str(b, cap, at, name);
    at = fmt_str(b, cap, at, "{");
    at = fmt_str(b, cap, at, label);
    at = fmt_str(b, cap, at, "=\"");
    at = fmt_str(b, cap, at, value);
    at = fmt_str(b, cap, at, "\"} ");
    at = fmt_u64(b, cap, at, v);
    at = fmt_str(b, cap, at, "\n");
    return at;
}

static int32_t m_type(char *b, int32_t cap, int32_t at, const char *name,
                      const char *type)
{
    at = fmt_str(b, cap, at, "# TYPE ");
    at = fmt_str(b, cap, at, name);
    at = fmt_str(b, cap, at, " ");
    at = fmt_str(b, cap, at, type);
    at = fmt_str(b, cap, at, "\n");
    return at;
}

/* Decimal for a replica slot. There are two, so this is enough. */
static const char *const slot_text[LOOP_MAX_REPLICAS] = { "0", "1" };

static const char *const refused_text[LOOP_REFUSED_KINDS] = {
    "not_leader", "no_replicas", "behind", "no_history", "storage", "other"
};

int32_t loop_metrics(loop_t *l, char *buf, int32_t cap)
{
    uint64_t now = os_now_ns();
    uint64_t cumulative = 0;
    int32_t  at = 0;
    int32_t  i;

    if (!l || !buf || cap <= 0)
        return 0;

    /* What this node is. Both samples always, so a query for the role
     * it does not have gets a zero rather than nothing. */
    at = m_type(buf, cap, at, "msgsrvd_role", "gauge");
    at = m_label(buf, cap, at, "msgsrvd_role", "role", "leader",
                 l->is_follower ? 0 : 1);
    at = m_label(buf, cap, at, "msgsrvd_role", "role", "follower",
                 l->is_follower ? 1 : 0);

    /* The log. */
    at = m_one(buf, cap, at, "msgsrvd_first_seq", "gauge",
               wal_first_seq(l->wal));
    at = m_one(buf, cap, at, "msgsrvd_durable_seq", "gauge",
               wal_durable_seq(l->wal));
    at = m_one(buf, cap, at, "msgsrvd_next_seq", "gauge",
               wal_next_seq(l->wal));
    at = m_one(buf, cap, at, "msgsrvd_segments", "gauge",
               (uint64_t)wal_segments(l->wal));
    at = m_one(buf, cap, at, "msgsrvd_segment_bytes", "gauge",
               (uint64_t)wal_seg_capacity(l->wal));

    /*
     * Segments are preallocated, so the log's footprint is what it has
     * been given and not what it has written into. That is the number
     * the volume runs out of.
     */
    at = m_one(buf, cap, at, "msgsrvd_wal_bytes", "gauge",
               (uint64_t)wal_segments(l->wal) *
               (uint64_t)wal_seg_capacity(l->wal));

    /* Retention, including its configuration: an endpoint that reports
     * only behaviour cannot say whether a node was asked for it. */
    at = m_one(buf, cap, at, "msgsrvd_retain_segments", "gauge",
               (uint64_t)l->retain_segments);
    at = m_one(buf, cap, at, "msgsrvd_replicas_expected", "gauge",
               (uint64_t)l->replicas_want);
    at = m_one(buf, cap, at, "msgsrvd_retain_floor", "gauge",
               l->retain_floor);
    at = m_one(buf, cap, at, "msgsrvd_retain_removed_total", "counter",
               l->retain_removed);
    at = m_one(buf, cap, at, "msgsrvd_retain_errors_total", "counter",
               l->retain_errors);

    /* Who is attached. */
    at = m_one(buf, cap, at, "msgsrvd_connections", "gauge",
               (uint64_t)loop_live_conns(l));
    at = m_one(buf, cap, at, "msgsrvd_sessions", "gauge",
               (uint64_t)l->sessions->count);
    at = m_one(buf, cap, at, "msgsrvd_sessions_evicted_total", "counter",
               l->sessions->evicted);
    at = m_one(buf, cap, at, "msgsrvd_accepted_total", "counter",
               l->accepted);
    at = m_one(buf, cap, at, "msgsrvd_closed_total", "counter", l->closed);

    /* Replication, from whichever side this node is on. */
    if (l->is_follower) {
        at = m_one(buf, cap, at, "msgsrvd_leader_connected", "gauge",
                   (l->repl_fd >= 0 && !l->repl_connecting) ? 1 : 0);
        at = m_one(buf, cap, at, "msgsrvd_repl_applied_total", "counter",
                   l->repl_records);
    } else {
        at = m_type(buf, cap, at, "msgsrvd_follower_live", "gauge");
        for (i = 0; i < LOOP_MAX_REPLICAS; i++)
            at = m_label(buf, cap, at, "msgsrvd_follower_live", "slot",
                         slot_text[i], l->peer_live[i] ? 1 : 0);

        at = m_type(buf, cap, at, "msgsrvd_follower_durable_seq", "gauge");
        for (i = 0; i < LOOP_MAX_REPLICAS; i++)
            at = m_label(buf, cap, at, "msgsrvd_follower_durable_seq",
                         "slot", slot_text[i], l->peer_durable[i]);

        /*
         * How long since each replica last said where it had got to.
         * This is what the "a follower has been gone too long" alert
         * reads, and it is in seconds because a lag in sequences says
         * nothing on a log that is not being written to.
         */
        at = m_type(buf, cap, at, "msgsrvd_follower_ack_age_seconds",
                    "gauge");
        for (i = 0; i < LOOP_MAX_REPLICAS; i++) {
            uint64_t age = 0;

            if (l->peer_ack_ns[i] > 0 && now > l->peer_ack_ns[i])
                age = now - l->peer_ack_ns[i];
            at = fmt_str(buf, cap, at, "msgsrvd_follower_ack_age_seconds"
                                       "{slot=\"");
            at = fmt_str(buf, cap, at, slot_text[i]);
            at = fmt_str(buf, cap, at, "\"} ");
            at = fmt_ns_seconds(buf, cap, at, age);
            at = fmt_str(buf, cap, at, "\n");
        }

        at = m_one(buf, cap, at, "msgsrvd_repl_records_total", "counter",
                   l->repl_records);
    }

    /* Writes and what they were answered with. */
    at = m_one(buf, cap, at, "msgsrvd_writes_total", "counter", l->writes);
    at = m_one(buf, cap, at, "msgsrvd_dedup_hits_total", "counter",
               l->dedup_hits);

    at = m_type(buf, cap, at, "msgsrvd_acks_total", "counter");
    at = m_label(buf, cap, at, "msgsrvd_acks_total", "durability", "append",
                 l->acks_append);
    at = m_label(buf, cap, at, "msgsrvd_acks_total", "durability", "sync",
                 l->acks_sync);
    at = m_label(buf, cap, at, "msgsrvd_acks_total", "durability",
                 "replicated", l->acks_repl);
    at = m_one(buf, cap, at, "msgsrvd_acks_degraded_total", "counter",
               l->repl_degraded);

    at = m_type(buf, cap, at, "msgsrvd_refused_total", "counter");
    for (i = 0; i < LOOP_REFUSED_KINDS; i++)
        at = m_label(buf, cap, at, "msgsrvd_refused_total", "reason",
                     refused_text[i], l->refused[i]);

    at = m_one(buf, cap, at, "msgsrvd_notified_total", "counter",
               l->notified);

    /* Flushes, and how long they took. */
    at = m_type(buf, cap, at, "msgsrvd_fsync_seconds", "histogram");
    for (i = 0; i < LOOP_FSYNC_BUCKETS; i++) {
        cumulative += l->fsync_bucket[i];
        at = m_label(buf, cap, at, "msgsrvd_fsync_seconds_bucket", "le",
                     fsync_bound_text[i], cumulative);
    }
    cumulative += l->fsync_bucket[LOOP_FSYNC_BUCKETS];
    at = m_label(buf, cap, at, "msgsrvd_fsync_seconds_bucket", "le", "+Inf",
                 cumulative);
    at = fmt_str(buf, cap, at, "msgsrvd_fsync_seconds_sum ");
    at = fmt_ns_seconds(buf, cap, at, l->fsync_ns_total);
    at = fmt_str(buf, cap, at, "\n");
    at = fmt_str(buf, cap, at, "msgsrvd_fsync_seconds_count ");
    at = fmt_u64(buf, cap, at, cumulative);
    at = fmt_str(buf, cap, at, "\n");
    at = m_one(buf, cap, at, "msgsrvd_flushes_total", "counter", l->flushes);

    /* The endpoint's own traffic. */
    at = m_one(buf, cap, at, "msgsrvd_scrapes_total", "counter",
               l->scrapes_served);
    at = m_one(buf, cap, at, "msgsrvd_scrapes_refused_total", "counter",
               l->scrapes_refused);

    /*
     * A document that hit the ceiling is refused rather than served.
     * Truncation is invisible to a scraper: what it would read is a
     * shorter document in which the missing metrics look as though
     * they had never existed. The last line is what proves the whole
     * of it fit.
     */
    if (at <= 0 || at >= cap)
        return 0;
    if (buf[at - 1] != '\n')
        return 0;
    return at;
}

/* ---- scrapes ---- */

static void scrape_release(loop_t *l, int32_t i)
{
    loop_scrape_t *sc = &l->scrapes[i];
    err_t          e;

    if (sc->fd < 0)
        return;

    err_init(&e);
    os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_DEL, sc->fd, 0, 0);
    os_close(&e, sc->fd);
    mem_zero((uint8_t *)sc, (int32_t)sizeof(*sc));
    sc->fd = -1;
}

/*
 * Watch for room to write instead of for more to read.
 *
 * Once there is an answer the request no longer matters, and anything
 * further the scraper says will not be looked at. Leaving readability
 * registered would report the same unread bytes on every pass, and a
 * loop woken by something it has decided to ignore does not sleep.
 */
static void scrape_answering(loop_t *l, int32_t i)
{
    loop_scrape_t *sc = &l->scrapes[i];
    err_t          e;

    if (sc->fd < 0 || sc->answering)
        return;

    err_init(&e);
    if (result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_MOD, sc->fd,
                               EPOLLOUT, UD_MAKE(OP_SCRAPE, i))))
        sc->answering = TRUE;
}

/* Push what is left of the response. */
static void scrape_send(loop_t *l, int32_t i)
{
    loop_scrape_t *sc = &l->scrapes[i];
    int32_t        res = 0;

    if (sc->fd < 0 || sc->out_len <= sc->sent)
        return;

    os_send(sc->fd, (const uint8_t *)scrape_out[i] + sc->sent,
            sc->out_len - sc->sent, MSG_NOSIGNAL | MSG_DONTWAIT, &res);

    if (res == -EAGAIN) {
        scrape_answering(l, i);
        return;
    }
    if (res <= 0) {
        /* The reader went away mid-answer. Nothing to report to. */
        scrape_release(l, i);
        return;
    }

    sc->sent += res;
    if (sc->sent >= sc->out_len) {
        l->scrapes_served++;
        scrape_release(l, i);
        return;
    }
    scrape_answering(l, i);
}

/*
 * Build the response for a request that has ended.
 *
 * A document that would not fit is answered with 503 rather than with
 * a shorter document, for the same reason loop_metrics refuses to
 * truncate: a metric that is missing reads as a metric that does not
 * exist, and a scraper cannot tell the two apart.
 */
static void scrape_answer(loop_t *l, int32_t i)
{
    loop_scrape_t *sc = &l->scrapes[i];
    int32_t        head = (int32_t)sizeof(metrics_head) - 1;
    int32_t        body;

    body = loop_metrics(l, scrape_out[i] + head, LOOP_METRICS_CAP - head);
    if (body <= 0) {
        static const char oops[] =
            "HTTP/1.0 503 Service Unavailable\r\n"
            "Content-Type: text/plain\r\n"
            "Connection: close\r\n"
            "\r\n";

        sc->out_len = (int32_t)sizeof(oops) - 1;
        mem_copy((uint8_t *)scrape_out[i], (const uint8_t *)oops,
                 sc->out_len);
        l->scrapes_refused++;
    } else {
        mem_copy((uint8_t *)scrape_out[i], (const uint8_t *)metrics_head,
                 head);
        sc->out_len = head + body;
    }

    sc->sent = 0;
    scrape_send(l, i);
}

/*
 * Read what a scraper sent, only to find where it stops.
 *
 * The head ends at a blank line. Both spellings are accepted because
 * accepting one costs nothing and a client that ends its lines with
 * bare newlines is not doing anything ambiguous.
 */
static void scrape_recv(loop_t *l, int32_t i)
{
    loop_scrape_t *sc = &l->scrapes[i];
    int32_t        res = 0;
    int32_t        at;

    if (sc->fd < 0)
        return;
    if (sc->out_len > 0)
        return;                     /* answered already; it can stop talking */

    if (sc->in_len >= LOOP_SCRAPE_CAP) {
        l->scrapes_refused++;
        scrape_release(l, i);
        return;
    }

    os_recv(sc->fd, scrape_in[i] + sc->in_len, LOOP_SCRAPE_CAP - sc->in_len,
            MSG_DONTWAIT, &res);

    if (res == -EAGAIN)
        return;
    if (res <= 0) {
        scrape_release(l, i);
        return;
    }

    sc->in_len += res;

    for (at = 1; at < sc->in_len; at++) {
        if (scrape_in[i][at] != '\n')
            continue;
        if (scrape_in[i][at - 1] == '\n' ||
            (at >= 3 && scrape_in[i][at - 1] == '\r' &&
             scrape_in[i][at - 2] == '\n')) {
            scrape_answer(l, i);
            return;
        }
    }

    /*
     * The head is longer than a request head has any reason to be.
     * Whatever is on the other end is not a scraper, and it is dropped
     * without an answer: replying would be describing this node to
     * something that could not ask for it properly.
     */
    if (sc->in_len >= LOOP_SCRAPE_CAP) {
        l->scrapes_refused++;
        scrape_release(l, i);
    }
}

static void scrape_accept(loop_t *l)
{
    err_t   e;
    int32_t fd = -1;
    int32_t i;
    int32_t slot = -1;

    if (l->stop || l->metrics_fd < 0)
        return;

    err_init(&e);
    if (!result_ok(os_accept4(&e, l->metrics_fd, SOCK_NONBLOCK | SOCK_CLOEXEC,
                              &fd)))
        return;

    for (i = 0; i < LOOP_MAX_SCRAPES; i++) {
        if (l->scrapes[i].fd < 0) {
            slot = i;
            break;
        }
    }

    /*
     * Every slot busy, so the oldest goes. Nothing here has a deadline
     * of its own: a connection that opens and then says nothing would
     * otherwise hold its slot for as long as it stayed open, and four
     * of those would put the endpoint out of reach for good. Losing
     * one answer to make room is the cheaper failure, since a scrape
     * carries no state and the next one asks the same question.
     */
    if (slot < 0) {
        uint64_t oldest = 0;

        for (i = 0; i < LOOP_MAX_SCRAPES; i++) {
            if (slot < 0 || l->scrapes[i].opened_ns < oldest) {
                oldest = l->scrapes[i].opened_ns;
                slot = i;
            }
        }
        l->scrapes_refused++;
        scrape_release(l, slot);
    }

    if (!result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_ADD, fd, EPOLLIN,
                                UD_MAKE(OP_SCRAPE, slot)))) {
        os_close(&e, fd);
        return;
    }

    mem_zero((uint8_t *)&l->scrapes[slot],
             (int32_t)sizeof(l->scrapes[slot]));
    l->scrapes[slot].fd = fd;
    l->scrapes[slot].opened_ns = os_now_ns();
}

/*
 * EPOLLERR and EPOLLHUP arrive whether they were asked for or not, and
 * both mean the same thing here as readable does: take the receive and
 * let its result end the connection.
 */
#define EV_READABLE (EPOLLIN | EPOLLERR | EPOLLHUP)

static void tick_event(tick_ctx_t *tc, uint64_t user_data, uint32_t events)
{
    loop_t *l = tc->l;
    int32_t slot = UD_SLOT(user_data);

    switch (UD_OP(user_data)) {
    case OP_LISTEN:
        do_accept(l);
        break;
    case OP_CONN:
        if (events & EV_READABLE)
            do_recv(tc, slot);
        /*
         * Staged output goes out even on a connection that is closing,
         * so a protocol error is reported before the socket does.
         */
        if (events & EPOLLOUT)
            do_send(l, slot);
        break;
    case OP_SIGNAL:
        do_signal(l);
        break;
    case OP_TIMER:
        do_timer(l);
        break;
    case OP_STREAM:
        if (l->repl_connecting)
            do_repl_dial(l);
        else {
            if (events & EV_READABLE)
                do_repl_recv(tc);
            if (events & EPOLLOUT)
                do_repl_send(l);
        }
        break;
    case OP_METRICS:
        scrape_accept(l);
        break;
    case OP_SCRAPE:
        if (events & EV_READABLE)
            scrape_recv(l, slot);
        if (events & EPOLLOUT)
            scrape_send(l, slot);
        break;
    default:
        break;
    }
}

/* ---- the arm phase ---- */

/*
 * What each connection is ready for, once the events of this pass have
 * all been dispatched and the flush they caused has happened.
 *
 * Staged bytes are sent here rather than waited on: a socket with room
 * takes them at once, and the ring did the same thing, submitting a
 * send at the end of a tick that had completed before the next one.
 * Only a send that could not take everything leaves EPOLLOUT behind.
 *
 * A slot is given up here and nowhere else. The events of the pass are
 * done with by now, so nothing is left that could still name it.
 */
static void arm_conns(loop_t *l)
{
    int32_t i;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        loop_conn_t *c = &l->conns[i];

        if (!c->in_use)
            continue;

        /*
         * Staged output goes out even on a connection being closed. A
         * protocol error is reported and then the connection ends, so
         * dropping the slot first would close it silently and leave
         * the client to guess.
         */
        if (c->send_len > 0 && !c->want_out && c->fd >= 0)
            conn_send(l, i);

        if (c->closing) {
            slot_release(l, i);
            continue;
        }

        /*
         * Everything parsed has been handled by now, so the room it
         * takes can go back. Doing this any earlier would move the
         * unparsed tail over the frames still being handled.
         */
        conn_compact(&c->conn);
        if (conn_recv_space(&c->conn) <= 0) {
            /* Nothing was consumed and there is no room to read more. */
            c->closing = TRUE;
            slot_release(l, i);
        }
    }
}

/*
 * The clock, created the first time something needs one: a replica has
 * attached, or this node has a leader to dial. A node with neither
 * never has a timer and is woken only by work.
 */
static void arm_timer_now(loop_t *l)
{
    err_t e;

    if (l->timer_fd >= 0)
        return;

    err_init(&e);
    if (!result_ok(os_timerfd(&e, TFD_CLOEXEC | TFD_NONBLOCK, &l->timer_fd))) {
        l->timer_fd = -1;
        return;
    }
    if (!result_ok(os_timerfd_period(&e, l->timer_fd, LOOP_TICK_NS)) ||
        !result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_ADD, l->timer_fd,
                                EPOLLIN, UD_MAKE(OP_TIMER, 0)))) {
        os_close(&e, l->timer_fd);
        l->timer_fd = -1;
    }
}

/* Dial the leader, once the wait since the last attempt has passed. */
static void repl_dial(loop_t *l)
{
    err_t    e;
    result_t r;
    int32_t  fd = -1;

    if (!l->is_follower || l->stop)
        return;
    if (l->repl_fd >= 0 || l->repl_connecting)
        return;
    if (l->tick < l->repl_next_dial)
        return;

    err_init(&e);
    if (!result_ok(os_socket(&e, AF_INET,
                             SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                             &fd))) {
        l->repl_next_dial = l->tick + LOOP_REDIAL_TICKS;
        return;
    }

    /*
     * Watched for writability, which is how a connect that has not
     * finished says it has.
     */
    if (!result_ok(os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_ADD, fd, EPOLLOUT,
                                UD_MAKE(OP_STREAM, 0)))) {
        os_close(&e, fd);
        l->repl_next_dial = l->tick + LOOP_REDIAL_TICKS;
        return;
    }

    l->repl_fd = fd;
    l->repl_connecting = TRUE;

    /*
     * A non-blocking connect almost always says it is still going, and
     * the writable report carries the answer. Loopback can finish it
     * here instead, and then there is nothing to wait for. Anything
     * else is a refusal, and goes to the same handler as one that
     * arrived later.
     */
    r = os_connect(&e, fd, &l->leader_addr);
    if (result_ok(r))
        on_repl_dial(l, 0);
    else if (r.detail != EINPROGRESS)
        on_repl_dial(l, -r.detail);
}

/*
 * The stream, in both directions at once. Nothing alternates here: the
 * records coming in and the acknowledgements going back are unrelated
 * and use separate buffers, and an acknowledgement held until the next
 * record arrived would be an acknowledgement that never arrived on an
 * idle leader.
 */
static void arm_stream(loop_t *l)
{
    if (l->repl_fd < 0 || l->repl_connecting || l->stop)
        return;

    if (l->repl_closing) {
        stream_release(l);
        return;
    }

    if (l->repl_send_len > 0 && !l->repl_want_out)
        stream_send(l);

    if (l->repl_closing)
        return;

    conn_compact(&l->repl_conn);
    if (conn_recv_space(&l->repl_conn) <= 0)
        l->repl_closing = TRUE;
}

/*
 * Record how long a flush took.
 *
 * Buckets are stored as the count that landed in each rather than
 * cumulatively, and summed on the way out. Storing them cumulatively
 * would mean touching every bucket above the one that was hit, which
 * is more work per flush to save arithmetic on a scrape that happens
 * once every few seconds.
 *
 * A clock that would not answer reports zero, and a zero start makes
 * the measurement land in the first bucket. That is a wrong sample
 * rather than a wrong total, and it cannot be told apart from a fast
 * flush here; the endpoint is the wrong place to argue about it.
 */
static void fsync_observe(loop_t *l, uint64_t began_ns)
{
    uint64_t ns = 0;
    int32_t  i;

    if (began_ns > 0) {
        uint64_t now = os_now_ns();

        if (now > began_ns)
            ns = now - began_ns;
    }

    l->fsync_ns_total += ns;
    for (i = 0; i < LOOP_FSYNC_BUCKETS; i++) {
        if (ns <= fsync_bound_ns[i]) {
            l->fsync_bucket[i]++;
            return;
        }
    }
    l->fsync_bucket[LOOP_FSYNC_BUCKETS]++;
}

/*
 * Delete segments this node no longer needs.
 *
 * The count is the limit that always applies, and it is what bounds
 * both disk use and the recovery scan. On a leader expecting replicas
 * the confirmed mark applies as well, and the lower of the two wins.
 *
 * Run at the end of a pass rather than beside the flush. The unlinks
 * and the directory fsync that makes them durable are storage latency
 * with nothing waiting on them, and putting them between the flush and
 * the acknowledgements it releases would add that latency to every
 * write in the batch.
 */
static void retain_pass(loop_t *l)
{
    uint64_t keep_from;
    int32_t  removed = 0;
    int32_t  segments;
    err_t    e;

    if (l->retain_segments <= 0)
        return;

    segments = wal_segments(l->wal);

    /*
     * A failed unlink is not retried every pass. A directory is
     * unlikely to change its mind within a tenth of a second, and a
     * fault that persists would otherwise put a line in the journal
     * ten times a second for as long as it lasted. The next rollover
     * is what makes the attempt worth repeating.
     */
    if (l->retain_failed) {
        if (segments == l->retain_seen_segs)
            return;
        l->retain_failed = FALSE;
    }
    l->retain_seen_segs = segments;

    keep_from = wal_retain_mark(l->wal, l->retain_segments);
    if (keep_from == 0)
        return;

    /*
     * A leader that expects replicas and has heard from none has a
     * floor of zero, so keep_from becomes 1 and nothing is below it.
     * That is the right state for a node just promoted: it deletes
     * nothing until the replicas that survived have caught up.
     */
    if (l->replicas_want > 0 && l->retain_floor + 1 < keep_from)
        keep_from = l->retain_floor + 1;

    if (keep_from <= wal_first_seq(l->wal))
        return;

    err_init(&e);
    if (!result_ok(wal_retain(l->wal, &e, keep_from, &removed))) {
        l->retain_errors++;
        l->retain_failed = TRUE;
        say("msgsrvd: retention could not remove a segment\n");
    }
    l->retain_removed += (uint64_t)removed;
}

/*
 * Which level an acknowledgement answered.
 *
 * Read off the flags going out rather than the ones that came in, so a
 * write that asked for a second copy and was told it has one copy is
 * counted as the local flush it actually got.
 */
static void ack_count(loop_t *l, uint16_t flags)
{
    if ((flags & MSG_FLAG_REPLICATED) && !(flags & MSG_FLAG_DEGRADED))
        l->acks_repl++;
    else if (flags & MSG_FLAG_SYNC)
        l->acks_sync++;
    else
        l->acks_append++;
}

/*
 * Send the acknowledgements this pass has made true.
 *
 * The local slot is answered by the flush that has just happened. The
 * replicated slot waits for a replica to confirm the same sequence,
 * and stops waiting when the deadline passes or when the last replica
 * goes away, since there is then nothing left that could confirm it.
 * What happens then is the writer's choice, made when it sent the
 * record: one copy and told so, or an error.
 */
static void acks_emit(loop_t *l)
{
    uint64_t best = repl_best(l);
    bool_t   live = repl_live(l);
    int32_t  i;

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        loop_conn_t *c = &l->conns[i];

        if (!c->in_use)
            continue;

        if (c->ack_local.due) {
            reply_ack(c, i, c->ack_local.flags, c->ack_local.wal_seq,
                      c->ack_local.client_seq);
            ack_count(l, c->ack_local.flags);
            ack_clear(&c->ack_local);
        }

        if (!c->ack_repl.due)
            continue;

        if (best >= c->ack_repl.wal_seq &&
            wal_durable_seq(l->wal) >= c->ack_repl.wal_seq) {
            reply_ack(c, i, c->ack_repl.flags, c->ack_repl.wal_seq,
                      c->ack_repl.client_seq);
            ack_count(l, c->ack_repl.flags);
            ack_clear(&c->ack_repl);
            continue;
        }

        if (live && l->tick < c->ack_repl.deadline)
            continue;

        if (c->ack_repl.degradable) {
            reply_ack(c, i, (uint16_t)(c->ack_repl.flags | MSG_FLAG_DEGRADED),
                      c->ack_repl.wal_seq, c->ack_repl.client_seq);
            ack_count(l, (uint16_t)(c->ack_repl.flags | MSG_FLAG_DEGRADED));
            l->repl_degraded++;
        } else {
            /*
             * The sequence names the highest record the refusal covers,
             * the same way an acknowledgement names the highest it
             * satisfies. Every record of this session at that level and
             * below it is refused, and the client resends or gives up.
             */
            reply_err(l, c, i, MSG_ERR_NO_REPLICAS, c->ack_repl.client_seq);
        }
        ack_clear(&c->ack_repl);
    }
}

/* Tell the leader how far this node's own flush has got. */
static void stream_report(loop_t *l)
{
    uint64_t durable;

    if (l->repl_fd < 0 || l->repl_connecting || l->repl_closing)
        return;

    durable = wal_durable_seq(l->wal);
    if (durable <= l->repl_acked)
        return;

    /*
     * Skipped rather than queued when the last one has not gone yet:
     * the next report carries a sequence at least as high, so nothing
     * is lost by not staging this one.
     */
    if (l->repl_send_len > 0)
        return;

    stream_frame(l, MSG_OP_REPL_ACK, durable);
    l->repl_acked = durable;
}

/* ---- public ---- */

result_t loop_init(loop_t *l, err_t *e, wal_t *wal,
                   session_table_t *sessions, uint32_t bind_ip,
                   uint16_t port)
{
    sockaddr_in_t addr;
    result_t      r;
    int32_t       one = 1;
    int32_t       i;

    mem_zero((uint8_t *)l, (int32_t)sizeof(*l));
    l->wal = wal;
    l->sessions = sessions;
    l->epoll_fd = -1;
    l->listen_fd = -1;
    l->signal_fd = -1;
    l->timer_fd = -1;
    l->repl_fd = -1;
    l->metrics_fd = -1;

    for (i = 0; i < LOOP_MAX_SCRAPES; i++)
        l->scrapes[i].fd = -1;
    for (i = 0; i < LOOP_MAX_CONNS; i++)
        slot_reset(l, i);
    for (i = 0; i < LOOP_MAX_REPLICAS; i++)
        wal_cursor_init(&l->peer_cursor[i]);

    r = os_epoll_create(e, EPOLL_CLOEXEC, &l->epoll_fd);
    if (!result_ok(r))
        return r;

    /*
     * The listener is non-blocking too. accept4 sets the flag on the
     * connection it returns, not on the socket it took it from, and an
     * accept that blocked after a readiness report that turned out to
     * be spurious would stall every other client's flush.
     */
    r = os_socket(e, AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                  &l->listen_fd);
    if (!result_ok(r)) {
        os_close(e, l->epoll_fd);
        l->epoll_fd = -1;
        return r;
    }

    os_setsockopt(e, l->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one,
                  (int32_t)sizeof(one));

    mem_zero((uint8_t *)&addr, (int32_t)sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr = htonl(bind_ip);

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

    r = os_epoll_ctl(e, l->epoll_fd, EPOLL_CTL_ADD, l->listen_fd, EPOLLIN,
                     UD_MAKE(OP_LISTEN, 0));
    if (!result_ok(r))
        goto fail;

    return RESULT_OK;

fail:
    os_close(e, l->listen_fd);
    l->listen_fd = -1;
    os_close(e, l->epoll_fd);
    l->epoll_fd = -1;
    return r;
}

result_t loop_tick(loop_t *l, err_t *e, bool_t wait)
{
    /*
     * Every descriptor the loop can hold at once: one per slot, plus
     * the listener, the signalfd, the timerfd and the follower's
     * stream. Sized so one wait can never leave an event behind.
     */
    /*
     * Room for every descriptor the loop can hold at once: a slot
     * each, the scrapes, and the five singletons (both listeners, the
     * signal, the timer and the stream). A short array is not wrong,
     * since epoll reports the rest on the next call, but it would put
     * off the flush that the connections already read are waiting for.
     */
    static epoll_event_t events[LOOP_MAX_CONNS + LOOP_MAX_SCRAPES + 5];
    static uint8_t scratch[WAL_REC_MAX_SIZE];
    tick_ctx_t tc;
    result_t   r;
    int32_t    timeout;
    int32_t    n = 0;
    int32_t    i;

    tc.l = l;
    tc.scratch = scratch;
    tc.scratch_len = (int32_t)sizeof(scratch);

    repl_dial(l);
    arm_conns(l);
    arm_stream(l);

    timeout = (wait && !l->progress) ? -1 : 0;
    l->progress = FALSE;

    r = os_epoll_wait(e, l->epoll_fd, events,
                      (int32_t)(sizeof(events) / sizeof(events[0])),
                      timeout, &n);
    if (!result_ok(r))
        return r;

    l->wal_dirty = FALSE;
    for (i = 0; i < n; i++)
        tick_event(&tc, events[i].data, events[i].events);

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
        uint64_t began = os_now_ns();

        r = wal_sync(l->wal, e);
        if (!result_ok(r))
            return r;
        l->flushes++;
        fsync_observe(l, began);
    }

    /* Only now is an acknowledgement true, so only now is it staged. */
    acks_emit(l);

    /*
     * Streaming comes after the flush for the same reason: a replica
     * is sent records that are on this node's disk, never records that
     * are only in its memory.
     */
    if (l->is_follower)
        stream_report(l);
    else
        repl_stream(l);

    /*
     * Subscribers are served last, after the flush and after the
     * replicas. A record reaches a client watching the log only once
     * it is on this node's disk.
     */
    subs_push(l, scratch, (int32_t)sizeof(scratch));

    /*
     * Last, once everything this pass promised has been served. A
     * cursor the delete invalidates is noticed on the next pass, which
     * is where it would have been noticed anyway had the delete landed
     * a moment later.
     */
    retain_pass(l);

    repl_dial(l);
    arm_conns(l);
    arm_stream(l);
    return RESULT_OK;
}

result_t loop_set_leader(loop_t *l, err_t *e, const sockaddr_in_t *addr,
                         const char *text)
{
    if (!l || !addr) {
        ERR_PUSH(e, ERR_INVALID);
        return RESULT_ERR(ERR_INVALID, 0);
    }

    l->is_follower = TRUE;
    l->leader_addr = *addr;
    l->leader_text = text;
    l->repl_next_dial = 0;
    arm_timer_now(l);

    if (l->timer_fd < 0) {
        /*
         * Without a clock the stream could be dialled once and never
         * again, which is worse than refusing to start.
         */
        ERR_PUSH(e, ERR_SYSCALL);
        return RESULT_ERR(ERR_SYSCALL, 0);
    }
    return RESULT_OK;
}

void loop_set_signal_fd(loop_t *l, int32_t fd)
{
    err_t e;

    err_init(&e);
    l->signal_fd = fd;
    if (fd >= 0)
        os_epoll_ctl(&e, l->epoll_fd, EPOLL_CTL_ADD, fd, EPOLLIN,
                     UD_MAKE(OP_SIGNAL, 0));
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

void loop_set_retention(loop_t *l, int32_t keep_segments, int32_t replicas)
{
    if (keep_segments < 0)
        keep_segments = 0;
    if (replicas < 0)
        replicas = 0;
    if (replicas > LOOP_MAX_REPLICAS)
        replicas = LOOP_MAX_REPLICAS;

    l->retain_segments = keep_segments;
    l->replicas_want = replicas;
    l->retain_floor = 0;
    l->retain_seen_segs = 0;
    l->retain_failed = FALSE;
}

result_t loop_set_metrics_port(loop_t *l, err_t *e, uint32_t bind_ip,
                               uint16_t port)
{
    sockaddr_in_t addr;
    result_t      r;
    int32_t       one = 1;

    r = os_socket(e, AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                  &l->metrics_fd);
    if (!result_ok(r))
        return r;

    os_setsockopt(e, l->metrics_fd, SOL_SOCKET, SO_REUSEADDR, &one,
                  (int32_t)sizeof(one));

    mem_zero((uint8_t *)&addr, (int32_t)sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr = htonl(bind_ip);

    r = os_bind(e, l->metrics_fd, &addr);
    if (!result_ok(r))
        goto fail;

    r = os_listen(e, l->metrics_fd, 8);
    if (!result_ok(r))
        goto fail;

    r = os_getsockname(e, l->metrics_fd, &addr);
    if (!result_ok(r))
        goto fail;
    l->metrics_port = (int32_t)ntohs(addr.sin_port);

    r = os_epoll_ctl(e, l->epoll_fd, EPOLL_CTL_ADD, l->metrics_fd, EPOLLIN,
                     UD_MAKE(OP_METRICS, 0));
    if (!result_ok(r))
        goto fail;

    return RESULT_OK;

fail:
    os_close(e, l->metrics_fd);
    l->metrics_fd = -1;
    return r;
}

void loop_shutdown(loop_t *l)
{
    err_t   e;
    int32_t i;

    err_init(&e);
    l->stop = TRUE;

    for (i = 0; i < LOOP_MAX_SCRAPES; i++) {
        if (l->scrapes[i].fd >= 0) {
            os_close(&e, l->scrapes[i].fd);
            l->scrapes[i].fd = -1;
        }
    }
    if (l->metrics_fd >= 0) {
        os_close(&e, l->metrics_fd);
        l->metrics_fd = -1;
    }

    for (i = 0; i < LOOP_MAX_CONNS; i++) {
        if (l->conns[i].in_use && l->conns[i].fd >= 0) {
            os_close(&e, l->conns[i].fd);
            slot_reset(l, i);
        }
    }

    for (i = 0; i < LOOP_MAX_REPLICAS; i++) {
        wal_cursor_close(&l->peer_cursor[i], &e);
        l->peer_live[i] = FALSE;
    }

    if (l->repl_fd >= 0) {
        os_close(&e, l->repl_fd);
        l->repl_fd = -1;
    }
    if (l->timer_fd >= 0) {
        os_close(&e, l->timer_fd);
        l->timer_fd = -1;
    }
    if (l->listen_fd >= 0) {
        os_close(&e, l->listen_fd);
        l->listen_fd = -1;
    }
    if (l->signal_fd >= 0) {
        os_close(&e, l->signal_fd);
        l->signal_fd = -1;
    }
    if (l->epoll_fd >= 0) {
        os_close(&e, l->epoll_fd);
        l->epoll_fd = -1;
    }
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

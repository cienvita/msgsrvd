#ifndef MSGSRVD_LOOP_H
#define MSGSRVD_LOOP_H

#include "core/types.h"
#include "core/err.h"
#include "proto/conn.h"
#include "wal/wal.h"
#include "server/session.h"
#include "sys/linux.h"

/*
 * Single-threaded event loop over one epoll instance.
 *
 * Group commit is the reason the loop is shaped the way it is. A tick
 * dispatches every event the kernel has ready, appending each write
 * it finds, and flushes once at the end. The batch is therefore
 * whatever arrived while the previous flush was in progress, with no
 * timer deciding when to stop waiting: the flush itself is the delay.
 * On a host where a flush costs milliseconds the batch grows to match,
 * and on one where it costs microseconds the batches stay small, which
 * is the behaviour wanted in both cases and needs no tuning.
 *
 * Acknowledgements are held back until after that flush, so a client
 * is told a record is durable only once it is. Everything else, PONG
 * and the session handshake and errors, is answered during the drain,
 * because none of it is a promise about storage.
 *
 * A connection reads and writes at once, one operation per direction,
 * each with its own buffer. An acknowledgement that waits on a replica
 * is staged passes after the write it answers, so a loop that held the
 * reply until the next request arrived would be waiting on a client
 * that is itself waiting on the reply.
 */

#define LOOP_MAX_CONNS    64
#define LOOP_RECV_CAP     8192
#define LOOP_SEND_CAP     8192

/*
 * Replication.
 *
 * A replica's buffers are larger than a client's because they carry
 * whole WAL records, which are a client's frame plus a record header.
 * A stream sized like a client connection would stall on the largest
 * record a client is allowed to send, and stall silently, since
 * nothing about that record is wrong.
 */
#define LOOP_MAX_REPLICAS 2
#define LOOP_REPL_CAP     16384

/*
 * The loop's only clock, and it is armed only where something has to
 * happen without a client or a peer causing it: a stream that has to
 * be dialled again, and a write that has to stop waiting for a second
 * copy. A node with no replication never arms it and its only timing
 * is still the flush.
 */
#define LOOP_TICK_NS            ((int64_t)100 * 1000 * 1000)
#define LOOP_REPL_DEADLINE_TICKS 10     /* about a second */
#define LOOP_REDIAL_TICKS        5      /* about half of one */

/*
 * Ticks a subscription waits for this node to reach the sequence it
 * was told to start no earlier than. A client that wrote to the leader
 * and reads from a replica is the case: the wait is the replica's own
 * flush, arriving behind the leader's.
 */
#define LOOP_SUB_DEADLINE_TICKS  5      /* about half a second */

/*
 * An acknowledgement owed to a client.
 *
 * There are two per connection because the levels are answered
 * separately: an ACK carries the flags it satisfies, and one taken
 * after the local flush cannot answer a write that asked for a second
 * copy. Each slot holds the highest sequence waiting at that level,
 * which is all a cumulative acknowledgement needs.
 */
typedef struct {
    uint64_t    client_seq;     /* highest client sequence waiting */
    uint64_t    wal_seq;        /* WAL sequence that covers it */
    uint32_t    deadline;       /* tick to stop waiting at; replicated only */
    uint16_t    flags;          /* durability the ACK will report */
    bool_t      due;
    bool_t      degradable;     /* the writer will take one copy instead */
} loop_ack_t;

/*
 * A connection's subscription.
 *
 * The cursor is the whole of it: where the stream has got to. Records
 * are sent from it after each flush, so a subscriber never sees a
 * record the node could still lose, and a connection that cannot take
 * them fast enough simply leaves the cursor where it is.
 */
typedef struct {
    wal_cursor_t cursor;
    uint64_t    key;            /* the key asked for, when not all_keys */
    uint64_t    min_seq;        /* hold the stream until this is durable */
    uint32_t    deadline;       /* tick to give up waiting for min_seq */
    uint64_t    until;          /* last sequence a read covers */
    bool_t      active;         /* sending */
    bool_t      waiting;        /* held for min_seq */
    bool_t      all_keys;
    bool_t      once;           /* a read, which ends; not a subscription */
    uint8_t     _pad[4];
} loop_sub_t;

/* Per-connection slot. */
typedef struct {
    conn_t      conn;
    int32_t     fd;             /* -1 when the slot is free */
    int32_t     send_len;       /* bytes staged in the send buffer */
    uint8_t    *send_buf;       /* the slot's buffer, or a replica's */
    int32_t     send_cap;
    int32_t     replica;        /* replica index, -1 for a client */
    uint64_t    session;        /* 0 until HELLO */
    loop_ack_t  ack_local;      /* owed once the flush lands */
    loop_ack_t  ack_repl;       /* owed once a replica confirms */
    loop_sub_t  sub;            /* what this connection is following */
    bool_t      in_use;
    bool_t      want_out;       /* EPOLLOUT is registered for this slot */
    bool_t      closing;
    uint8_t     _pad[5];
} loop_conn_t;

typedef struct {
    int32_t         epoll_fd;
    wal_t          *wal;
    int32_t         listen_fd;
    int32_t         signal_fd;      /* -1 when nothing is watching signals */
    int32_t         port;           /* host byte order, after binding */
    bool_t          stop;

    loop_conn_t     conns[LOOP_MAX_CONNS];

    /*
     * Not owned. The caller builds it from the log before the loop
     * starts, which is what lets deduplication survive a restart.
     */
    session_table_t *sessions;

    /* Set during a tick when a record was appended and not yet flushed */
    bool_t          wal_dirty;

    /*
     * Set when a pass moved a subscription or a replica's stream on
     * and may have more of it to give.
     *
     * The ring woke the loop with completions of its own, so a pass
     * that had sent something was always followed by another. epoll
     * reports only what the outside world does, and a log being read
     * out is nobody's doing but this loop's, so a pass that made
     * progress says so and the next one does not wait to be told.
     */
    bool_t          progress;
    uint8_t         _pad2[2];

    /*
     * Replication.
     *
     * A node is a follower when it has been given a leader to stream
     * from, and a leader otherwise. The two roles do not overlap: a
     * follower refuses client writes with the leader's address, and a
     * leader has no one to stream from.
     */
    bool_t          is_follower;
    bool_t          repl_connecting;
    bool_t          repl_want_out;
    bool_t          repl_closing;
    uint8_t         _pad3[4];
    int32_t         timer_fd;       /* -1 until something needs a clock */
    uint32_t        tick;           /* timer expiries seen */

    const char     *leader_text;    /* address as text, for NOT_LEADER */
    sockaddr_in_t   leader_addr;    /* read by the kernel at connect time */

    /* Follower: the one stream from the leader. */
    int32_t         repl_fd;        /* -1 when it is down */
    int32_t         repl_send_len;
    uint32_t        repl_next_dial; /* tick to try again at */
    conn_t          repl_conn;
    uint64_t        repl_acked;     /* durable sequence last reported */
    uint16_t        repl_last_err;  /* last refusal reported, 0 for none */
    uint8_t         _pad5[6];

    /* Leader: what each attached replica has confirmed. */
    bool_t          peer_live[LOOP_MAX_REPLICAS];
    uint8_t         _pad4[6];
    uint64_t        peer_durable[LOOP_MAX_REPLICAS];
    wal_cursor_t    peer_cursor[LOOP_MAX_REPLICAS];

    /* Counters, for the metrics endpoint and for tests */
    uint64_t        accepted;
    uint64_t        closed;
    uint64_t        writes;
    uint64_t        dedup_hits;
    uint64_t        flushes;
    uint64_t        repl_records;   /* records streamed, or applied */
    uint64_t        repl_degraded;  /* writes acknowledged without a copy */
    uint64_t        notified;       /* records pushed to subscribers */
} loop_t;

/*
 * Watch a signalfd. A readable one stops the loop after the pass it
 * arrives in, so a shutdown never lands between an append and the
 * flush that makes it durable.
 */
void loop_set_signal_fd(loop_t *l, int32_t fd);

/*
 * Make this node a follower of the leader at addr.
 *
 * text is the same address in the form a client can dial, and is what
 * a write arriving here is answered with. It is not copied, so it has
 * to outlive the loop.
 *
 * The replica dials the leader rather than the other way round,
 * because the sequence the stream starts from is the replica's to
 * name.
 */
result_t loop_set_leader(loop_t *l, err_t *e, const sockaddr_in_t *addr,
                         const char *text);

/*
 * Bind, listen, and prepare the epoll instance. port 0 asks the kernel
 * to choose one, which it reports back in l->port.
 *
 * bind_ip is in host byte order. A node on the fleet binds its mesh
 * address and nothing else: the port carries client writes and the
 * replication stream both, and neither has any business on a public
 * interface.
 */
result_t loop_init(loop_t *l, err_t *e, wal_t *wal,
                   session_table_t *sessions, uint32_t bind_ip,
                   uint16_t port);

/*
 * One pass: dispatch every descriptor the kernel reports ready, flush
 * the log once if anything was appended, then send the replies that
 * were waiting on it.
 *
 * With wait set, the loop blocks until at least one descriptor is
 * ready. Without it the pass returns having done whatever was
 * possible, which is what a caller driving the loop by hand wants.
 */
result_t loop_tick(loop_t *l, err_t *e, bool_t wait);

/* Tick until stopped. */
result_t loop_run(loop_t *l, err_t *e);

/* Close every connection and the listener. */
void loop_shutdown(loop_t *l);

/*
 * What a finished send does with the slot, given the bytes it moved or
 * the errno it failed with. Not static because the self-check calls it
 * directly: the failure it guards against needs a send to fail while
 * bytes are staged, and the kernel cannot be relied on to produce that
 * on cue.
 */
void loop_send_done(loop_t *l, int32_t slot, int32_t res);

/* Connections currently occupying a slot. */
int32_t loop_live_conns(const loop_t *l);

#endif /* MSGSRVD_LOOP_H */

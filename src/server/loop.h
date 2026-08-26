#ifndef MSGSRVD_LOOP_H
#define MSGSRVD_LOOP_H

#include "core/types.h"
#include "core/err.h"
#include "proto/conn.h"
#include "wal/wal.h"
#include "io/uring.h"
#include "sys/linux.h"

/*
 * Single-threaded event loop over one io_uring.
 *
 * Group commit is the reason the loop is shaped the way it is. A tick
 * drains every completion the kernel has ready, appending each write
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
 * A connection alternates strictly between reading and writing: the
 * next read is submitted only once the reply to the last one has gone.
 * A client that pipelines sends many frames per read, so this costs
 * throughput nothing, and it keeps one buffer per direction per
 * connection with no question of reusing one while the kernel still
 * has it.
 */

#define LOOP_MAX_CONNS    64
#define LOOP_RECV_CAP     8192
#define LOOP_SEND_CAP     8192
#define LOOP_MAX_SESSIONS 256

/* Per-connection slot. */
typedef struct {
    conn_t      conn;
    int32_t     fd;             /* -1 when the slot is free */
    int32_t     send_len;       /* bytes staged in the send buffer */
    uint64_t    session;        /* 0 until HELLO */
    uint64_t    ack_client_seq; /* highest client sequence awaiting an ACK */
    uint64_t    ack_wal_seq;    /* WAL sequence that will cover it */
    uint16_t    ack_flags;      /* durability the ACK will report */
    bool_t      in_use;
    bool_t      recv_pending;   /* a read is with the kernel */
    bool_t      send_pending;   /* a write is with the kernel */
    bool_t      ack_due;        /* an ACK is owed once the flush lands */
    bool_t      closing;
    uint8_t     _pad[2];
} loop_conn_t;

/*
 * A session is a client's identity for deduplication. It lives only in
 * memory: after a restart every session is unknown, which is the
 * documented answer to a resume the server cannot honour, and the
 * client opens a new one. Rebuilding the table from the log so that
 * deduplication survives a restart needs the session records the
 * format reserves and is not done here.
 */
typedef struct {
    uint64_t    id;
    uint64_t    last_client_seq;    /* highest made durable */
} loop_session_t;

typedef struct {
    uring_t         ring;
    wal_t          *wal;
    int32_t         listen_fd;
    int32_t         port;           /* host byte order, after binding */
    bool_t          accept_pending;
    bool_t          stop;
    uint8_t         _pad[2];

    loop_conn_t     conns[LOOP_MAX_CONNS];
    loop_session_t  sessions[LOOP_MAX_SESSIONS];
    int32_t         session_count;
    uint64_t        next_session;

    /* Set during a tick when a record was appended and not yet flushed */
    bool_t          wal_dirty;
    uint8_t         _pad2[3];

    /* Counters, for the metrics endpoint and for tests */
    uint64_t        accepted;
    uint64_t        closed;
    uint64_t        writes;
    uint64_t        dedup_hits;
    uint64_t        flushes;
} loop_t;

/*
 * Bind, listen, and prepare the ring. port 0 asks the kernel to choose
 * one, which it reports back in l->port.
 */
result_t loop_init(loop_t *l, err_t *e, wal_t *wal, uint16_t port,
                   uint32_t ring_entries);

/*
 * One pass: submit what is queued, take the completions that are
 * ready, flush the log once if anything was appended, then send the
 * replies that were waiting on it.
 *
 * With wait set, the loop blocks until at least one completion is
 * ready. Without it the pass returns having done whatever was
 * possible, which is what a caller driving the loop by hand wants.
 */
result_t loop_tick(loop_t *l, err_t *e, bool_t wait);

/* Tick until stopped. */
result_t loop_run(loop_t *l, err_t *e);

/* Close every connection and the listener. */
void loop_shutdown(loop_t *l);

/* Connections currently occupying a slot. */
int32_t loop_live_conns(const loop_t *l);

#endif /* MSGSRVD_LOOP_H */

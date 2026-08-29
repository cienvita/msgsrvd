#ifndef MSGSRVD_SELFCHECK_H
#define MSGSRVD_SELFCHECK_H

#include "core/types.h"
#include "core/arena.h"
#include "core/err.h"

/*
 * In-process self-checks.
 *
 * Each returns 0 on success and 1 on failure, and logs what it
 * verified in the debug build. They run in dependency order: a codec
 * failure makes every result above it meaningless, so the caller stops
 * at the first one that fails.
 *
 * These are the project's tests. There is no external harness, because
 * a no-libc binary is easier to exercise from inside itself than to
 * link into one.
 */

int selfcheck_arena(arena_t *a);
int selfcheck_crc32c(void);
int selfcheck_header(void);
int selfcheck_payloads(void);
int selfcheck_wal_record(void);
int selfcheck_wal_segment(void);
int selfcheck_wal(void);
int selfcheck_wal_index(void);
int selfcheck_sessions(void);
int selfcheck_loop(void);
int selfcheck_session_recovery(void);
int selfcheck_send_failure(void);
int selfcheck_replication(void);
int selfcheck_conn_framing(void);
int selfcheck_conn_session(void);
int selfcheck_uring(void);
int selfcheck_err(err_t *e);

/* Run every check in order. Returns 0 if all passed. */
int selfcheck_run(arena_t *a, err_t *e);

#endif /* MSGSRVD_SELFCHECK_H */

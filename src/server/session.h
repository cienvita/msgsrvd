#ifndef MSGSRVD_SESSION_H
#define MSGSRVD_SESSION_H

#include "core/types.h"
#include "wal/record.h"

/*
 * Client sessions, the identity deduplication is keyed on.
 *
 * The table is rebuilt from the log at startup rather than written to
 * it. Every record already carries the session that produced it and
 * that session's own sequence, and recovery already reads every
 * record, so the highest sequence per session is there to be counted.
 * A separate session record would be a second copy of something the
 * log already states.
 *
 * Reusing an identifier is the hazard this has to avoid. Handing a new
 * client an id that appears in the log would measure its first write
 * against a stranger's high-water mark and discard it as a duplicate,
 * silently. So recovery tracks the largest id it sees and the next one
 * issued starts above it, whether or not that session is still in the
 * table.
 *
 * A session that opened but never wrote leaves no trace and is
 * forgotten. That costs nothing: with no records there is nothing to
 * deduplicate against, and the client is told the session is unknown
 * and opens another.
 */

#define SESSION_MAX 1024

typedef struct {
    uint64_t    id;
    uint64_t    last_client_seq;    /* highest made durable */
} session_t;

typedef struct {
    session_t   entries[SESSION_MAX];
    int32_t     count;
    int32_t     _pad;
    uint64_t    next_id;            /* id the next new session will get */
    uint64_t    evicted;            /* dropped to make room, for reporting */
} session_table_t;

void session_table_init(session_table_t *t);

/* The session with this id, or NULL. */
session_t *session_lookup(session_table_t *t, uint64_t id);

/*
 * Issue a new session. When the table is full the lowest id is
 * dropped, which is the oldest, since ids only increase. A client
 * whose session went that way is told it is unknown on resume, the
 * same answer an expired one gets, and opens a new one.
 */
session_t *session_create(session_table_t *t);

/*
 * Account for a record read during recovery. Raises that session's
 * high-water mark and, more importantly, keeps the next id issued
 * above every id the log mentions.
 */
void session_observe(session_table_t *t, uint64_t id, uint64_t client_seq);

/* wal_rec_cb_t shape, so the table can be filled straight from a scan. */
void session_from_record(void *ctx, const wal_rec_t *r);

#endif /* MSGSRVD_SESSION_H */

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
 * silently.
 *
 * So an id is drawn, not counted. A counter would have to say what it
 * had already issued, and the only place to say it that survives both
 * retention and a promotion is the log, which means a record format
 * for it and a flush before an id can be handed out. Drawing from the
 * kernel's pool needs neither: an id that was issued once is not
 * issued again because there are 2^64 of them, and that argument
 * holds whatever retention has deleted and whichever node is leading.
 *
 * The odds are worth stating rather than assuming. Against a million
 * ids the node might still care about, a fresh one collides with
 * probability around 2^-44, and a draw is checked against the live
 * table anyway, so a collision has to be with a session that is only
 * in the log. The checksum this log trusts to tell a good record from
 * a corrupted one is 2^-32, so the weaker of the two guarantees is
 * not this one.
 *
 * Ids therefore carry no order, and nothing may infer age from one.
 * Age is `seen`, which counts arrivals and never leaves memory: it
 * does not have to survive anything, because a table rebuilt from the
 * log is rebuilt in the log's own order.
 *
 * A session that opened but never wrote leaves no trace and is
 * forgotten. That costs nothing: with no records there is nothing to
 * deduplicate against, and the client is told the session is unknown
 * and opens another. It is the case the drawn id matters most for,
 * since a counter could not have known such a session existed.
 */

#define SESSION_MAX 1024

typedef struct {
    uint64_t    id;
    uint64_t    last_client_seq;    /* highest made durable */
    uint64_t    seen;               /* arrival order, for choosing what goes */
} session_t;

typedef struct {
    session_t   entries[SESSION_MAX];
    int32_t     count;
    int32_t     _pad;
    uint64_t    next_seen;          /* the next arrival's order */
    uint64_t    evicted;            /* dropped to make room, for reporting */
} session_table_t;

void session_table_init(session_table_t *t);

/* The session with this id, or NULL. */
session_t *session_lookup(session_table_t *t, uint64_t id);

/*
 * Issue a new session, with an id drawn from the kernel.
 *
 * NULL when no id could be drawn, which is the kernel refusing to
 * give one. Refusing the session is the only safe answer: the
 * alternative is an id from a source the argument above does not
 * cover.
 *
 * When the table is full the least recently seen session is dropped. A
 * client whose session went that way is told it is unknown on resume,
 * the same answer an expired one gets, and opens a new one.
 */
session_t *session_create(session_table_t *t);

/*
 * Account for a record read during recovery, or written now. Raises
 * that session's high-water mark and marks it as the most recently
 * seen, which is what keeps the sessions a client might still resend
 * for ahead of the ones that went quiet.
 */
void session_observe(session_table_t *t, uint64_t id, uint64_t client_seq);

/*
 * Mark a session already in hand as the most recently seen.
 *
 * What session_observe does about age, for a caller that has the entry
 * and does not need it found. The write path is that caller, and a
 * lookup there would walk the table once per record to learn something
 * it already knows.
 */
void session_touch(session_table_t *t, session_t *s);

/* wal_rec_cb_t shape, so the table can be filled straight from a scan. */
void session_from_record(void *ctx, const wal_rec_t *r);

#endif /* MSGSRVD_SESSION_H */

#include "server/session.h"
#include "core/mem.h"
#include "sys/os.h"

void session_table_init(session_table_t *t)
{
    mem_zero((uint8_t *)t, (int32_t)sizeof(*t));
    t->next_seen = 1;
}

session_t *session_lookup(session_table_t *t, uint64_t id)
{
    int32_t i;

    if (id == 0)
        return NULL;

    for (i = 0; i < t->count; i++) {
        if (t->entries[i].id == id)
            return &t->entries[i];
    }
    return NULL;
}

/*
 * Index of the least recently seen session, which is the one to drop.
 *
 * Arrival order and not the id: an id says nothing about age now that
 * it is drawn rather than counted. What this keeps is the sessions a
 * client is most likely to resend for, since the last thing a session
 * did before a reconnect was write.
 */
static int32_t oldest_index(const session_table_t *t)
{
    int32_t i;
    int32_t lowest = 0;

    for (i = 1; i < t->count; i++) {
        if (t->entries[i].seen < t->entries[lowest].seen)
            lowest = i;
    }
    return lowest;
}

/*
 * A slot for a session arriving now, evicting if that is what it takes.
 *
 * The arrival is stamped here and not by the caller, so there is no
 * path onto the table that leaves a slot carrying the age of whatever
 * was in it before. An unstamped slot would not merely be wrong about
 * one session: it would make every entry look the same age and turn
 * eviction into a fixed choice of the first slot.
 */
static session_t *slot_for_new(session_table_t *t)
{
    session_t *s;

    if (t->count >= SESSION_MAX) {
        s = &t->entries[oldest_index(t)];
        t->evicted++;
    } else {
        s = &t->entries[t->count];
        t->count++;
    }

    s->last_client_seq = 0;
    s->seen = t->next_seen++;
    return s;
}

session_t *session_create(session_table_t *t)
{
    session_t *s;
    uint64_t   id = 0;
    int32_t    tries;

    /*
     * A draw that lands on a live session is redrawn rather than
     * accepted, so a collision can only ever be with a session that is
     * in the log and not in the table. Retried a few times and then
     * given up on: at 2^64 the loop is not what protects against a
     * collision, and a pool answering the same number every time is
     * broken in a way no number of retries improves.
     */
    for (tries = 0; tries < 8; tries++) {
        uint64_t candidate = os_random_u64();

        if (candidate != 0 && !session_lookup(t, candidate)) {
            id = candidate;
            break;
        }
    }
    if (id == 0)
        return NULL;

    s = slot_for_new(t);
    s->id = id;
    return s;
}

void session_observe(session_table_t *t, uint64_t id, uint64_t client_seq)
{
    session_t *s;

    if (id == 0)
        return;         /* internal record, no client behind it */

    s = session_lookup(t, id);
    if (!s) {
        /*
         * Recovery walks the log forward, so a session appearing now
         * is more recent than everything already held and takes a slot
         * on those terms. There is nothing to compare ids against: the
         * order that matters is the order the records arrive in.
         */
        s = slot_for_new(t);
        s->id = id;
    }

    if (client_seq > s->last_client_seq)
        s->last_client_seq = client_seq;
    s->seen = t->next_seen++;
}

void session_touch(session_table_t *t, session_t *s)
{
    if (t && s)
        s->seen = t->next_seen++;
}

void session_from_record(void *ctx, const wal_rec_t *r)
{
    session_observe((session_table_t *)ctx, r->session, r->client_seq);
}

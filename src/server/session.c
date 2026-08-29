#include "server/session.h"
#include "core/mem.h"

void session_table_init(session_table_t *t)
{
    mem_zero((uint8_t *)t, (int32_t)sizeof(*t));
    t->next_id = 1;
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

/* Index of the lowest id held, which is the oldest session. */
static int32_t oldest_index(const session_table_t *t)
{
    int32_t i;
    int32_t lowest = 0;

    for (i = 1; i < t->count; i++) {
        if (t->entries[i].id < t->entries[lowest].id)
            lowest = i;
    }
    return lowest;
}

session_t *session_create(session_table_t *t)
{
    session_t *s;

    if (t->count >= SESSION_MAX) {
        s = &t->entries[oldest_index(t)];
        t->evicted++;
    } else {
        s = &t->entries[t->count];
        t->count++;
    }

    s->id = t->next_id++;
    s->last_client_seq = 0;
    return s;
}

void session_observe(session_table_t *t, uint64_t id, uint64_t client_seq)
{
    session_t *s;

    if (id == 0)
        return;         /* internal record, no client behind it */

    /*
     * Do this before anything else. Even a session too old to keep a
     * row for must push the next id past its own, or a future client
     * inherits its sequence numbers.
     */
    if (id >= t->next_id)
        t->next_id = id + 1;

    s = session_lookup(t, id);
    if (s) {
        if (client_seq > s->last_client_seq)
            s->last_client_seq = client_seq;
        return;
    }

    if (t->count < SESSION_MAX) {
        s = &t->entries[t->count];
        t->count++;
    } else {
        int32_t oldest = oldest_index(t);

        /* Nothing to gain by replacing a newer session with an older one. */
        if (id < t->entries[oldest].id) {
            t->evicted++;
            return;
        }
        s = &t->entries[oldest];
        t->evicted++;
    }

    s->id = id;
    s->last_client_seq = client_seq;
}

void session_from_record(void *ctx, const wal_rec_t *r)
{
    session_observe((session_table_t *)ctx, r->session, r->client_seq);
}

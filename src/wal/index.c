#include "wal/index.h"
#include "core/mem.h"

void wal_index_init(wal_index_t *ix)
{
    mem_zero((uint8_t *)ix, (int32_t)sizeof(*ix));
}

static wal_index_key_t *find(wal_index_t *ix, uint64_t key)
{
    int32_t i;

    for (i = 0; i < ix->count; i++) {
        if (ix->keys[i].key == key)
            return &ix->keys[i];
    }
    return NULL;
}

void wal_index_add(wal_index_t *ix, const wal_rec_t *r)
{
    wal_index_key_t *k;

    if (!ix || !r || r->seq == 0)
        return;

    k = find(ix, r->partition_key);
    if (!k) {
        if (ix->count >= WAL_INDEX_KEYS) {
            /*
             * No room. Readers after this key are told nothing rather
             * than told something that is only true of the keys that
             * did fit.
             */
            ix->unknown++;
            return;
        }
        k = &ix->keys[ix->count++];
        k->key = r->partition_key;
        k->first = r->seq;
        k->count = 0;
    }

    k->last = r->seq;
    k->count++;
}

bool_t wal_index_range(const wal_index_t *ix, uint64_t key, uint64_t from,
                       uint64_t *out_from, uint64_t *out_last)
{
    int32_t i;

    if (!ix || !out_from || !out_last)
        return FALSE;

    for (i = 0; i < ix->count; i++) {
        const wal_index_key_t *k = &ix->keys[i];

        if (k->key != key)
            continue;

        /*
         * Nothing of this key exists before its first record, so a
         * reader asking from earlier than that starts there instead.
         * Asking from later is left alone: where its next record is
         * after that point is not something this knows.
         */
        *out_from = (from < k->first) ? k->first : from;
        *out_last = k->last;
        return TRUE;
    }

    return FALSE;
}

bool_t wal_index_complete(const wal_index_t *ix)
{
    return (ix && ix->unknown == 0) ? TRUE : FALSE;
}

int32_t wal_index_count(const wal_index_t *ix)
{
    return ix ? ix->count : 0;
}

const wal_index_key_t *wal_index_at(const wal_index_t *ix, int32_t i)
{
    if (!ix || i < 0 || i >= ix->count)
        return NULL;
    return &ix->keys[i];
}

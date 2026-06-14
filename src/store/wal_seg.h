#ifndef MSGSRVD_WAL_SEG_H
#define MSGSRVD_WAL_SEG_H

#include "core/types.h"

/*
 * WAL segment file naming.
 *
 * A segment's filename is its base wal_seq as 16 lowercase hex digits
 * followed by ".wal", e.g. "000000000000002a.wal". Fixed width so a
 * lexicographic directory listing is also sequence order, and so the
 * name parses back without scanning.
 *
 * Pure formatting, no I/O. The append path names the active segment;
 * recovery enumerates a directory and parses each name back to a base
 * sequence.
 */

/* Filename length excluding the terminating NUL: 16 hex + ".wal". */
#define WAL_SEG_NAME_LEN 20

/*
 * Format base_seq into out as "<16 hex>.wal". out must hold at least
 * WAL_SEG_NAME_LEN + 1 bytes; a terminating NUL is written.
 */
static inline void wal_seg_name(uint64_t base_seq, char *out)
{
    static const char hexd[] = "0123456789abcdef";
    int32_t i;

    for (i = 15; i >= 0; i--) {
        out[i] = hexd[base_seq & 0xF];
        base_seq >>= 4;
    }
    out[16] = '.';
    out[17] = 'w';
    out[18] = 'a';
    out[19] = 'l';
    out[20] = '\0';
}

/*
 * Parse a segment filename back to its base sequence. Returns TRUE and
 * fills *base_seq on a well-formed "<16 hex>.wal" name (lowercase hex,
 * NUL-terminated at WAL_SEG_NAME_LEN). Returns FALSE otherwise.
 */
static inline bool_t wal_seg_parse(const char *name, uint64_t *base_seq)
{
    uint64_t v = 0;
    int32_t i;

    for (i = 0; i < 16; i++) {
        char ch = name[i];
        uint64_t d;

        if (ch >= '0' && ch <= '9')
            d = (uint64_t)(ch - '0');
        else if (ch >= 'a' && ch <= 'f')
            d = (uint64_t)(ch - 'a' + 10);
        else
            return FALSE;

        v = (v << 4) | d;
    }

    if (name[16] != '.' || name[17] != 'w' || name[18] != 'a' ||
        name[19] != 'l' || name[20] != '\0')
        return FALSE;

    *base_seq = v;
    return TRUE;
}

#endif /* MSGSRVD_WAL_SEG_H */

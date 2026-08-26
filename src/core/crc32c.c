#include "core/crc32c.h"

/*
 * CPUID leaf 1, ECX bit 20 is SSE4.2. Called once at startup; the
 * result is not cached here because nothing calls it on a hot path.
 */
bool_t crc32c_available(void)
{
    uint32_t eax, ebx, ecx, edx;

    __asm__ volatile("cpuid"
                     : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx)
                     : "a"(1), "c"(0));
    (void)eax;
    (void)ebx;
    (void)edx;

    return (ecx & (1u << 20)) ? TRUE : FALSE;
}

/*
 * The target attribute enables the instruction for this function
 * alone, so the rest of the program stays at the baseline ISA and the
 * Makefile needs no -msse4.2.
 *
 * Eight bytes at a time, then the tail. The 8-byte step reads through
 * a uint64_t copied out of the buffer rather than a cast, since WAL
 * records are 8-byte aligned on disk but a caller checksumming a
 * payload slice need not be.
 */
__attribute__((target("sse4.2")))
uint32_t crc32c(uint32_t crc, const uint8_t *buf, int32_t len)
{
    uint32_t c = ~crc;
    int32_t  i = 0;

    if (!buf || len <= 0)
        return crc;

    while (len - i >= 8) {
        uint64_t chunk;
        int32_t  j;

        for (j = 0; j < 8; j++)
            ((uint8_t *)&chunk)[j] = buf[i + j];

        c = (uint32_t)__builtin_ia32_crc32di(c, chunk);
        i += 8;
    }

    while (i < len) {
        c = __builtin_ia32_crc32qi(c, buf[i]);
        i++;
    }

    return ~c;
}

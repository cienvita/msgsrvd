#include "core/types.h"
#include "core/arena.h"
#include "core/err.h"
#include "core/debug.h"
#include "core/crc32c.h"
#include "proto/msg.h"
#include "wal/wal.h"
#include "server/loop.h"
#include "server/selfcheck.h"
#include "sys/os.h"

/*
 * Entry point.
 *
 * Two modes, chosen explicitly rather than by build: --selfcheck runs
 * the in-process checks and exits, and --dir runs the server. Neither
 * is the default, because starting a durable log is not something to
 * do by accident on a bare invocation.
 */

#define ARENA_SIZE       (4 * 1024 * 1024)
#define DEFAULT_PORT     7400
#define DEFAULT_SEG_SIZE ((int64_t)256 * 1024 * 1024)

static uint8_t arena_buf[ARENA_SIZE];

/* Scan and recovery buffer: big enough for the largest record on disk. */
static uint8_t scratch[WAL_REC_MAX_SIZE];

/* ---- output, without libc ---- */

static int32_t str_len(const char *s)
{
    int32_t n = 0;

    while (s[n] != '\0')
        n++;
    return n;
}

static void out(int32_t fd, const char *s)
{
    os_write_raw(fd, s, (size_t)str_len(s));
}

static void out_u64(int32_t fd, uint64_t v)
{
    char    digits[24];
    int32_t n = 0;

    if (v == 0)
        digits[n++] = '0';
    while (v > 0) {
        digits[n++] = (char)('0' + (int32_t)(v % 10));
        v /= 10;
    }
    while (n > 0) {
        char c = digits[--n];
        os_write_raw(fd, &c, 1);
    }
}

/* ---- arguments ---- */

static bool_t str_eq(const char *a, const char *b)
{
    int32_t i = 0;

    while (a[i] != '\0' && b[i] != '\0') {
        if (a[i] != b[i])
            return FALSE;
        i++;
    }
    return (a[i] == b[i]) ? TRUE : FALSE;
}

/*
 * Decimal only, and no silent truncation: a size given wrongly should
 * stop the server rather than quietly become a different size.
 */
static bool_t parse_u64(const char *s, uint64_t *out_v)
{
    uint64_t v = 0;
    int32_t  i = 0;

    if (!s || s[0] == '\0')
        return FALSE;

    while (s[i] != '\0') {
        if (s[i] < '0' || s[i] > '9')
            return FALSE;
        if (v > (uint64_t)0xFFFFFFFFFFFFFFFFULL / 10)
            return FALSE;
        v = v * 10 + (uint64_t)(s[i] - '0');
        i++;
    }

    *out_v = v;
    return TRUE;
}

static void usage(void)
{
    out(2,
        "msgsrvd\n"
        "\n"
        "  msgsrvd --dir PATH [--port N] [--segment-size BYTES]\n"
        "      Serve the write-ahead log in PATH. The directory must\n"
        "      exist. --port 0 asks the kernel to choose one.\n"
        "      Defaults: --port 7400, --segment-size 268435456.\n"
        "\n"
        "  msgsrvd --selfcheck\n"
        "      Run the in-process checks and exit.\n");
}

/* ---- server ---- */

static int run_server(const char *dir, uint16_t port, int64_t seg_size)
{
    err_t      e;
    wal_t      wal;
    wal_open_t info;
    loop_t     loop;
    int32_t    sig_fd = -1;
    uint64_t   mask = SIGMASK(SIGINT) | SIGMASK(SIGTERM);
    int        rc = 1;

    err_init(&e);

    /*
     * The checksum runs on an instruction the WAL cannot do without.
     * Refusing here beats taking SIGILL on the first record written.
     */
    if (!crc32c_available()) {
        out(2, "msgsrvd: this CPU has no SSE4.2, which CRC32C needs\n");
        return 1;
    }

    if (!result_ok(wal_open(&wal, &e, dir, seg_size, scratch,
                            (int32_t)sizeof(scratch), &info))) {
        out(2, "msgsrvd: cannot open the log in ");
        out(2, dir);
        out(2, "\n");
        dbg_err_print(&e);
        return 1;
    }

    out(1, "msgsrvd: log ");
    out(1, dir);
    out(1, " segments=");
    out_u64(1, (uint64_t)info.segments);
    out(1, " records=");
    out_u64(1, info.records);
    out(1, " first=");
    out_u64(1, info.first_seq);
    out(1, " last=");
    out_u64(1, info.last_seq);
    if (info.torn)
        out(1, " (a torn tail was dropped)");
    out(1, "\n");

    /*
     * Block first, then take the descriptor. Between the two the
     * signals are held rather than defaulting to killing the process.
     */
    if (!result_ok(os_sigblock(&e, mask)) ||
        !result_ok(os_signalfd(&e, mask, SFD_CLOEXEC, &sig_fd))) {
        out(2, "msgsrvd: cannot take a signal descriptor\n");
        goto out_wal;
    }

    if (!result_ok(loop_init(&loop, &e, &wal, port, 256))) {
        out(2, "msgsrvd: cannot start the event loop. If this is an EL\n"
               "kernel, check kernel.io_uring_disabled: it ships at 2,\n"
               "which turns io_uring off for everything.\n");
        dbg_err_print(&e);
        os_close(&e, sig_fd);
        goto out_wal;
    }
    loop_set_signal_fd(&loop, sig_fd);

    out(1, "msgsrvd: listening on 127.0.0.1:");
    out_u64(1, (uint64_t)loop.port);
    out(1, "\n");

    if (!result_ok(loop_run(&loop, &e))) {
        out(2, "msgsrvd: the event loop stopped on an error\n");
        dbg_err_print(&e);
        loop_shutdown(&loop);
        goto out_wal;
    }

    loop_shutdown(&loop);

    /*
     * Anything appended in the pass that saw the signal has not been
     * acknowledged, but flushing it costs one operation and keeps the
     * log ending on a record boundary rather than a dropped tail.
     */
    if (!result_ok(wal_sync(&wal, &e))) {
        out(2, "msgsrvd: the final flush failed\n");
        dbg_err_print(&e);
        goto out_wal;
    }

    out(1, "msgsrvd: stopped at sequence ");
    out_u64(1, wal_durable_seq(&wal));
    out(1, "\n");
    rc = 0;

out_wal:
    wal_close(&wal, &e);
    return rc;
}

int main(int argc, char **argv)
{
    const char *dir = NULL;
    uint64_t    port = DEFAULT_PORT;
    uint64_t    seg_size = (uint64_t)DEFAULT_SEG_SIZE;
    bool_t      selfcheck = FALSE;
    int32_t     i;

    for (i = 1; i < argc; i++) {
        /*
         * A known flag missing its value is a different mistake from
         * an unknown flag, and saying "unrecognised" about a flag that
         * is in the usage text sends the reader looking for a typo
         * that is not there.
         */
        if ((str_eq(argv[i], "--dir") || str_eq(argv[i], "--port") ||
             str_eq(argv[i], "--segment-size")) && i + 1 >= argc) {
            out(2, "msgsrvd: ");
            out(2, argv[i]);
            out(2, " needs a value\n\n");
            usage();
            return 2;
        }

        if (str_eq(argv[i], "--selfcheck")) {
            selfcheck = TRUE;
        } else if (str_eq(argv[i], "--dir")) {
            dir = argv[++i];
        } else if (str_eq(argv[i], "--port")) {
            if (!parse_u64(argv[++i], &port) || port > 65535) {
                out(2, "msgsrvd: --port must be 0 to 65535\n");
                return 2;
            }
        } else if (str_eq(argv[i], "--segment-size")) {
            if (!parse_u64(argv[++i], &seg_size) ||
                seg_size < (uint64_t)WAL_REC_MAX_SIZE) {
                out(2, "msgsrvd: --segment-size must hold one whole record\n");
                return 2;
            }
        } else {
            out(2, "msgsrvd: unrecognised argument: ");
            out(2, argv[i]);
            out(2, "\n\n");
            usage();
            return 2;
        }
    }

    if (selfcheck) {
        arena_t arena;
        err_t   err;

        arena_init(&arena, arena_buf, ARENA_SIZE);
        err_init(&err);

        DBG_LOG("msgsrvd: self-check, protocol v%d, header %d bytes",
                MSG_VERSION, MSG_HEADER_SIZE);

        if (selfcheck_run(&arena, &err)) {
            out(2, "msgsrvd: self-check failed\n");
            return 1;
        }
        out(1, "msgsrvd: self-check passed\n");
        return 0;
    }

    if (!dir) {
        usage();
        return 2;
    }

    return run_server(dir, (uint16_t)port, (int64_t)seg_size);
}

#include "core/types.h"
#include "core/arena.h"
#include "core/err.h"
#include "core/debug.h"
#include "core/crc32c.h"
#include "proto/msg.h"
#include "wal/wal.h"
#include "server/loop.h"
#include "server/session.h"
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

/*
 * Parse "A.B.C.D" and report where it stopped. Numeric only: there is
 * no resolver here and a node's peers are mesh addresses, which are
 * the thing a name would have to resolve to anyway.
 */
static bool_t parse_ip(const char *s, uint32_t *out_ip, int32_t *out_at)
{
    uint32_t ip = 0;
    int32_t  i = 0;
    int32_t  octet;

    if (!s)
        return FALSE;

    for (octet = 0; octet < 4; octet++) {
        uint32_t v = 0;
        int32_t  digits = 0;

        while (s[i] >= '0' && s[i] <= '9') {
            v = v * 10 + (uint32_t)(s[i] - '0');
            if (v > 255)
                return FALSE;
            digits++;
            i++;
        }
        if (digits == 0)
            return FALSE;

        ip = (ip << 8) | v;

        if (octet < 3) {
            if (s[i] != '.')
                return FALSE;
            i++;
        }
    }

    *out_ip = ip;
    if (out_at)
        *out_at = i;
    return TRUE;
}

/* Parse "A.B.C.D:PORT" into a socket address. */
static bool_t parse_addr(const char *s, sockaddr_in_t *out)
{
    uint32_t ip = 0;
    uint64_t port = 0;
    int32_t  i = 0;

    if (!parse_ip(s, &ip, &i))
        return FALSE;

    if (s[i] != ':')
        return FALSE;
    i++;

    if (!parse_u64(s + i, &port) || port == 0 || port > 65535)
        return FALSE;

    mem_zero((uint8_t *)out, (int32_t)sizeof(*out));
    out->sin_family = AF_INET;
    out->sin_port = htons((uint16_t)port);
    out->sin_addr = htonl(ip);
    return TRUE;
}

static void usage(void)
{
    out(2,
        "msgsrvd\n"
        "\n"
        "  msgsrvd --dir PATH [--bind ADDR] [--port N]\n"
        "          [--segment-size BYTES] [--leader ADDR:PORT]\n"
        "      Serve the write-ahead log in PATH. The directory must\n"
        "      exist. --port 0 asks the kernel to choose one.\n"
        "      Defaults: --bind 127.0.0.1, --port 7400,\n"
        "      --segment-size 268435456.\n"
        "\n"
        "      With --leader this node is a replica: it streams that\n"
        "      node's log into its own and answers a client's write\n"
        "      with the leader's address. Without it, it is the leader\n"
        "      and replicas stream from it.\n"
        "\n"
        "  msgsrvd --dir PATH --inspect\n"
        "      Scan the log, say what is in it, and exit. Nothing is\n"
        "      listened on. Answers which of two nodes is further\n"
        "      along, which is what a promotion has to know.\n"
        "\n"
        "  msgsrvd --selfcheck\n"
        "      Run the in-process checks and exit.\n");
}

/* ---- log ---- */

/* The one line both modes print about a log they have just scanned. */
static void report_log(const char *dir, const wal_open_t *info,
                       int32_t sessions)
{
    out(1, "msgsrvd: log ");
    out(1, dir);
    out(1, " segments=");
    out_u64(1, (uint64_t)info->segments);
    out(1, " records=");
    out_u64(1, info->records);
    out(1, " first=");
    out_u64(1, info->first_seq);
    out(1, " last=");
    out_u64(1, info->last_seq);
    out(1, " sessions=");
    out_u64(1, (uint64_t)sessions);
    if (info->torn)
        out(1, " (a torn tail was dropped)");
    out(1, "\n");
}

/*
 * Scan a log and say what is in it, without listening for anything.
 *
 * This is what a failover asks: of two replicas, which one is further
 * along. Nothing else can answer it while the node is stopped, and
 * starting a node to find out means binding a port and joining a
 * cluster to read one number.
 */
static int inspect_log(const char *dir, int64_t seg_size)
{
    err_t           e;
    wal_t           wal;
    wal_open_t      info;
    session_table_t sessions;

    err_init(&e);

    if (!crc32c_available()) {
        out(2, "msgsrvd: this CPU has no SSE4.2, which CRC32C needs\n");
        return 1;
    }

    session_table_init(&sessions);

    if (!result_ok(wal_open(&wal, &e, dir, seg_size, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &sessions))) {
        out(2, "msgsrvd: cannot open the log in ");
        out(2, dir);
        out(2, "\n");
        dbg_err_print(&e);
        return 1;
    }

    report_log(dir, &info, sessions.count);

    /*
     * What keys the log holds, which nothing else can answer: there is
     * no verb that lists them and no metric that counts them.
     */
    {
        const wal_index_t *ix = wal_index(&wal);
        int32_t            i;

        for (i = 0; i < wal_index_count(ix); i++) {
            const wal_index_key_t *k = wal_index_at(ix, i);

            out(1, "msgsrvd:   key ");
            out_u64(1, k->key);
            out(1, " records=");
            out_u64(1, k->count);
            out(1, " first=");
            out_u64(1, k->first);
            out(1, " last=");
            out_u64(1, k->last);
            out(1, "\n");
        }
        if (ix->unknown > 0) {
            out(1, "msgsrvd:   and ");
            out_u64(1, ix->unknown);
            out(1, " records whose keys did not fit the index\n");
        }
    }

    wal_close(&wal, &e);
    return 0;
}

/* ---- server ---- */

static int run_server(const char *dir, uint32_t bind_ip, const char *bind_text,
                      uint16_t port, int64_t seg_size, const char *leader)
{
    sockaddr_in_t   leader_addr;
    err_t           e;
    wal_t           wal;
    wal_open_t      info;
    loop_t          loop;
    session_table_t sessions;
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

    /*
     * The scan fills the session table as it goes, so deduplication
     * picks up where the last run left off instead of starting blank
     * and storing a client's retries a second time.
     */
    session_table_init(&sessions);

    if (!result_ok(wal_open(&wal, &e, dir, seg_size, scratch,
                            (int32_t)sizeof(scratch), &info,
                            session_from_record, &sessions))) {
        out(2, "msgsrvd: cannot open the log in ");
        out(2, dir);
        out(2, "\n");
        dbg_err_print(&e);
        return 1;
    }

    report_log(dir, &info, sessions.count);

    /*
     * Block first, then take the descriptor. Between the two the
     * signals are held rather than defaulting to killing the process.
     */
    if (!result_ok(os_sigblock(&e, mask)) ||
        !result_ok(os_signalfd(&e, mask, SFD_CLOEXEC | SFD_NONBLOCK, &sig_fd))) {
        out(2, "msgsrvd: cannot take a signal descriptor\n");
        goto out_wal;
    }

    if (leader && !parse_addr(leader, &leader_addr)) {
        out(2, "msgsrvd: --leader wants an address like 192.0.2.13:7400\n");
        goto out_wal;
    }

    if (!result_ok(loop_init(&loop, &e, &wal, &sessions, bind_ip, port))) {
        out(2, "msgsrvd: cannot start the event loop\n");
        dbg_err_print(&e);
        os_close(&e, sig_fd);
        goto out_wal;
    }
    loop_set_signal_fd(&loop, sig_fd);

    if (leader && !result_ok(loop_set_leader(&loop, &e, &leader_addr,
                                             leader))) {
        out(2, "msgsrvd: cannot take a timer, which a replica needs\n");
        dbg_err_print(&e);
        loop_shutdown(&loop);
        goto out_wal;
    }

    out(1, "msgsrvd: listening on ");
    out(1, bind_text);
    out(1, ":");
    out_u64(1, (uint64_t)loop.port);
    if (leader) {
        out(1, " replica of ");
        out(1, leader);
    } else {
        out(1, " leader");
    }
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
    const char *leader = NULL;
    const char *bind_text = "127.0.0.1";
    uint32_t    bind_ip = 0x7F000001;
    uint64_t    port = DEFAULT_PORT;
    uint64_t    seg_size = (uint64_t)DEFAULT_SEG_SIZE;
    bool_t      selfcheck = FALSE;
    bool_t      inspect = FALSE;
    int32_t     i;

    for (i = 1; i < argc; i++) {
        /*
         * A known flag missing its value is a different mistake from
         * an unknown flag, and saying "unrecognised" about a flag that
         * is in the usage text sends the reader looking for a typo
         * that is not there.
         */
        if ((str_eq(argv[i], "--dir") || str_eq(argv[i], "--port") ||
             str_eq(argv[i], "--segment-size") ||
             str_eq(argv[i], "--bind") ||
             str_eq(argv[i], "--leader")) && i + 1 >= argc) {
            out(2, "msgsrvd: ");
            out(2, argv[i]);
            out(2, " needs a value\n\n");
            usage();
            return 2;
        }

        if (str_eq(argv[i], "--selfcheck")) {
            selfcheck = TRUE;
        } else if (str_eq(argv[i], "--inspect")) {
            inspect = TRUE;
        } else if (str_eq(argv[i], "--dir")) {
            dir = argv[++i];
        } else if (str_eq(argv[i], "--port")) {
            if (!parse_u64(argv[++i], &port) || port > 65535) {
                out(2, "msgsrvd: --port must be 0 to 65535\n");
                return 2;
            }
        } else if (str_eq(argv[i], "--bind")) {
            bind_text = argv[++i];
            if (!parse_ip(bind_text, &bind_ip, NULL)) {
                out(2, "msgsrvd: --bind wants an address like 192.0.2.13\n");
                return 2;
            }
        } else if (str_eq(argv[i], "--leader")) {
            leader = argv[++i];
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

    if (inspect)
        return inspect_log(dir, (int64_t)seg_size);

    return run_server(dir, bind_ip, bind_text, (uint16_t)port,
                      (int64_t)seg_size, leader);
}

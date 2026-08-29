//! msgsrv: driving and watching a cluster by hand.
//!
//! A tool for a person at a terminal, not a harness. It writes one
//! record at a time and says what came back, reads a node's log off
//! disk, and shows how far each node has got. Between them that is
//! enough to watch replication happen, watch it stop, and watch a
//! promotion put it back.
//!
//! Reading logs off disk is not a shortcut around a client API; there
//! is no read path in the server yet, so the files are the only way to
//! see what a node holds.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use msgsrv_client::log;
use msgsrv_client::proto::{self, err, flag, op, Header, HEADER_SIZE};
use msgsrv_client::{Client, Config, Durability, Watch};

const USAGE: &str = "\
msgsrv - drive and watch a msgsrvd cluster by hand

  msgsrv write --addr IP:PORT [--level L] [--count N] [--rate R]
               [--text S] [--key N]
      Write records one at a time and print what came back for each.
      Levels: appended, sync, replicated, degraded. Default: sync,
      two a second, until interrupted.

      A write sent to a replica is not refused to your face: the
      client is told where the leader is and goes there, so the record
      lands and the only sign is the address in `status`.

  msgsrv status --addr IP:PORT [--addr IP:PORT ...]
      One line per node: whether it answers, and whether it is the
      leader. Asking costs one empty record on the leader, because
      nothing in the protocol asks a node what it is; the answer only
      exists as a refusal to a write.

  msgsrv notify --addr IP:PORT [--key N] [--from SEQ] [--min-seq N]
      Follow the log over the network and print records as the node
      sends them. --from replays from a sequence first; without it
      only what is written from now on arrives. --key follows one key,
      and without it every key does.

      Any node will do, leader or replica: a subscription is served
      from the log the node holds. --min-seq is the sequence an
      acknowledgement gave you, and holds the answer back until this
      node has caught up that far.

  msgsrv read --addr IP:PORT [--key N] [--min-seq N]
      Everything the node holds for a key, and then stop.

  msgsrv tail --dir PATH [--from SEQ] [--follow]
      Print records out of a node's log, and keep printing them as
      they arrive with --follow.

  msgsrv keys --dir PATH
      What keys a log holds: where each one starts, where it ends, and
      how many records it has. Counted from the files, so it works on
      a node that is stopped. Nothing asks a running node this over
      the network; there is no verb for it.

  msgsrv watch --node NAME=PATH [--node NAME=PATH ...]
      A line per node, refreshed in place: records held, last
      sequence, and how far behind the furthest node it is. This is
      the replication view.
";

/// A mistake in how the tool was called: worth showing the usage.
fn die(msg: &str) -> ! {
    eprintln!("msgsrv: {msg}\n\n{USAGE}");
    std::process::exit(2);
}

/// Something the tool asked for and did not get. The usage would not
/// help, so it is not printed.
fn fail(msg: &str) -> ! {
    eprintln!("msgsrv: {msg}");
    std::process::exit(1);
}

struct Args {
    addrs: Vec<SocketAddr>,
    nodes: Vec<(String, PathBuf)>,
    dir: Option<PathBuf>,
    level: Durability,
    count: u64,
    rate: f64,
    text: String,
    key: u64,
    from: u64,
    min_seq: u64,
    key_set: bool,
    follow: bool,
}

fn parse(argv: &[String]) -> Args {
    let mut a = Args {
        addrs: Vec::new(),
        nodes: Vec::new(),
        dir: None,
        level: Durability::Sync,
        count: 0,
        rate: 2.0,
        text: String::new(),
        key: 0,
        from: 0,
        min_seq: 0,
        key_set: false,
        follow: false,
    };

    let mut i = 0;
    while i < argv.len() {
        let val = || -> String {
            argv.get(i + 1)
                .unwrap_or_else(|| die(&format!("{} needs a value", argv[i])))
                .clone()
        };
        match argv[i].as_str() {
            "--addr" => {
                a.addrs
                    .push(val().parse().unwrap_or_else(|_| die("--addr wants IP:PORT")))
            }
            "--node" => {
                let v = val();
                let (name, path) = v
                    .split_once('=')
                    .unwrap_or_else(|| die("--node wants NAME=PATH"));
                a.nodes.push((name.to_string(), PathBuf::from(path)));
            }
            "--dir" => a.dir = Some(PathBuf::from(val())),
            "--level" => {
                a.level = match val().as_str() {
                    "appended" => Durability::Appended,
                    "sync" => Durability::Sync,
                    "replicated" => Durability::Replicated,
                    "degraded" => Durability::ReplicatedOrDegraded,
                    other => die(&format!("unknown level {other}")),
                }
            }
            "--count" => a.count = val().parse().unwrap_or_else(|_| die("--count wants a number")),
            "--rate" => a.rate = val().parse().unwrap_or_else(|_| die("--rate wants a number")),
            "--text" => a.text = val(),
            "--key" => {
                a.key = val().parse().unwrap_or_else(|_| die("--key wants a number"));
                a.key_set = true;
            }
            "--min-seq" => {
                a.min_seq = val().parse().unwrap_or_else(|_| die("--min-seq wants a number"))
            }
            "--from" => a.from = val().parse().unwrap_or_else(|_| die("--from wants a number")),
            "--follow" | "-f" => {
                a.follow = true;
                i -= 1; /* no value */
            }
            other => die(&format!("unrecognised argument {other}")),
        }
        i += 2;
    }
    a
}

/* ---- write ---- */

async fn cmd_write(a: Args) {
    let addr = *a.addrs.first().unwrap_or_else(|| die("write needs --addr"));

    let mut cfg = Config::new(vec![addr]);
    cfg.name = String::from("msgsrv-cli");
    let client = Client::connect(cfg)
        .await
        .unwrap_or_else(|e| fail(&format!("cannot reach {addr}: {e}")));

    let level = match a.level {
        Durability::Appended => "appended",
        Durability::Sync => "sync",
        Durability::Replicated => "replicated",
        Durability::ReplicatedOrDegraded => "degraded",
    };
    println!("session {} at {addr}, writing {level}", client.session());
    println!();

    let gap = if a.rate > 0.0 {
        Duration::from_secs_f64(1.0 / a.rate)
    } else {
        Duration::ZERO
    };

    let mut n = 0u64;
    loop {
        n += 1;
        if a.count > 0 && n > a.count {
            break;
        }

        let body = if a.text.is_empty() {
            format!("record {n}")
        } else {
            format!("{} {n}", a.text)
        };

        let began = Instant::now();
        let sent = tokio::time::Instant::now();
        match client.write(a.key, body.clone().into_bytes(), a.level).await {
            Ok(ack) => println!(
                "  #{n:<4} wal={:<6} {:<11} {:>7.1?}  {body}",
                ack.seq.map(|s| s.to_string()).unwrap_or_else(|| String::from("?")),
                if ack.degraded { "DEGRADED" } else { level },
                began.elapsed()
            ),
            Err(e) => println!("  #{n:<4} refused    {e}"),
        }

        if !gap.is_zero() {
            tokio::time::sleep_until(sent + gap).await;
        }
    }
}

/* ---- notify and read ---- */

async fn cmd_watch(a: Args, once: bool) {
    let addr = *a
        .addrs
        .first()
        .unwrap_or_else(|| die("this needs --addr"));

    let mut cfg = Config::new(vec![addr]);
    cfg.name = String::from("msgsrv-cli");
    let client = Client::connect(cfg)
        .await
        .unwrap_or_else(|e| fail(&format!("cannot reach {addr}: {e}")));

    let watch = Watch {
        key: if a.key_set { Some(a.key) } else { None },
        from_seq: a.from,
        min_seq: a.min_seq,
    };

    let what = if once { "reading" } else { "following" };
    let scope = match watch.key {
        Some(k) => format!("key {k}"),
        None => String::from("every key"),
    };

    let mut sub = match if once {
        client.read(watch).await
    } else {
        client.subscribe(watch).await
    } {
        Ok(s) => s,
        Err(e) => fail(&format!("{addr} refused: {e}")),
    };

    println!("{what} {scope} on {addr}, from sequence {}", sub.start());
    println!();

    let mut n = 0u64;
    while let Some(r) = sub.next().await {
        n += 1;
        println!(
            "  seq={:<8} key={:<6} {}",
            r.seq,
            r.partition_key,
            String::from_utf8_lossy(&r.data)
        );
    }

    if once {
        println!("\n  {n} records");
    } else {
        println!("\n  the stream ended after {n} records");
    }
}

/* ---- status ---- */

/// Ask a node whether it is the leader, the only way there is.
///
/// There is no verb for it. A write is refused by a replica with the
/// leader's address, and taken by a leader, so asking costs one empty
/// record on whichever node turns out to be in charge.
fn probe(addr: SocketAddr) -> String {
    let mut sock = match TcpStream::connect_timeout(&addr, Duration::from_secs(3)) {
        Ok(s) => s,
        Err(e) => return format!("down       {e}"),
    };
    let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));

    let hello = proto::encode_hello(0, "msgsrv-cli");
    let h = Header::new(op::HELLO, 0, hello.len() as u32, 0, 0);
    let mut out = h.encode().to_vec();
    out.extend_from_slice(&hello);
    if sock.write_all(&out).is_err() {
        return String::from("down       the connection went away");
    }

    let (h, _) = match read_frame(&mut sock) {
        Some(f) => f,
        None => return String::from("down       no answer to a session request"),
    };
    if h.op != op::ACK {
        return String::from("odd        answered a session request with something else");
    }
    let session = h.sequence;

    let w = Header::new(op::WRITE, flag::ACK_REQ | flag::SYNC, 0, 0, 1);
    if sock.write_all(&w.encode()).is_err() {
        return format!("up         session={session}, then the connection went away");
    }

    match read_frame(&mut sock) {
        Some((h, _)) if h.op == op::ACK => {
            format!("up         session={session}  LEADER  (log at {})", h.sequence)
        }
        Some((_, payload)) => match proto::decode_err(&payload) {
            Ok((err::NOT_LEADER, Some(text))) => {
                format!("up         session={session}  replica of {text}")
            }
            Ok((code, _)) => format!("up         session={session}  refused: {}", err::name(code)),
            Err(e) => format!("up         session={session}  unreadable refusal: {e}"),
        },
        None => format!("up         session={session}, then silence"),
    }
}

fn read_frame(sock: &mut TcpStream) -> Option<(Header, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 512];
    loop {
        if buf.len() >= HEADER_SIZE {
            let h = Header::decode(&buf).ok()?;
            let total = HEADER_SIZE + h.payload_len as usize;
            if buf.len() >= total {
                return Some((h, buf[HEADER_SIZE..total].to_vec()));
            }
        }
        let n = sock.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn cmd_status(a: Args) {
    if a.addrs.is_empty() {
        die("status needs at least one --addr");
    }
    for addr in &a.addrs {
        println!("{:<22} {}", addr.to_string(), probe(*addr));
    }
}

/* ---- tail ---- */

async fn cmd_tail(a: Args) {
    let dir = a.dir.unwrap_or_else(|| die("tail needs --dir"));
    let mut at = a.from;

    loop {
        let records = log::read_dir(&dir);
        let fresh: Vec<_> = records.into_iter().filter(|r| r.seq >= at).collect();
        for r in fresh {
            println!(
                "  seq={:<8} key={:<6} session={:<4} client_seq={:<8} {}",
                r.seq,
                r.partition_key,
                r.session,
                r.client_seq,
                r.text()
            );
            at = r.seq + 1;
        }

        if !a.follow {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/* ---- keys ---- */

fn cmd_keys(a: Args) {
    let dir = a.dir.unwrap_or_else(|| die("keys needs --dir"));
    let keys = log::keys(&dir);

    if keys.is_empty() {
        println!("  the log holds nothing");
        return;
    }

    for k in &keys {
        println!(
            "  key={:<8} records={:<8} first={:<8} last={}",
            k.key, k.count, k.first, k.last
        );
    }
    println!("\n  {} keys", keys.len());
}

/* ---- watch ---- */

async fn cmd_nodes_watch(a: Args) {
    if a.nodes.is_empty() {
        die("watch needs at least one --node NAME=PATH");
    }

    let mut drawn = 0;
    loop {
        let seen: Vec<(String, usize, u64)> = a
            .nodes
            .iter()
            .map(|(name, path)| {
                let recs = log::read_dir(path);
                (name.clone(), recs.len(), recs.last().map(|r| r.seq).unwrap_or(0))
            })
            .collect();

        let furthest = seen.iter().map(|(_, _, s)| *s).max().unwrap_or(0);

        if drawn > 0 {
            print!("\x1b[{drawn}A");
        }
        for (name, records, last) in &seen {
            let lag = furthest - last;
            let note = if *last == 0 {
                String::from("empty")
            } else if lag == 0 {
                String::from("up to date")
            } else {
                format!("{lag} behind")
            };
            println!("\r\x1b[K  {name:<8} records={records:<8} last={last:<8} {note}");
        }
        drawn = seen.len();

        let _ = std::io::stdout().flush();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.is_empty() || argv[0] == "--help" || argv[0] == "-h" {
        println!("{USAGE}");
        return;
    }

    let cmd = argv[0].clone();
    let args = parse(&argv[1..]);

    match cmd.as_str() {
        "write" => cmd_write(args).await,
        "notify" => cmd_watch(args, false).await,
        "read" => cmd_watch(args, true).await,
        "status" => cmd_status(args),
        "tail" => cmd_tail(args).await,
        "keys" => cmd_keys(args),
        "watch" => cmd_nodes_watch(args).await,
        other => die(&format!("unknown command {other}")),
    }
}

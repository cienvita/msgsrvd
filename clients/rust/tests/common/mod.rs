#![allow(dead_code)] /* each test binary uses a different part of it */

//! Shared harness: running a real msgsrvd and talking to it.
//!
//! The binary is built by the repo's Makefile; the tests do not build
//! it. Point MSGSRVD_BIN at it to use another one.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

use msgsrv_client::proto::{self, flag, op, Header, HEADER_SIZE};
use msgsrv_client::Config;

/* Above WAL_REC_MAX_SIZE, and small enough that a few thousand
 * records roll a segment. */
const SEGMENT_SIZE: &str = "2097152";

pub fn binary() -> PathBuf {
    if let Ok(p) = std::env::var("MSGSRVD_BIN") {
        return PathBuf::from(p);
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../build/msgsrvd")
}

pub struct Server {
    child: Child,
    pub port: u16,
    /// Records the recovery scan found at startup.
    pub records: u64,
}

impl Server {
    pub async fn start(dir: &Path, port: u16) -> Server {
        Server::spawn(dir, port, None).await
    }

    /// A node that streams the leader's log into its own.
    pub async fn start_replica(dir: &Path, port: u16, leader: u16) -> Server {
        Server::spawn(dir, port, Some(leader)).await
    }

    async fn spawn(dir: &Path, port: u16, leader: Option<u16>) -> Server {
        let bin = binary();
        assert!(
            bin.exists(),
            "no msgsrvd at {}, run make msgsrvd in the repo root",
            bin.display()
        );

        let mut cmd = Command::new(&bin);
        cmd.arg("--dir")
            .arg(dir)
            .arg("--port")
            .arg(port.to_string())
            .arg("--segment-size")
            .arg(SEGMENT_SIZE);
        if let Some(leader) = leader {
            cmd.arg("--leader").arg(format!("127.0.0.1:{leader}"));
        }

        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn msgsrvd");

        let stdout = child.stdout.take().expect("stdout");
        let mut lines = BufReader::new(stdout).lines();
        let mut records = 0u64;
        let mut bound = None;
        let mut said = Vec::new();

        while let Some(line) = lines.next_line().await.expect("read msgsrvd stdout") {
            if let Some(rest) = line.split("records=").nth(1) {
                records = rest
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse().ok())
                    .expect("records count");
            }
            if let Some(rest) = line.split("listening on ").nth(1) {
                /* The line names the role after the address. */
                let addr: SocketAddr = rest
                    .split_whitespace()
                    .next()
                    .expect("listen address")
                    .parse()
                    .expect("listen address");
                bound = Some(addr.port());
                break;
            }
            said.push(line);
        }

        /* Nothing reads the rest of stdout, so drain it rather than
         * let the server block on a full pipe. */
        tokio::spawn(async move { while lines.next_line().await.unwrap_or(None).is_some() {} });

        Server {
            child,
            port: bound.unwrap_or_else(|| panic!("msgsrvd did not start, it said: {said:#?}")),
            records,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.port).parse().unwrap()
    }

    /// Kill without warning, then wait for the port to come back.
    ///
    /// Everything acknowledged is fsync'd, so a hard kill is the
    /// honest way to end a server here. Reaping the child does not
    /// mean its sockets are out of the table yet, and a test that
    /// restarts on the same port arrives before they are, so this
    /// waits for the thing that actually matters: that the port can be
    /// bound again.
    pub async fn kill(&mut self) {
        let _ = self.child.kill().await;

        for _ in 0..200 {
            if std::net::TcpListener::bind(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "port {} did not come back after the server was killed",
            self.port
        );
    }
}

pub fn config(addr: SocketAddr) -> Config {
    let mut cfg = Config::new(vec![addr]);
    cfg.name = String::from("integration");
    cfg.write_deadline = Duration::from_secs(20);
    cfg
}

/// Records the log holds, according to a fresh recovery scan.
pub async fn recovered_records(dir: &Path, port: u16) -> u64 {
    let mut s = Server::start(dir, port).await;
    let n = s.records;
    s.kill().await;
    n
}

/// A port to run one server on, from below the ephemeral range.
///
/// Not `bind(0)`. A test kills its server and starts another on the
/// same port while a client is reconnecting into it, and a connection
/// to an ephemeral-range port with nothing listening can be given that
/// same port as its source, connect to itself, and hold the port
/// against the server that wants it back. The real server listens on
/// 7400, which is below the range, so this stays a test artefact.
pub fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

pub fn test_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(21400);

    for _ in 0..200 {
        let port = NEXT.fetch_add(1, Ordering::Relaxed);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free port in the test range");
}

/* ---- raw protocol helpers, for what the client API does not expose ---- */

pub async fn send_frame(sock: &mut TcpStream, h: &Header, payload: &[u8]) {
    let mut f = Vec::new();
    f.extend_from_slice(&h.encode());
    f.extend_from_slice(payload);
    sock.write_all(&f).await.unwrap();
}

pub async fn read_frame(sock: &mut TcpStream) -> (Header, Vec<u8>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 512];

    loop {
        if buf.len() >= HEADER_SIZE {
            let h = Header::decode(&buf).unwrap();
            let total = HEADER_SIZE + h.payload_len as usize;
            if buf.len() >= total {
                return (h, buf[HEADER_SIZE..total].to_vec());
            }
        }
        let n = sock.read(&mut chunk).await.unwrap();
        assert!(n > 0, "server closed the connection");
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// HELLO on a fresh connection. Returns (session, durable client seq).
pub async fn hello(sock: &mut TcpStream, session: u64) -> Result<(u64, u64), u16> {
    let payload = proto::encode_hello(session, "raw");
    let h = Header::new(op::HELLO, 0, payload.len() as u32, 0, 0);
    send_frame(sock, &h, &payload).await;

    let (h, payload) = read_frame(sock).await;
    match h.op {
        op::ACK => Ok((h.sequence, proto::decode_ack(&payload).unwrap())),
        op::ERR => Err(proto::decode_err(&payload).unwrap().0),
        other => panic!("hello answered with op {other}"),
    }
}

pub async fn raw_write(sock: &mut TcpStream, client_seq: u64, body: &[u8]) -> Header {
    let h = Header::new(
        op::WRITE,
        flag::ACK_REQ | flag::SYNC,
        body.len() as u32,
        7,
        client_seq,
    );
    send_frame(sock, &h, body).await;
    read_frame(sock).await.0
}

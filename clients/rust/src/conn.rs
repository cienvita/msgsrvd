//! The connection task.
//!
//! One task owns the socket, the session, and the queue of records
//! that have been sent and not yet acknowledged. Callers reach it
//! through a channel and wait on a oneshot, so nothing but this task
//! touches the wire.
//!
//! The queue is what makes a lost connection survivable. Records stay
//! in it until an acknowledgement covers them, and a reconnection
//! resumes the session and sends the queue again from the front. The
//! server deduplicates on (session, client sequence), so a record it
//! already holds is acknowledged rather than stored a second time,
//! which is what lets the client resend without having to work out
//! what got through.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::client::{Cmd, Config, Notification, Request, WatchRequest, WriteAck};
use crate::error::{Error, Result};
use crate::proto::{self, err, flag, op, Header, HEADER_SIZE};

/// A record sent and not yet acknowledged.
struct Pending {
    client_seq: u64,
    /// The whole frame, kept for resend. Holding the encoded form
    /// costs a header per record and makes a resend a copy.
    frame: Vec<u8>,
    /// Flags an acknowledgement must carry to count for this record.
    need: u16,
    waiter: Option<oneshot::Sender<Result<WriteAck>>>,
}

impl Pending {
    fn resolve(&mut self, r: Result<WriteAck>) {
        if let Some(w) = self.waiter.take() {
            let _ = w.send(r);
        }
    }
}

struct Frame {
    header: Header,
    payload: Vec<u8>,
}

/// What this connection is following, and where it has got to.
struct Watching {
    key: Option<u64>,
    min_seq: u64,
    /// The first sequence not yet delivered, which is where a
    /// reconnection asks to start again.
    next_seq: u64,
    /// A read, which ends of its own accord. A subscription does not.
    once: bool,
    tx: mpsc::Sender<Notification>,
    /// Waiting for the server to say it took the request.
    ready: Option<oneshot::Sender<Result<u64>>>,
}

/// What a select pass produced. The arms only carry a value out; the
/// state changes happen after, where nothing is borrowed.
enum Ev {
    Read(std::io::Result<usize>),
    Wrote(std::io::Result<usize>),
    Req(Option<Cmd>),
}

pub(crate) struct Task {
    cfg: Arc<Config>,
    rx: mpsc::Receiver<Cmd>,

    /// Shared with the handle, which reports it.
    session: Arc<AtomicU64>,
    next_seq: u64,

    inflight: VecDeque<Pending>,
    inflight_bytes: usize,

    /// Staged outbound bytes. Frames accumulate here and go out in one
    /// write per pass, so a burst of small records becomes one socket
    /// write and one larger batch for the server to commit.
    out: Vec<u8>,
    inbuf: Vec<u8>,

    /// Address a NOT_LEADER pointed at, tried ahead of the configured
    /// list.
    leader: Option<SocketAddr>,
    addr_idx: usize,

    /// The one subscription or read this connection carries.
    watch: Option<Watching>,
}

impl Task {
    pub(crate) fn new(
        cfg: Arc<Config>,
        rx: mpsc::Receiver<Cmd>,
        session: Arc<AtomicU64>,
    ) -> Task {
        Task {
            cfg,
            rx,
            session,
            next_seq: 1,
            inflight: VecDeque::new(),
            inflight_bytes: 0,
            out: Vec::new(),
            inbuf: Vec::new(),
            leader: None,
            addr_idx: 0,
            watch: None,
        }
    }

    pub(crate) async fn run(mut self, ready: oneshot::Sender<Result<()>>) {
        /*
         * The first attempt is not retried. A server that is down at
         * startup is the caller's to handle, and reporting it as a
         * timeout on some later write would hide it.
         */
        let mut sock = match self.dial().await {
            Ok(s) => s,
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };
        if ready.send(Ok(())).is_err() {
            return; /* the caller gave up while we were connecting */
        }

        let mut closed = false;
        loop {
            match self.serve(&mut sock, &mut closed).await {
                Ok(()) => {
                    debug!("connection task finished");
                    return;
                }
                Err(e) => {
                    if is_permanent(&e) {
                        warn!(error = %e, "connection failed for good");
                        self.fail_all(&e);
                        return;
                    }
                    warn!(error = %e, inflight = self.inflight.len(),
                          "connection lost, reconnecting");
                }
            }

            match self.redial(&mut closed).await {
                Some(s) => sock = s,
                None => return,
            }
        }
    }

    fn session(&self) -> u64 {
        self.session.load(Ordering::Relaxed)
    }

    /* ---- connecting ---- */

    fn next_addr(&mut self) -> SocketAddr {
        if let Some(a) = self.leader.take() {
            return a;
        }
        let a = self.cfg.addrs[self.addr_idx % self.cfg.addrs.len()];
        self.addr_idx += 1;
        a
    }

    async fn dial(&mut self) -> Result<TcpStream> {
        let addr = self.next_addr();
        let connect = TcpStream::connect(addr);

        let mut sock = match timeout(self.cfg.connect_timeout, connect).await {
            Ok(r) => r?,
            Err(_) => return Err(Error::Timeout),
        };
        /* Every write here is latency-bound and already coalesced. */
        sock.set_nodelay(true)?;

        match timeout(self.cfg.connect_timeout, self.handshake(&mut sock)).await {
            Ok(r) => r?,
            Err(_) => return Err(Error::Timeout),
        }
        debug!(%addr, session = self.session(), "connected");
        Ok(sock)
    }

    /// Reconnect until it works, queueing whatever callers send in the
    /// meantime. `None` when there is nothing left to do.
    async fn redial(&mut self, closed: &mut bool) -> Option<TcpStream> {
        let mut wait = self.cfg.backoff_initial;

        loop {
            if *closed && self.inflight.is_empty() {
                return None;
            }

            let accepting = !*closed && self.has_window();
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                req = self.rx.recv(), if accepting => {
                    match req {
                        Some(Cmd::Write(r)) => self.queue(r),
                        Some(Cmd::Watch(w)) => self.queue_watch(w),
                        None => *closed = true,
                    }
                    continue;
                }
            }

            match self.dial().await {
                Ok(s) => return Some(s),
                Err(e) => {
                    if is_permanent(&e) {
                        warn!(error = %e, "reconnection refused for good");
                        self.fail_all(&e);
                        return None;
                    }
                    debug!(error = %e, "reconnection attempt failed");
                    wait = (wait * 2).min(self.cfg.backoff_max);
                }
            }
        }
    }

    /// HELLO, then stage the whole in-flight queue for resend.
    async fn handshake(&mut self, sock: &mut TcpStream) -> Result<()> {
        let mut want = self.session();

        /* Whatever the dead connection left half-read belongs to it. */
        self.inbuf.clear();

        loop {
            let payload = proto::encode_hello(want, &self.cfg.name);
            let h = Header::new(op::HELLO, 0, payload.len() as u32, 0, 0);
            let mut frame = Vec::with_capacity(HEADER_SIZE + payload.len());
            frame.extend_from_slice(&h.encode());
            frame.extend_from_slice(&payload);
            sock.write_all(&frame).await?;

            let fr = self.read_frame(sock).await?;
            match fr.header.op {
                op::ACK => {
                    let mark = proto::decode_ack(&fr.payload)?;
                    self.session.store(fr.header.sequence, Ordering::Relaxed);

                    if want == 0 {
                        /*
                         * A new session numbers from 1, so anything
                         * still in flight has to be renumbered before
                         * it is sent again. Its old numbers belong to
                         * a session the server no longer has, and
                         * would be deduplicated against nothing.
                         */
                        self.renumber();
                    } else {
                        /*
                         * The mark is the highest sequence the server
                         * holds durably for this session. Our counter
                         * should already be past it; take the higher
                         * of the two rather than reuse a number.
                         */
                        self.next_seq = self.next_seq.max(mark + 1);
                    }

                    /*
                     * Everything in flight goes again, including
                     * records the mark says are durable. Resending a
                     * durable record costs one round trip and returns
                     * a real WAL sequence and the durability level
                     * actually reached; resolving it here from the
                     * mark alone could only report neither.
                     */
                    self.out.clear();
                    for p in &self.inflight {
                        self.out.extend_from_slice(&p.frame);
                    }

                    /*
                     * A subscription goes again from the record after
                     * the last one delivered, so a reconnection
                     * repeats records rather than skipping them. A
                     * read cannot be resumed: it was a view of the log
                     * at a moment that has passed, and the caller sees
                     * its stream end.
                     */
                    match self.watch.as_ref() {
                        Some(w) if w.once => {
                            debug!("a read did not survive the reconnection");
                            self.watch = None;
                        }
                        Some(w) => {
                            let from = w.next_seq;
                            debug!(from, "following again");
                            self.stage_watch(from);
                        }
                        None => {}
                    }
                    if !self.inflight.is_empty() {
                        debug!(
                            records = self.inflight.len(),
                            mark, "resending after resume"
                        );
                    }
                    return Ok(());
                }
                op::ERR => {
                    let (code, text) = proto::decode_err(&fr.payload)?;
                    if code == err::SESSION_UNKNOWN && want != 0 {
                        /*
                         * The server has forgotten the session, so
                         * deduplication cannot help what is in flight.
                         * Opening a new one and resending is the only
                         * way forward, and it can duplicate: delivery
                         * is at least once, and this is where the
                         * "once" is lost.
                         */
                        warn!(
                            session = self.session(),
                            "session unknown to the server, opening a new one"
                        );
                        self.session.store(0, Ordering::Relaxed);
                        want = 0;
                        continue;
                    }
                    if code == err::NOT_LEADER {
                        self.leader = text.as_deref().and_then(parse_addr);
                    }
                    return Err(Error::Server { code, text });
                }
                _ => return Err(Error::Protocol("hello answered with another verb")),
            }
        }
    }

    /* ---- serving ---- */

    /// Pump the connection until it fails or there is nothing left to
    /// do. `Ok(())` means the client is gone and everything it asked
    /// for has been answered.
    async fn serve(&mut self, sock: &mut TcpStream, closed: &mut bool) -> Result<()> {
        let mut rbuf = vec![0u8; 16 * 1024];
        let (mut rd, mut wr) = sock.split();

        loop {
            while let Some(fr) = self.take_frame()? {
                self.on_frame(fr)?;
            }

            if *closed && self.inflight.is_empty() && self.out.is_empty() {
                return Ok(());
            }

            let accepting = !*closed && self.has_window();
            /*
             * The staged bytes move into a local for the pass. The
             * write future then borrows nothing from self, which is
             * what lets the other two arms take it mutably.
             */
            let mut chunk = std::mem::take(&mut self.out);
            let sending = !chunk.is_empty();

            let ev = tokio::select! {
                biased;
                r = rd.read(&mut rbuf) => Ev::Read(r),
                w = wr.write(&chunk), if sending => Ev::Wrote(w),
                req = self.rx.recv(), if accepting => Ev::Req(req),
            };

            match ev {
                Ev::Wrote(r) => {
                    let n = r?;
                    if n == 0 {
                        return Err(Error::Protocol("socket accepted no bytes"));
                    }
                    chunk.drain(..n);
                }
                Ev::Read(r) => {
                    let n = r?;
                    if n == 0 {
                        return Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "server closed the connection",
                        )));
                    }
                    self.inbuf.extend_from_slice(&rbuf[..n]);
                }
                Ev::Req(Some(Cmd::Write(req))) => self.queue(req),
                Ev::Req(Some(Cmd::Watch(w))) => self.queue_watch(w),
                Ev::Req(None) => *closed = true,
            }

            /*
             * Whatever did not go out, plus anything staged by this
             * pass, in that order. A cancelled write wrote nothing, so
             * the chunk is still whole.
             */
            if !chunk.is_empty() || !self.out.is_empty() {
                chunk.extend_from_slice(&self.out);
                self.out = chunk;
            }
        }
    }

    fn on_frame(&mut self, fr: Frame) -> Result<()> {
        match fr.header.op {
            op::ACK => self.on_ack(&fr),
            op::ERR => self.on_err(&fr),
            op::NOTIFY => self.on_notify(&fr),
            op::PONG => Ok(()),
            _ => Err(Error::Protocol("unexpected verb from the server")),
        }
    }

    /*
     * A record, or the end of a read.
     *
     * A subscriber that has stopped reading loses the subscription
     * rather than quietly missing records: the channel closing is
     * something the caller can see, and a gap in a stream is not.
     */
    fn on_notify(&mut self, fr: &Frame) -> Result<()> {
        let w = match self.watch.as_mut() {
            Some(w) => w,
            None => return Ok(()), /* a stream this client has dropped */
        };

        if fr.header.flags & flag::LAST != 0 {
            debug!(seq = fr.header.sequence, "the read ended");
            self.watch = None;
            return Ok(());
        }

        w.next_seq = fr.header.sequence + 1;
        let note = Notification {
            seq: fr.header.sequence,
            partition_key: fr.header.partition_key,
            data: fr.payload.clone(),
        };

        if w.tx.try_send(note).is_err() {
            warn!("the subscriber is not keeping up, or has gone");
            self.watch = None;
        }
        Ok(())
    }

    /// One acknowledgement covers every record of this session at or
    /// below the sequence it names.
    fn on_ack(&mut self, fr: &Frame) -> Result<()> {
        /*
         * A subscription or a read is answered with an empty ACK
         * naming where the stream starts. A write's is never empty: it
         * carries the cumulative client sequence, so the two cannot be
         * taken for each other on a connection doing both.
         */
        if fr.payload.is_empty() {
            if let Some(w) = self.watch.as_mut() {
                if let Some(ready) = w.ready.take() {
                    let _ = ready.send(Ok(fr.header.sequence));
                    return Ok(());
                }
            }
            return Err(Error::Protocol("an acknowledgement for nothing"));
        }

        let mark = proto::decode_ack(&fr.payload)?;
        let degraded = fr.header.flags & flag::DEGRADED != 0;

        loop {
            match self.inflight.front() {
                Some(p) if p.client_seq <= mark => {
                    /*
                     * An acknowledgement for a weaker level does not
                     * settle a record that asked for a stronger one,
                     * even though it names a sequence past it.
                     */
                    if p.need & fr.header.flags != p.need {
                        break;
                    }
                }
                _ => break,
            }

            let mut p = self.inflight.pop_front().unwrap();
            self.inflight_bytes -= p.frame.len();
            p.resolve(Ok(WriteAck {
                seq: Some(fr.header.sequence),
                degraded,
            }));
        }
        Ok(())
    }

    fn on_err(&mut self, fr: &Frame) -> Result<()> {
        let (code, text) = proto::decode_err(&fr.payload)?;

        if code == err::NOT_LEADER {
            /*
             * A write reached a follower. Nothing is failed: the queue
             * goes again to the address it named.
             */
            self.leader = text.as_deref().and_then(parse_addr);
            return Err(Error::Server { code, text });
        }

        if err::is_fatal(code) {
            return Err(Error::Server { code, text });
        }

        /*
         * These three answer a subscription or a read and nothing
         * else, so a request still waiting for its reply owns them.
         */
        if matches!(code, err::BEHIND | err::NO_HISTORY | err::UNSUPPORTED) {
            if let Some(w) = self.watch.as_mut() {
                if let Some(ready) = w.ready.take() {
                    let _ = ready.send(Err(Error::Server { code, text }));
                    self.watch = None;
                    return Ok(());
                }
            }
        }

        if code == err::NO_REPLICAS {
            /*
             * Cumulative, the way an acknowledgement is: it names the
             * highest sequence it covers, and covers every record that
             * asked for a second copy at or below it. Records at a
             * weaker level are not refused by it, since the leader has
             * them and their level does not depend on a replica.
             */
            if self.fail_replicated_through(fr.header.sequence) {
                return Ok(());
            }
            return Err(Error::NoReplicas);
        }

        let e = if code == err::STORAGE {
            Error::Storage
        } else {
            Error::Server { code, text }
        };

        /*
         * The server echoes the sequence of the frame it refused, so
         * the record it belongs to can be failed on its own and the
         * rest of the queue left alone. A refusal that names nothing
         * we hold is about the connection instead.
         */
        if !self.fail_seq(fr.header.sequence, &e) {
            return Err(e);
        }
        Ok(())
    }

    /* ---- queue ---- */

    fn has_window(&self) -> bool {
        self.inflight.len() < self.cfg.window && self.inflight_bytes < self.cfg.window_bytes
    }

    fn queue(&mut self, req: Request) {
        let seq = self.next_seq;
        self.next_seq += 1;

        let h = Header::new(
            op::WRITE,
            req.flags,
            req.data.len() as u32,
            req.partition_key,
            seq,
        );
        let mut frame = Vec::with_capacity(HEADER_SIZE + req.data.len());
        frame.extend_from_slice(&h.encode());
        frame.extend_from_slice(&req.data);

        self.out.extend_from_slice(&frame);
        self.inflight_bytes += frame.len();
        self.inflight.push_back(Pending {
            client_seq: seq,
            frame,
            need: req.required,
            waiter: Some(req.waiter),
        });
    }

    /// Ask the node to start sending records.
    ///
    /// Only one of these lives on a connection, which the server
    /// enforces as well; a second is refused rather than replacing the
    /// first.
    fn queue_watch(&mut self, w: WatchRequest) {
        if self.watch.is_some() {
            let _ = w.ready.send(Err(Error::Protocol(
                "this connection already carries a subscription",
            )));
            return;
        }

        let from = w.from_seq;
        self.watch = Some(Watching {
            key: w.key,
            min_seq: w.min_seq,
            next_seq: from,
            once: w.once,
            tx: w.tx,
            ready: Some(w.ready),
        });
        self.stage_watch(from);
    }

    /// Put the request itself on the wire, at whatever sequence the
    /// stream should now start from.
    fn stage_watch(&mut self, from: u64) {
        let w = match self.watch.as_ref() {
            Some(w) => w,
            None => return,
        };

        let flags = if w.key.is_none() { flag::ALL_KEYS } else { 0 };
        let key = w.key.unwrap_or(0);
        let (op, payload) = if w.once {
            (op::READ, proto::encode_read(w.min_seq))
        } else {
            (op::SUBSCRIBE, proto::encode_subscribe(w.min_seq, from))
        };

        let h = Header::new(op, flags, payload.len() as u32, key, 0);
        self.out.extend_from_slice(&h.encode());
        self.out.extend_from_slice(&payload);
    }

    /// Renumber the queue for a session that starts again at 1.
    fn renumber(&mut self) {
        let mut seq = 1u64;
        for p in &mut self.inflight {
            p.client_seq = seq;
            Header::patch_sequence(&mut p.frame, seq);
            seq += 1;
        }
        self.next_seq = seq;
    }

    /// Fail the one record with this client sequence.
    ///
    /// Its number is then never used, which leaves a gap in the
    /// session's sequence. The server compares against a high-water
    /// mark rather than counting, so a gap costs nothing.
    fn fail_seq(&mut self, client_seq: u64, e: &Error) -> bool {
        let at = self
            .inflight
            .iter()
            .position(|p| p.client_seq == client_seq);
        match at {
            Some(i) => {
                let mut p = self.inflight.remove(i).unwrap();
                self.inflight_bytes -= p.frame.len();
                p.resolve(Err(e.shared()));
                true
            }
            None => false,
        }
    }

    /*
     * Fail every record that asked for replication up to client_seq.
     * FALSE when the queue held none, which means the refusal was
     * about something this client is not waiting for and the
     * connection is what has to be questioned.
     */
    fn fail_replicated_through(&mut self, client_seq: u64) -> bool {
        let mut hit = false;
        let mut kept = VecDeque::with_capacity(self.inflight.len());

        while let Some(mut p) = self.inflight.pop_front() {
            if p.client_seq <= client_seq && p.need & flag::REPLICATED != 0 {
                self.inflight_bytes -= p.frame.len();
                p.resolve(Err(Error::NoReplicas));
                hit = true;
            } else {
                kept.push_back(p);
            }
        }

        self.inflight = kept;
        hit
    }

    fn fail_all(&mut self, e: &Error) {
        while let Some(mut p) = self.inflight.pop_front() {
            p.resolve(Err(e.shared()));
        }
        self.inflight_bytes = 0;
        self.out.clear();
        self.rx.close();
    }

    /* ---- framing ---- */

    /// Take one complete frame out of the receive buffer.
    fn take_frame(&mut self) -> Result<Option<Frame>> {
        if self.inbuf.len() < HEADER_SIZE {
            return Ok(None);
        }
        let header = Header::decode(&self.inbuf)?;
        let total = HEADER_SIZE + header.payload_len as usize;
        if self.inbuf.len() < total {
            return Ok(None);
        }

        let payload = self.inbuf[HEADER_SIZE..total].to_vec();
        self.inbuf.drain(..total);
        Ok(Some(Frame { header, payload }))
    }

    /// Read frames until one is complete. Used during the handshake,
    /// where there is nothing else to do.
    async fn read_frame(&mut self, sock: &mut TcpStream) -> Result<Frame> {
        let mut buf = [0u8; 1024];
        loop {
            if let Some(fr) = self.take_frame()? {
                return Ok(fr);
            }
            let n = sock.read(&mut buf).await?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed the connection during the handshake",
                )));
            }
            self.inbuf.extend_from_slice(&buf[..n]);
        }
    }
}

/// Whether reconnecting could ever help.
fn is_permanent(e: &Error) -> bool {
    match e {
        Error::Protocol(_) => true,
        Error::Server { code, .. } => err::is_fatal(*code),
        _ => false,
    }
}

fn parse_addr(text: &str) -> Option<SocketAddr> {
    text.parse().ok()
}

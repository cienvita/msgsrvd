//! The public surface: configuration, durability levels, and the
//! handle callers hold.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::conn::Task;
use crate::error::{Error, Result};
use crate::proto::{flag, MAX_PAYLOAD};

/// What a write waits for before it is reported durable.
///
/// The level is per write and maps onto the header flags the server
/// reads. A session that mixes levels is legal but keeps a separate
/// high-water mark per level on both sides, so one client per level is
/// the simpler arrangement and the one this client is tuned for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// The record is in the leader's log buffer. Says nothing about
    /// storage: a leader that dies here loses it.
    Appended,
    /// The flush covering the record has completed on the leader.
    Sync,
    /// The leader and at least one follower have both flushed it.
    ///
    /// A leader with no follower attached refuses this with
    /// [`Error::NoReplicas`] rather than accepting it on a promise of
    /// a copy that does not exist.
    Replicated,
    /// As `Replicated`, but a leader-only copy is accepted when no
    /// follower answers in time. The acknowledgement says which of the
    /// two happened, in [`WriteAck::degraded`].
    ReplicatedOrDegraded,
}

impl Durability {
    /// Header flags sent with the record.
    fn flags(self) -> u16 {
        match self {
            Durability::Appended => flag::ACK_REQ,
            Durability::Sync => flag::ACK_REQ | flag::SYNC,
            Durability::Replicated => flag::ACK_REQ | flag::SYNC | flag::REPLICATED,
            Durability::ReplicatedOrDegraded => {
                flag::ACK_REQ | flag::SYNC | flag::REPLICATED | flag::ALLOW_DEGRADED
            }
        }
    }

    /// Flags an acknowledgement has to carry before it counts for this
    /// record. ALLOW_DEGRADED is a request, not a guarantee, so it is
    /// not among them.
    fn required(self) -> u16 {
        self.flags() & (flag::ACK_REQ | flag::SYNC | flag::REPLICATED)
    }
}

/// What the server said about a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteAck {
    /// WAL sequence that covers the record.
    ///
    /// An acknowledgement names the last sequence of the batch it
    /// covers rather than this record's own, so it is an upper bound.
    /// That is what `min_seq` wants on a later read: waiting for a
    /// sequence at or past the record cannot serve a stale answer.
    ///
    /// `None` when the record was confirmed by a session resume, which
    /// names a client sequence and not a WAL one.
    pub seq: Option<u64>,

    /// The record is on the leader only. Set on a write that asked for
    /// [`Durability::ReplicatedOrDegraded`] and did not get a second
    /// copy.
    pub degraded: bool,
}

/// Connection settings.
#[derive(Clone, Debug)]
pub struct Config {
    /// Nodes to try, in order. The leader is found by trying them
    /// until one accepts a HELLO, and after that by following the
    /// address a NOT_LEADER carries.
    pub addrs: Vec<SocketAddr>,

    /// Client name. Reaches the server's logs and nothing else.
    pub name: String,

    /// Records that may be unacknowledged at once.
    pub window: usize,

    /// Bytes that may be unacknowledged at once. Unacknowledged
    /// records are held for resend, so this is also the memory the
    /// connection can hold.
    pub window_bytes: usize,

    /// Largest record accepted. The protocol cap; a server build
    /// bounds it further by its receive buffer.
    pub max_payload: usize,

    /// How long a write waits for its acknowledgement, reconnects
    /// included.
    pub write_deadline: Duration,

    /// How long one connection attempt, handshake included, may take.
    pub connect_timeout: Duration,

    /// First and largest wait between reconnection attempts.
    pub backoff_initial: Duration,
    pub backoff_max: Duration,

    /// Writes that may be queued before `write` waits for room.
    pub queue: usize,

    /// Records held for a subscriber that is not reading them yet. A
    /// subscriber that falls this far behind loses its subscription,
    /// which it is told about, rather than quietly missing records.
    pub notify_queue: usize,
}

impl Config {
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        Config {
            addrs,
            name: String::from("msgsrv-client"),
            window: 1024,
            window_bytes: 4 * 1024 * 1024,
            max_payload: MAX_PAYLOAD,
            write_deadline: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_secs(2),
            queue: 256,
            notify_queue: 1024,
        }
    }
}

pub(crate) struct Request {
    pub partition_key: u64,
    pub data: Vec<u8>,
    pub required: u16,
    pub flags: u16,
    pub waiter: oneshot::Sender<Result<WriteAck>>,
}

/// A record the server pushed, because it was asked to.
#[derive(Clone, Debug)]
pub struct Notification {
    /// The record's own WAL sequence on the node that sent it.
    pub seq: u64,
    pub partition_key: u64,
    pub data: Vec<u8>,
}

/// What a subscription or a read asks for.
#[derive(Clone, Copy, Debug, Default)]
pub struct Watch {
    /// The key to follow, or every key.
    pub key: Option<u64>,
    /// Where a subscription starts: 0 for whatever is written next,
    /// or the first sequence not yet handled. A read ignores it and
    /// starts at the oldest record the node still holds.
    pub from_seq: u64,
    /// Do not answer until the node has flushed this far. This is
    /// read-your-writes across nodes: pass the sequence an
    /// acknowledgement gave you and a replica will not show you a
    /// view older than your own write.
    pub min_seq: u64,
}

/// Records arriving from a subscription or a read.
///
/// A read's stream ends once the node has shown everything it held
/// when the read was made. A subscription's ends when the client is
/// dropped, or when the connection is lost in a way that cannot be
/// resumed.
#[derive(Debug)]
pub struct Subscription {
    pub(crate) rx: mpsc::Receiver<Notification>,
    pub(crate) start: u64,
}

impl Subscription {
    /// The sequence the stream started from.
    pub fn start(&self) -> u64 {
        self.start
    }

    pub async fn next(&mut self) -> Option<Notification> {
        self.rx.recv().await
    }
}

pub(crate) struct WatchRequest {
    pub key: Option<u64>,
    pub from_seq: u64,
    pub min_seq: u64,
    /// A read, which ends. Otherwise a subscription, which does not.
    pub once: bool,
    pub tx: mpsc::Sender<Notification>,
    pub ready: oneshot::Sender<Result<u64>>,
}

pub(crate) enum Cmd {
    Write(Request),
    Watch(WatchRequest),
}

/// A client, holding one connection to the leader.
///
/// Cloning shares the connection and its session. Dropping the last
/// clone stops the connection once whatever is in flight has been
/// acknowledged.
#[derive(Clone, Debug)]
pub struct Client {
    tx: mpsc::Sender<Cmd>,
    cfg: Arc<Config>,
    session: Arc<AtomicU64>,
}

impl Client {
    /// Connect, open a session, and start the connection task.
    ///
    /// The first handshake is awaited, so a server that is down or
    /// speaking another version is reported here rather than as a
    /// timeout on the first write. Losing the connection later is not
    /// an error: it reconnects and resends.
    pub async fn connect(cfg: Config) -> Result<Client> {
        if cfg.addrs.is_empty() {
            return Err(Error::Protocol("no server addresses configured"));
        }
        if cfg.window == 0 || cfg.window_bytes == 0 {
            return Err(Error::Protocol("window must allow at least one record"));
        }

        let cfg = Arc::new(cfg);
        let (tx, rx) = mpsc::channel(cfg.queue);
        let (ready_tx, ready_rx) = oneshot::channel();
        let session = Arc::new(AtomicU64::new(0));

        tokio::spawn(Task::new(cfg.clone(), rx, session.clone()).run(ready_tx));

        match ready_rx.await {
            Ok(r) => r?,
            Err(_) => return Err(Error::Closed),
        }

        Ok(Client { tx, cfg, session })
    }

    /// The session the connection is bound to. Only useful in logs.
    ///
    /// A reconnection resumes the same session, so this changes only
    /// when the server has forgotten it and the client has had to open
    /// another, which is the one case where a resend can duplicate.
    pub fn session(&self) -> u64 {
        self.session.load(Ordering::Relaxed)
    }

    /// Follow the log as it grows.
    ///
    /// The records the node sends are the ones it has flushed, so a
    /// subscriber is told about a record when the node holding it
    /// could survive losing power rather than when it merely has it.
    ///
    /// A connection carries one of these at a time. A connection lost
    /// and remade resumes from the record after the last one
    /// delivered, so a reconnection repeats records rather than
    /// skipping them.
    pub async fn subscribe(&self, watch: Watch) -> Result<Subscription> {
        self.watch(watch, false).await
    }

    /// Everything the node holds for a key, and then the end.
    ///
    /// Bounded at both ends: it starts at the oldest record still on
    /// disk and stops at whatever was durable when the node took the
    /// request, so the stream finishes rather than following the log.
    pub async fn read(&self, watch: Watch) -> Result<Subscription> {
        self.watch(watch, true).await
    }

    async fn watch(&self, w: Watch, once: bool) -> Result<Subscription> {
        let (tx, rx) = mpsc::channel(self.cfg.notify_queue);
        let (ready, done) = oneshot::channel();

        let req = Cmd::Watch(WatchRequest {
            key: w.key,
            from_seq: w.from_seq,
            min_seq: w.min_seq,
            once,
            tx,
            ready,
        });

        self.tx.send(req).await.map_err(|_| Error::Closed)?;
        let start = done.await.map_err(|_| Error::Closed)??;
        Ok(Subscription { rx, start })
    }

    /// Append a record and wait for it to reach `durability`.
    ///
    /// The record is queued, pipelined with everything else in flight,
    /// and resolved when a cumulative acknowledgement covers it. A
    /// connection lost in the meantime is reconnected and the record
    /// resent; the server discards the copy it already holds.
    ///
    /// [`Error::Timeout`] means the deadline passed, not that the
    /// record was refused. It may still be appended, and a resend of
    /// it under the same session would be deduplicated rather than
    /// stored twice.
    pub async fn write(
        &self,
        partition_key: u64,
        data: impl Into<Vec<u8>>,
        durability: Durability,
    ) -> Result<WriteAck> {
        let data = data.into();
        if data.len() > self.cfg.max_payload {
            return Err(Error::PayloadTooBig {
                len: data.len(),
                max: self.cfg.max_payload,
            });
        }

        let (waiter, done) = oneshot::channel();
        let req = Cmd::Write(Request {
            partition_key,
            data,
            required: durability.required(),
            flags: durability.flags(),
            waiter,
        });

        let deadline = self.cfg.write_deadline;
        let send = async {
            self.tx.send(req).await.map_err(|_| Error::Closed)?;
            done.await.map_err(|_| Error::Closed)?
        };

        match tokio::time::timeout(deadline, send).await {
            Ok(r) => r,
            Err(_) => Err(Error::Timeout),
        }
    }
}

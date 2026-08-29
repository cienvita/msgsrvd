//! Rust client for msgsrvd.
//!
//! ```no_run
//! use msgsrv_client::{Client, Config, Durability};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let cfg = Config::new(vec!["192.0.2.13:7400".parse()?]);
//! let c = Client::connect(cfg).await?;
//! let ack = c.write(42, b"hello".to_vec(), Durability::Sync).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Writes are pipelined. `write` queues the record and waits for the
//! acknowledgement that covers it, so many writes can be outstanding at
//! once and the server answers a batch of them with one frame. The
//! latency of a single write is a round trip plus a flush, and is not
//! what bounds throughput; the window is.
//!
//! A record is held until it is acknowledged, so a connection lost
//! mid-flight is reconnected, the session resumed and the queue sent
//! again. The server deduplicates on the session and the client's own
//! sequence, which is what makes an unconditional resend safe.
//!
//! It also follows the log. `subscribe` yields records as the node
//! flushes them, from a sequence or from whatever is written next, and
//! a connection remade carries on from the record after the last one
//! delivered. `read` is the same stream with both ends fixed: what the
//! node held when it was asked, and then the end.
//!
//! Any node serves either one from its own copy of the log, so a
//! client can follow a replica while writing to the leader.

mod client;
mod conn;
pub mod error;
pub mod log;
pub mod proto;

pub use client::{Client, Config, Durability, Notification, Subscription, Watch, WriteAck};
pub use error::{Error, Result};

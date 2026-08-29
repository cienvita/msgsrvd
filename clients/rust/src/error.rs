//! Errors the client reports.

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// The socket, or the address the connection was made to.
    Io(std::io::Error),

    /// The frame was well-formed and the server refused it. `code` is
    /// one of `proto::err`.
    Server { code: u16, text: Option<String> },

    /// A write asked for a second copy and the leader has no follower
    /// that can confirm one. Separate from `Server` because the caller
    /// has a decision to make: wait, or accept a leader-only copy with
    /// `Durability::ReplicatedOrDegraded`.
    NoReplicas,

    /// The node could not store the record: out of space, or a disk
    /// that is failing. Nothing about the record is wrong, so sending
    /// it to the same node again will not help.
    Storage,

    /// The server sent something this client cannot make sense of. The
    /// connection is dropped, since nothing after it can be trusted.
    Protocol(&'static str),

    /// The record is larger than the configured cap. Refused here
    /// rather than on the wire, where it would cost the connection.
    PayloadTooBig { len: usize, max: usize },

    /// No acknowledgement within the write deadline. The record may
    /// still be in flight: this says nothing about whether it landed,
    /// only that the wait ended.
    Timeout,

    /// The connection task has stopped, so nothing further will be
    /// sent. Its own error was reported to whoever was waiting at the
    /// time.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Server { code, text } => {
                write!(
                    f,
                    "server refused the frame: {}",
                    crate::proto::err::name(*code)
                )?;
                if let Some(t) = text {
                    write!(f, " ({t})")?;
                }
                Ok(())
            }
            Error::NoReplicas => write!(f, "no follower can confirm a second copy"),
            Error::Storage => write!(f, "the node could not store the record"),
            Error::Protocol(what) => write!(f, "protocol: {what}"),
            Error::PayloadTooBig { len, max } => {
                write!(f, "record of {len} bytes is over the {max} byte cap")
            }
            Error::Timeout => write!(f, "no acknowledgement within the deadline"),
            Error::Closed => write!(f, "the connection task has stopped"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/* An error handed to more than one waiter cannot be cloned, since
 * io::Error is not Clone. Waiters that share one connection failure
 * get a description of it instead of the value. */
impl Error {
    pub(crate) fn shared(&self) -> Error {
        match self {
            Error::Io(e) => Error::Io(std::io::Error::new(e.kind(), e.to_string())),
            Error::Server { code, text } => Error::Server {
                code: *code,
                text: text.clone(),
            },
            Error::NoReplicas => Error::NoReplicas,
            Error::Storage => Error::Storage,
            Error::Protocol(w) => Error::Protocol(w),
            Error::PayloadTooBig { len, max } => Error::PayloadTooBig {
                len: *len,
                max: *max,
            },
            Error::Timeout => Error::Timeout,
            Error::Closed => Error::Closed,
        }
    }
}

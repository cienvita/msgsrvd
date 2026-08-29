//! Wire codec for the msgsrvd protocol, version 2.
//!
//! The server memcpys its structs straight to and from the wire, so
//! every layout here is a C layout: little-endian, naturally aligned,
//! with the same explicit padding the C struct carries. Nothing is
//! parsed, each field is read from a fixed offset.
//!
//! The tests assert byte offsets against literals rather than round
//! tripping through this module, since a round trip cannot notice that
//! both halves of it moved together.

use crate::error::{Error, Result};

/// "MSGV" little-endian.
pub const MAGIC: u32 = 0x4D53_4756;

/// Protocol version this client speaks. The server accepts 2 only.
pub const VERSION: u16 = 2;

pub const HEADER_SIZE: usize = 32;

/// Largest payload the protocol allows on any frame.
///
/// A server build bounds it further by its per-connection receive
/// buffer, and answers `PAYLOAD_TOO_BIG` for anything larger. The
/// current server reads 8 KiB per connection, so records well under
/// that are what it is built for; clients coalesce small records
/// rather than sending large ones.
pub const MAX_PAYLOAD: usize = 1024 * 1024;

/// Longest client name carried by HELLO.
pub const MAX_NAME: usize = 64;

/// Longest text carried by an ERR payload.
pub const MAX_ERR_TEXT: usize = 64;

/// Operations. Server-to-client verbs are here for decoding only.
pub mod op {
    pub const WRITE: u16 = 1;
    pub const READ: u16 = 2;
    pub const DELETE: u16 = 3;
    pub const SUBSCRIBE: u16 = 4;
    pub const NOTIFY: u16 = 5;
    pub const ACK: u16 = 6;
    pub const ERR: u16 = 7;
    pub const PING: u16 = 8;
    pub const PONG: u16 = 9;
    pub const HELLO: u16 = 10;
}

/// Header flags.
pub mod flag {
    /// Sender wants an ACK.
    pub const ACK_REQ: u16 = 1 << 0;
    /// Flush before the ACK.
    pub const SYNC: u16 = 1 << 1;
    /// A follower has to confirm before the ACK.
    pub const REPLICATED: u16 = 1 << 2;
    /// Take the write even when no follower answers.
    pub const ALLOW_DEGRADED: u16 = 1 << 3;
    /// On a response: accepted, but the second copy does not exist.
    pub const DEGRADED: u16 = 1 << 4;
    /// Final record of a subscription stream.
    pub const LAST: u16 = 1 << 5;
    /// Subscribe or read: every key, rather than the header's.
    pub const ALL_KEYS: u16 = 1 << 6;
}

/// Error codes carried in an ERR payload.
///
/// 1 to 4 are framing violations and the server closes the connection
/// after sending one. 5 to 10 leave the connection up and name
/// something the client is expected to act on.
pub mod err {
    pub const OK: u16 = 0;
    pub const BAD_MAGIC: u16 = 1;
    pub const BAD_VERSION: u16 = 2;
    pub const BAD_OP: u16 = 3;
    pub const PAYLOAD_TOO_BIG: u16 = 4;
    pub const NOT_LEADER: u16 = 5;
    pub const BEHIND: u16 = 6;
    pub const NO_SESSION: u16 = 7;
    pub const SESSION_UNKNOWN: u16 = 8;
    pub const UNSUPPORTED: u16 = 9;
    pub const NO_REPLICAS: u16 = 10;
    pub const NO_HISTORY: u16 = 11;
    pub const STORAGE: u16 = 12;

    /// Whether the server closes the connection after sending `code`.
    pub fn is_fatal(code: u16) -> bool {
        matches!(code, BAD_MAGIC | BAD_VERSION | BAD_OP | NO_SESSION)
    }

    /// Short name for logs and error messages.
    pub fn name(code: u16) -> &'static str {
        match code {
            OK => "OK",
            BAD_MAGIC => "BAD_MAGIC",
            BAD_VERSION => "BAD_VERSION",
            BAD_OP => "BAD_OP",
            PAYLOAD_TOO_BIG => "PAYLOAD_TOO_BIG",
            NOT_LEADER => "NOT_LEADER",
            BEHIND => "BEHIND",
            NO_SESSION => "NO_SESSION",
            SESSION_UNKNOWN => "SESSION_UNKNOWN",
            UNSUPPORTED => "UNSUPPORTED",
            NO_REPLICAS => "NO_REPLICAS",
            NO_HISTORY => "NO_HISTORY",
            STORAGE => "STORAGE",
            _ => "UNKNOWN",
        }
    }
}

/// The 32-byte frame header.
///
/// `sequence` is dual-use, as on the server: the client-assigned
/// sequence on a WRITE, and on an ACK either the WAL sequence the
/// record landed at or, when acknowledging HELLO, the session id.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub version: u16,
    pub flags: u16,
    pub op: u16,
    pub record_type: u16,
    pub payload_len: u32,
    pub partition_key: u64,
    pub sequence: u64,
}

impl Header {
    pub fn new(op: u16, flags: u16, payload_len: u32, partition_key: u64, sequence: u64) -> Self {
        Header {
            version: VERSION,
            flags,
            op,
            record_type: 0,
            payload_len,
            partition_key,
            sequence,
        }
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut b = [0u8; HEADER_SIZE];
        b[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        b[4..6].copy_from_slice(&self.version.to_le_bytes());
        b[6..8].copy_from_slice(&self.flags.to_le_bytes());
        b[8..10].copy_from_slice(&self.op.to_le_bytes());
        b[10..12].copy_from_slice(&self.record_type.to_le_bytes());
        b[12..16].copy_from_slice(&self.payload_len.to_le_bytes());
        b[16..24].copy_from_slice(&self.partition_key.to_le_bytes());
        b[24..32].copy_from_slice(&self.sequence.to_le_bytes());
        b
    }

    pub fn decode(buf: &[u8]) -> Result<Header> {
        if buf.len() < HEADER_SIZE {
            return Err(Error::Protocol("header short"));
        }
        if u32::from_le_bytes(buf[0..4].try_into().unwrap()) != MAGIC {
            return Err(Error::Protocol("header magic"));
        }
        let h = Header {
            version: u16::from_le_bytes(buf[4..6].try_into().unwrap()),
            flags: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            op: u16::from_le_bytes(buf[8..10].try_into().unwrap()),
            record_type: u16::from_le_bytes(buf[10..12].try_into().unwrap()),
            payload_len: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            partition_key: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            sequence: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        };
        if h.version != VERSION {
            return Err(Error::Protocol("server speaks another version"));
        }
        if h.payload_len as usize > MAX_PAYLOAD {
            return Err(Error::Protocol("payload over the cap"));
        }
        Ok(h)
    }

    /// Overwrite the sequence field of an already encoded frame.
    ///
    /// A resend under a new session has to renumber, since client
    /// sequences are dense and per session. Rewriting the eight bytes
    /// beats holding the record twice.
    pub fn patch_sequence(frame: &mut [u8], sequence: u64) {
        frame[24..32].copy_from_slice(&sequence.to_le_bytes());
    }
}

/// HELLO payload: 16 bytes, then `name_len` bytes of name.
pub fn encode_hello(session: u64, name: &str) -> Vec<u8> {
    let name = name.as_bytes();
    let name = &name[..name.len().min(MAX_NAME)];

    let mut p = Vec::with_capacity(16 + name.len());
    p.extend_from_slice(&session.to_le_bytes());
    p.extend_from_slice(&(name.len() as u16).to_le_bytes());
    p.extend_from_slice(&[0u8; 6]); /* _pad0, _pad1 */
    p.extend_from_slice(name);
    p
}

/// SUBSCRIBE payload: where to start, and how current to be first.
pub fn encode_subscribe(min_seq: u64, from_seq: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(16);
    p.extend_from_slice(&min_seq.to_le_bytes());
    p.extend_from_slice(&from_seq.to_le_bytes());
    p
}

/// READ payload: how current to be before answering.
pub fn encode_read(min_seq: u64) -> Vec<u8> {
    min_seq.to_le_bytes().to_vec()
}

/// ACK payload: the cumulative client sequence.
pub fn decode_ack(payload: &[u8]) -> Result<u64> {
    if payload.len() < 8 {
        return Err(Error::Protocol("ack payload short"));
    }
    Ok(u64::from_le_bytes(payload[0..8].try_into().unwrap()))
}

/// ERR payload: code, then optional text.
pub fn decode_err(payload: &[u8]) -> Result<(u16, Option<String>)> {
    if payload.len() < 8 {
        return Err(Error::Protocol("err payload short"));
    }
    let code = u16::from_le_bytes(payload[0..2].try_into().unwrap());
    let text_len = u16::from_le_bytes(payload[2..4].try_into().unwrap()) as usize;

    if text_len > MAX_ERR_TEXT || payload.len() < 8 + text_len {
        return Err(Error::Protocol("err text length"));
    }
    let text = if text_len == 0 {
        None
    } else {
        Some(String::from_utf8_lossy(&payload[8..8 + text_len]).into_owned())
    };
    Ok((code, text))
}

#[cfg(test)]
mod tests {
    use super::*;

    /* Offsets, not a round trip: the point is to catch this side
     * drifting from the C struct, which a round trip cannot see. */
    #[test]
    fn header_bytes() {
        let h = Header::new(
            op::WRITE,
            flag::ACK_REQ | flag::SYNC,
            5,
            0x1122_3344_5566_7788,
            7,
        );
        let b = h.encode();

        assert_eq!(&b[0..4], &[0x56, 0x47, 0x53, 0x4D]); /* "VGSM" on the wire */
        assert_eq!(&b[4..6], &2u16.to_le_bytes());
        assert_eq!(&b[6..8], &3u16.to_le_bytes());
        assert_eq!(&b[8..10], &1u16.to_le_bytes());
        assert_eq!(&b[10..12], &0u16.to_le_bytes());
        assert_eq!(&b[12..16], &5u32.to_le_bytes());
        assert_eq!(&b[16..24], &0x1122_3344_5566_7788u64.to_le_bytes());
        assert_eq!(&b[24..32], &7u64.to_le_bytes());
        assert_eq!(b.len(), HEADER_SIZE);
    }

    #[test]
    fn header_round_trip() {
        let h = Header::new(op::HELLO, 0, 16, 0, 0);
        assert_eq!(Header::decode(&h.encode()).unwrap(), h);
    }

    #[test]
    fn header_rejects_foreign_frames() {
        let mut b = Header::new(op::ACK, 0, 0, 0, 0).encode();
        b[0] ^= 0xFF;
        assert!(Header::decode(&b).is_err());

        let mut b = Header::new(op::ACK, 0, 0, 0, 0).encode();
        b[4..6].copy_from_slice(&1u16.to_le_bytes());
        assert!(Header::decode(&b).is_err());

        assert!(Header::decode(&[0u8; 8]).is_err());
    }

    #[test]
    fn hello_layout() {
        let p = encode_hello(9, "ab");
        assert_eq!(p.len(), 18);
        assert_eq!(&p[0..8], &9u64.to_le_bytes());
        assert_eq!(&p[8..10], &2u16.to_le_bytes());
        assert_eq!(&p[10..16], &[0u8; 6]);
        assert_eq!(&p[16..18], b"ab");
    }

    /* The server validates name_len against the frame length, so an
     * over-long name has to be cut here rather than declared. */
    #[test]
    fn hello_name_is_bounded() {
        let p = encode_hello(0, &"x".repeat(MAX_NAME * 2));
        assert_eq!(p.len(), 16 + MAX_NAME);
        assert_eq!(&p[8..10], &(MAX_NAME as u16).to_le_bytes());
    }

    #[test]
    fn ack_payload() {
        assert_eq!(decode_ack(&41u64.to_le_bytes()).unwrap(), 41);
        assert!(decode_ack(&[0u8; 4]).is_err());
    }

    #[test]
    fn err_payload() {
        let mut p = Vec::new();
        p.extend_from_slice(&err::NOT_LEADER.to_le_bytes());
        p.extend_from_slice(&4u16.to_le_bytes());
        p.extend_from_slice(&0u32.to_le_bytes());
        p.extend_from_slice(b"h:80");

        let (code, text) = decode_err(&p).unwrap();
        assert_eq!(code, err::NOT_LEADER);
        assert_eq!(text.as_deref(), Some("h:80"));

        /* A text length the frame cannot back is a lie, not a short read. */
        let mut bad = p.clone();
        bad[2..4].copy_from_slice(&40u16.to_le_bytes());
        assert!(decode_err(&bad).is_err());
    }
}

//! Reading a log directory from outside the server.
//!
//! A second decoder for the on-disk format, written from the layout in
//! the server's `wal/record.h` rather than sharing code with it. The
//! server owns the checksums; nothing here verifies one, because a
//! record that fails its checksum is one the server's own recovery
//! scan drops before anyone sees it.
//!
//! This exists because there is no read path yet. Until a client can
//! ask a node what it holds, looking at the files is the only way to
//! see inside a log, and both the kill tests and the lab tool do that.

use std::path::{Path, PathBuf};

/// One record, as it sits in a segment file.
#[derive(Clone, Debug)]
pub struct Record {
    pub seq: u64,
    pub session: u64,
    pub client_seq: u64,
    /// The key the record was written under, which is what a reader
    /// asks for and what the server indexes by.
    pub partition_key: u64,
    pub payload: Vec<u8>,
}

impl Record {
    /// The payload as text, for a log whose records are readable.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.payload).into_owned()
    }
}

/// Fixed part of a record on disk.
pub const REC_HEADER_SIZE: usize = 56;

/// Every record in a log directory, in sequence order.
pub fn read_dir(dir: &Path) -> Vec<Record> {
    let mut names: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| {
                let p = e.ok()?.path();
                (p.extension()? == "seg").then_some(p)
            })
            .collect(),
        Err(_) => return Vec::new(),
    };
    names.sort();

    let mut out = Vec::new();
    for name in names {
        let bytes = match std::fs::read(&name) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let mut at = 0usize;

        while at + REC_HEADER_SIZE <= bytes.len() {
            let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap()) as usize;
            let seq = u64::from_le_bytes(bytes[at + 16..at + 24].try_into().unwrap());

            /* Preallocated space reads back as zeros, and a real
             * record's sequence starts at 1, so this is the end. */
            if seq == 0 {
                break;
            }

            let size = REC_HEADER_SIZE + ((len + 7) & !7);
            if at + size > bytes.len() {
                break;
            }

            out.push(Record {
                seq,
                session: u64::from_le_bytes(bytes[at + 24..at + 32].try_into().unwrap()),
                client_seq: u64::from_le_bytes(bytes[at + 32..at + 40].try_into().unwrap()),
                partition_key: u64::from_le_bytes(
                    bytes[at + 48..at + 56].try_into().unwrap(),
                ),
                payload: bytes[at + REC_HEADER_SIZE..at + REC_HEADER_SIZE + len].to_vec(),
            });
            at += size;
        }
    }

    out.sort_by_key(|r| r.seq);
    out
}

/// What a log holds for each key: where it starts, where it ends, and
/// how many records it has.
///
/// The same summary the server keeps in memory and prints for
/// `--inspect`, counted here from the files instead, so it can be had
/// without logging in to the node that owns them.
#[derive(Clone, Debug)]
pub struct KeySummary {
    pub key: u64,
    pub first: u64,
    pub last: u64,
    pub count: u64,
}

pub fn keys(dir: &Path) -> Vec<KeySummary> {
    let mut out: Vec<KeySummary> = Vec::new();

    for r in read_dir(dir) {
        match out.iter_mut().find(|k| k.key == r.partition_key) {
            Some(k) => {
                k.last = r.seq;
                k.count += 1;
            }
            None => out.push(KeySummary {
                key: r.partition_key,
                first: r.seq,
                last: r.seq,
                count: 1,
            }),
        }
    }

    out.sort_by_key(|k| k.key);
    out
}

/// The highest sequence a log holds, read without starting the node.
///
/// This is what a promotion compares between two replicas, and what
/// tells a watcher how far behind a replica is.
pub fn last_seq(dir: &Path) -> u64 {
    read_dir(dir).last().map(|r| r.seq).unwrap_or(0)
}

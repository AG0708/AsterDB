//! Durable page-oriented storage for `AsterDB`.
//!
//! The crate deliberately owns its binary formats instead of serializing Rust
//! structs. Every multi-byte field is little-endian, every page and WAL frame
//! is checksummed, and torn WAL tails are distinguished from corruption in the
//! middle of the log.

#![forbid(unsafe_code)]

pub mod btree;
pub mod buffer;
pub mod checksum;
pub mod disk;
pub mod heap;
pub mod page;
pub mod recovery;
pub mod wal;

use std::{fmt, io};

/// Stable on-disk page identifier. Page zero is available to higher layers for
/// a superblock; allocators in this crate start at page one by default.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageId(pub u64);

/// Byte offset of a WAL frame's first byte.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Lsn(pub u64);

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    InvalidPage(String),
    ChecksumMismatch { expected: u32, actual: u32 },
    CorruptWal { offset: u64, reason: String },
    BufferPoolExhausted,
    PagePinned(PageId),
    RecordTooLarge { bytes: usize, available: usize },
    KeyTooLarge { bytes: usize },
    NotFound(String),
    Invariant(String),
    InjectedFault { operation: String, ordinal: u64 },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "storage I/O error: {err}"),
            Self::InvalidPage(reason) => write!(f, "invalid page: {reason}"),
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
                )
            }
            Self::CorruptWal { offset, reason } => {
                write!(f, "corrupt WAL at byte {offset}: {reason}")
            }
            Self::BufferPoolExhausted => write!(f, "all buffer-pool frames are pinned"),
            Self::PagePinned(id) => write!(f, "page {} is still pinned", id.0),
            Self::RecordTooLarge { bytes, available } => {
                write!(
                    f,
                    "record requires {bytes} bytes, only {available} available"
                )
            }
            Self::KeyTooLarge { bytes } => write!(f, "key/value entry of {bytes} bytes cannot fit"),
            Self::NotFound(value) => write!(f, "not found: {value}"),
            Self::Invariant(reason) => write!(f, "storage invariant failed: {reason}"),
            Self::InjectedFault { operation, ordinal } => {
                write!(f, "injected {operation} fault at operation {ordinal}")
            }
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

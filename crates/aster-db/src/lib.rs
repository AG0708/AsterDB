//! Integrated standalone database facade.
//!
//! This crate is the vertical boundary joining the SQL frontend, the MVCC
//! state machine, and checksummed WAL-before-data persistence. Mutating state
//! is constructed on a private engine clone and private B+ tree page images;
//! neither is published until the durable pager has committed the complete
//! redo group.

#![forbid(unsafe_code)]

mod catalog;
mod database;
mod error;
mod executor;
mod persistence;

pub use database::{
    CheckpointInfo, CommitInfo, Database, DatabaseOptions, DatabaseSnapshot, DatabaseStatus,
    ExecutionResult, MutationPreparation, PreparedMutation, QueryResult, TransactionInfo,
};
pub use error::{DatabaseError, Result};

pub use aster_core::{DataType, Value};

/// Hard wire-compatible result bound. Larger results fail closed instead of
/// silently truncating a SQL result set.
pub const MAX_RESULT_ROWS: usize = 16_384;

/// Bounds a Cartesian product or other physical-plan intermediate before it
/// can exhaust the process while the final result is small.
pub const MAX_INTERMEDIATE_ROWS: usize = 1_000_000;

/// Maximum canonical engine snapshot accepted by the standalone pager.
pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;

//! Deterministic relational execution and MVCC state-machine boundary.
//!
//! The engine deliberately has no dependency on Raft or a particular storage
//! implementation. A leader constructs a canonical [`TxnAttempt`], Raft orders
//! its bytes, and every replica calls [`Engine::apply`] with the committed log
//! index. This crate then performs the conflict check and assigns that index to
//! every version made visible by the transaction.

#![forbid(unsafe_code)]

mod catalog;
mod engine;
mod error;
mod expression;
mod query;
mod transaction;

pub use catalog::{Catalog, CatalogMutation, IndexMeta, TableMeta};
pub use engine::{
    AbortReason, ApplyOutcome, ClientRecord, CommitResult, Engine, EngineSnapshot, EngineStats,
    RequestRejection, SnapshotRecord, VacuumReport,
};
pub use error::{EngineError, Result};
pub use expression::{BinaryOp, Expr, Truth, UnaryOp};
pub use query::{OrderBy, Query, ScanSource};
pub use transaction::{Mutation, Transaction, TxnAttempt, TxnWrite};

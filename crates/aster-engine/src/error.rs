use aster_core::Error as CoreError;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EngineError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("unknown table {0}")]
    UnknownTable(u64),
    #[error("unknown index {0}")]
    UnknownIndex(u64),
    #[error("unknown column `{column}` in table `{table}`")]
    UnknownColumn { table: String, column: String },
    #[error("transaction {0} is already finished")]
    TransactionFinished(u64),
    #[error("snapshot {requested} is below the retained MVCC floor {floor}")]
    SnapshotTooOld { requested: u64, floor: u64 },
    #[error("non-contiguous apply index: expected {expected}, got {actual}")]
    ApplyGap { expected: u64, actual: u64 },
    #[error("replayed Raft index {index} with a different command hash")]
    ApplyHashMismatch { index: u64 },
    #[error("provided command hash does not match canonical transaction bytes")]
    CommandHashMismatch,
    #[error("invalid transaction attempt: {0}")]
    InvalidAttempt(String),
    #[error("invalid snapshot: {0}")]
    InvalidSnapshot(String),
    #[error("expression evaluation failed: {0}")]
    Expression(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;

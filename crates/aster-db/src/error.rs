use std::io;

use aster_engine::{AbortReason, EngineError, RequestRejection};
use aster_sql::SqlError;
use aster_storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error(transparent)]
    Sql(#[from] SqlError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("database I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("transaction {0} does not exist")]
    TransactionNotFound(u64),
    #[error("invalid transaction control: {0}")]
    TransactionControl(String),
    #[error("transaction aborted: {0:?}")]
    TransactionAborted(AbortReason),
    #[error("client request rejected: {0:?}")]
    RequestRejected(RequestRejection),
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("database corruption detected: {0}")]
    Corruption(String),
    #[error("internal database invariant failed: {0}")]
    Invariant(String),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;

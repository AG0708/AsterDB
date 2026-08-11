use aster_core::{IndexId, TableId, Value};
use serde::{Deserialize, Serialize};

use crate::Expr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScanSource {
    Table(TableId),
    SecondaryEq { index_id: IndexId, value: Value },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBy {
    pub expression: Expr,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    pub source: ScanSource,
    pub filter: Option<Expr>,
    /// An empty projection returns the source row unchanged.
    pub projection: Vec<Expr>,
    pub order_by: Vec<OrderBy>,
    pub offset: usize,
    pub limit: Option<usize>,
}

impl Query {
    #[must_use]
    pub const fn table(table_id: TableId) -> Self {
        Self {
            source: ScanSource::Table(table_id),
            filter: None,
            projection: Vec::new(),
            order_by: Vec::new(),
            offset: 0,
            limit: None,
        }
    }
}

use std::collections::BTreeSet;

use aster_core::{IndexId, Schema, TableId};

use crate::ast::{Ident, Span};
use crate::{Result, SqlError, SqlErrorKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub id: IndexId,
    pub name: String,
    pub table_id: TableId,
    pub column_index: usize,
}

/// Read-only catalog surface required by semantic analysis and optimization.
pub trait Catalog {
    fn resolve_table(&self, name: &Ident) -> Option<TableDef>;
    fn resolve_index(&self, name: &Ident) -> Option<IndexDef>;
    fn indexes_for_table(&self, table_id: TableId) -> Vec<IndexDef>;
}

/// Deterministic in-memory catalog for planning, tests, and embedded users.
#[derive(Debug, Clone, Default)]
pub struct MemoryCatalog {
    tables: Vec<TableDef>,
    indexes: Vec<IndexDef>,
}

impl MemoryCatalog {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tables: Vec::new(),
            indexes: Vec::new(),
        }
    }

    pub fn add_table(&mut self, table: TableDef) -> Result<()> {
        table.schema.validate().map_err(|error| {
            SqlError::new(SqlErrorKind::Constraint, error.to_string(), Span::default())
        })?;
        if self.tables.iter().any(|candidate| {
            candidate.id == table.id || candidate.name.eq_ignore_ascii_case(&table.name)
        }) || self
            .indexes
            .iter()
            .any(|candidate| candidate.name.eq_ignore_ascii_case(&table.name))
        {
            return Err(SqlError::new(
                SqlErrorKind::Constraint,
                format!("duplicate table id or name `{}`", table.name),
                Span::default(),
            ));
        }
        self.tables.push(table);
        Ok(())
    }

    pub fn add_index(&mut self, index: IndexDef) -> Result<()> {
        if self.indexes.iter().any(|candidate| {
            candidate.id == index.id || candidate.name.eq_ignore_ascii_case(&index.name)
        }) || self
            .tables
            .iter()
            .any(|candidate| candidate.name.eq_ignore_ascii_case(&index.name))
        {
            return Err(SqlError::new(
                SqlErrorKind::Constraint,
                format!("duplicate index id or name `{}`", index.name),
                Span::default(),
            ));
        }
        let table = self
            .tables
            .iter()
            .find(|table| table.id == index.table_id)
            .ok_or_else(|| {
                SqlError::new(
                    SqlErrorKind::Bind,
                    format!("index `{}` references an unknown table", index.name),
                    Span::default(),
                )
            })?;
        if index.column_index >= table.schema.columns.len() {
            return Err(SqlError::new(
                SqlErrorKind::Constraint,
                format!(
                    "index `{}` references column {}, but table `{}` has {} columns",
                    index.name,
                    index.column_index,
                    table.name,
                    table.schema.columns.len()
                ),
                Span::default(),
            ));
        }
        self.indexes.push(index);
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        let mut names = BTreeSet::new();
        for table in &self.tables {
            let canonical = table.name.to_ascii_lowercase();
            if !names.insert(canonical) {
                return Err(SqlError::new(
                    SqlErrorKind::Constraint,
                    "catalog contains duplicate object names",
                    Span::default(),
                ));
            }
        }
        for index in &self.indexes {
            let canonical = index.name.to_ascii_lowercase();
            if !names.insert(canonical) {
                return Err(SqlError::new(
                    SqlErrorKind::Constraint,
                    "catalog contains duplicate object names",
                    Span::default(),
                ));
            }
        }
        Ok(())
    }
}

impl Catalog for MemoryCatalog {
    fn resolve_table(&self, name: &Ident) -> Option<TableDef> {
        self.tables
            .iter()
            .find(|table| name.matches(&table.name))
            .cloned()
    }

    fn resolve_index(&self, name: &Ident) -> Option<IndexDef> {
        self.indexes
            .iter()
            .find(|index| name.matches(&index.name))
            .cloned()
    }

    fn indexes_for_table(&self, table_id: TableId) -> Vec<IndexDef> {
        self.indexes
            .iter()
            .filter(|index| index.table_id == table_id)
            .cloned()
            .collect()
    }
}

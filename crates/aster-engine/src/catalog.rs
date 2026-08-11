use std::collections::{BTreeMap, BTreeSet};

use aster_core::{Error as CoreError, IndexId, Schema, TableId};
use serde::{Deserialize, Serialize};

use crate::{EngineError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMeta {
    pub id: TableId,
    pub name: String,
    pub schema: Schema,
    pub primary_key_column: usize,
}

impl TableMeta {
    pub fn new(id: TableId, name: impl Into<String>, schema: Schema) -> Result<Self> {
        let name = name.into();
        validate_identifier(&name, "table")?;
        if id == 0 {
            return Err(CoreError::Constraint("table id zero is reserved".into()).into());
        }
        let primary_key_column = schema.validate()?;
        Ok(Self {
            id,
            name,
            schema,
            primary_key_column,
        })
    }

    pub fn column(&self, name: &str) -> Result<usize> {
        self.schema
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| EngineError::UnknownColumn {
                table: self.name.clone(),
                column: name.to_owned(),
            })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_identifier(&self.name, "table")?;
        if self.id == 0 {
            return Err(CoreError::Constraint("table id zero is reserved".into()).into());
        }
        let actual = self.schema.validate()?;
        if actual != self.primary_key_column {
            return Err(EngineError::InvalidSnapshot(format!(
                "table {} stores primary-key column {}, schema says {actual}",
                self.id, self.primary_key_column
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMeta {
    pub id: IndexId,
    pub name: String,
    pub table_id: TableId,
    pub column: usize,
}

impl IndexMeta {
    pub fn new(
        id: IndexId,
        name: impl Into<String>,
        table: &TableMeta,
        column: usize,
    ) -> Result<Self> {
        let name = name.into();
        validate_identifier(&name, "index")?;
        if id == 0 {
            return Err(CoreError::Constraint("index id zero is reserved".into()).into());
        }
        if column >= table.schema.columns.len() {
            return Err(CoreError::Constraint(format!(
                "index column {column} is outside table `{}`",
                table.name
            ))
            .into());
        }
        Ok(Self {
            id,
            name,
            table_id: table.id,
            column,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogMutation {
    CreateTable(TableMeta),
    CreateIndex(IndexMeta),
}

impl CatalogMutation {
    pub(crate) const fn sort_key(&self) -> (u8, u64) {
        match self {
            Self::CreateTable(table) => (0, table.id),
            Self::CreateIndex(index) => (1, index.id),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    schema_epoch: u64,
    tables: BTreeMap<TableId, TableMeta>,
    table_names: BTreeMap<String, TableId>,
    indexes: BTreeMap<IndexId, IndexMeta>,
    index_names: BTreeMap<String, IndexId>,
}

impl Catalog {
    #[must_use]
    pub const fn schema_epoch(&self) -> u64 {
        self.schema_epoch
    }

    #[must_use]
    pub fn table(&self, id: TableId) -> Option<&TableMeta> {
        self.tables.get(&id)
    }

    #[must_use]
    pub fn table_named(&self, name: &str) -> Option<&TableMeta> {
        self.table_names
            .get(&normalize(name))
            .and_then(|id| self.tables.get(id))
    }

    #[must_use]
    pub fn index(&self, id: IndexId) -> Option<&IndexMeta> {
        self.indexes.get(&id)
    }

    #[must_use]
    pub fn index_named(&self, name: &str) -> Option<&IndexMeta> {
        self.index_names
            .get(&normalize(name))
            .and_then(|id| self.indexes.get(id))
    }

    #[must_use]
    pub fn tables(&self) -> impl ExactSizeIterator<Item = &TableMeta> {
        self.tables.values()
    }

    #[must_use]
    pub fn indexes(&self) -> impl ExactSizeIterator<Item = &IndexMeta> {
        self.indexes.values()
    }

    pub fn indexes_for_table(&self, table_id: TableId) -> impl Iterator<Item = &IndexMeta> {
        self.indexes
            .values()
            .filter(move |index| index.table_id == table_id)
    }

    #[must_use]
    pub fn next_table_id(&self) -> TableId {
        self.tables
            .last_key_value()
            .map_or(1, |(id, _)| id.saturating_add(1))
    }

    #[must_use]
    pub fn next_index_id(&self) -> IndexId {
        self.indexes
            .last_key_value()
            .map_or(1, |(id, _)| id.saturating_add(1))
    }

    pub(crate) fn apply_mutations(
        &mut self,
        mutations: &[CatalogMutation],
    ) -> Result<Vec<IndexId>> {
        let mut new_indexes = Vec::new();
        for mutation in mutations {
            match mutation {
                CatalogMutation::CreateTable(table) => self.create_table(table.clone())?,
                CatalogMutation::CreateIndex(index) => {
                    self.create_index(index.clone())?;
                    new_indexes.push(index.id);
                }
            }
        }
        if !mutations.is_empty() {
            self.schema_epoch = self
                .schema_epoch
                .checked_add(1)
                .ok_or_else(|| CoreError::Invariant("schema epoch overflow".into()))?;
        }
        Ok(new_indexes)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let mut table_names = BTreeSet::new();
        for (id, table) in &self.tables {
            if *id != table.id {
                return Err(EngineError::InvalidSnapshot(format!(
                    "table map key {id} differs from metadata id {}",
                    table.id
                )));
            }
            table.validate()?;
            if !table_names.insert(normalize(&table.name)) {
                return Err(EngineError::InvalidSnapshot(format!(
                    "duplicate table name `{}`",
                    table.name
                )));
            }
        }
        let expected_table_names: BTreeMap<_, _> = self
            .tables
            .values()
            .map(|table| (normalize(&table.name), table.id))
            .collect();
        if expected_table_names != self.table_names {
            return Err(EngineError::InvalidSnapshot(
                "table-name lookup does not match table metadata".into(),
            ));
        }

        let mut index_names = BTreeSet::new();
        for (id, index) in &self.indexes {
            if *id != index.id || index.id == 0 {
                return Err(EngineError::InvalidSnapshot(format!(
                    "invalid index map entry {id}"
                )));
            }
            validate_identifier(&index.name, "index")?;
            let table = self
                .tables
                .get(&index.table_id)
                .ok_or(EngineError::UnknownTable(index.table_id))?;
            if index.column >= table.schema.columns.len() {
                return Err(EngineError::InvalidSnapshot(format!(
                    "index {} references missing column {}",
                    index.id, index.column
                )));
            }
            if !index_names.insert(normalize(&index.name)) {
                return Err(EngineError::InvalidSnapshot(format!(
                    "duplicate index name `{}`",
                    index.name
                )));
            }
        }
        let expected_index_names: BTreeMap<_, _> = self
            .indexes
            .values()
            .map(|index| (normalize(&index.name), index.id))
            .collect();
        if expected_index_names != self.index_names {
            return Err(EngineError::InvalidSnapshot(
                "index-name lookup does not match index metadata".into(),
            ));
        }
        Ok(())
    }

    fn create_table(&mut self, table: TableMeta) -> Result<()> {
        table.validate()?;
        if self.tables.contains_key(&table.id) {
            return Err(
                CoreError::Constraint(format!("table id {} already exists", table.id)).into(),
            );
        }
        let normalized = normalize(&table.name);
        if self.table_names.contains_key(&normalized) {
            return Err(
                CoreError::Constraint(format!("table `{}` already exists", table.name)).into(),
            );
        }
        self.table_names.insert(normalized, table.id);
        self.tables.insert(table.id, table);
        Ok(())
    }

    fn create_index(&mut self, index: IndexMeta) -> Result<()> {
        validate_identifier(&index.name, "index")?;
        if index.id == 0 {
            return Err(CoreError::Constraint("index id zero is reserved".into()).into());
        }
        if self.indexes.contains_key(&index.id) {
            return Err(
                CoreError::Constraint(format!("index id {} already exists", index.id)).into(),
            );
        }
        let normalized = normalize(&index.name);
        if self.index_names.contains_key(&normalized) {
            return Err(
                CoreError::Constraint(format!("index `{}` already exists", index.name)).into(),
            );
        }
        let table = self
            .tables
            .get(&index.table_id)
            .ok_or(EngineError::UnknownTable(index.table_id))?;
        if index.column >= table.schema.columns.len() {
            return Err(CoreError::Constraint(format!(
                "index {} references column {} outside table `{}`",
                index.id, index.column, table.name
            ))
            .into());
        }
        self.index_names.insert(normalized, index.id);
        self.indexes.insert(index.id, index);
        Ok(())
    }
}

fn normalize(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn validate_identifier(name: &str, kind: &str) -> Result<()> {
    let mut chars = name.chars();
    let valid_first = chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic());
    if !valid_first || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        return Err(CoreError::Constraint(format!("invalid {kind} identifier `{name}`")).into());
    }
    Ok(())
}

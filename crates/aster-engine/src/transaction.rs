use std::collections::BTreeMap;

use aster_core::codec::{encode_ordered, encode_row, encode_value};
use aster_core::{
    ClientRequestId, Column, DataType, Error as CoreError, Row, TableId, TxnId, Value,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CatalogMutation, EngineError, IndexMeta, Result, TableMeta};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    Put(Row),
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnWrite {
    pub table_id: TableId,
    pub primary_key: Value,
    pub mutation: Mutation,
}

impl TxnWrite {
    pub(crate) fn logical_key(&self) -> Result<LogicalKey> {
        LogicalKey::new(self.table_id, &self.primary_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LogicalKey {
    pub table_id: TableId,
    pub primary_key: Vec<u8>,
}

impl LogicalKey {
    pub fn new(table_id: TableId, primary_key: &Value) -> Result<Self> {
        // `encode_ordered` is the physical ordering; `encode_value` first
        // enforces the core value-size boundary.
        let mut validation = Vec::new();
        encode_value(primary_key, &mut validation)?;
        let mut encoded = Vec::new();
        encode_ordered(primary_key, &mut encoded);
        Ok(Self {
            table_id,
            primary_key: encoded,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {
    id: TxnId,
    read_ts: u64,
    schema_epoch: u64,
    writes: BTreeMap<LogicalKey, TxnWrite>,
    catalog_mutations: Vec<CatalogMutation>,
}

impl Transaction {
    pub(crate) fn new(id: TxnId, read_ts: u64, schema_epoch: u64) -> Self {
        Self {
            id,
            read_ts,
            schema_epoch,
            writes: BTreeMap::new(),
            catalog_mutations: Vec::new(),
        }
    }

    #[must_use]
    pub const fn id(&self) -> TxnId {
        self.id
    }

    #[must_use]
    pub const fn read_ts(&self) -> u64 {
        self.read_ts
    }

    #[must_use]
    pub const fn schema_epoch(&self) -> u64 {
        self.schema_epoch
    }

    #[must_use]
    pub fn write_count(&self) -> usize {
        self.writes.len()
    }

    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.writes.is_empty() && self.catalog_mutations.is_empty()
    }

    pub fn rollback(self) {
        // A transaction-private overlay has no shared side effects. Consuming
        // the handle makes rollback explicit and leaves nothing to undo.
    }

    #[must_use]
    pub fn into_attempt(self, request: ClientRequestId, leader_term: u64) -> TxnAttempt {
        TxnAttempt {
            request,
            leader_term,
            read_ts: self.read_ts,
            schema_epoch: self.schema_epoch,
            writes: self.writes.into_values().collect(),
            catalog_mutations: self.catalog_mutations,
        }
    }

    pub(crate) fn overlay(&self, key: &LogicalKey) -> Option<&Mutation> {
        self.writes.get(key).map(|write| &write.mutation)
    }

    pub(crate) fn writes(&self) -> impl Iterator<Item = (&LogicalKey, &TxnWrite)> {
        self.writes.iter()
    }

    pub(crate) fn stage_write(&mut self, write: TxnWrite) -> Result<()> {
        let key = write.logical_key()?;
        self.writes.insert(key, write);
        Ok(())
    }

    pub(crate) fn stage_catalog(&mut self, mutation: CatalogMutation) {
        self.catalog_mutations.push(mutation);
        self.catalog_mutations
            .sort_by_key(CatalogMutation::sort_key);
    }

    pub(crate) fn staged_table(&self, id: TableId) -> Option<&TableMeta> {
        self.catalog_mutations
            .iter()
            .find_map(|mutation| match mutation {
                CatalogMutation::CreateTable(table) if table.id == id => Some(table),
                _ => None,
            })
    }

    pub(crate) fn staged_index(&self, id: u64) -> Option<&IndexMeta> {
        self.catalog_mutations
            .iter()
            .find_map(|mutation| match mutation {
                CatalogMutation::CreateIndex(index) if index.id == id => Some(index),
                _ => None,
            })
    }

    pub(crate) fn catalog_mutations(&self) -> &[CatalogMutation] {
        &self.catalog_mutations
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnAttempt {
    request: ClientRequestId,
    leader_term: u64,
    read_ts: u64,
    schema_epoch: u64,
    writes: Vec<TxnWrite>,
    catalog_mutations: Vec<CatalogMutation>,
}

impl TxnAttempt {
    #[must_use]
    pub const fn request(&self) -> &ClientRequestId {
        &self.request
    }

    #[must_use]
    pub const fn leader_term(&self) -> u64 {
        self.leader_term
    }

    #[must_use]
    pub const fn read_ts(&self) -> u64 {
        self.read_ts
    }

    #[must_use]
    pub const fn schema_epoch(&self) -> u64 {
        self.schema_epoch
    }

    #[must_use]
    pub fn writes(&self) -> &[TxnWrite] {
        &self.writes
    }

    #[must_use]
    pub fn catalog_mutations(&self) -> &[CatalogMutation] {
        &self.catalog_mutations
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate_canonical()?;
        let mut output = Vec::new();
        output.extend_from_slice(b"ASTRTXN\x01");
        output.extend_from_slice(&self.request.client_id);
        put_u64(&mut output, self.request.sequence);
        put_u64(&mut output, self.leader_term);
        put_u64(&mut output, self.read_ts);
        put_u64(&mut output, self.schema_epoch);
        put_len(&mut output, self.writes.len())?;
        for write in &self.writes {
            put_u64(&mut output, write.table_id);
            put_value(&mut output, &write.primary_key)?;
            match &write.mutation {
                Mutation::Put(row) => {
                    output.push(1);
                    let encoded = encode_row(row)?;
                    put_bytes(&mut output, &encoded)?;
                }
                Mutation::Delete => output.push(2),
            }
        }
        put_len(&mut output, self.catalog_mutations.len())?;
        for mutation in &self.catalog_mutations {
            match mutation {
                CatalogMutation::CreateTable(table) => {
                    output.push(1);
                    put_u64(&mut output, table.id);
                    put_string(&mut output, &table.name)?;
                    put_len(&mut output, table.schema.columns.len())?;
                    for column in &table.schema.columns {
                        put_column(&mut output, column)?;
                    }
                    put_u64(
                        &mut output,
                        u64::try_from(table.primary_key_column).map_err(|_| {
                            EngineError::InvalidAttempt("primary-key column overflow".into())
                        })?,
                    );
                }
                CatalogMutation::CreateIndex(index) => {
                    output.push(2);
                    put_u64(&mut output, index.id);
                    put_string(&mut output, &index.name)?;
                    put_u64(&mut output, index.table_id);
                    put_u64(
                        &mut output,
                        u64::try_from(index.column).map_err(|_| {
                            EngineError::InvalidAttempt("index column overflow".into())
                        })?,
                    );
                }
            }
        }
        Ok(output)
    }

    pub fn command_hash(&self) -> Result<[u8; 32]> {
        let digest = Sha256::digest(self.canonical_bytes()?);
        Ok(digest.into())
    }

    pub(crate) fn validate_canonical(&self) -> Result<()> {
        let mut previous: Option<LogicalKey> = None;
        for write in &self.writes {
            let key = write.logical_key()?;
            if previous.as_ref().is_some_and(|prior| prior >= &key) {
                return Err(EngineError::InvalidAttempt(
                    "writes must be strictly ordered by logical primary key".into(),
                ));
            }
            previous = Some(key);
        }
        if self
            .catalog_mutations
            .windows(2)
            .any(|pair| pair[0].sort_key() >= pair[1].sort_key())
        {
            return Err(EngineError::InvalidAttempt(
                "catalog mutations must have unique canonical ids".into(),
            ));
        }
        Ok(())
    }
}

fn put_column(output: &mut Vec<u8>, column: &Column) -> Result<()> {
    put_string(output, &column.name)?;
    output.push(match column.data_type {
        DataType::Int64 => 1,
        DataType::Bool => 2,
        DataType::Text => 3,
        DataType::Bytes => 4,
    });
    output.push(u8::from(column.nullable));
    output.push(u8::from(column.primary_key));
    Ok(())
}

fn put_value(output: &mut Vec<u8>, value: &Value) -> Result<()> {
    let mut encoded = Vec::new();
    encode_value(value, &mut encoded)?;
    put_bytes(output, &encoded)
}

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    put_bytes(output, value.as_bytes())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_len(output, value.len())?;
    output.extend_from_slice(value);
    Ok(())
}

fn put_len(output: &mut Vec<u8>, value: usize) -> Result<()> {
    let length = u32::try_from(value)
        .map_err(|_| CoreError::LimitExceeded("canonical command field is too large".into()))?;
    output.extend_from_slice(&length.to_be_bytes());
    Ok(())
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

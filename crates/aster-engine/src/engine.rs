use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use aster_core::codec::{encode_ordered, encode_value};
use aster_core::{
    ClientRequestId, Error as CoreError, IndexId, Row, Schema, TableId, TxnId, Value,
};
use serde::{Deserialize, Serialize};

use crate::transaction::LogicalKey;
use crate::{
    Catalog, CatalogMutation, EngineError, IndexMeta, Mutation, OrderBy, Query, Result, ScanSource,
    TableMeta, Transaction, TxnAttempt, TxnWrite,
};

const SNAPSHOT_FORMAT: u32 = 2;
const MAX_SNAPSHOT_RECORD_BYTES: usize = 64 * 1024 * 1024;
const RECORD_META: u8 = 0;
const RECORD_CATALOG: u8 = 1;
const RECORD_PRIMARY: u8 = 2;
const RECORD_SECONDARY: u8 = 3;
const RECORD_CLIENT: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbortReason {
    WriteConflict {
        table_id: TableId,
        primary_key: Value,
        winning_commit: u64,
    },
    SchemaChanged {
        expected: u64,
        actual: u64,
    },
    SnapshotVacuumed {
        read_ts: u64,
        retained_floor: u64,
    },
    Validation(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitResult {
    Committed {
        commit_index: u64,
        affected_rows: usize,
        schema_epoch: u64,
    },
    Aborted {
        abort_index: u64,
        reason: AbortReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestRejection {
    FirstSequenceMustBeOne { actual: u64 },
    SequenceGap { expected: u64, actual: u64 },
    SequenceTooOld { latest: u64, actual: u64 },
    SequenceHashMismatch { sequence: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyOutcome {
    Applied(CommitResult),
    Duplicate(CommitResult),
    Rejected(RequestRejection),
    /// A committed Raft leader no-op advances the MVCC timestamp space without
    /// changing catalog, row, or client-request state.
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRecord {
    pub sequence: u64,
    /// Hash of the logical client request, stable across leader terms and
    /// transaction re-preparation. The alias permits format-version detection
    /// to produce a clear error for pre-v2 snapshots.
    #[serde(alias = "command_hash")]
    pub request_hash: [u8; 32],
    pub result: CommitResult,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineStats {
    pub committed_transactions: u64,
    pub aborted_transactions: u64,
    pub duplicate_requests: u64,
    pub rejected_requests: u64,
    pub primary_versions: u64,
    pub secondary_versions: u64,
    pub vacuumed_primary_versions: u64,
    pub vacuumed_secondary_versions: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VacuumReport {
    pub previous_floor: u64,
    pub retained_floor: u64,
    pub primary_versions_removed: usize,
    pub secondary_versions_removed: usize,
    pub empty_primary_chains_removed: usize,
    pub empty_secondary_chains_removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RowVersion {
    commit_index: u64,
    row: Option<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct SecondaryKey {
    index_id: IndexId,
    secondary_value: Vec<u8>,
    primary_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SecondaryVersion {
    commit_index: u64,
    present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PrimarySnapshotEntry {
    table_id: TableId,
    primary_key: Vec<u8>,
    versions: Vec<RowVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SecondarySnapshotEntry {
    key: SecondaryKey,
    versions: Vec<SecondaryVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClientSnapshotEntry {
    client_id: [u8; 16],
    record: ClientRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineSnapshot {
    format: u32,
    catalog: Catalog,
    primary: Vec<PrimarySnapshotEntry>,
    secondary: Vec<SecondarySnapshotEntry>,
    clients: Vec<ClientSnapshotEntry>,
    last_applied: u64,
    last_apply_hash: Option<[u8; 32]>,
    last_apply_outcome: Option<ApplyOutcome>,
    retained_floor: u64,
    next_txn_id: TxnId,
    stats: EngineStats,
}

/// One deterministic logical record in an engine snapshot.
///
/// Storage adapters persist these records as independent B+ tree keys. This
/// keeps the durable tree aligned with catalog, MVCC, secondary-index, and
/// idempotency boundaries instead of treating the entire engine as one blob.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub kind: u8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotMeta {
    format: u32,
    last_applied: u64,
    last_apply_hash: Option<[u8; 32]>,
    last_apply_outcome: Option<ApplyOutcome>,
    retained_floor: u64,
    next_txn_id: TxnId,
    stats: EngineStats,
}

impl EngineSnapshot {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|error| EngineError::InvalidSnapshot(format!("encode failed: {error}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|error| EngineError::InvalidSnapshot(format!("decode failed: {error}")))
    }

    /// Decompose a snapshot into canonical, independently persistent records.
    pub fn to_records(&self) -> Result<Vec<SnapshotRecord>> {
        let meta = SnapshotMeta {
            format: self.format,
            last_applied: self.last_applied,
            last_apply_hash: self.last_apply_hash,
            last_apply_outcome: self.last_apply_outcome.clone(),
            retained_floor: self.retained_floor,
            next_txn_id: self.next_txn_id,
            stats: self.stats.clone(),
        };
        let mut records = vec![
            snapshot_record(RECORD_META, Vec::new(), &meta)?,
            snapshot_record(RECORD_CATALOG, Vec::new(), &self.catalog)?,
        ];
        for entry in &self.primary {
            records.push(snapshot_record(
                RECORD_PRIMARY,
                primary_record_key(entry)?,
                entry,
            )?);
        }
        for entry in &self.secondary {
            records.push(snapshot_record(
                RECORD_SECONDARY,
                secondary_record_key(&entry.key)?,
                entry,
            )?);
        }
        for entry in &self.clients {
            records.push(snapshot_record(
                RECORD_CLIENT,
                entry.client_id.to_vec(),
                entry,
            )?);
        }
        records.sort_by(|left, right| {
            (left.kind, left.key.as_slice()).cmp(&(right.kind, right.key.as_slice()))
        });
        Ok(records)
    }

    /// Reconstruct a snapshot from logical records in any input order.
    pub fn from_records(records: &[SnapshotRecord]) -> Result<Self> {
        let mut ordered = records.to_vec();
        ordered.sort_by(|left, right| {
            (left.kind, left.key.as_slice()).cmp(&(right.kind, right.key.as_slice()))
        });
        for record in &ordered {
            validate_record_size(record)?;
        }
        if ordered.windows(2).any(|pair| {
            pair[0].kind == pair[1].kind && pair[0].key.as_slice() == pair[1].key.as_slice()
        }) {
            return Err(EngineError::InvalidSnapshot(
                "duplicate logical snapshot record".into(),
            ));
        }

        let mut meta = None;
        let mut catalog = None;
        let mut primary = Vec::new();
        let mut secondary = Vec::new();
        let mut clients = Vec::new();
        for record in ordered {
            match record.kind {
                RECORD_META => {
                    if !record.key.is_empty() {
                        return Err(EngineError::InvalidSnapshot(
                            "metadata record key must be empty".into(),
                        ));
                    }
                    meta = Some(decode_snapshot_record(&record)?);
                }
                RECORD_CATALOG => {
                    if !record.key.is_empty() {
                        return Err(EngineError::InvalidSnapshot(
                            "catalog record key must be empty".into(),
                        ));
                    }
                    catalog = Some(decode_snapshot_record(&record)?);
                }
                RECORD_PRIMARY => {
                    let entry: PrimarySnapshotEntry = decode_snapshot_record(&record)?;
                    if primary_record_key(&entry)? != record.key {
                        return Err(EngineError::InvalidSnapshot(
                            "primary record key disagrees with value".into(),
                        ));
                    }
                    primary.push(entry);
                }
                RECORD_SECONDARY => {
                    let entry: SecondarySnapshotEntry = decode_snapshot_record(&record)?;
                    if secondary_record_key(&entry.key)? != record.key {
                        return Err(EngineError::InvalidSnapshot(
                            "secondary record key disagrees with value".into(),
                        ));
                    }
                    secondary.push(entry);
                }
                RECORD_CLIENT => {
                    let entry: ClientSnapshotEntry = decode_snapshot_record(&record)?;
                    if entry.client_id.as_slice() != record.key {
                        return Err(EngineError::InvalidSnapshot(
                            "client record key disagrees with value".into(),
                        ));
                    }
                    clients.push(entry);
                }
                other => {
                    return Err(EngineError::InvalidSnapshot(format!(
                        "unknown logical snapshot record kind {other}"
                    )));
                }
            }
        }
        let meta: SnapshotMeta =
            meta.ok_or_else(|| EngineError::InvalidSnapshot("missing metadata record".into()))?;
        let catalog: Catalog =
            catalog.ok_or_else(|| EngineError::InvalidSnapshot("missing catalog record".into()))?;
        let snapshot = Self {
            format: meta.format,
            catalog,
            primary,
            secondary,
            clients,
            last_applied: meta.last_applied,
            last_apply_hash: meta.last_apply_hash,
            last_apply_outcome: meta.last_apply_outcome,
            retained_floor: meta.retained_floor,
            next_txn_id: meta.next_txn_id,
            stats: meta.stats,
        };
        // Run the same cross-record validation used by normal engine restore.
        Engine::from_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }
}

fn snapshot_record<T: Serialize>(kind: u8, key: Vec<u8>, value: &T) -> Result<SnapshotRecord> {
    let value = serde_json::to_vec(value)
        .map_err(|error| EngineError::InvalidSnapshot(format!("record encode failed: {error}")))?;
    let record = SnapshotRecord { kind, key, value };
    validate_record_size(&record)?;
    Ok(record)
}

fn decode_snapshot_record<T: for<'de> Deserialize<'de>>(record: &SnapshotRecord) -> Result<T> {
    serde_json::from_slice(&record.value).map_err(|error| {
        EngineError::InvalidSnapshot(format!(
            "record kind {} decode failed: {error}",
            record.kind
        ))
    })
}

fn validate_record_size(record: &SnapshotRecord) -> Result<()> {
    if record.key.len() > MAX_SNAPSHOT_RECORD_BYTES
        || record.value.len() > MAX_SNAPSHOT_RECORD_BYTES
    {
        return Err(EngineError::InvalidSnapshot(format!(
            "logical snapshot record exceeds {MAX_SNAPSHOT_RECORD_BYTES} byte limit"
        )));
    }
    Ok(())
}

fn primary_record_key(entry: &PrimarySnapshotEntry) -> Result<Vec<u8>> {
    let length = u32::try_from(entry.primary_key.len())
        .map_err(|_| EngineError::InvalidSnapshot("primary snapshot key is too large".into()))?;
    let mut key = Vec::with_capacity(12 + entry.primary_key.len());
    key.extend_from_slice(&entry.table_id.to_be_bytes());
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(&entry.primary_key);
    Ok(key)
}

fn secondary_record_key(key: &SecondaryKey) -> Result<Vec<u8>> {
    let secondary_len = u32::try_from(key.secondary_value.len())
        .map_err(|_| EngineError::InvalidSnapshot("secondary snapshot key is too large".into()))?;
    let primary_len = u32::try_from(key.primary_key.len())
        .map_err(|_| EngineError::InvalidSnapshot("primary snapshot key is too large".into()))?;
    let mut encoded = Vec::with_capacity(16 + key.secondary_value.len() + key.primary_key.len());
    encoded.extend_from_slice(&key.index_id.to_be_bytes());
    encoded.extend_from_slice(&secondary_len.to_be_bytes());
    encoded.extend_from_slice(&key.secondary_value);
    encoded.extend_from_slice(&primary_len.to_be_bytes());
    encoded.extend_from_slice(&key.primary_key);
    Ok(encoded)
}

#[derive(Debug, Clone)]
pub struct Engine {
    catalog: Catalog,
    primary: BTreeMap<LogicalKey, Vec<RowVersion>>,
    secondary: BTreeMap<SecondaryKey, Vec<SecondaryVersion>>,
    clients: BTreeMap<[u8; 16], ClientRecord>,
    last_applied: u64,
    last_apply_hash: Option<[u8; 32]>,
    last_apply_outcome: Option<ApplyOutcome>,
    retained_floor: u64,
    next_txn_id: TxnId,
    stats: EngineStats,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            catalog: Catalog::default(),
            primary: BTreeMap::new(),
            secondary: BTreeMap::new(),
            clients: BTreeMap::new(),
            last_applied: 0,
            last_apply_hash: None,
            last_apply_outcome: None,
            retained_floor: 0,
            next_txn_id: 1,
            stats: EngineStats::default(),
        }
    }
}

impl Engine {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    #[must_use]
    pub const fn last_applied(&self) -> u64 {
        self.last_applied
    }

    #[must_use]
    pub const fn last_apply_hash(&self) -> Option<[u8; 32]> {
        self.last_apply_hash
    }

    #[must_use]
    pub const fn retained_floor(&self) -> u64 {
        self.retained_floor
    }

    #[must_use]
    pub const fn stats(&self) -> &EngineStats {
        &self.stats
    }

    #[must_use]
    pub fn client_record(&self, client_id: &[u8; 16]) -> Option<&ClientRecord> {
        self.clients.get(client_id)
    }

    #[must_use]
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn begin(&mut self) -> Result<Transaction> {
        self.begin_at(self.last_applied, self.catalog.schema_epoch())
    }

    /// Creates a transaction-private overlay at the current snapshot without
    /// advancing local transaction-id state. Replicated leaders use this to
    /// prepare a canonical attempt before consensus; every durable state change
    /// still occurs only when that attempt is applied at its Raft index.
    pub fn prepare_transaction(&self) -> Result<Transaction> {
        if self.last_applied < self.retained_floor {
            return Err(EngineError::SnapshotTooOld {
                requested: self.last_applied,
                floor: self.retained_floor,
            });
        }
        Ok(Transaction::new(
            0,
            self.last_applied,
            self.catalog.schema_epoch(),
        ))
    }

    pub fn begin_at(&mut self, read_ts: u64, schema_epoch: u64) -> Result<Transaction> {
        if read_ts < self.retained_floor {
            return Err(EngineError::SnapshotTooOld {
                requested: read_ts,
                floor: self.retained_floor,
            });
        }
        if read_ts > self.last_applied {
            return Err(CoreError::Constraint(format!(
                "snapshot {read_ts} is ahead of applied index {}",
                self.last_applied
            ))
            .into());
        }
        if schema_epoch > self.catalog.schema_epoch() {
            return Err(CoreError::Constraint(format!(
                "schema epoch {schema_epoch} is ahead of current epoch {}",
                self.catalog.schema_epoch()
            ))
            .into());
        }
        let id = self.next_txn_id;
        self.next_txn_id = self
            .next_txn_id
            .checked_add(1)
            .ok_or_else(|| CoreError::Invariant("transaction id overflow".into()))?;
        Ok(Transaction::new(id, read_ts, schema_epoch))
    }

    pub fn create_table(
        &self,
        transaction: &mut Transaction,
        name: impl Into<String>,
        schema: Schema,
    ) -> Result<TableId> {
        self.ensure_transaction_snapshot(transaction)?;
        let mut id = self.catalog.next_table_id();
        while transaction.staged_table(id).is_some() {
            id = id
                .checked_add(1)
                .ok_or_else(|| CoreError::Invariant("table id overflow".into()))?;
        }
        let table = TableMeta::new(id, name, schema)?;
        if self.catalog.table_named(&table.name).is_some()
            || transaction.catalog_mutations().iter().any(|mutation| {
                matches!(mutation, CatalogMutation::CreateTable(existing) if existing.name.eq_ignore_ascii_case(&table.name))
            })
        {
            return Err(CoreError::Constraint(format!(
                "table `{}` already exists",
                table.name
            ))
            .into());
        }
        transaction.stage_catalog(CatalogMutation::CreateTable(table));
        Ok(id)
    }

    pub fn create_index(
        &self,
        transaction: &mut Transaction,
        name: impl Into<String>,
        table_id: TableId,
        column: usize,
    ) -> Result<IndexId> {
        self.ensure_transaction_snapshot(transaction)?;
        let table = self.table_for_transaction(transaction, table_id)?;
        let mut id = self.catalog.next_index_id();
        while transaction.staged_index(id).is_some() {
            id = id
                .checked_add(1)
                .ok_or_else(|| CoreError::Invariant("index id overflow".into()))?;
        }
        let index = IndexMeta::new(id, name, table, column)?;
        if self.catalog.index_named(&index.name).is_some()
            || transaction.catalog_mutations().iter().any(|mutation| {
                matches!(mutation, CatalogMutation::CreateIndex(existing) if existing.name.eq_ignore_ascii_case(&index.name))
            })
        {
            return Err(CoreError::Constraint(format!(
                "index `{}` already exists",
                index.name
            ))
            .into());
        }
        transaction.stage_catalog(CatalogMutation::CreateIndex(index));
        Ok(id)
    }

    pub fn get(
        &self,
        transaction: &Transaction,
        table_id: TableId,
        primary_key: &Value,
    ) -> Result<Option<Row>> {
        self.ensure_transaction_snapshot(transaction)?;
        self.validate_primary_key(transaction, table_id, primary_key)?;
        let key = LogicalKey::new(table_id, primary_key)?;
        Ok(self.read_key(transaction, &key))
    }

    pub fn insert(&self, transaction: &mut Transaction, table_id: TableId, row: Row) -> Result<()> {
        let primary_key = self.validate_row(transaction, table_id, &row)?;
        if self.get(transaction, table_id, &primary_key)?.is_some() {
            return Err(CoreError::Constraint(format!(
                "duplicate primary key {}",
                display_value(&primary_key)
            ))
            .into());
        }
        transaction.stage_write(TxnWrite {
            table_id,
            primary_key,
            mutation: Mutation::Put(row),
        })
    }

    pub fn upsert(&self, transaction: &mut Transaction, table_id: TableId, row: Row) -> Result<()> {
        let primary_key = self.validate_row(transaction, table_id, &row)?;
        transaction.stage_write(TxnWrite {
            table_id,
            primary_key,
            mutation: Mutation::Put(row),
        })
    }

    pub fn update(
        &self,
        transaction: &mut Transaction,
        table_id: TableId,
        primary_key: &Value,
        row: Row,
    ) -> Result<bool> {
        if self.get(transaction, table_id, primary_key)?.is_none() {
            return Ok(false);
        }
        let row_key = self.validate_row(transaction, table_id, &row)?;
        if &row_key != primary_key {
            return Err(CoreError::Constraint(
                "UPDATE cannot change the primary-key column".into(),
            )
            .into());
        }
        transaction.stage_write(TxnWrite {
            table_id,
            primary_key: primary_key.clone(),
            mutation: Mutation::Put(row),
        })?;
        Ok(true)
    }

    pub fn delete(
        &self,
        transaction: &mut Transaction,
        table_id: TableId,
        primary_key: &Value,
    ) -> Result<bool> {
        if self.get(transaction, table_id, primary_key)?.is_none() {
            return Ok(false);
        }
        transaction.stage_write(TxnWrite {
            table_id,
            primary_key: primary_key.clone(),
            mutation: Mutation::Delete,
        })?;
        Ok(true)
    }

    pub fn scan_table(&self, transaction: &Transaction, table_id: TableId) -> Result<Vec<Row>> {
        self.ensure_transaction_snapshot(transaction)?;
        self.table_for_transaction(transaction, table_id)?;
        let mut keys: BTreeSet<LogicalKey> = self
            .primary
            .keys()
            .filter(|key| key.table_id == table_id)
            .cloned()
            .collect();
        keys.extend(
            transaction
                .writes()
                .filter(|(key, _)| key.table_id == table_id)
                .map(|(key, _)| key.clone()),
        );
        Ok(keys
            .iter()
            .filter_map(|key| self.read_key(transaction, key))
            .collect())
    }

    pub fn scan_secondary(
        &self,
        transaction: &Transaction,
        index_id: IndexId,
        value: &Value,
    ) -> Result<Vec<Row>> {
        self.ensure_transaction_snapshot(transaction)?;
        let index = self
            .catalog
            .index(index_id)
            .or_else(|| transaction.staged_index(index_id))
            .ok_or(EngineError::UnknownIndex(index_id))?;
        if value.is_null() {
            return Ok(Vec::new());
        }
        let mut encoded_value = Vec::new();
        encode_value(value, &mut encoded_value)?;
        encoded_value.clear();
        encode_ordered(value, &mut encoded_value);

        let mut keys = BTreeSet::new();
        for (key, versions) in &self.secondary {
            if key.index_id == index_id
                && key.secondary_value == encoded_value
                && visible_secondary(versions, transaction.read_ts())
            {
                keys.insert(LogicalKey {
                    table_id: index.table_id,
                    primary_key: key.primary_key.clone(),
                });
            }
        }
        keys.extend(
            transaction
                .writes()
                .filter(|(key, _)| key.table_id == index.table_id)
                .map(|(key, _)| key.clone()),
        );

        let mut rows = Vec::new();
        for key in keys {
            if let Some(row) = self.read_key(transaction, &key)
                && row.values.get(index.column) == Some(value)
            {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    pub fn execute_query(&self, transaction: &Transaction, query: &Query) -> Result<Vec<Row>> {
        let rows = match &query.source {
            ScanSource::Table(table_id) => self.scan_table(transaction, *table_id)?,
            ScanSource::SecondaryEq { index_id, value } => {
                self.scan_secondary(transaction, *index_id, value)?
            }
        };

        let mut evaluated = Vec::new();
        for row in rows {
            if query
                .filter
                .as_ref()
                .is_some_and(|filter| !filter.evaluate_predicate(&row).unwrap_or(false))
            {
                // Re-evaluate to preserve a type error rather than treating it
                // as a false predicate.
                if let Some(filter) = &query.filter {
                    filter.evaluate_predicate(&row)?;
                }
                continue;
            }
            let sort_keys = query
                .order_by
                .iter()
                .map(|order| {
                    order
                        .expression
                        .evaluate(&row)
                        .and_then(|value| sort_key(&value))
                })
                .collect::<Result<Vec<_>>>()?;
            evaluated.push((row, sort_keys));
        }

        if !query.order_by.is_empty() {
            evaluated.sort_by(|left, right| compare_sort_keys(&left.1, &right.1, &query.order_by));
        }

        let available = evaluated.len().saturating_sub(query.offset);
        let take = query.limit.unwrap_or(available).min(available);
        evaluated
            .into_iter()
            .skip(query.offset)
            .take(take)
            .map(|(row, _)| {
                if query.projection.is_empty() {
                    Ok(row)
                } else {
                    query
                        .projection
                        .iter()
                        .map(|expression| expression.evaluate(&row))
                        .collect::<Result<Vec<_>>>()
                        .map(|values| Row { values })
                }
            })
            .collect()
    }

    pub fn apply(
        &mut self,
        index: u64,
        command_hash: [u8; 32],
        attempt: &TxnAttempt,
    ) -> Result<ApplyOutcome> {
        if attempt.command_hash()? != command_hash {
            return Err(EngineError::CommandHashMismatch);
        }
        self.apply_with_request_hash(index, command_hash, command_hash, attempt)
    }

    /// Applies a replicated entry while keeping entry replay identity separate
    /// from client retry identity. `apply_hash` identifies the exact committed
    /// log payload at `index`; `request_hash` identifies the logical request
    /// across leader terms and re-preparation.
    pub fn apply_with_request_hash(
        &mut self,
        index: u64,
        apply_hash: [u8; 32],
        request_hash: [u8; 32],
        attempt: &TxnAttempt,
    ) -> Result<ApplyOutcome> {
        attempt.validate_canonical()?;
        if index == self.last_applied {
            if self.last_apply_hash != Some(apply_hash) {
                return Err(EngineError::ApplyHashMismatch { index });
            }
            return self.last_apply_outcome.clone().ok_or_else(|| {
                EngineError::InvalidSnapshot("last apply outcome is missing".into())
            });
        }
        let expected = self
            .last_applied
            .checked_add(1)
            .ok_or_else(|| CoreError::Invariant("applied index overflow".into()))?;
        if index != expected {
            return Err(EngineError::ApplyGap {
                expected,
                actual: index,
            });
        }

        let outcome = match self.admit_request(attempt.request(), request_hash) {
            Admission::New => {
                let result = self.evaluate_and_apply(index, attempt);
                self.clients.insert(
                    attempt.request().client_id,
                    ClientRecord {
                        sequence: attempt.request().sequence,
                        request_hash,
                        result: result.clone(),
                    },
                );
                ApplyOutcome::Applied(result)
            }
            Admission::Duplicate(result) => {
                self.stats.duplicate_requests = self.stats.duplicate_requests.saturating_add(1);
                ApplyOutcome::Duplicate(result)
            }
            Admission::Rejected(rejection) => {
                self.stats.rejected_requests = self.stats.rejected_requests.saturating_add(1);
                ApplyOutcome::Rejected(rejection)
            }
        };
        self.last_applied = index;
        self.last_apply_hash = Some(apply_hash);
        self.last_apply_outcome = Some(outcome.clone());
        Ok(outcome)
    }

    /// Durably advances the replicated state machine for a committed Raft
    /// no-op. No-op indexes must occupy the same contiguous timestamp space as
    /// transaction commands so commit timestamps remain Raft log indexes.
    pub fn apply_noop(&mut self, index: u64, entry_hash: [u8; 32]) -> Result<ApplyOutcome> {
        if index == self.last_applied {
            if self.last_apply_hash != Some(entry_hash)
                || self.last_apply_outcome != Some(ApplyOutcome::Noop)
            {
                return Err(EngineError::ApplyHashMismatch { index });
            }
            return Ok(ApplyOutcome::Noop);
        }
        let expected = self
            .last_applied
            .checked_add(1)
            .ok_or_else(|| CoreError::Invariant("applied index overflow".into()))?;
        if index != expected {
            return Err(EngineError::ApplyGap {
                expected,
                actual: index,
            });
        }
        self.last_applied = index;
        self.last_apply_hash = Some(entry_hash);
        self.last_apply_outcome = Some(ApplyOutcome::Noop);
        Ok(ApplyOutcome::Noop)
    }

    pub fn vacuum_offline(&mut self, retain_from: u64) -> Result<VacuumReport> {
        if retain_from > self.last_applied {
            return Err(CoreError::Constraint(format!(
                "vacuum floor {retain_from} is ahead of applied index {}",
                self.last_applied
            ))
            .into());
        }
        let previous_floor = self.retained_floor;
        let floor = retain_from.max(previous_floor);
        let primary_before = total_versions(&self.primary);
        let primary_chains_before = self.primary.len();
        for versions in self.primary.values_mut() {
            vacuum_rows(versions, floor);
        }
        self.primary.retain(|_, versions| !versions.is_empty());

        let secondary_before = total_versions(&self.secondary);
        let secondary_chains_before = self.secondary.len();
        for versions in self.secondary.values_mut() {
            vacuum_secondary(versions, floor);
        }
        self.secondary.retain(|_, versions| !versions.is_empty());

        let primary_removed = primary_before - total_versions(&self.primary);
        let secondary_removed = secondary_before - total_versions(&self.secondary);
        self.retained_floor = floor;
        self.stats.vacuumed_primary_versions = self
            .stats
            .vacuumed_primary_versions
            .saturating_add(primary_removed as u64);
        self.stats.vacuumed_secondary_versions = self
            .stats
            .vacuumed_secondary_versions
            .saturating_add(secondary_removed as u64);
        self.refresh_version_stats();
        Ok(VacuumReport {
            previous_floor,
            retained_floor: floor,
            primary_versions_removed: primary_removed,
            secondary_versions_removed: secondary_removed,
            empty_primary_chains_removed: primary_chains_before - self.primary.len(),
            empty_secondary_chains_removed: secondary_chains_before - self.secondary.len(),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            format: SNAPSHOT_FORMAT,
            catalog: self.catalog.clone(),
            primary: self
                .primary
                .iter()
                .map(|(key, versions)| PrimarySnapshotEntry {
                    table_id: key.table_id,
                    primary_key: key.primary_key.clone(),
                    versions: versions.clone(),
                })
                .collect(),
            secondary: self
                .secondary
                .iter()
                .map(|(key, versions)| SecondarySnapshotEntry {
                    key: key.clone(),
                    versions: versions.clone(),
                })
                .collect(),
            clients: self
                .clients
                .iter()
                .map(|(client_id, record)| ClientSnapshotEntry {
                    client_id: *client_id,
                    record: record.clone(),
                })
                .collect(),
            last_applied: self.last_applied,
            last_apply_hash: self.last_apply_hash,
            last_apply_outcome: self.last_apply_outcome.clone(),
            retained_floor: self.retained_floor,
            next_txn_id: self.next_txn_id,
            stats: self.stats.clone(),
        }
    }

    pub fn from_snapshot(snapshot: EngineSnapshot) -> Result<Self> {
        if snapshot.format != SNAPSHOT_FORMAT {
            return Err(EngineError::InvalidSnapshot(format!(
                "unsupported engine snapshot format {}",
                snapshot.format
            )));
        }
        snapshot.catalog.validate()?;
        if snapshot.retained_floor > snapshot.last_applied {
            return Err(EngineError::InvalidSnapshot(
                "retained floor exceeds last applied index".into(),
            ));
        }
        if (snapshot.last_applied == 0)
            != (snapshot.last_apply_hash.is_none() && snapshot.last_apply_outcome.is_none())
        {
            return Err(EngineError::InvalidSnapshot(
                "last apply metadata is inconsistent".into(),
            ));
        }
        if snapshot.last_applied > 0
            && (snapshot.last_apply_hash.is_none() || snapshot.last_apply_outcome.is_none())
        {
            return Err(EngineError::InvalidSnapshot(
                "last apply metadata is incomplete".into(),
            ));
        }

        let mut primary = BTreeMap::new();
        for entry in snapshot.primary {
            validate_row_versions(&entry.versions, snapshot.last_applied)?;
            let key = LogicalKey {
                table_id: entry.table_id,
                primary_key: entry.primary_key,
            };
            if primary.insert(key, entry.versions).is_some() {
                return Err(EngineError::InvalidSnapshot(
                    "duplicate primary version chain".into(),
                ));
            }
        }
        let mut secondary = BTreeMap::new();
        for entry in snapshot.secondary {
            validate_secondary_versions(&entry.versions, snapshot.last_applied)?;
            if snapshot.catalog.index(entry.key.index_id).is_none() {
                return Err(EngineError::InvalidSnapshot(format!(
                    "secondary chain references missing index {}",
                    entry.key.index_id
                )));
            }
            if secondary.insert(entry.key, entry.versions).is_some() {
                return Err(EngineError::InvalidSnapshot(
                    "duplicate secondary version chain".into(),
                ));
            }
        }
        let mut clients = BTreeMap::new();
        for entry in snapshot.clients {
            if entry.record.sequence == 0 {
                return Err(EngineError::InvalidSnapshot(
                    "client sequence zero cannot be durable".into(),
                ));
            }
            if clients.insert(entry.client_id, entry.record).is_some() {
                return Err(EngineError::InvalidSnapshot(
                    "duplicate client record".into(),
                ));
            }
        }
        let mut engine = Self {
            catalog: snapshot.catalog,
            primary,
            secondary,
            clients,
            last_applied: snapshot.last_applied,
            last_apply_hash: snapshot.last_apply_hash,
            last_apply_outcome: snapshot.last_apply_outcome,
            retained_floor: snapshot.retained_floor,
            next_txn_id: snapshot.next_txn_id.max(1),
            stats: snapshot.stats,
        };
        engine.refresh_version_stats();
        Ok(engine)
    }

    fn ensure_transaction_snapshot(&self, transaction: &Transaction) -> Result<()> {
        if transaction.read_ts() < self.retained_floor {
            return Err(EngineError::SnapshotTooOld {
                requested: transaction.read_ts(),
                floor: self.retained_floor,
            });
        }
        if transaction.read_ts() > self.last_applied {
            return Err(CoreError::Invariant("transaction reads a future snapshot".into()).into());
        }
        Ok(())
    }

    fn table_for_transaction<'a>(
        &'a self,
        transaction: &'a Transaction,
        table_id: TableId,
    ) -> Result<&'a TableMeta> {
        transaction
            .staged_table(table_id)
            .or_else(|| self.catalog.table(table_id))
            .ok_or(EngineError::UnknownTable(table_id))
    }

    fn validate_primary_key(
        &self,
        transaction: &Transaction,
        table_id: TableId,
        primary_key: &Value,
    ) -> Result<()> {
        let table = self.table_for_transaction(transaction, table_id)?;
        let column = &table.schema.columns[table.primary_key_column];
        if primary_key.is_null() || primary_key.data_type() != Some(column.data_type) {
            return Err(CoreError::Type(format!(
                "primary key for `{}` expects {}, got {}",
                table.name,
                column.data_type,
                primary_key.type_name()
            ))
            .into());
        }
        let mut validation = Vec::new();
        encode_value(primary_key, &mut validation)?;
        Ok(())
    }

    fn validate_row(
        &self,
        transaction: &Transaction,
        table_id: TableId,
        row: &Row,
    ) -> Result<Value> {
        self.ensure_transaction_snapshot(transaction)?;
        let table = self.table_for_transaction(transaction, table_id)?;
        table.schema.validate_row(row)?;
        let primary_key = row.values[table.primary_key_column].clone();
        self.validate_primary_key(transaction, table_id, &primary_key)?;
        Ok(primary_key)
    }

    fn read_key(&self, transaction: &Transaction, key: &LogicalKey) -> Option<Row> {
        match transaction.overlay(key) {
            Some(Mutation::Put(row)) => Some(row.clone()),
            Some(Mutation::Delete) => None,
            None => self
                .primary
                .get(key)
                .and_then(|versions| visible_row(versions, transaction.read_ts())),
        }
    }

    fn admit_request(&self, request: &ClientRequestId, hash: [u8; 32]) -> Admission {
        let Some(record) = self.clients.get(&request.client_id) else {
            return if request.sequence == 1 {
                Admission::New
            } else {
                Admission::Rejected(RequestRejection::FirstSequenceMustBeOne {
                    actual: request.sequence,
                })
            };
        };
        match request.sequence.cmp(&record.sequence) {
            Ordering::Less => Admission::Rejected(RequestRejection::SequenceTooOld {
                latest: record.sequence,
                actual: request.sequence,
            }),
            Ordering::Equal if hash == record.request_hash => {
                Admission::Duplicate(record.result.clone())
            }
            Ordering::Equal => Admission::Rejected(RequestRejection::SequenceHashMismatch {
                sequence: request.sequence,
            }),
            Ordering::Greater if request.sequence == record.sequence.saturating_add(1) => {
                Admission::New
            }
            Ordering::Greater => Admission::Rejected(RequestRejection::SequenceGap {
                expected: record.sequence.saturating_add(1),
                actual: request.sequence,
            }),
        }
    }

    fn evaluate_and_apply(&mut self, index: u64, attempt: &TxnAttempt) -> CommitResult {
        let result = self.validate_attempt_for_apply(attempt);
        let (next_catalog, new_indexes) = match result {
            Ok(value) => value,
            Err(reason) => {
                self.stats.aborted_transactions = self.stats.aborted_transactions.saturating_add(1);
                return CommitResult::Aborted {
                    abort_index: index,
                    reason,
                };
            }
        };

        let previous_catalog = self.catalog.clone();
        let before_rows: Vec<_> = match attempt
            .writes()
            .iter()
            .map(|write| {
                write.logical_key().map(|key| {
                    let old = self
                        .primary
                        .get(&key)
                        .and_then(|versions| visible_row(versions, self.last_applied));
                    (key, old)
                })
            })
            .collect::<Result<Vec<_>>>()
        {
            Ok(rows) => rows,
            Err(error) => {
                self.stats.aborted_transactions = self.stats.aborted_transactions.saturating_add(1);
                return CommitResult::Aborted {
                    abort_index: index,
                    reason: AbortReason::Validation(error.to_string()),
                };
            }
        };

        self.catalog = next_catalog;
        for (write, (key, old_row)) in attempt.writes().iter().zip(before_rows) {
            let new_row = match &write.mutation {
                Mutation::Put(row) => Some(row.clone()),
                Mutation::Delete => None,
            };
            for secondary_index in previous_catalog.indexes_for_table(write.table_id) {
                self.update_secondary(
                    secondary_index,
                    &key,
                    old_row.as_ref(),
                    new_row.as_ref(),
                    index,
                );
            }
            let versions = self.primary.entry(key).or_default();
            versions.insert(
                0,
                RowVersion {
                    commit_index: index,
                    row: new_row,
                },
            );
        }
        for new_index in new_indexes {
            self.backfill_index(&new_index, index);
        }

        self.stats.committed_transactions = self.stats.committed_transactions.saturating_add(1);
        self.refresh_version_stats();
        CommitResult::Committed {
            commit_index: index,
            affected_rows: attempt.writes().len(),
            schema_epoch: self.catalog.schema_epoch(),
        }
    }

    fn validate_attempt_for_apply(
        &self,
        attempt: &TxnAttempt,
    ) -> std::result::Result<(Catalog, Vec<IndexMeta>), AbortReason> {
        if attempt.read_ts() < self.retained_floor {
            return Err(AbortReason::SnapshotVacuumed {
                read_ts: attempt.read_ts(),
                retained_floor: self.retained_floor,
            });
        }
        if attempt.read_ts() > self.last_applied {
            return Err(AbortReason::Validation(format!(
                "read timestamp {} is ahead of applied index {}",
                attempt.read_ts(),
                self.last_applied
            )));
        }
        if attempt.schema_epoch() != self.catalog.schema_epoch() {
            return Err(AbortReason::SchemaChanged {
                expected: attempt.schema_epoch(),
                actual: self.catalog.schema_epoch(),
            });
        }
        if let Err(error) = attempt.validate_canonical() {
            return Err(AbortReason::Validation(error.to_string()));
        }

        let mut next_catalog = self.catalog.clone();
        let new_index_ids = next_catalog
            .apply_mutations(attempt.catalog_mutations())
            .map_err(|error| AbortReason::Validation(error.to_string()))?;
        let new_indexes = new_index_ids
            .into_iter()
            .map(|index_id| {
                next_catalog.index(index_id).cloned().ok_or_else(|| {
                    AbortReason::Validation(format!(
                        "catalog mutation returned missing index {index_id}"
                    ))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        for write in attempt.writes() {
            let table = next_catalog.table(write.table_id).ok_or_else(|| {
                AbortReason::Validation(format!("unknown table {}", write.table_id))
            })?;
            let primary_column = &table.schema.columns[table.primary_key_column];
            if write.primary_key.is_null()
                || write.primary_key.data_type() != Some(primary_column.data_type)
            {
                return Err(AbortReason::Validation(format!(
                    "invalid primary key for table `{}`",
                    table.name
                )));
            }
            if let Mutation::Put(row) = &write.mutation {
                table
                    .schema
                    .validate_row(row)
                    .map_err(|error| AbortReason::Validation(error.to_string()))?;
                if row.values[table.primary_key_column] != write.primary_key {
                    return Err(AbortReason::Validation(format!(
                        "row primary key differs from write key in table `{}`",
                        table.name
                    )));
                }
            }
            let key = write
                .logical_key()
                .map_err(|error| AbortReason::Validation(error.to_string()))?;
            if let Some(winning_commit) = self
                .primary
                .get(&key)
                .and_then(|versions| versions.first())
                .map(|version| version.commit_index)
                .filter(|commit| *commit > attempt.read_ts())
            {
                return Err(AbortReason::WriteConflict {
                    table_id: write.table_id,
                    primary_key: write.primary_key.clone(),
                    winning_commit,
                });
            }
        }
        Ok((next_catalog, new_indexes))
    }

    fn update_secondary(
        &mut self,
        index: &IndexMeta,
        primary_key: &LogicalKey,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
        commit_index: u64,
    ) {
        let old_value = old_row.and_then(|row| row.values.get(index.column));
        let new_value = new_row.and_then(|row| row.values.get(index.column));
        if old_value == new_value {
            return;
        }
        if let Some(value) = old_value.filter(|value| !value.is_null()) {
            self.push_secondary(index.id, value, primary_key, commit_index, false);
        }
        if let Some(value) = new_value.filter(|value| !value.is_null()) {
            self.push_secondary(index.id, value, primary_key, commit_index, true);
        }
    }

    fn push_secondary(
        &mut self,
        index_id: IndexId,
        value: &Value,
        primary_key: &LogicalKey,
        commit_index: u64,
        present: bool,
    ) {
        let mut encoded = Vec::new();
        encode_ordered(value, &mut encoded);
        self.secondary
            .entry(SecondaryKey {
                index_id,
                secondary_value: encoded,
                primary_key: primary_key.primary_key.clone(),
            })
            .or_default()
            .insert(
                0,
                SecondaryVersion {
                    commit_index,
                    present,
                },
            );
    }

    fn backfill_index(&mut self, index: &IndexMeta, commit_index: u64) {
        let entries: Vec<_> = self
            .primary
            .iter()
            .filter(|(key, _)| key.table_id == index.table_id)
            .filter_map(|(key, versions)| {
                visible_row(versions, commit_index).and_then(|row| {
                    let value = row.values[index.column].clone();
                    (!value.is_null()).then(|| (key.clone(), value))
                })
            })
            .collect();
        for (key, value) in entries {
            self.push_secondary(index.id, &value, &key, commit_index, true);
        }
    }

    fn refresh_version_stats(&mut self) {
        self.stats.primary_versions = total_versions(&self.primary) as u64;
        self.stats.secondary_versions = total_versions(&self.secondary) as u64;
    }
}

enum Admission {
    New,
    Duplicate(CommitResult),
    Rejected(RequestRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SortKey {
    Null,
    Encoded(Vec<u8>),
}

fn sort_key(value: &Value) -> Result<SortKey> {
    if value.is_null() {
        return Ok(SortKey::Null);
    }
    let mut validation = Vec::new();
    encode_value(value, &mut validation)?;
    let mut encoded = Vec::new();
    encode_ordered(value, &mut encoded);
    Ok(SortKey::Encoded(encoded))
}

fn compare_sort_keys(left: &[SortKey], right: &[SortKey], order: &[OrderBy]) -> Ordering {
    left.iter()
        .zip(right)
        .zip(order)
        .find_map(|((left, right), order)| {
            let ordering = match (left, right) {
                (SortKey::Null, SortKey::Null) => Ordering::Equal,
                (SortKey::Null, _) => {
                    if order.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, SortKey::Null) => {
                    if order.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (SortKey::Encoded(left), SortKey::Encoded(right)) => left.cmp(right),
            };
            let ordering = if order.descending
                && !matches!((left, right), (SortKey::Null, _) | (_, SortKey::Null))
            {
                ordering.reverse()
            } else {
                ordering
            };
            (ordering != Ordering::Equal).then_some(ordering)
        })
        .unwrap_or(Ordering::Equal)
}

fn visible_row(versions: &[RowVersion], read_ts: u64) -> Option<Row> {
    versions
        .iter()
        .find(|version| version.commit_index <= read_ts)
        .and_then(|version| version.row.clone())
}

fn visible_secondary(versions: &[SecondaryVersion], read_ts: u64) -> bool {
    versions
        .iter()
        .find(|version| version.commit_index <= read_ts)
        .is_some_and(|version| version.present)
}

fn total_versions<K, V>(map: &BTreeMap<K, Vec<V>>) -> usize {
    map.values().map(Vec::len).sum()
}

fn vacuum_rows(versions: &mut Vec<RowVersion>, floor: u64) {
    let mut retained: Vec<_> = versions
        .iter()
        .take_while(|version| version.commit_index > floor)
        .cloned()
        .collect();
    if let Some(baseline) = versions
        .iter()
        .find(|version| version.commit_index <= floor)
        .filter(|version| version.row.is_some())
    {
        retained.push(baseline.clone());
    }
    *versions = retained;
}

fn vacuum_secondary(versions: &mut Vec<SecondaryVersion>, floor: u64) {
    let mut retained: Vec<_> = versions
        .iter()
        .take_while(|version| version.commit_index > floor)
        .cloned()
        .collect();
    if let Some(baseline) = versions
        .iter()
        .find(|version| version.commit_index <= floor)
        .filter(|version| version.present)
    {
        retained.push(baseline.clone());
    }
    *versions = retained;
}

fn validate_row_versions(versions: &[RowVersion], last_applied: u64) -> Result<()> {
    if versions.is_empty()
        || versions
            .windows(2)
            .any(|pair| pair[0].commit_index <= pair[1].commit_index)
        || versions
            .iter()
            .any(|version| version.commit_index == 0 || version.commit_index > last_applied)
    {
        return Err(EngineError::InvalidSnapshot(
            "primary version chain is empty, unordered, or out of range".into(),
        ));
    }
    Ok(())
}

fn validate_secondary_versions(versions: &[SecondaryVersion], last_applied: u64) -> Result<()> {
    if versions.is_empty()
        || versions
            .windows(2)
            .any(|pair| pair[0].commit_index <= pair[1].commit_index)
        || versions
            .iter()
            .any(|version| version.commit_index == 0 || version.commit_index > last_applied)
    {
        return Err(EngineError::InvalidSnapshot(
            "secondary version chain is empty, unordered, or out of range".into(),
        ));
    }
    Ok(())
}

fn display_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Int64(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Text(value) => format!("'{value}'"),
        Value::Bytes(value) => format!("<{} bytes>", value.len()),
    }
}

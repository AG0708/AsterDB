use std::collections::BTreeMap;
use std::path::Path;

use aster_core::codec::encode_row;
use aster_core::{ClientRequestId, Row, TxnId, Value};
use aster_engine::{
    ApplyOutcome, CommitResult, Engine, EngineSnapshot, RequestRejection, Transaction,
};
use aster_sql::eval::validate_parameters;
use aster_sql::plan::{BoundStatement, PhysicalPlan};
use aster_sql::{bind, optimize, parse_statement};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::catalog::EngineCatalog;
use crate::error::{DatabaseError, Result};
use crate::executor::{StatementOutput, execute_bound, explain_statement};
use crate::persistence::{
    DurableRequestRecord, DurableRequests, FilePersistence, validate_durable_requests,
};

const STANDALONE_LEADER_TERM: u64 = 1;
const REPLICATED_MUTATION_FORMAT: u16 = 1;
const DATABASE_SNAPSHOT_FORMAT: u16 = 1;
const MAX_REPLICATED_COMMAND_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_ACTIVE_TRANSACTIONS: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseOptions {
    pub max_active_transactions: usize,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            max_active_transactions: DEFAULT_MAX_ACTIVE_TRANSACTIONS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub affected_rows: u64,
    pub applied_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionInfo {
    pub transaction_id: TxnId,
    pub read_ts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    /// Visibility timestamp assigned by the deterministic state machine. A
    /// duplicate request returns its original commit index.
    pub commit_index: u64,
    pub affected_rows: u64,
    pub schema_epoch: u64,
    /// Latest locally applied log index, which may be newer than
    /// `commit_index` for an idempotent duplicate.
    pub applied_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub applied_index: u64,
    pub checkpoint_lsn: u64,
    pub through_lsn: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseSnapshot {
    applied_index: u64,
    state_hash: [u8; 32],
    bytes: Vec<u8>,
}

impl DatabaseSnapshot {
    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn state_hash(&self) -> [u8; 32] {
        self.state_hash
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseStatus {
    pub applied_index: u64,
    pub schema_epoch: u64,
    pub retained_floor: u64,
    pub active_transactions: u64,
    pub database_pages: u64,
    pub wal_bytes: u64,
    pub checkpoint_lsn: u64,
    pub root_page: Option<u64>,
    pub recovery_groups_examined: u64,
    pub recovery_groups_replayed: u64,
    pub recovery_pages_replayed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionResult {
    Query(QueryResult),
    Transaction(TransactionInfo),
    Committed(CommitInfo),
    RolledBack,
    Explain { plan: String, applied_index: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMutation {
    bytes: Vec<u8>,
}

impl PreparedMutation {
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationPreparation {
    Propose(PreparedMutation),
    Replay(ExecutionResult),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicatedMutation {
    format: u16,
    attempt: aster_engine::TxnAttempt,
    fingerprint: [u8; 32],
    affected_rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatabaseSnapshotEnvelope {
    format: u16,
    engine: EngineSnapshot,
    requests: Vec<DatabaseSnapshotRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatabaseSnapshotRequest {
    client_id: [u8; 16],
    record: DurableRequestRecord,
}

struct DatabaseState {
    engine: Engine,
    active: BTreeMap<TxnId, Transaction>,
    requests: DurableRequests,
}

/// Thread-safe standalone database facade.
///
/// Mutations are first applied to a private engine clone and private B+ tree
/// pages. The shared engine is replaced only after the WAL group, data pages,
/// and alternating superblock are durable.
pub struct Database {
    state: Mutex<DatabaseState>,
    persistence: FilePersistence,
    max_active_transactions: usize,
}

impl Database {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(directory, DatabaseOptions::default())
    }

    pub fn open_with_options(
        directory: impl AsRef<Path>,
        options: DatabaseOptions,
    ) -> Result<Self> {
        if options.max_active_transactions == 0 {
            return Err(DatabaseError::ResourceLimit(
                "maximum active transaction count must be positive".into(),
            ));
        }
        let (persistence, engine, requests) = FilePersistence::open(directory)?;
        Ok(Self {
            state: Mutex::new(DatabaseState {
                engine,
                active: BTreeMap::new(),
                requests,
            }),
            persistence,
            max_active_transactions: options.max_active_transactions,
        })
    }

    pub fn begin(&self) -> Result<TransactionInfo> {
        let mut state = self.state.lock();
        if state.active.len() >= self.max_active_transactions {
            return Err(DatabaseError::ResourceLimit(format!(
                "active transaction limit {} reached",
                self.max_active_transactions
            )));
        }
        let transaction = state.engine.begin()?;
        let info = TransactionInfo {
            transaction_id: transaction.id(),
            read_ts: transaction.read_ts(),
        };
        if state.active.insert(transaction.id(), transaction).is_some() {
            return Err(DatabaseError::Invariant(format!(
                "transaction id {} was reused while active",
                info.transaction_id
            )));
        }
        Ok(info)
    }

    pub fn execute(
        &self,
        client_id: [u8; 16],
        sequence: u64,
        transaction_id: Option<TxnId>,
        sql: &str,
        parameters: &[Value],
    ) -> Result<ExecutionResult> {
        let parsed = parse_statement(sql)?;

        // Transaction control is catalog-independent, so bind it against an
        // empty view first and dispatch through the same public methods used by
        // the protocol's dedicated operations.
        match &parsed.value {
            aster_sql::Statement::Begin => {
                if transaction_id.is_some() {
                    return Err(DatabaseError::TransactionControl(
                        "BEGIN cannot run inside an active transaction".into(),
                    ));
                }
                if !parameters.is_empty() {
                    return Err(DatabaseError::TransactionControl(
                        "BEGIN does not accept parameters".into(),
                    ));
                }
                return self.begin().map(ExecutionResult::Transaction);
            }
            aster_sql::Statement::Commit => {
                let transaction_id = transaction_id.ok_or_else(|| {
                    DatabaseError::TransactionControl(
                        "COMMIT requires an active transaction id".into(),
                    )
                })?;
                if !parameters.is_empty() {
                    return Err(DatabaseError::TransactionControl(
                        "COMMIT does not accept parameters".into(),
                    ));
                }
                return self
                    .commit(client_id, sequence, transaction_id)
                    .map(ExecutionResult::Committed);
            }
            aster_sql::Statement::Rollback => {
                let transaction_id = transaction_id.ok_or_else(|| {
                    DatabaseError::TransactionControl(
                        "ROLLBACK requires an active transaction id".into(),
                    )
                })?;
                if !parameters.is_empty() {
                    return Err(DatabaseError::TransactionControl(
                        "ROLLBACK does not accept parameters".into(),
                    ));
                }
                self.rollback(transaction_id)?;
                return Ok(ExecutionResult::RolledBack);
            }
            _ => {}
        }

        let mut state = self.state.lock();
        if let Some(transaction_id) = transaction_id {
            Self::execute_in_transaction(&mut state, transaction_id, &parsed, parameters)
        } else {
            self.execute_autocommit(&mut state, client_id, sequence, sql, &parsed, parameters)
        }
    }

    /// Executes a query or EXPLAIN after a replicated runtime has completed a
    /// `ReadIndex` barrier. Mutating and transaction-control statements are
    /// rejected so no caller can bypass consensus through the read path.
    pub fn execute_read_only(
        &self,
        client_id: [u8; 16],
        sequence: u64,
        sql: &str,
        parameters: &[Value],
    ) -> Result<ExecutionResult> {
        let parsed = parse_statement(sql)?;
        if !matches!(
            parsed.value,
            aster_sql::Statement::Select(_) | aster_sql::Statement::Explain(_)
        ) {
            return Err(DatabaseError::TransactionControl(
                "linearizable read path accepts SELECT or EXPLAIN only".into(),
            ));
        }
        self.execute(client_id, sequence, None, sql, parameters)
    }

    pub fn commit(
        &self,
        client_id: [u8; 16],
        sequence: u64,
        transaction_id: TxnId,
    ) -> Result<CommitInfo> {
        let mut state = self.state.lock();
        let fingerprint = commit_fingerprint(transaction_id);
        let Some(transaction) = state.active.remove(&transaction_id) else {
            // A response can be lost after the commit becomes durable. The
            // engine's latest per-client result permits a restart-safe retry
            // of that exact client sequence without another apply.
            if let Some(record) = replay_record(&state.requests, client_id, sequence, fingerprint)?
            {
                return commit_info_from_result(&record.result, state.engine.last_applied());
            }
            return Err(DatabaseError::TransactionNotFound(transaction_id));
        };
        let affected_rows = usize_to_u64(transaction.write_count(), "affected row count")?;
        match self.apply_transaction(
            &mut state,
            &transaction,
            client_id,
            sequence,
            fingerprint,
            affected_rows,
        ) {
            Ok(ApplyOutcome::Applied(result) | ApplyOutcome::Duplicate(result)) => {
                commit_info_from_result(&result, state.engine.last_applied())
            }
            Ok(ApplyOutcome::Rejected(rejection)) => {
                state.active.insert(transaction_id, transaction);
                Err(DatabaseError::RequestRejected(rejection))
            }
            Ok(ApplyOutcome::Noop) => {
                state.active.insert(transaction_id, transaction);
                Err(DatabaseError::Invariant(
                    "transaction commit produced a Raft no-op".into(),
                ))
            }
            Err(error) => {
                state.active.insert(transaction_id, transaction);
                Err(error)
            }
        }
    }

    pub fn rollback(&self, transaction_id: TxnId) -> Result<()> {
        let transaction = self
            .state
            .lock()
            .active
            .remove(&transaction_id)
            .ok_or(DatabaseError::TransactionNotFound(transaction_id))?;
        transaction.rollback();
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<CheckpointInfo> {
        // Serialize checkpoint publication with state-machine apply.
        let state = self.state.lock();
        let checkpoint = self.persistence.checkpoint()?;
        if checkpoint.applied_index != state.engine.last_applied() {
            return Err(DatabaseError::Invariant(format!(
                "checkpoint index {} disagrees with engine {}",
                checkpoint.applied_index,
                state.engine.last_applied()
            )));
        }
        Ok(CheckpointInfo {
            applied_index: checkpoint.applied_index,
            checkpoint_lsn: checkpoint.end_lsn.0,
            through_lsn: checkpoint.through_lsn.0,
        })
    }

    /// Creates a deterministic logical snapshot only after the database
    /// checkpoint at the same applied index is durable. Active explicit
    /// transactions are excluded rather than silently serialized.
    pub fn create_snapshot(&self) -> Result<DatabaseSnapshot> {
        let state = self.state.lock();
        if !state.active.is_empty() {
            return Err(DatabaseError::ResourceLimit(
                "cannot snapshot while explicit transactions are active".into(),
            ));
        }
        let checkpoint = self.persistence.checkpoint()?;
        if checkpoint.applied_index != state.engine.last_applied() {
            return Err(DatabaseError::Invariant(
                "snapshot checkpoint disagrees with engine applied index".into(),
            ));
        }
        validate_durable_requests(&state.engine, &state.requests)?;
        let state_hash = state.engine.last_apply_hash().ok_or_else(|| {
            DatabaseError::Invariant("cannot snapshot the unapplied genesis state".into())
        })?;
        let envelope = DatabaseSnapshotEnvelope {
            format: DATABASE_SNAPSHOT_FORMAT,
            engine: state.engine.snapshot(),
            requests: state
                .requests
                .iter()
                .map(|(client_id, record)| DatabaseSnapshotRequest {
                    client_id: *client_id,
                    record: record.clone(),
                })
                .collect(),
        };
        let bytes = serde_json::to_vec(&envelope).map_err(|error| {
            DatabaseError::Invariant(format!("database snapshot encode failed: {error}"))
        })?;
        if bytes.len() > crate::MAX_SNAPSHOT_BYTES {
            return Err(DatabaseError::ResourceLimit(format!(
                "database snapshot is {} bytes; maximum is {}",
                bytes.len(),
                crate::MAX_SNAPSHOT_BYTES
            )));
        }
        Ok(DatabaseSnapshot {
            applied_index: state.engine.last_applied(),
            state_hash,
            bytes,
        })
    }

    /// Installs a validated logical snapshot through the storage layer's
    /// copy-on-write publication point. Repeating an already installed image
    /// is a no-op, which lets the runtime complete an interrupted cross-file
    /// snapshot-install intent after restart.
    pub fn install_snapshot(&self, bytes: &[u8]) -> Result<()> {
        let (engine, requests) = decode_database_snapshot(bytes)?;
        self.install_decoded_snapshot(engine, requests)
    }

    /// Installs a snapshot only when its embedded state-machine boundary is the
    /// exact Raft index expected by the caller. This check happens before any
    /// database pages are written, so corrupt or mismatched Raft metadata cannot
    /// publish an otherwise well-formed snapshot at the wrong boundary.
    pub fn install_snapshot_at(&self, expected_index: u64, bytes: &[u8]) -> Result<()> {
        let (engine, requests) = decode_database_snapshot(bytes)?;
        if engine.last_applied() != expected_index {
            return Err(DatabaseError::Corruption(format!(
                "database snapshot index {} disagrees with Raft boundary {expected_index}",
                engine.last_applied()
            )));
        }
        self.install_decoded_snapshot(engine, requests)
    }

    fn install_decoded_snapshot(&self, engine: Engine, requests: DurableRequests) -> Result<()> {
        let mut state = self.state.lock();
        if !state.active.is_empty() {
            return Err(DatabaseError::ResourceLimit(
                "cannot install a snapshot while explicit transactions are active".into(),
            ));
        }
        match engine.last_applied().cmp(&state.engine.last_applied()) {
            std::cmp::Ordering::Less => {
                return Err(DatabaseError::Corruption(format!(
                    "snapshot index {} is behind local database index {}",
                    engine.last_applied(),
                    state.engine.last_applied()
                )));
            }
            std::cmp::Ordering::Equal => {
                if engine.snapshot() != state.engine.snapshot() || requests != state.requests {
                    return Err(DatabaseError::Corruption(
                        "snapshot at the local applied index has different state".into(),
                    ));
                }
                return Ok(());
            }
            std::cmp::Ordering::Greater => {}
        }
        self.persistence.install_state(&engine, &requests)?;
        state.engine = engine;
        state.requests = requests;
        Ok(())
    }

    pub fn status(&self) -> Result<DatabaseStatus> {
        let state = self.state.lock();
        let storage = self.persistence.status()?;
        if state.engine.last_applied() != storage.superblock.applied_index {
            return Err(DatabaseError::Invariant(
                "shared engine and durable pager have different applied indices".into(),
            ));
        }
        Ok(DatabaseStatus {
            applied_index: state.engine.last_applied(),
            schema_epoch: state.engine.catalog().schema_epoch(),
            retained_floor: state.engine.retained_floor(),
            active_transactions: u64::try_from(state.active.len()).map_err(|_| {
                DatabaseError::ResourceLimit("active transaction count exceeds u64".into())
            })?,
            database_pages: storage.database_pages,
            wal_bytes: storage.wal_bytes,
            checkpoint_lsn: storage.superblock.checkpoint_lsn.0,
            root_page: storage.superblock.root_directory.map(|page| page.0),
            recovery_groups_examined: usize_to_u64(
                storage.recovery.groups_examined,
                "recovery group count",
            )?,
            recovery_groups_replayed: usize_to_u64(
                storage.recovery.groups_replayed,
                "replayed group count",
            )?,
            recovery_pages_replayed: usize_to_u64(
                storage.recovery.pages_replayed,
                "replayed page count",
            )?,
        })
    }

    /// Builds a deterministic autocommit DDL/DML command without changing the
    /// shared engine, allocator, or durable files. Replicated runtimes must
    /// serialize this preparation with proposal completion so its snapshot
    /// cannot drift behind an earlier uncommitted local proposal.
    pub fn prepare_replicated_mutation(
        &self,
        leader_term: u64,
        client_id: [u8; 16],
        sequence: u64,
        sql: &str,
        parameters: &[Value],
    ) -> Result<MutationPreparation> {
        let parsed = parse_statement(sql)?;
        if !is_mutating_statement(&parsed.value) {
            return Err(DatabaseError::TransactionControl(
                "replicated proposals accept autocommit DDL/DML only".into(),
            ));
        }
        let fingerprint = sql_fingerprint(sql, parameters)?;
        let state = self.state.lock();
        if let Some(record) = replay_record(&state.requests, client_id, sequence, fingerprint)? {
            return mutation_result_from_record(&record, state.engine.last_applied())
                .map(MutationPreparation::Replay);
        }

        let mut transaction = state.engine.prepare_transaction()?;
        let output =
            Self::prepare_and_execute_output(&state.engine, &mut transaction, &parsed, parameters)?;
        let StatementOutput::Mutation { affected_rows } = output else {
            return Err(DatabaseError::Invariant(
                "replicated mutation prepared a non-mutation output".into(),
            ));
        };
        let command = ReplicatedMutation {
            format: REPLICATED_MUTATION_FORMAT,
            attempt: transaction.into_attempt(
                ClientRequestId {
                    client_id,
                    sequence,
                },
                leader_term,
            ),
            fingerprint,
            affected_rows: usize_to_u64(affected_rows, "affected row count")?,
        };
        let bytes = serde_json::to_vec(&command).map_err(|error| {
            DatabaseError::Invariant(format!("replicated command encode failed: {error}"))
        })?;
        if bytes.len() > MAX_REPLICATED_COMMAND_BYTES {
            return Err(DatabaseError::ResourceLimit(format!(
                "replicated command is {} bytes; maximum is {MAX_REPLICATED_COMMAND_BYTES}",
                bytes.len()
            )));
        }
        Ok(MutationPreparation::Propose(PreparedMutation { bytes }))
    }

    /// Applies one quorum-committed command at its exact Raft log index. The
    /// same index and command are restart-idempotent; a gap, different command,
    /// or stale index fails closed.
    pub fn apply_replicated(&self, index: u64, bytes: &[u8]) -> Result<ExecutionResult> {
        if bytes.len() > MAX_REPLICATED_COMMAND_BYTES {
            return Err(DatabaseError::ResourceLimit(format!(
                "replicated command is {} bytes; maximum is {MAX_REPLICATED_COMMAND_BYTES}",
                bytes.len()
            )));
        }
        let command: ReplicatedMutation = serde_json::from_slice(bytes).map_err(|error| {
            DatabaseError::Corruption(format!("replicated command decode failed: {error}"))
        })?;
        if command.format != REPLICATED_MUTATION_FORMAT {
            return Err(DatabaseError::Corruption(format!(
                "unsupported replicated command format {}",
                command.format
            )));
        }
        let client_id = command.attempt.request().client_id;
        let sequence = command.attempt.request().sequence;
        let apply_hash: [u8; 32] = Sha256::digest(bytes).into();
        let mut state = self.state.lock();
        if index < state.engine.last_applied() {
            return Err(DatabaseError::Corruption(format!(
                "replicated apply index {index} is behind durable database index {}",
                state.engine.last_applied()
            )));
        }
        let mut candidate = state.engine.clone();
        let outcome = candidate.apply_with_request_hash(
            index,
            apply_hash,
            command.fingerprint,
            &command.attempt,
        )?;
        if index == state.engine.last_applied() {
            validate_durable_replay(
                &state.requests,
                client_id,
                sequence,
                command.fingerprint,
                command.affected_rows,
                &outcome,
            )?;
        } else {
            self.publish_candidate(
                &mut state,
                candidate,
                &outcome,
                client_id,
                sequence,
                command.fingerprint,
                command.affected_rows,
            )?;
        }
        let response_affected_rows = match &outcome {
            ApplyOutcome::Applied(_) | ApplyOutcome::Duplicate(_) => {
                state
                    .requests
                    .get(&client_id)
                    .filter(|record| {
                        record.sequence == sequence && record.fingerprint == command.fingerprint
                    })
                    .ok_or_else(|| {
                        DatabaseError::Invariant(
                            "successful replicated apply has no matching durable request record"
                                .into(),
                        )
                    })?
                    .affected_rows
            }
            ApplyOutcome::Rejected(_) | ApplyOutcome::Noop => command.affected_rows,
        };
        replicated_outcome(outcome, response_affected_rows, index)
    }

    /// Applies a committed Raft leader no-op so MVCC timestamps remain exactly
    /// aligned with Raft log indexes.
    pub fn apply_raft_noop(&self, index: u64, term: u64) -> Result<()> {
        let hash = raft_noop_hash(index, term);
        let mut state = self.state.lock();
        if index < state.engine.last_applied() {
            return Err(DatabaseError::Corruption(format!(
                "Raft no-op index {index} is behind durable database index {}",
                state.engine.last_applied()
            )));
        }
        let mut candidate = state.engine.clone();
        let outcome = candidate.apply_noop(index, hash)?;
        if outcome != ApplyOutcome::Noop {
            return Err(DatabaseError::Invariant(
                "Raft no-op produced a transaction outcome".into(),
            ));
        }
        if index > state.engine.last_applied() {
            self.persistence
                .persist_state(&candidate, &state.requests)?;
            state.engine = candidate;
        }
        Ok(())
    }

    fn execute_in_transaction(
        state: &mut DatabaseState,
        transaction_id: TxnId,
        parsed: &aster_sql::ast::Spanned<aster_sql::Statement>,
        parameters: &[Value],
    ) -> Result<ExecutionResult> {
        let original = state
            .active
            .remove(&transaction_id)
            .ok_or(DatabaseError::TransactionNotFound(transaction_id))?;
        let mut staged = original.clone();
        let result = Self::prepare_and_execute(&state.engine, &mut staged, parsed, parameters);
        match result {
            Ok(result) => {
                state.active.insert(transaction_id, staged);
                Ok(result)
            }
            Err(error) => {
                // SQL statements are atomic within a transaction: discard any
                // writes staged before an expression or constraint failed.
                state.active.insert(transaction_id, original);
                Err(error)
            }
        }
    }

    fn execute_autocommit(
        &self,
        state: &mut DatabaseState,
        client_id: [u8; 16],
        sequence: u64,
        sql: &str,
        parsed: &aster_sql::ast::Spanned<aster_sql::Statement>,
        parameters: &[Value],
    ) -> Result<ExecutionResult> {
        let fingerprint = sql_fingerprint(sql, parameters)?;
        if is_mutating_statement(&parsed.value)
            && let Some(record) = replay_record(&state.requests, client_id, sequence, fingerprint)?
        {
            return mutation_result_from_record(&record, state.engine.last_applied());
        }
        let mut transaction = state.engine.begin()?;
        let output =
            Self::prepare_and_execute_output(&state.engine, &mut transaction, parsed, parameters)?;
        if transaction.is_read_only() {
            return statement_output(output, state.engine.last_applied());
        }
        let affected_rows = match &output {
            StatementOutput::Mutation { affected_rows } => {
                usize_to_u64(*affected_rows, "affected row count")?
            }
            StatementOutput::Query { .. } | StatementOutput::Explain(_) => {
                return Err(DatabaseError::Invariant(
                    "read-only output unexpectedly staged a mutation".into(),
                ));
            }
        };
        let outcome = self.apply_transaction(
            state,
            &transaction,
            client_id,
            sequence,
            fingerprint,
            affected_rows,
        )?;
        match outcome {
            ApplyOutcome::Applied(result) | ApplyOutcome::Duplicate(result) => {
                let info = commit_info_from_result(&result, state.engine.last_applied())?;
                Ok(ExecutionResult::Query(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    affected_rows,
                    applied_index: info.applied_index,
                }))
            }
            ApplyOutcome::Rejected(rejection) => Err(DatabaseError::RequestRejected(rejection)),
            ApplyOutcome::Noop => Err(DatabaseError::Invariant(
                "autocommit transaction produced a Raft no-op".into(),
            )),
        }
    }

    fn prepare_and_execute(
        engine: &Engine,
        transaction: &mut Transaction,
        parsed: &aster_sql::ast::Spanned<aster_sql::Statement>,
        parameters: &[Value],
    ) -> Result<ExecutionResult> {
        let output = Self::prepare_and_execute_output(engine, transaction, parsed, parameters)?;
        statement_output(output, engine.last_applied())
    }

    fn prepare_and_execute_output(
        engine: &Engine,
        transaction: &mut Transaction,
        parsed: &aster_sql::ast::Spanned<aster_sql::Statement>,
        parameters: &[Value],
    ) -> Result<StatementOutput> {
        let binding_catalog = EngineCatalog::new(engine, Some(transaction));
        let bound = bind(parsed, &binding_catalog)?;
        validate_parameters(&bound.parameters, parameters)?;
        if matches!(bound.statement, BoundStatement::Transaction(_)) {
            return Err(DatabaseError::TransactionControl(
                "transaction control must be dispatched before execution".into(),
            ));
        }

        // Staged indexes are visible to name binding but are deliberately not
        // selected as access paths until commit has backfilled their entries.
        let planning_catalog = EngineCatalog::new(engine, None);
        match &bound.statement {
            BoundStatement::Explain(inner) => {
                if matches!(inner.as_ref(), BoundStatement::Transaction(_)) {
                    return Err(DatabaseError::TransactionControl(
                        "EXPLAIN does not accept transaction control".into(),
                    ));
                }
                let physical = physical_plan(inner, &planning_catalog);
                explain_statement(inner, physical.as_ref())
            }
            statement => {
                let physical = physical_plan(statement, &planning_catalog);
                execute_bound(
                    engine,
                    transaction,
                    statement,
                    physical.as_ref(),
                    parameters,
                )
            }
        }
    }

    fn apply_transaction(
        &self,
        state: &mut DatabaseState,
        transaction: &Transaction,
        client_id: [u8; 16],
        sequence: u64,
        fingerprint: [u8; 32],
        affected_rows: u64,
    ) -> Result<ApplyOutcome> {
        let attempt = transaction.clone().into_attempt(
            ClientRequestId {
                client_id,
                sequence,
            },
            STANDALONE_LEADER_TERM,
        );
        let command_hash = attempt.command_hash()?;
        let apply_index = state
            .engine
            .last_applied()
            .checked_add(1)
            .ok_or_else(|| DatabaseError::Invariant("applied index overflow".into()))?;
        let mut candidate = state.engine.clone();
        let outcome =
            candidate.apply_with_request_hash(apply_index, command_hash, fingerprint, &attempt)?;
        self.publish_candidate(
            state,
            candidate,
            &outcome,
            client_id,
            sequence,
            fingerprint,
            affected_rows,
        )?;
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_candidate(
        &self,
        state: &mut DatabaseState,
        candidate: Engine,
        outcome: &ApplyOutcome,
        client_id: [u8; 16],
        sequence: u64,
        fingerprint: [u8; 32],
        affected_rows: u64,
    ) -> Result<()> {
        let mut candidate_requests = state.requests.clone();
        match outcome {
            ApplyOutcome::Applied(result) => {
                candidate_requests.insert(
                    client_id,
                    DurableRequestRecord {
                        sequence,
                        fingerprint,
                        result: result.clone(),
                        affected_rows,
                    },
                );
            }
            ApplyOutcome::Duplicate(result) => {
                if let Some(existing) = candidate_requests.get(&client_id) {
                    if existing.sequence != sequence
                        || existing.fingerprint != fingerprint
                        || &existing.result != result
                    {
                        return Err(DatabaseError::Invariant(
                            "engine duplicate disagrees with durable request fingerprint".into(),
                        ));
                    }
                } else {
                    candidate_requests.insert(
                        client_id,
                        DurableRequestRecord {
                            sequence,
                            fingerprint,
                            result: result.clone(),
                            affected_rows,
                        },
                    );
                }
            }
            ApplyOutcome::Rejected(_) => {}
            ApplyOutcome::Noop => {
                return Err(DatabaseError::Invariant(
                    "transaction publication received a Raft no-op".into(),
                ));
            }
        }
        self.persistence
            .persist_state(&candidate, &candidate_requests)?;
        state.engine = candidate;
        state.requests = candidate_requests;
        Ok(())
    }
}

fn decode_database_snapshot(bytes: &[u8]) -> Result<(Engine, DurableRequests)> {
    if bytes.is_empty() || bytes.len() > crate::MAX_SNAPSHOT_BYTES {
        return Err(DatabaseError::ResourceLimit(format!(
            "database snapshot length {} is outside 1..={} bytes",
            bytes.len(),
            crate::MAX_SNAPSHOT_BYTES
        )));
    }
    let envelope: DatabaseSnapshotEnvelope = serde_json::from_slice(bytes).map_err(|error| {
        DatabaseError::Corruption(format!("database snapshot decode failed: {error}"))
    })?;
    if envelope.format != DATABASE_SNAPSHOT_FORMAT {
        return Err(DatabaseError::Corruption(format!(
            "unsupported database snapshot format {}",
            envelope.format
        )));
    }
    let engine = Engine::from_snapshot(envelope.engine)?;
    let mut requests = DurableRequests::new();
    for request in envelope.requests {
        if requests.insert(request.client_id, request.record).is_some() {
            return Err(DatabaseError::Corruption(
                "database snapshot contains a duplicate client record".into(),
            ));
        }
    }
    validate_durable_requests(&engine, &requests)?;
    Ok((engine, requests))
}

fn physical_plan(statement: &BoundStatement, catalog: &EngineCatalog) -> Option<PhysicalPlan> {
    if let BoundStatement::Query { plan, .. } = statement {
        Some(optimize(plan, catalog))
    } else {
        None
    }
}

fn statement_output(output: StatementOutput, applied_index: u64) -> Result<ExecutionResult> {
    match output {
        StatementOutput::Query { columns, rows } => Ok(ExecutionResult::Query(QueryResult {
            columns,
            rows,
            affected_rows: 0,
            applied_index,
        })),
        StatementOutput::Mutation { affected_rows } => Ok(ExecutionResult::Query(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: usize_to_u64(affected_rows, "affected row count")?,
            applied_index,
        })),
        StatementOutput::Explain(plan) => Ok(ExecutionResult::Explain {
            plan,
            applied_index,
        }),
    }
}

fn commit_info_from_result(result: &CommitResult, applied_index: u64) -> Result<CommitInfo> {
    match result {
        CommitResult::Committed {
            commit_index,
            affected_rows,
            schema_epoch,
        } => Ok(CommitInfo {
            commit_index: *commit_index,
            affected_rows: usize_to_u64(*affected_rows, "affected row count")?,
            schema_epoch: *schema_epoch,
            applied_index,
        }),
        CommitResult::Aborted { reason, .. } => {
            Err(DatabaseError::TransactionAborted(reason.clone()))
        }
    }
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| DatabaseError::ResourceLimit(format!("{label} exceeds u64")))
}

fn replay_record(
    requests: &DurableRequests,
    client_id: [u8; 16],
    sequence: u64,
    fingerprint: [u8; 32],
) -> Result<Option<DurableRequestRecord>> {
    let Some(record) = requests.get(&client_id) else {
        return Ok(None);
    };
    if record.sequence != sequence {
        return Ok(None);
    }
    if record.fingerprint != fingerprint {
        return Err(DatabaseError::RequestRejected(
            RequestRejection::SequenceHashMismatch { sequence },
        ));
    }
    Ok(Some(record.clone()))
}

fn mutation_result_from_record(
    record: &DurableRequestRecord,
    applied_index: u64,
) -> Result<ExecutionResult> {
    commit_info_from_result(&record.result, applied_index)?;
    Ok(ExecutionResult::Query(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        affected_rows: record.affected_rows,
        applied_index,
    }))
}

fn replicated_outcome(
    outcome: ApplyOutcome,
    affected_rows: u64,
    applied_index: u64,
) -> Result<ExecutionResult> {
    match outcome {
        ApplyOutcome::Applied(result) | ApplyOutcome::Duplicate(result) => {
            commit_info_from_result(&result, applied_index)?;
            Ok(ExecutionResult::Query(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                affected_rows,
                applied_index,
            }))
        }
        ApplyOutcome::Rejected(rejection) => Err(DatabaseError::RequestRejected(rejection)),
        ApplyOutcome::Noop => Err(DatabaseError::Invariant(
            "replicated transaction decoded as a Raft no-op".into(),
        )),
    }
}

fn validate_durable_replay(
    requests: &DurableRequests,
    client_id: [u8; 16],
    sequence: u64,
    fingerprint: [u8; 32],
    affected_rows: u64,
    outcome: &ApplyOutcome,
) -> Result<()> {
    match outcome {
        ApplyOutcome::Applied(result) | ApplyOutcome::Duplicate(result) => {
            let record = requests.get(&client_id).ok_or_else(|| {
                DatabaseError::Corruption(
                    "durable replicated apply has no request fingerprint".into(),
                )
            })?;
            if record.sequence != sequence
                || record.fingerprint != fingerprint
                || record.affected_rows != affected_rows
                || &record.result != result
            {
                return Err(DatabaseError::Corruption(
                    "durable replicated request metadata disagrees with replay".into(),
                ));
            }
            Ok(())
        }
        // A rejected sequence deliberately leaves the prior per-client record
        // untouched; the exact entry hash was checked by Engine::apply.
        ApplyOutcome::Rejected(_) => Ok(()),
        ApplyOutcome::Noop => Err(DatabaseError::Corruption(
            "transaction replay found a durable Raft no-op".into(),
        )),
    }
}

fn raft_noop_hash(index: u64, term: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"ASTRAFTNOOP\x01");
    hasher.update(index.to_be_bytes());
    hasher.update(term.to_be_bytes());
    hasher.finalize().into()
}

fn sql_fingerprint(sql: &str, parameters: &[Value]) -> Result<[u8; 32]> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(b"ASTDBREQ\x01SQL");
    put_fingerprint_bytes(&mut encoded, sql.as_bytes())?;
    let parameters = encode_row(&Row {
        values: parameters.to_vec(),
    })
    .map_err(aster_engine::EngineError::from)?;
    put_fingerprint_bytes(&mut encoded, &parameters)?;
    Ok(Sha256::digest(encoded).into())
}

fn commit_fingerprint(transaction_id: TxnId) -> [u8; 32] {
    let mut encoded = Vec::with_capacity(20);
    encoded.extend_from_slice(b"ASTDBREQ\x01COMMIT");
    encoded.extend_from_slice(&transaction_id.to_be_bytes());
    Sha256::digest(encoded).into()
}

fn put_fingerprint_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u64::try_from(value.len()).map_err(|_| {
        DatabaseError::ResourceLimit("request fingerprint input is too large".into())
    })?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn is_mutating_statement(statement: &aster_sql::Statement) -> bool {
    matches!(
        statement,
        aster_sql::Statement::CreateTable(_)
            | aster_sql::Statement::CreateIndex(_)
            | aster_sql::Statement::Insert(_)
            | aster_sql::Statement::Update(_)
            | aster_sql::Statement::Delete(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const CLIENT: [u8; 16] = [3; 16];

    fn query(database: &Database, sequence: u64, sql: &str) -> QueryResult {
        match database.execute(CLIENT, sequence, None, sql, &[]).unwrap() {
            ExecutionResult::Query(result) => result,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn durable_sql_vertical_slice_survives_reopen() {
        let directory = TempDir::new().unwrap();
        {
            let database = Database::open(directory.path()).unwrap();
            query(
                &database,
                1,
                "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL, active BOOL)",
            );
            query(&database, 2, "INSERT INTO users VALUES (1, 'Ada', true)");
            query(&database, 3, "INSERT INTO users VALUES (2, 'Grace', false)");
            let result = query(
                &database,
                99,
                "SELECT id, name FROM users WHERE active = true ORDER BY id",
            );
            assert_eq!(
                result.rows,
                vec![Row {
                    values: vec![Value::Int64(1), Value::Text("Ada".into())]
                }]
            );
            assert_eq!(database.status().unwrap().applied_index, 3);
        }
        {
            let reopened = Database::open(directory.path()).unwrap();
            let result = query(&reopened, 100, "SELECT name FROM users ORDER BY id");
            assert_eq!(
                result.rows,
                vec![
                    Row {
                        values: vec![Value::Text("Ada".into())]
                    },
                    Row {
                        values: vec![Value::Text("Grace".into())]
                    }
                ]
            );
            assert_eq!(reopened.status().unwrap().applied_index, 3);
        }
    }

    #[test]
    fn explicit_commit_rollback_and_retry_are_correct() {
        let directory = TempDir::new().unwrap();
        let database = Database::open(directory.path()).unwrap();
        query(
            &database,
            1,
            "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64 NOT NULL)",
        );

        let rolled_back = database.begin().unwrap();
        database
            .execute(
                CLIENT,
                2,
                Some(rolled_back.transaction_id),
                "INSERT INTO accounts VALUES (1, 10)",
                &[],
            )
            .unwrap();
        database.rollback(rolled_back.transaction_id).unwrap();
        assert!(
            query(&database, 88, "SELECT id FROM accounts")
                .rows
                .is_empty()
        );

        let transaction = database.begin().unwrap();
        database
            .execute(
                CLIENT,
                2,
                Some(transaction.transaction_id),
                "INSERT INTO accounts VALUES (1, 10)",
                &[],
            )
            .unwrap();
        let committed = database
            .commit(CLIENT, 2, transaction.transaction_id)
            .unwrap();
        assert_eq!(committed.affected_rows, 1);
        let retry = database
            .commit(CLIENT, 2, transaction.transaction_id)
            .unwrap();
        assert_eq!(retry, committed);
        assert_eq!(
            query(&database, 89, "SELECT balance FROM accounts").rows,
            vec![Row {
                values: vec![Value::Int64(10)]
            }]
        );
    }

    #[test]
    fn index_join_aggregate_parameters_and_checkpoint() {
        let directory = TempDir::new().unwrap();
        let database = Database::open(directory.path()).unwrap();
        query(
            &database,
            1,
            "CREATE TABLE teams (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
        );
        query(
            &database,
            2,
            "CREATE TABLE scores (id INT64 PRIMARY KEY, team_id INT64 NOT NULL, points INT64)",
        );
        query(&database, 3, "INSERT INTO teams VALUES (1, 'red')");
        query(&database, 4, "INSERT INTO teams VALUES (2, 'blue')");
        query(&database, 5, "INSERT INTO scores VALUES (10, 1, 7)");
        query(&database, 6, "INSERT INTO scores VALUES (11, 1, 5)");
        query(&database, 7, "INSERT INTO scores VALUES (12, 2, 3)");
        query(&database, 8, "CREATE INDEX scores_team ON scores (team_id)");

        let result = database
            .execute(
                CLIENT,
                77,
                None,
                "SELECT teams.name, SUM(scores.points) AS total FROM teams JOIN scores ON teams.id = scores.team_id WHERE scores.team_id >= ? GROUP BY teams.name ORDER BY total DESC LIMIT 2",
                &[Value::Int64(1)],
            )
            .unwrap();
        let ExecutionResult::Query(result) = result else {
            panic!("expected query result");
        };
        assert_eq!(
            result.rows,
            vec![
                Row {
                    values: vec![Value::Text("red".into()), Value::Int64(12)]
                },
                Row {
                    values: vec![Value::Text("blue".into()), Value::Int64(3)]
                }
            ]
        );
        let checkpoint = database.checkpoint().unwrap();
        assert_eq!(checkpoint.applied_index, 8);
        assert!(checkpoint.checkpoint_lsn > 0);
    }

    #[test]
    fn statement_error_does_not_partially_change_explicit_transaction() {
        let directory = TempDir::new().unwrap();
        let database = Database::open(directory.path()).unwrap();
        query(
            &database,
            1,
            "CREATE TABLE numbers (id INT64 PRIMARY KEY, value INT64 NOT NULL)",
        );
        query(&database, 2, "INSERT INTO numbers VALUES (1, 10)");
        query(&database, 3, "INSERT INTO numbers VALUES (2, 20)");
        let transaction = database.begin().unwrap();
        assert!(
            database
                .execute(
                    CLIENT,
                    4,
                    Some(transaction.transaction_id),
                    "UPDATE numbers SET value = NULL",
                    &[],
                )
                .is_err()
        );
        database
            .commit(CLIENT, 4, transaction.transaction_id)
            .unwrap();
        assert_eq!(
            query(&database, 90, "SELECT value FROM numbers ORDER BY id").rows,
            vec![
                Row {
                    values: vec![Value::Int64(10)]
                },
                Row {
                    values: vec![Value::Int64(20)]
                }
            ]
        );
    }

    #[test]
    fn large_values_span_tree_chunks_and_survive_checkpoint_restart() {
        let directory = TempDir::new().unwrap();
        let value = "λ-data-".repeat(2_000);
        {
            let database = Database::open(directory.path()).unwrap();
            query(
                &database,
                1,
                "CREATE TABLE documents (id INT64 PRIMARY KEY, body TEXT NOT NULL)",
            );
            let inserted = database
                .execute(
                    CLIENT,
                    2,
                    None,
                    "INSERT INTO documents VALUES (1, ?)",
                    &[Value::Text(value.clone())],
                )
                .unwrap();
            assert!(matches!(
                inserted,
                ExecutionResult::Query(QueryResult {
                    affected_rows: 1,
                    ..
                })
            ));
            database.checkpoint().unwrap();
        }
        let reopened = Database::open(directory.path()).unwrap();
        assert_eq!(
            query(&reopened, 70, "SELECT body FROM documents").rows,
            vec![Row {
                values: vec![Value::Text(value)]
            }]
        );
    }

    #[test]
    fn conflicting_commit_aborts_durably_and_sequence_mismatch_can_retry() {
        let directory = TempDir::new().unwrap();
        let database = Database::open(directory.path()).unwrap();
        query(
            &database,
            1,
            "CREATE TABLE counters (id INT64 PRIMARY KEY, value INT64 NOT NULL)",
        );
        query(&database, 2, "INSERT INTO counters VALUES (1, 0)");
        let first = database.begin().unwrap();
        let second = database.begin().unwrap();
        database
            .execute(
                CLIENT,
                3,
                Some(first.transaction_id),
                "UPDATE counters SET value = 1 WHERE id = 1",
                &[],
            )
            .unwrap();
        database
            .execute(
                CLIENT,
                4,
                Some(second.transaction_id),
                "UPDATE counters SET value = 2 WHERE id = 1",
                &[],
            )
            .unwrap();
        database.commit(CLIENT, 3, first.transaction_id).unwrap();
        assert!(matches!(
            database.commit(CLIENT, 4, second.transaction_id),
            Err(DatabaseError::TransactionAborted(_))
        ));
        assert_eq!(database.status().unwrap().applied_index, 4);
        drop(database);

        let reopened = Database::open(directory.path()).unwrap();
        assert_eq!(reopened.status().unwrap().applied_index, 4);
        assert_eq!(
            query(&reopened, 80, "SELECT value FROM counters").rows,
            vec![Row {
                values: vec![Value::Int64(1)]
            }]
        );

        let transaction = reopened.begin().unwrap();
        reopened
            .execute(
                CLIENT,
                4,
                Some(transaction.transaction_id),
                "INSERT INTO counters VALUES (2, 2)",
                &[],
            )
            .unwrap();
        assert!(matches!(
            reopened.commit(CLIENT, 4, transaction.transaction_id),
            Err(DatabaseError::RequestRejected(_))
        ));
        reopened
            .commit(CLIENT, 5, transaction.transaction_id)
            .unwrap();
        assert_eq!(reopened.status().unwrap().applied_index, 6);
    }

    #[test]
    fn autocommit_retry_is_payload_checked_and_restart_safe() {
        let directory = TempDir::new().unwrap();
        let statement = "INSERT INTO tokens VALUES (1, 'once')";
        {
            let database = Database::open(directory.path()).unwrap();
            query(
                &database,
                1,
                "CREATE TABLE tokens (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
            );
            assert_eq!(query(&database, 2, statement).affected_rows, 1);
            let before_retry = database.status().unwrap().applied_index;
            assert_eq!(query(&database, 2, statement).affected_rows, 1);
            assert_eq!(database.status().unwrap().applied_index, before_retry);
            assert!(matches!(
                database.execute(
                    CLIENT,
                    2,
                    None,
                    "INSERT INTO tokens VALUES (2, 'different')",
                    &[],
                ),
                Err(DatabaseError::RequestRejected(
                    RequestRejection::SequenceHashMismatch { sequence: 2 }
                ))
            ));
            assert_eq!(database.status().unwrap().applied_index, before_retry);
        }

        let reopened = Database::open(directory.path()).unwrap();
        assert_eq!(query(&reopened, 2, statement).affected_rows, 1);
        assert_eq!(reopened.status().unwrap().applied_index, 2);
        assert_eq!(
            query(&reopened, 90, "SELECT id, value FROM tokens").rows,
            vec![Row {
                values: vec![Value::Int64(1), Value::Text("once".into())]
            }]
        );
    }

    #[test]
    fn replicated_prepare_is_private_and_exact_index_apply_is_idempotent() {
        let leader_directory = TempDir::new().unwrap();
        let follower_directory = TempDir::new().unwrap();
        let leader = Database::open(leader_directory.path()).unwrap();
        let follower = Database::open(follower_directory.path()).unwrap();
        leader.apply_raft_noop(1, 4).unwrap();
        follower.apply_raft_noop(1, 4).unwrap();

        let prepared = leader
            .prepare_replicated_mutation(
                4,
                CLIENT,
                1,
                "CREATE TABLE replicated (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
                &[],
            )
            .unwrap();
        let MutationPreparation::Propose(prepared) = prepared else {
            panic!("new request unexpectedly replayed");
        };
        assert_eq!(leader.status().unwrap().applied_index, 1);
        assert!(
            leader
                .execute(CLIENT, 90, None, "SELECT id FROM replicated", &[])
                .is_err()
        );

        for database in [&leader, &follower] {
            let applied = database.apply_replicated(2, prepared.bytes()).unwrap();
            assert!(matches!(
                applied,
                ExecutionResult::Query(QueryResult {
                    affected_rows: 0,
                    applied_index: 2,
                    ..
                })
            ));
            database.apply_replicated(2, prepared.bytes()).unwrap();
            assert_eq!(database.status().unwrap().applied_index, 2);
        }

        let replay = leader
            .prepare_replicated_mutation(
                4,
                CLIENT,
                1,
                "CREATE TABLE replicated (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
                &[],
            )
            .unwrap();
        assert!(matches!(replay, MutationPreparation::Replay(_)));
        assert!(leader.apply_raft_noop(4, 4).is_err());

        drop(leader);
        drop(follower);
        for directory in [leader_directory.path(), follower_directory.path()] {
            let reopened = Database::open(directory).unwrap();
            assert_eq!(reopened.status().unwrap().applied_index, 2);
            assert!(
                query(&reopened, 80, "SELECT id FROM replicated")
                    .rows
                    .is_empty()
            );
        }
    }

    #[test]
    fn old_term_entry_and_reprepared_retry_share_logical_request_identity() {
        let directory = TempDir::new().unwrap();
        let database = Database::open(directory.path()).unwrap();
        let statement = "CREATE TABLE term_retry (id INT64 PRIMARY KEY, value TEXT NOT NULL)";
        let first = database
            .prepare_replicated_mutation(3, CLIENT, 1, statement, &[])
            .unwrap();
        let retry = database
            .prepare_replicated_mutation(4, CLIENT, 1, statement, &[])
            .unwrap();
        let MutationPreparation::Propose(first) = first else {
            panic!("first request unexpectedly replayed");
        };
        let MutationPreparation::Propose(retry) = retry else {
            panic!("uncommitted retry unexpectedly replayed");
        };
        assert_ne!(
            first.bytes(),
            retry.bytes(),
            "leader term must make exact log payloads distinct"
        );

        database.apply_replicated(1, first.bytes()).unwrap();
        let duplicated = database.apply_replicated(2, retry.bytes()).unwrap();
        assert!(matches!(
            duplicated,
            ExecutionResult::Query(QueryResult {
                affected_rows: 0,
                applied_index: 2,
                ..
            })
        ));
        assert_eq!(database.status().unwrap().applied_index, 2);

        drop(database);
        let reopened = Database::open(directory.path()).unwrap();
        assert!(matches!(
            reopened
                .prepare_replicated_mutation(5, CLIENT, 1, statement, &[])
                .unwrap(),
            MutationPreparation::Replay(_)
        ));
    }

    #[test]
    fn first_committed_retry_owns_result_across_different_snapshots() {
        const SETUP_CLIENT: [u8; 16] = [8; 16];
        const STATEMENT: &str = "UPDATE retry_rows SET value = 'updated' WHERE id = 1";

        for newer_attempt_commits_first in [false, true] {
            let directory = TempDir::new().unwrap();
            let database = Database::open(directory.path()).unwrap();
            let MutationPreparation::Propose(create) = database
                .prepare_replicated_mutation(
                    1,
                    SETUP_CLIENT,
                    1,
                    "CREATE TABLE retry_rows (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
                    &[],
                )
                .unwrap()
            else {
                panic!("setup create unexpectedly replayed");
            };
            database.apply_replicated(1, create.bytes()).unwrap();

            // The old-term form sees no matching row and stages no write.
            let MutationPreparation::Propose(older) = database
                .prepare_replicated_mutation(2, CLIENT, 1, STATEMENT, &[])
                .unwrap()
            else {
                panic!("old-term update unexpectedly replayed");
            };

            let MutationPreparation::Propose(insert) = database
                .prepare_replicated_mutation(
                    2,
                    SETUP_CLIENT,
                    2,
                    "INSERT INTO retry_rows VALUES (1, 'original')",
                    &[],
                )
                .unwrap()
            else {
                panic!("setup insert unexpectedly replayed");
            };
            database.apply_replicated(2, insert.bytes()).unwrap();

            // The re-prepared form has the same logical client request but a
            // newer term/read timestamp, one staged write, and affected_rows=1.
            let MutationPreparation::Propose(newer) = database
                .prepare_replicated_mutation(3, CLIENT, 1, STATEMENT, &[])
                .unwrap()
            else {
                panic!("new-term update unexpectedly replayed");
            };
            assert_ne!(older.bytes(), newer.bytes());

            let (first, second, expected_affected, expected_value) = if newer_attempt_commits_first
            {
                (&newer, &older, 1, "updated")
            } else {
                (&older, &newer, 0, "original")
            };
            let first_result = database.apply_replicated(3, first.bytes()).unwrap();
            let second_result = database.apply_replicated(4, second.bytes()).unwrap();
            for result in [first_result, second_result] {
                assert!(matches!(
                    result,
                    ExecutionResult::Query(QueryResult {
                        affected_rows,
                        ..
                    }) if affected_rows == expected_affected
                ));
            }
            let rows = database
                .execute_read_only(CLIENT, 99, "SELECT value FROM retry_rows WHERE id = 1", &[])
                .unwrap();
            let ExecutionResult::Query(rows) = rows else {
                panic!("expected query result");
            };
            assert_eq!(
                rows.rows,
                vec![Row {
                    values: vec![Value::Text(expected_value.into())]
                }]
            );
            assert_eq!(database.status().unwrap().applied_index, 4);
        }
    }

    #[test]
    fn active_transaction_count_is_hard_bounded() {
        let directory = TempDir::new().unwrap();
        let database = Database::open_with_options(
            directory.path(),
            DatabaseOptions {
                max_active_transactions: 1,
            },
        )
        .unwrap();
        let first = database.begin().unwrap();
        assert!(matches!(
            database.begin(),
            Err(DatabaseError::ResourceLimit(_))
        ));
        database.rollback(first.transaction_id).unwrap();
        assert!(database.begin().is_ok());
    }

    #[test]
    fn logical_snapshot_installs_idempotently_and_survives_restart() {
        let source_directory = TempDir::new().unwrap();
        let target_directory = TempDir::new().unwrap();
        let source = Database::open(source_directory.path()).unwrap();
        query(
            &source,
            1,
            "CREATE TABLE snapshots (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
        );
        query(&source, 2, "INSERT INTO snapshots VALUES (1, 'durable')");
        let snapshot = source.create_snapshot().unwrap();
        assert_eq!(snapshot.applied_index(), 2);
        assert_ne!(snapshot.state_hash(), [0; 32]);

        let target = Database::open(target_directory.path()).unwrap();
        assert!(target.install_snapshot_at(3, snapshot.bytes()).is_err());
        assert_eq!(target.status().unwrap().applied_index, 0);
        target.install_snapshot_at(2, snapshot.bytes()).unwrap();
        target.install_snapshot(snapshot.bytes()).unwrap();
        assert_eq!(target.status().unwrap().applied_index, 2);
        assert_eq!(
            query(&target, 90, "SELECT id, value FROM snapshots").rows,
            vec![Row {
                values: vec![Value::Int64(1), Value::Text("durable".into())]
            }]
        );
        let mut corrupted = snapshot.bytes().to_vec();
        corrupted[0] ^= 0x80;
        assert!(target.install_snapshot(&corrupted).is_err());

        drop(target);
        let reopened = Database::open(target_directory.path()).unwrap();
        assert_eq!(reopened.status().unwrap().applied_index, 2);
        assert_eq!(
            query(&reopened, 91, "SELECT value FROM snapshots").rows,
            vec![Row {
                values: vec![Value::Text("durable".into())]
            }]
        );
    }
}

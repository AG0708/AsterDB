//! Persistent, bounded runtime for the pure `AsterDB` Raft core.
//!
//! The runtime is intentionally separate from the standalone server. It owns
//! consensus storage, peer networking, randomized timers, and the ordering
//! boundary that prevents a client acknowledgement from preceding local
//! durable database apply.

#![forbid(unsafe_code)]

mod stable;

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aster_core::{NodeId, Value};
use aster_db::{Database, ExecutionResult, MutationPreparation};
use aster_protocol::{
    DEFAULT_MAX_FRAME_BYTES, PeerEnvelope, WireMessage, read_message, write_message,
};
use aster_raft::{
    Action, CommandId, Configuration, EntryPayload, Input, Message, PersistentSnapshot, Raft, Role,
    StableState, StorageMutation, Tick,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval, timeout};

use crate::stable::StableStore;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("runtime state is corrupt: {0}")]
    Corruption(String),
    #[error("runtime resource limit exceeded: {0}")]
    Limit(String),
    #[error("unsupported runtime operation: {0}")]
    Unsupported(String),
    #[error("invalid runtime configuration: {0}")]
    Configuration(String),
    #[error("database failed: {0}")]
    Database(#[from] aster_db::DatabaseError),
    #[error("Raft recovery failed: {0}")]
    Recovery(#[from] aster_raft::RecoveryError),
    #[error("node is not leader; leader hint: {leader_hint:?}")]
    NotLeader { leader_hint: Option<u64> },
    #[error("another replicated proposal is awaiting commit")]
    ProposalBusy,
    #[error("committed database command was rejected: {0}")]
    CommandRejected(String),
    #[error("runtime request timed out")]
    Timeout,
    #[error("runtime has shut down")]
    Shutdown,
    #[error("Raft fenced the node: {0}")]
    Fenced(String),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub node_id: NodeId,
    /// Fixed voter-to-peer-listener map. It must contain this node.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub data_directory: PathBuf,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub check_quorum_interval: Duration,
    pub network_timeout: Duration,
    pub request_timeout: Duration,
    pub event_queue_capacity: usize,
    pub max_peer_connections: usize,
    pub max_frame_bytes: usize,
    /// Build a durable state-machine snapshot after this many retained Raft
    /// entries. Entries after the applied boundary remain as a suffix.
    pub snapshot_threshold_entries: usize,
    /// Build a snapshot when the encoded retained Raft log reaches this many
    /// bytes, even if its entry count is lower.
    pub snapshot_threshold_bytes: usize,
    /// Deterministic election jitter for replayable tests. `None` seeds from
    /// operating-system entropy and is the production default.
    pub rng_seed: Option<u64>,
}

impl RuntimeConfig {
    #[must_use]
    pub fn localhost(
        node_id: NodeId,
        peers: BTreeMap<NodeId, SocketAddr>,
        data_directory: PathBuf,
    ) -> Self {
        Self {
            node_id,
            peers,
            data_directory,
            election_timeout_min: Duration::from_millis(350),
            election_timeout_max: Duration::from_millis(700),
            heartbeat_interval: Duration::from_millis(75),
            check_quorum_interval: Duration::from_millis(900),
            network_timeout: Duration::from_millis(250),
            request_timeout: Duration::from_secs(5),
            event_queue_capacity: 1_024,
            max_peer_connections: 256,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            snapshot_threshold_entries: 256,
            snapshot_threshold_bytes: 64 * 1024 * 1024,
            rng_seed: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if !self.peers.contains_key(&self.node_id) || self.peers.is_empty() {
            return Err(RuntimeError::Configuration(
                "peer map must contain the local voter".into(),
            ));
        }
        if self
            .peers
            .values()
            .any(|address| !address.ip().is_loopback())
        {
            return Err(RuntimeError::Configuration(
                "initial replicated runtime accepts localhost peer addresses only".into(),
            ));
        }
        if self.election_timeout_min.is_zero()
            || self.election_timeout_min > self.election_timeout_max
            || self.heartbeat_interval.is_zero()
            || self.check_quorum_interval <= self.heartbeat_interval
            || self.network_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.event_queue_capacity == 0
            || self.max_peer_connections == 0
            || self.max_frame_bytes == 0
            || self.snapshot_threshold_entries == 0
            || self.snapshot_threshold_bytes == 0
        {
            return Err(RuntimeError::Configuration(
                "timeouts and queue/frame bounds are invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeStatus {
    pub node_id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_log_index: u64,
    pub database_applied_index: u64,
    pub database_pages: u64,
    /// Current durable WAL file length. This is deliberately named as bytes,
    /// not an LSN, because the database status API reports physical length.
    pub wal_bytes: u64,
    pub active_transactions: u64,
    /// Latest database boundary compacted into a durable Raft snapshot.
    pub snapshot_index: Option<u64>,
    /// Logical database-snapshot payload size, excluding its durable sidecar
    /// framing and metadata.
    pub snapshot_bytes: Option<u64>,
    /// Entries retained strictly after the snapshot boundary (or from genesis
    /// when no snapshot exists).
    pub retained_log_entries: u64,
    /// JSON-encoded bytes occupied by the retained Raft entries.
    pub retained_log_bytes: u64,
}

#[derive(Clone)]
pub struct RuntimeHandle {
    node_id: NodeId,
    events: mpsc::Sender<Event>,
    request_timeout: Duration,
}

impl RuntimeHandle {
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn propose_sql(
        &self,
        client_id: [u8; 16],
        sequence: u64,
        sql: impl Into<String>,
        parameters: Vec<Value>,
    ) -> Result<ExecutionResult> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(Event::Propose {
            client_id,
            sequence,
            sql: sql.into(),
            parameters,
            response,
        })?;
        receive(receiver, self.request_timeout).await
    }

    pub async fn linearizable_query(
        &self,
        client_id: [u8; 16],
        sequence: u64,
        sql: impl Into<String>,
        parameters: Vec<Value>,
    ) -> Result<ExecutionResult> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(Event::Query {
            client_id,
            sequence,
            sql: sql.into(),
            parameters,
            response,
        })?;
        receive(receiver, self.request_timeout).await
    }

    pub async fn status(&self) -> Result<RuntimeStatus> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(Event::Status { response })?;
        receive(receiver, self.request_timeout).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (response, receiver) = oneshot::channel();
        self.enqueue(Event::Shutdown { response })?;
        receive(receiver, self.request_timeout).await
    }

    fn enqueue(&self, event: Event) -> Result<()> {
        self.events.try_send(event).map_err(|error| match error {
            TrySendError::Full(_) => {
                RuntimeError::Limit("replicated runtime event queue is full".into())
            }
            TrySendError::Closed(_) => RuntimeError::Shutdown,
        })
    }
}

pub struct RunningNode {
    pub handle: RuntimeHandle,
    task: JoinHandle<Result<()>>,
}

impl RunningNode {
    pub async fn shutdown(self) -> Result<()> {
        self.handle.shutdown().await?;
        self.task
            .await
            .map_err(|error| RuntimeError::Fenced(format!("runtime task failed: {error}")))?
    }

    pub fn abort(&self) {
        self.task.abort();
    }

    pub async fn crash(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

pub async fn start(config: RuntimeConfig) -> Result<RunningNode> {
    config.validate()?;
    let local_address = *config
        .peers
        .get(&config.node_id)
        .ok_or_else(|| RuntimeError::Configuration("local peer address is missing".into()))?;
    let listener = TcpListener::bind(local_address).await?;
    let database = Arc::new(Database::open(config.data_directory.join("database"))?);
    let mut storage = StableStore::open(config.data_directory.join("raft"))?;
    recover_pending_snapshot_install(&database, &mut storage)?;
    reconcile_database(&database, &storage)?;
    let configuration = Configuration::new(config.peers.keys().copied())
        .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
    let stable_state = storage.state();
    let next_command = next_command_counter(config.node_id, &stable_state)?;
    let raft = Raft::recover(config.node_id, configuration, stable_state)?;
    let (events, receiver) = mpsc::channel(config.event_queue_capacity);
    let handle = RuntimeHandle {
        node_id: config.node_id,
        events: events.clone(),
        request_timeout: config.request_timeout,
    };
    let mut runtime = Runtime::new(config, database, raft, storage, events, next_command);
    let task = tokio::spawn(async move { runtime.run(listener, receiver).await });
    Ok(RunningNode { handle, task })
}

fn recover_pending_snapshot_install(database: &Database, storage: &mut StableStore) -> Result<()> {
    let Some(pending) = storage.pending_install().cloned() else {
        return Ok(());
    };
    database.install_snapshot_at(
        pending.snapshot.metadata.last_included_index,
        &pending.snapshot.data,
    )?;
    storage.complete_snapshot_install()
}

async fn receive<T>(
    receiver: oneshot::Receiver<Result<T>>,
    request_timeout: Duration,
) -> Result<T> {
    timeout(request_timeout, receiver)
        .await
        .map_err(|_| RuntimeError::Timeout)?
        .map_err(|_| RuntimeError::Shutdown)?
}

fn reconcile_database(database: &Database, storage: &StableStore) -> Result<()> {
    let database_index = database.status()?.applied_index;
    let stable = storage.state();
    if database_index < stable.applied_index || database_index > stable.hard_state.commit_index {
        return Err(RuntimeError::Corruption(format!(
            "database applied index {database_index} is outside durable Raft applied/commit [{}, {}]",
            stable.applied_index, stable.hard_state.commit_index
        )));
    }
    if database_index > stable.applied_index.saturating_add(1) {
        return Err(RuntimeError::Corruption(format!(
            "database is more than one apply ahead of Raft marker: database {database_index}, marker {}",
            stable.applied_index
        )));
    }
    Ok(())
}

fn next_command_counter(node_id: NodeId, state: &StableState) -> Result<u64> {
    let maximum = state.entries.iter().filter_map(|entry| {
        let EntryPayload::Command { id, .. } = entry.payload else {
            return None;
        };
        let owner = u64::try_from(id >> 64).ok()?;
        (owner == node_id).then(|| u64::try_from(id & u128::from(u64::MAX)).ok())?
    });
    maximum
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| RuntimeError::Limit("proposal id space exhausted".into()))
}

enum Event {
    Peer {
        from: NodeId,
        message: Message,
    },
    Propose {
        client_id: [u8; 16],
        sequence: u64,
        sql: String,
        parameters: Vec<Value>,
        response: oneshot::Sender<Result<ExecutionResult>>,
    },
    Query {
        client_id: [u8; 16],
        sequence: u64,
        sql: String,
        parameters: Vec<Value>,
        response: oneshot::Sender<Result<ExecutionResult>>,
    },
    Status {
        response: oneshot::Sender<Result<RuntimeStatus>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<()>>,
    },
}

struct PendingProposal {
    id: CommandId,
    response: oneshot::Sender<Result<ExecutionResult>>,
    applied: Option<(u64, Result<ExecutionResult>)>,
}

struct PendingRead {
    client_id: [u8; 16],
    sequence: u64,
    sql: String,
    parameters: Vec<Value>,
    response: oneshot::Sender<Result<ExecutionResult>>,
}

struct Deadlines {
    election: Instant,
    heartbeat: Instant,
    check_quorum: Instant,
}

struct Runtime {
    config: RuntimeConfig,
    database: Arc<Database>,
    raft: Raft,
    storage: StableStore,
    events: mpsc::Sender<Event>,
    inbound_permits: Arc<Semaphore>,
    outbound_permits: Arc<Semaphore>,
    correlation: Arc<AtomicU64>,
    rng: SmallRng,
    deadlines: Deadlines,
    next_command: u64,
    next_read: u64,
    pending_proposal: Option<PendingProposal>,
    pending_reads: BTreeMap<Vec<u8>, PendingRead>,
}

impl Runtime {
    fn new(
        config: RuntimeConfig,
        database: Arc<Database>,
        raft: Raft,
        storage: StableStore,
        events: mpsc::Sender<Event>,
        next_command: u64,
    ) -> Self {
        let now = Instant::now();
        let max_peer_connections = config.max_peer_connections;
        let rng = match config.rng_seed {
            Some(seed) => SmallRng::seed_from_u64(seed),
            None => SmallRng::from_os_rng(),
        };
        let mut runtime = Self {
            config,
            database,
            raft,
            storage,
            events,
            inbound_permits: Arc::new(Semaphore::new(max_peer_connections)),
            outbound_permits: Arc::new(Semaphore::new(max_peer_connections)),
            correlation: Arc::new(AtomicU64::new(1)),
            rng,
            deadlines: Deadlines {
                election: now,
                heartbeat: now,
                check_quorum: now,
            },
            next_command,
            next_read: 1,
            pending_proposal: None,
            pending_reads: BTreeMap::new(),
        };
        runtime.reset_election();
        runtime.deadlines.heartbeat = now + runtime.config.heartbeat_interval;
        runtime.deadlines.check_quorum = now + runtime.config.check_quorum_interval;
        runtime
    }

    async fn run(
        &mut self,
        listener: TcpListener,
        mut receiver: mpsc::Receiver<Event>,
    ) -> Result<()> {
        // Reissue committed-but-not-marked state-machine applies before this
        // node can campaign or serve a client.
        let actions = self.raft.step(Input::Tick(Tick::Heartbeat));
        self.execute_actions(actions)?;
        self.maybe_snapshot()?;

        let mut ticker = interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                _ = ticker.tick() => self.on_timer()?,
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    self.accept_peer(stream);
                }
                event = receiver.recv() => {
                    let Some(event) = event else { return Ok(()); };
                    if self.on_event(event)? {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn on_timer(&mut self) -> Result<()> {
        let now = Instant::now();
        if self.raft.role() == Role::Leader {
            if now >= self.deadlines.heartbeat {
                self.deadlines.heartbeat = now + self.config.heartbeat_interval;
                let actions = self.raft.step(Input::Tick(Tick::Heartbeat));
                self.execute_actions(actions)?;
            }
            if now >= self.deadlines.check_quorum {
                self.deadlines.check_quorum = now + self.config.check_quorum_interval;
                let actions = self.raft.step(Input::Tick(Tick::CheckQuorum));
                self.execute_actions(actions)?;
            }
        } else if now >= self.deadlines.election {
            self.reset_election();
            let actions = self.raft.step(Input::Tick(Tick::Election));
            self.execute_actions(actions)?;
        }
        self.maybe_snapshot()
    }

    fn on_event(&mut self, event: Event) -> Result<bool> {
        match event {
            Event::Peer { from, message } => {
                let actions = self.raft.step(Input::Message { from, message });
                self.execute_actions(actions)?;
            }
            Event::Propose {
                client_id,
                sequence,
                sql,
                parameters,
                response,
            } => self.propose(client_id, sequence, &sql, &parameters, response)?,
            Event::Query {
                client_id,
                sequence,
                sql,
                parameters,
                response,
            } => self.read(client_id, sequence, sql, parameters, response)?,
            Event::Status { response } => {
                let _ = response.send(self.status());
            }
            Event::Shutdown { response } => {
                self.reject_pending(&RuntimeError::Shutdown);
                let _ = response.send(Ok(()));
                return Ok(true);
            }
        }
        self.maybe_snapshot()?;
        Ok(false)
    }

    fn propose(
        &mut self,
        client_id: [u8; 16],
        sequence: u64,
        sql: &str,
        parameters: &[Value],
        response: oneshot::Sender<Result<ExecutionResult>>,
    ) -> Result<()> {
        if self.raft.role() != Role::Leader {
            let _ = response.send(Err(RuntimeError::NotLeader {
                leader_hint: self.raft.leader_id(),
            }));
            return Ok(());
        }
        if self.pending_proposal.is_some() {
            let _ = response.send(Err(RuntimeError::ProposalBusy));
            return Ok(());
        }
        let preparation = self.database.prepare_replicated_mutation(
            self.raft.term(),
            client_id,
            sequence,
            sql,
            parameters,
        );
        let preparation = match preparation {
            Ok(preparation) => preparation,
            Err(error) => {
                let _ = response.send(Err(RuntimeError::Database(error)));
                return Ok(());
            }
        };
        let MutationPreparation::Propose(command) = preparation else {
            let MutationPreparation::Replay(result) = preparation else {
                unreachable!();
            };
            let _ = response.send(Ok(result));
            return Ok(());
        };
        let id = (u128::from(self.config.node_id) << 64) | u128::from(self.next_command);
        self.next_command = self
            .next_command
            .checked_add(1)
            .ok_or_else(|| RuntimeError::Limit("proposal id space exhausted".into()))?;
        self.pending_proposal = Some(PendingProposal {
            id,
            response,
            applied: None,
        });
        let actions = self.raft.step(Input::Propose {
            id,
            command: command.into_bytes(),
        });
        self.execute_actions(actions)
    }

    fn read(
        &mut self,
        client_id: [u8; 16],
        sequence: u64,
        sql: String,
        parameters: Vec<Value>,
        response: oneshot::Sender<Result<ExecutionResult>>,
    ) -> Result<()> {
        if self.raft.role() != Role::Leader {
            let _ = response.send(Err(RuntimeError::NotLeader {
                leader_hint: self.raft.leader_id(),
            }));
            return Ok(());
        }
        if self.pending_reads.len() >= self.config.event_queue_capacity {
            let _ = response.send(Err(RuntimeError::Limit(
                "pending ReadIndex request limit reached".into(),
            )));
            return Ok(());
        }
        let mut context = Vec::with_capacity(16);
        context.extend_from_slice(&self.config.node_id.to_be_bytes());
        context.extend_from_slice(&self.next_read.to_be_bytes());
        self.next_read = self
            .next_read
            .checked_add(1)
            .ok_or_else(|| RuntimeError::Limit("read context space exhausted".into()))?;
        self.pending_reads.insert(
            context.clone(),
            PendingRead {
                client_id,
                sequence,
                sql,
                parameters,
                response,
            },
        );
        let actions = self.raft.step(Input::ReadIndex { context });
        self.execute_actions(actions)
    }

    fn execute_actions(&mut self, actions: Vec<Action>) -> Result<()> {
        for action in actions {
            match action {
                Action::Persist(StorageMutation::InstallSnapshot {
                    snapshot,
                    retained_entries,
                    hard_state,
                }) => self.install_received_snapshot(&snapshot, &retained_entries, hard_state)?,
                Action::Persist(mutation) => self.storage.persist(&mutation)?,
                Action::Send { to, message } => self.send_peer(to, &message)?,
                Action::ResetElectionTimer => self.reset_election(),
                Action::ResetHeartbeatTimer => {
                    self.deadlines.heartbeat = Instant::now() + self.config.heartbeat_interval;
                }
                Action::RoleChanged { role, .. } => {
                    if role == Role::Leader {
                        self.deadlines.check_quorum =
                            Instant::now() + self.config.check_quorum_interval;
                    } else {
                        self.reset_election();
                        self.reject_pending(&RuntimeError::NotLeader {
                            leader_hint: self.raft.leader_id(),
                        });
                    }
                }
                Action::Apply(entry) => self.apply_entry(&entry)?,
                Action::ProposalCommitted { id, index } => {
                    if self
                        .pending_proposal
                        .as_ref()
                        .is_none_or(|pending| pending.id != id)
                    {
                        // A newly elected leader can finish a command proposed
                        // before its process lifetime; no local waiter exists.
                        continue;
                    }
                    let pending = self.pending_proposal.take().ok_or_else(|| {
                        RuntimeError::Fenced("matching proposal waiter disappeared".into())
                    })?;
                    let (applied_index, result) = pending.applied.ok_or_else(|| {
                        RuntimeError::Fenced(format!(
                            "proposal {id} acknowledged before local database apply"
                        ))
                    })?;
                    if applied_index != index {
                        return Err(RuntimeError::Fenced(format!(
                            "proposal {id} applied at {applied_index}, committed at {index}"
                        )));
                    }
                    let _ = pending.response.send(result);
                }
                Action::ProposalRejected { id, leader_hint } => {
                    if self
                        .pending_proposal
                        .as_ref()
                        .is_some_and(|pending| pending.id == id)
                    {
                        let pending = self.pending_proposal.take().ok_or_else(|| {
                            RuntimeError::Fenced("matching proposal waiter disappeared".into())
                        })?;
                        let _ = pending
                            .response
                            .send(Err(RuntimeError::NotLeader { leader_hint }));
                    }
                }
                Action::ReadReady { context, index } => {
                    if let Some(pending) = self.pending_reads.remove(&context) {
                        if self.database.status()?.applied_index < index {
                            return Err(RuntimeError::Fenced(format!(
                                "ReadIndex {index} became ready before durable database apply"
                            )));
                        }
                        let result = self
                            .database
                            .execute_read_only(
                                pending.client_id,
                                pending.sequence,
                                &pending.sql,
                                &pending.parameters,
                            )
                            .map_err(RuntimeError::Database);
                        let _ = pending.response.send(result);
                    }
                }
                Action::ReadRejected { context, .. } => {
                    if let Some(pending) = self.pending_reads.remove(&context) {
                        let _ = pending.response.send(Err(RuntimeError::NotLeader {
                            leader_hint: self.raft.leader_id(),
                        }));
                    }
                }
                Action::SnapshotRejected { reason } => {
                    return Err(RuntimeError::Fenced(format!(
                        "locally built snapshot was rejected: {reason}"
                    )));
                }
                Action::Fatal { reason } => return Err(RuntimeError::Fenced(reason)),
            }
        }
        Ok(())
    }

    fn install_received_snapshot(
        &mut self,
        snapshot: &PersistentSnapshot,
        retained_entries: &[aster_raft::LogEntry],
        hard_state: aster_raft::HardState,
    ) -> Result<()> {
        self.storage
            .begin_snapshot_install(snapshot, retained_entries, hard_state)?;
        self.database
            .install_snapshot_at(snapshot.metadata.last_included_index, &snapshot.data)?;
        self.storage.complete_snapshot_install()
    }

    fn maybe_snapshot(&mut self) -> Result<()> {
        if self.storage.pending_install().is_some() {
            return Err(RuntimeError::Fenced(
                "snapshot install intent remained pending in the live runtime".into(),
            ));
        }
        let retained_entries = self.storage.retained_entry_count();
        let retained_bytes = self.storage.retained_entry_bytes();
        if retained_entries < self.config.snapshot_threshold_entries
            && retained_bytes < self.config.snapshot_threshold_bytes
        {
            return Ok(());
        }

        let applied_index = self.raft.applied_index();
        if applied_index == 0
            || self
                .storage
                .snapshot_index()
                .is_some_and(|index| index >= applied_index)
        {
            return Ok(());
        }
        let database_index = self.database.status()?.applied_index;
        if database_index != applied_index {
            return Err(RuntimeError::Fenced(format!(
                "cannot snapshot Raft index {applied_index} while database is at {database_index}"
            )));
        }
        let included_term = self.storage.term_at(applied_index).ok_or_else(|| {
            RuntimeError::Corruption(format!(
                "durable Raft log has no term at snapshot boundary {applied_index}"
            ))
        })?;
        let database_snapshot = self.database.create_snapshot()?;
        if database_snapshot.applied_index() != applied_index {
            return Err(RuntimeError::Fenced(format!(
                "database snapshot boundary {} changed while Raft expected {applied_index}",
                database_snapshot.applied_index()
            )));
        }
        let configuration = Configuration::new(self.config.peers.keys().copied())
            .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
        let snapshot = PersistentSnapshot::new(
            applied_index,
            included_term,
            configuration,
            database_snapshot.into_bytes(),
        );
        let actions = self.raft.step(Input::SnapshotBuilt(snapshot));
        self.execute_actions(actions)
    }

    fn apply_entry(&mut self, entry: &aster_raft::LogEntry) -> Result<()> {
        let command_result = match &entry.payload {
            EntryPayload::Noop => {
                self.database.apply_raft_noop(entry.index, entry.term)?;
                None
            }
            EntryPayload::Command { id, bytes } => {
                let result = match self.database.apply_replicated(entry.index, bytes) {
                    Ok(result) => Ok(result),
                    Err(error) => {
                        if self.database.status()?.applied_index == entry.index {
                            Err(RuntimeError::CommandRejected(error.to_string()))
                        } else {
                            return Err(RuntimeError::Database(error));
                        }
                    }
                };
                Some((*id, result))
            }
        };
        self.storage.mark_applied(entry.index)?;
        if let Some((id, result)) = command_result
            && let Some(pending) = self.pending_proposal.as_mut()
            && pending.id == id
        {
            pending.applied = Some((entry.index, result));
        }
        Ok(())
    }

    fn send_peer(&mut self, to: NodeId, message: &Message) -> Result<()> {
        let Some(address) = self.config.peers.get(&to).copied() else {
            return Err(RuntimeError::Fenced(format!(
                "Raft attempted to send to unknown voter {to}"
            )));
        };
        let payload = serde_json::to_vec(&message)
            .map_err(|error| RuntimeError::Corruption(format!("encode peer message: {error}")))?;
        if payload.len() > self.config.max_frame_bytes {
            return Err(RuntimeError::Limit(format!(
                "peer message is {} bytes; frame maximum is {}",
                payload.len(),
                self.config.max_frame_bytes
            )));
        }
        let envelope = PeerEnvelope {
            from: self.config.node_id,
            to,
            term: self.raft.term(),
            correlation_id: self.correlation.fetch_add(1, Ordering::Relaxed),
            payload,
        };
        let permits = Arc::clone(&self.outbound_permits);
        let network_timeout = self.config.network_timeout;
        let max_frame_bytes = self.config.max_frame_bytes;
        tokio::spawn(async move {
            let Ok(permit) = permits.try_acquire_owned() else {
                return;
            };
            let _permit = permit;
            let sent = async {
                let mut stream = TcpStream::connect(address).await?;
                write_message(&mut stream, &WireMessage::Peer(envelope), max_frame_bytes)
                    .await
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
            };
            let _ = timeout(network_timeout, sent).await;
        });
        Ok(())
    }

    fn accept_peer(&self, mut stream: TcpStream) {
        let permits = Arc::clone(&self.inbound_permits);
        let events = self.events.clone();
        let peers = self.config.peers.clone();
        let node_id = self.config.node_id;
        let network_timeout = self.config.network_timeout;
        let max_frame_bytes = self.config.max_frame_bytes;
        tokio::spawn(async move {
            let Ok(permit) = permits.try_acquire_owned() else {
                return;
            };
            let _permit = permit;
            let Ok(Ok(Some(WireMessage::Peer(envelope)))) =
                timeout(network_timeout, read_message(&mut stream, max_frame_bytes)).await
            else {
                return;
            };
            if envelope.to != node_id
                || envelope.from == node_id
                || !peers.contains_key(&envelope.from)
                || envelope.payload.len() > max_frame_bytes
            {
                return;
            }
            let Ok(message) = serde_json::from_slice::<Message>(&envelope.payload) else {
                return;
            };
            let _ = events.try_send(Event::Peer {
                from: envelope.from,
                message,
            });
        });
    }

    fn status(&self) -> Result<RuntimeStatus> {
        let database = self.database.status()?;
        let retained_log_entries = u64::try_from(self.storage.retained_entry_count())
            .map_err(|_| RuntimeError::Limit("retained Raft entry count exceeds u64".into()))?;
        let retained_log_bytes = u64::try_from(self.storage.retained_entry_bytes())
            .map_err(|_| RuntimeError::Limit("retained Raft log size exceeds u64".into()))?;
        Ok(RuntimeStatus {
            node_id: self.config.node_id,
            role: self.raft.role(),
            term: self.raft.term(),
            leader_id: self.raft.leader_id(),
            commit_index: self.raft.commit_index(),
            applied_index: self.raft.applied_index(),
            last_log_index: self.raft.last_log_index(),
            database_applied_index: database.applied_index,
            database_pages: database.database_pages,
            wal_bytes: database.wal_bytes,
            active_transactions: database.active_transactions,
            snapshot_index: self.storage.snapshot_index(),
            snapshot_bytes: self.storage.snapshot_bytes(),
            retained_log_entries,
            retained_log_bytes,
        })
    }

    fn reset_election(&mut self) {
        let min = self.config.election_timeout_min.as_millis();
        let max = self.config.election_timeout_max.as_millis();
        let sampled = self.rng.random_range(min..=max);
        let millis = u64::try_from(sampled).unwrap_or(u64::MAX);
        self.deadlines.election = Instant::now() + Duration::from_millis(millis);
    }

    fn reject_pending(&mut self, error: &RuntimeError) {
        if let Some(pending) = self.pending_proposal.take() {
            let _ = pending
                .response
                .send(Err(pending_error(error, self.raft.leader_id())));
        }
        for (_, pending) in std::mem::take(&mut self.pending_reads) {
            let _ = pending
                .response
                .send(Err(pending_error(error, self.raft.leader_id())));
        }
    }
}

fn pending_error(error: &RuntimeError, default_leader: Option<NodeId>) -> RuntimeError {
    match error {
        RuntimeError::Shutdown => RuntimeError::Shutdown,
        RuntimeError::NotLeader { leader_hint } => RuntimeError::NotLeader {
            leader_hint: *leader_hint,
        },
        other => RuntimeError::Fenced(format!(
            "pending request cancelled: {other}; leader hint {default_leader:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::TcpListener as StdTcpListener;

    use aster_core::{Row, Value};
    use aster_db::{Database, ExecutionResult};
    use tempfile::TempDir;
    use tokio::time::sleep;

    use super::*;

    const CLIENT: [u8; 16] = [0xA5; 16];
    const ELECTION_SEED_BASE: u64 = 0xA57E_0000;

    #[tokio::test]
    async fn saturated_event_queue_fails_without_blocking() {
        let (events, _receiver) = mpsc::channel(1);
        let (response, _) = oneshot::channel();
        assert!(events.try_send(Event::Status { response }).is_ok());
        let handle = RuntimeHandle {
            node_id: 1,
            events,
            request_timeout: Duration::from_secs(30),
        };
        assert!(matches!(handle.status().await, Err(RuntimeError::Limit(_))));
    }

    #[test]
    fn proposal_counter_continues_after_restart() {
        let state = StableState {
            entries: vec![aster_raft::LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Command {
                    id: (u128::from(7_u64) << 64) | u128::from(41_u64),
                    bytes: Vec::new(),
                },
            }],
            ..StableState::default()
        };
        assert_eq!(next_command_counter(7, &state).unwrap(), 42);
        assert_eq!(next_command_counter(8, &state).unwrap(), 1);
    }

    #[test]
    fn startup_completes_snapshot_install_before_and_after_database_publication() {
        let source_directory = TempDir::new().unwrap();
        let source = Database::open(source_directory.path()).unwrap();
        source.apply_raft_noop(1, 1).unwrap();
        let database_snapshot = source.create_snapshot().unwrap();
        let configuration = Configuration::new([1, 2, 3]).unwrap();
        let snapshot = PersistentSnapshot::new(1, 1, configuration, database_snapshot.into_bytes());
        let hard_state = aster_raft::HardState {
            current_term: 1,
            voted_for: None,
            commit_index: 1,
        };

        for database_was_published in [false, true] {
            let target_directory = TempDir::new().unwrap();
            let database = Database::open(target_directory.path().join("database")).unwrap();
            {
                let mut storage = StableStore::open(target_directory.path().join("raft")).unwrap();
                storage
                    .begin_snapshot_install(&snapshot, &[], hard_state)
                    .unwrap();
            }
            if database_was_published {
                database.install_snapshot_at(1, &snapshot.data).unwrap();
            }

            let mut reopened = StableStore::open(target_directory.path().join("raft")).unwrap();
            recover_pending_snapshot_install(&database, &mut reopened).unwrap();
            assert!(reopened.pending_install().is_none());
            assert_eq!(reopened.state().applied_index, 1);
            assert_eq!(reopened.snapshot_index(), Some(1));
            assert_eq!(database.status().unwrap().applied_index, 1);
        }
    }

    fn reserve_addresses(count: usize) -> Vec<SocketAddr> {
        let listeners = (0..count)
            .map(|_| StdTcpListener::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        let addresses = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap())
            .collect();
        drop(listeners);
        addresses
    }

    fn configs(root: &TempDir) -> BTreeMap<NodeId, RuntimeConfig> {
        let addresses = reserve_addresses(3);
        let peers = (1_u64..=3).zip(addresses).collect::<BTreeMap<_, _>>();
        peers
            .keys()
            .copied()
            .map(|node_id| {
                let mut config = RuntimeConfig::localhost(
                    node_id,
                    peers.clone(),
                    root.path().join(format!("node-{node_id}")),
                );
                config.election_timeout_min = Duration::from_millis(120);
                config.election_timeout_max = Duration::from_millis(280);
                config.heartbeat_interval = Duration::from_millis(30);
                config.check_quorum_interval = Duration::from_millis(350);
                config.network_timeout = Duration::from_millis(100);
                config.request_timeout = Duration::from_secs(3);
                config.rng_seed = Some(ELECTION_SEED_BASE + node_id);
                (node_id, config)
            })
            .collect()
    }

    async fn wait_for_leader(
        handles: &BTreeMap<NodeId, RuntimeHandle>,
        allowed: &[NodeId],
    ) -> NodeId {
        for _ in 0..120 {
            let mut leaders = Vec::new();
            for node_id in allowed {
                if let Some(handle) = handles.get(node_id)
                    && let Ok(status) = handle.status().await
                    && status.role == Role::Leader
                {
                    leaders.push(*node_id);
                }
            }
            if leaders.len() == 1 {
                return leaders[0];
            }
            sleep(Duration::from_millis(50)).await;
        }
        panic!("cluster did not elect exactly one leader");
    }

    async fn wait_applied(
        handles: &BTreeMap<NodeId, RuntimeHandle>,
        allowed: &[NodeId],
        index: u64,
    ) {
        for _ in 0..120 {
            let mut ready = true;
            for node_id in allowed {
                let status = handles[node_id].status().await;
                if !status.is_ok_and(|status| status.database_applied_index >= index) {
                    ready = false;
                    break;
                }
            }
            if ready {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
        panic!("nodes did not apply through index {index}");
    }

    async fn wait_snapshot(
        handles: &BTreeMap<NodeId, RuntimeHandle>,
        allowed: &[NodeId],
        minimum_index: u64,
    ) {
        for _ in 0..160 {
            let mut ready = true;
            for node_id in allowed {
                let status = handles[node_id].status().await;
                if !status.is_ok_and(|status| {
                    status
                        .snapshot_index
                        .is_some_and(|index| index >= minimum_index)
                }) {
                    ready = false;
                    break;
                }
            }
            if ready {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
        panic!("nodes did not compact through snapshot index {minimum_index}");
    }

    async fn propose_until_success(
        handles: &BTreeMap<NodeId, RuntimeHandle>,
        allowed: &[NodeId],
        sequence: u64,
        sql: &str,
    ) -> ExecutionResult {
        let mut last_error = String::new();
        for _ in 0..12 {
            let leader = wait_for_leader(handles, allowed).await;
            match handles[&leader]
                .propose_sql(CLIENT, sequence, sql, Vec::new())
                .await
            {
                Ok(result) => return result,
                Err(error) => {
                    last_error = error.to_string();
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!("replicated proposal did not succeed: {last_error}");
    }

    async fn query_until_success(
        handles: &BTreeMap<NodeId, RuntimeHandle>,
        allowed: &[NodeId],
        client_id: [u8; 16],
        sequence: u64,
        sql: &str,
    ) -> ExecutionResult {
        let mut last_error = String::new();
        for _ in 0..12 {
            let leader = wait_for_leader(handles, allowed).await;
            match handles[&leader]
                .linearizable_query(client_id, sequence, sql, Vec::new())
                .await
            {
                Ok(result) => return result,
                Err(error) => {
                    last_error = error.to_string();
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        panic!("linearizable query did not succeed: {last_error}");
    }

    fn applied_index(result: &ExecutionResult) -> u64 {
        match result {
            ExecutionResult::Query(result) => result.applied_index,
            other => panic!("expected mutation result, got {other:?}"),
        }
    }

    // This is intentionally one narrative fault scenario: breaking it into
    // fixtures would obscure the exact kill/restart/minority/heal sequence.
    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn real_tcp_cluster_failover_restart_minority_and_convergence() {
        let root = TempDir::new().unwrap();
        let configs = configs(&root);
        let mut nodes = BTreeMap::new();
        let mut handles = BTreeMap::new();
        for (node_id, config) in &configs {
            let node = start(config.clone()).await.unwrap();
            handles.insert(*node_id, node.handle.clone());
            nodes.insert(*node_id, node);
        }

        let all = [1, 2, 3];
        let created = propose_until_success(
            &handles,
            &all,
            1,
            "CREATE TABLE events (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
        )
        .await;
        let created_index = applied_index(&created);
        let inserted = propose_until_success(
            &handles,
            &all,
            2,
            "INSERT INTO events VALUES (1, 'before-failover')",
        )
        .await;
        let first_index = applied_index(&inserted);
        assert!(first_index > created_index);
        wait_applied(&handles, &all, first_index).await;

        let leader = wait_for_leader(&handles, &all).await;
        nodes.remove(&leader).unwrap().crash().await;
        handles.remove(&leader);
        let survivors = all
            .into_iter()
            .filter(|node_id| *node_id != leader)
            .collect::<Vec<_>>();
        let inserted = propose_until_success(
            &handles,
            &survivors,
            3,
            "INSERT INTO events VALUES (2, 'after-failover')",
        )
        .await;
        let second_index = applied_index(&inserted);

        let restarted = start(configs[&leader].clone()).await.unwrap();
        handles.insert(leader, restarted.handle.clone());
        nodes.insert(leader, restarted);
        wait_applied(&handles, &[1, 2, 3], second_index).await;

        let current_leader = wait_for_leader(&handles, &[1, 2, 3]).await;
        let isolated_followers = [1, 2, 3]
            .into_iter()
            .filter(|node_id| *node_id != current_leader)
            .collect::<Vec<_>>();
        for node_id in &isolated_followers {
            nodes.remove(node_id).unwrap().crash().await;
            handles.remove(node_id);
        }
        let minority = handles[&current_leader]
            .propose_sql(
                CLIENT,
                4,
                "INSERT INTO events VALUES (3, 'after-heal')",
                Vec::new(),
            )
            .await;
        assert!(minority.is_err(), "minority leader acknowledged a write");

        for node_id in &isolated_followers {
            let restarted = start(configs[node_id].clone()).await.unwrap();
            handles.insert(*node_id, restarted.handle.clone());
            nodes.insert(*node_id, restarted);
        }
        let healed = propose_until_success(
            &handles,
            &[1, 2, 3],
            4,
            "INSERT INTO events VALUES (3, 'after-heal')",
        )
        .await;
        let healed_index = applied_index(&healed);
        wait_applied(&handles, &[1, 2, 3], healed_index).await;

        for (_, node) in nodes {
            node.shutdown().await.unwrap();
        }
        for node_id in 1..=3 {
            let database =
                Database::open(configs[&node_id].data_directory.join("database")).unwrap();
            let result = database
                .execute_read_only(CLIENT, 99, "SELECT id, value FROM events ORDER BY id", &[])
                .unwrap();
            let ExecutionResult::Query(result) = result else {
                panic!("expected query result");
            };
            assert_eq!(
                result.rows,
                vec![
                    Row {
                        values: vec![Value::Int64(1), Value::Text("before-failover".into()),],
                    },
                    Row {
                        values: vec![Value::Int64(2), Value::Text("after-failover".into()),],
                    },
                    Row {
                        values: vec![Value::Int64(3), Value::Text("after-heal".into()),],
                    },
                ]
            );
        }
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn real_tcp_snapshot_catches_up_lagging_follower_and_survives_restart() {
        let root = TempDir::new().unwrap();
        let mut configs = configs(&root);
        for config in configs.values_mut() {
            config.snapshot_threshold_entries = 4;
            config.snapshot_threshold_bytes = 1024 * 1024;
        }
        let mut nodes = BTreeMap::new();
        let mut handles = BTreeMap::new();
        for (node_id, config) in &configs {
            let node = start(config.clone()).await.unwrap();
            handles.insert(*node_id, node.handle.clone());
            nodes.insert(*node_id, node);
        }

        let all = [1, 2, 3];
        let created = propose_until_success(
            &handles,
            &all,
            1,
            "CREATE TABLE snapshots (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
        )
        .await;
        let created_index = applied_index(&created);
        wait_applied(&handles, &all, created_index).await;

        let leader = wait_for_leader(&handles, &all).await;
        let lagging = all.into_iter().find(|node_id| *node_id != leader).unwrap();
        nodes.remove(&lagging).unwrap().crash().await;
        handles.remove(&lagging);
        let survivors = all
            .into_iter()
            .filter(|node_id| *node_id != lagging)
            .collect::<Vec<_>>();

        let mut final_index = created_index;
        for row_id in 1_u64..=16 {
            let value = format!("value-{row_id}-{}", "x".repeat(2_048));
            let sql = format!("INSERT INTO snapshots VALUES ({row_id}, '{value}')");
            let inserted = propose_until_success(&handles, &survivors, row_id + 1, &sql).await;
            final_index = applied_index(&inserted);
        }
        wait_applied(&handles, &survivors, final_index).await;
        let compacted_through = final_index.saturating_sub(3);
        wait_snapshot(&handles, &survivors, compacted_through).await;
        for node_id in &survivors {
            let status = handles[node_id].status().await.unwrap();
            assert!(status.snapshot_index.unwrap() > created_index);
            assert!(status.snapshot_bytes.unwrap() > 16 * 1024);
            assert!(status.retained_log_entries < 4);
        }

        let restarted = start(configs[&lagging].clone()).await.unwrap();
        handles.insert(lagging, restarted.handle.clone());
        nodes.insert(lagging, restarted);
        wait_applied(&handles, &all, final_index).await;
        wait_snapshot(&handles, &[lagging], compacted_through).await;
        let installed_status = handles[&lagging].status().await.unwrap();
        let installed_boundary = installed_status
            .snapshot_index
            .expect("caught-up follower must publish a snapshot boundary");
        assert!(installed_boundary >= compacted_through);
        assert!(installed_status.snapshot_bytes.unwrap() > 16 * 1024);

        nodes.remove(&lagging).unwrap().crash().await;
        handles.remove(&lagging);
        let restarted = start(configs[&lagging].clone()).await.unwrap();
        handles.insert(lagging, restarted.handle.clone());
        nodes.insert(lagging, restarted);
        wait_applied(&handles, &all, final_index).await;
        let restarted_boundary = handles[&lagging].status().await.unwrap().snapshot_index;
        assert!(
            restarted_boundary.is_some_and(|index| index >= installed_boundary),
            "restart must preserve the installed boundary, but may compact farther"
        );

        let query = query_until_success(
            &handles,
            &all,
            [0xB4; 16],
            1,
            "SELECT id, value FROM snapshots ORDER BY id",
        )
        .await;
        let ExecutionResult::Query(query) = query else {
            panic!("expected query result");
        };
        assert_eq!(query.rows.len(), 16);

        let inserted = propose_until_success(
            &handles,
            &all,
            18,
            "INSERT INTO snapshots VALUES (17, 'after-snapshot-restart')",
        )
        .await;
        let suffix_index = applied_index(&inserted);
        wait_applied(&handles, &all, suffix_index).await;

        for (_, node) in nodes {
            node.shutdown().await.unwrap();
        }
        for node_id in all {
            let database =
                Database::open(configs[&node_id].data_directory.join("database")).unwrap();
            let result = database
                .execute_read_only([0xB5; 16], 1, "SELECT id FROM snapshots ORDER BY id", &[])
                .unwrap();
            let ExecutionResult::Query(result) = result else {
                panic!("expected query result");
            };
            assert_eq!(result.rows.len(), 17);
        }
    }
}

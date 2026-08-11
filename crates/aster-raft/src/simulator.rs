//! Seeded deterministic fault simulator for the Raft core.
//!
//! The simulator models durable disks separately from live nodes. Crashing a
//! node discards all volatile protocol state, and restart reconstructs it only
//! from actions that reached the modeled persistence barrier. The transport can
//! delay, drop, duplicate, reorder, and hold packets behind directional
//! partitions. Every transition is followed by executable safety invariants.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    Action, CommandId, Configuration, EntryPayload, HardState, Input, LogEntry, Message, NodeId,
    PersistentSnapshot, Raft, Role, StableState, StorageMutation, Tick,
};

const ELECTION_MIN_TICKS: u64 = 9;
const ELECTION_MAX_TICKS: u64 = 18;
const HEARTBEAT_TICKS: u64 = 2;
const CHECK_QUORUM_TICKS: u64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultProfile {
    /// Independent probability per packet in thousandths.
    pub drop_per_mille: u16,
    /// Independent probability per packet in thousandths.
    pub duplicate_per_mille: u16,
    /// Delivery delay is sampled uniformly from `0..=max_delay_ticks`.
    pub max_delay_ticks: u64,
    /// Protects a diagnostic run from unbounded retained partition traffic.
    pub max_queued_messages: usize,
    /// Payload-aware bound; packet count alone does not constrain large
    /// `AppendEntries` or snapshot chunks.
    pub max_queued_bytes: usize,
}

impl FaultProfile {
    #[must_use]
    pub const fn reliable() -> Self {
        Self {
            drop_per_mille: 0,
            duplicate_per_mille: 0,
            max_delay_ticks: 0,
            max_queued_messages: 100_000,
            max_queued_bytes: 64 * 1024 * 1024,
        }
    }

    #[must_use]
    pub const fn hostile() -> Self {
        Self {
            drop_per_mille: 120,
            duplicate_per_mille: 150,
            max_delay_ticks: 8,
            max_queued_messages: 4_096,
            max_queued_bytes: 8 * 1024 * 1024,
        }
    }
}

impl Default for FaultProfile {
    fn default() -> Self {
        Self::reliable()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimEvent {
    Leader {
        node: NodeId,
        term: u64,
    },
    Applied {
        node: NodeId,
        index: u64,
    },
    Committed {
        node: NodeId,
        id: CommandId,
        index: u64,
    },
    ReadReady {
        node: NodeId,
        context: Vec<u8>,
        index: u64,
    },
    Fatal {
        node: NodeId,
        reason: String,
    },
}

#[derive(Debug, Clone)]
struct Envelope {
    sequence: u64,
    from: NodeId,
    to: NodeId,
    deliver_at: u64,
    message: Message,
    byte_cost: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntryIdentity {
    term: u64,
    payload_hash: [u8; 32],
}

impl EntryIdentity {
    fn of(entry: &LogEntry) -> Self {
        let mut hash = Sha256::new();
        match &entry.payload {
            EntryPayload::Noop => hash.update([0]),
            EntryPayload::Command { id, bytes } => {
                hash.update([1]);
                hash.update(id.to_le_bytes());
                hash.update((bytes.len() as u64).to_le_bytes());
                hash.update(bytes);
            }
        }
        Self {
            term: entry.term,
            payload_hash: hash.finalize().into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SimDisk {
    stable: StableState,
    state_bytes: Vec<u8>,
    applied: BTreeMap<u64, EntryIdentity>,
}

impl SimDisk {
    fn base_index(&self) -> u64 {
        self.stable
            .snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.metadata.last_included_index)
    }

    fn base_term(&self) -> u64 {
        self.stable
            .snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.metadata.last_included_term)
    }

    fn last_index(&self) -> u64 {
        self.stable
            .entries
            .last()
            .map_or_else(|| self.base_index(), |entry| entry.index)
    }

    fn entry(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.base_index() {
            return None;
        }
        let offset = usize::try_from(index - self.base_index() - 1).ok()?;
        self.stable.entries.get(offset)
    }

    fn known_identity(&self, index: u64) -> Option<EntryIdentity> {
        self.entry(index).map(EntryIdentity::of).or_else(|| {
            (index == self.base_index() && index != 0).then(|| EntryIdentity {
                term: self.base_term(),
                // Snapshot metadata deliberately commits only the boundary
                // term; payload identity before compaction is unknown.
                payload_hash: [0; 32],
            })
        })
    }

    fn state_hash(&self) -> [u8; 32] {
        Sha256::digest(&self.state_bytes).into()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SimError {
    #[error("invalid simulator configuration: {0}")]
    Configuration(String),
    #[error("seed {seed}, step {step}: {message}")]
    Invariant {
        seed: u64,
        step: u64,
        message: String,
    },
    #[error("node {0} is unavailable")]
    NodeUnavailable(NodeId),
    #[error("node {node} could not recover: {message}")]
    Recovery { node: NodeId, message: String },
    #[error("seed {seed} exhausted deterministic step budget {budget}")]
    StepBudget { seed: u64, budget: u64 },
}

/// A fully replayable deterministic cluster.
#[derive(Debug)]
pub struct Simulator {
    seed: u64,
    rng: SmallRng,
    configuration: Configuration,
    profile: FaultProfile,
    now: u64,
    step_number: u64,
    next_sequence: u64,
    step_budget: u64,
    queued_bytes: usize,
    nodes: BTreeMap<NodeId, Raft>,
    disks: BTreeMap<NodeId, SimDisk>,
    election_deadline: BTreeMap<NodeId, u64>,
    heartbeat_deadline: BTreeMap<NodeId, u64>,
    quorum_deadline: BTreeMap<NodeId, u64>,
    blocked: BTreeSet<(NodeId, NodeId)>,
    messages: Vec<Envelope>,
    leaders_by_term: BTreeMap<u64, NodeId>,
    votes_by_term: BTreeMap<(NodeId, u64), NodeId>,
    committed: BTreeMap<u64, EntryIdentity>,
    captured_commit: BTreeMap<NodeId, u64>,
    events: Vec<SimEvent>,
    trace: VecDeque<String>,
}

impl Simulator {
    pub fn new(
        seed: u64,
        voters: impl IntoIterator<Item = NodeId>,
        profile: FaultProfile,
    ) -> Result<Self, SimError> {
        let configuration = Configuration::new(voters)
            .map_err(|error| SimError::Configuration(error.to_string()))?;
        Self::with_configuration(seed, configuration, profile)
    }

    pub fn with_configuration(
        seed: u64,
        configuration: Configuration,
        profile: FaultProfile,
    ) -> Result<Self, SimError> {
        let mut simulator = Self {
            seed,
            rng: SmallRng::seed_from_u64(seed),
            configuration,
            profile,
            now: 0,
            step_number: 0,
            next_sequence: 0,
            step_budget: 100_000,
            queued_bytes: 0,
            nodes: BTreeMap::new(),
            disks: BTreeMap::new(),
            election_deadline: BTreeMap::new(),
            heartbeat_deadline: BTreeMap::new(),
            quorum_deadline: BTreeMap::new(),
            blocked: BTreeSet::new(),
            messages: Vec::new(),
            leaders_by_term: BTreeMap::new(),
            votes_by_term: BTreeMap::new(),
            committed: BTreeMap::new(),
            captured_commit: BTreeMap::new(),
            events: Vec::new(),
            trace: VecDeque::new(),
        };
        let ids = simulator
            .configuration
            .voters()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for id in ids {
            simulator.disks.insert(id, SimDisk::default());
            simulator.captured_commit.insert(id, 0);
            simulator.restart(id)?;
        }
        simulator.assert_invariants()?;
        Ok(simulator)
    }

    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    #[must_use]
    pub const fn now(&self) -> u64 {
        self.now
    }

    #[must_use]
    pub fn events(&self) -> &[SimEvent] {
        &self.events
    }

    pub fn take_events(&mut self) -> Vec<SimEvent> {
        std::mem::take(&mut self.events)
    }

    /// Sets a hard deterministic transition budget. Exhaustion is an error,
    /// which keeps CI bounded even if a liveness regression generates traffic
    /// forever at the application layer.
    pub const fn set_step_budget(&mut self, budget: u64) {
        self.step_budget = budget;
    }

    #[must_use]
    pub fn trace(&self) -> &VecDeque<String> {
        &self.trace
    }

    #[must_use]
    pub fn live_nodes(&self) -> BTreeSet<NodeId> {
        self.nodes.keys().copied().collect()
    }

    #[must_use]
    pub fn leaders(&self) -> Vec<(NodeId, u64)> {
        self.nodes
            .iter()
            .filter_map(|(id, node)| (node.role() == Role::Leader).then_some((*id, node.term())))
            .collect()
    }

    #[must_use]
    pub fn committed_index(&self, node: NodeId) -> Option<u64> {
        self.disks
            .get(&node)
            .map(|disk| disk.stable.hard_state.commit_index)
    }

    #[must_use]
    pub fn applied_index(&self, node: NodeId) -> Option<u64> {
        self.disks.get(&node).map(|disk| disk.stable.applied_index)
    }

    #[must_use]
    pub fn state_hash(&self, node: NodeId) -> Option<[u8; 32]> {
        self.disks.get(&node).map(SimDisk::state_hash)
    }

    #[must_use]
    pub fn durable_state(&self, node: NodeId) -> Option<&StableState> {
        self.disks.get(&node).map(|disk| &disk.stable)
    }

    pub fn campaign(&mut self, node: NodeId) -> Result<(), SimError> {
        self.input(node, Input::Tick(Tick::Election))
    }

    pub fn propose(
        &mut self,
        node: NodeId,
        id: CommandId,
        command: Vec<u8>,
    ) -> Result<(), SimError> {
        self.input(node, Input::Propose { id, command })
    }

    pub fn read_index(&mut self, node: NodeId, context: Vec<u8>) -> Result<(), SimError> {
        self.input(node, Input::ReadIndex { context })
    }

    /// Builds a valid snapshot from the simulator's durable state-machine
    /// image and gives it to the local Raft core for compaction.
    pub fn snapshot(&mut self, node: NodeId, index: u64) -> Result<(), SimError> {
        let disk = self
            .disks
            .get(&node)
            .ok_or(SimError::NodeUnavailable(node))?;
        if index != disk.stable.applied_index {
            return Err(self.invariant(format!(
                "simulator snapshots must use current applied index {}; got {index}",
                disk.stable.applied_index
            )));
        }
        let term = disk
            .entry(index)
            .map(|entry| entry.term)
            .or_else(|| (index == disk.base_index()).then(|| disk.base_term()))
            .ok_or_else(|| self.invariant(format!("missing snapshot boundary {index}")))?;
        let snapshot = PersistentSnapshot::new(
            index,
            term,
            self.configuration.clone(),
            disk.state_bytes.clone(),
        );
        self.input(node, Input::SnapshotBuilt(snapshot))
    }

    pub fn crash(&mut self, node: NodeId) -> Result<(), SimError> {
        if self.nodes.remove(&node).is_none() {
            return Err(SimError::NodeUnavailable(node));
        }
        self.election_deadline.remove(&node);
        self.heartbeat_deadline.remove(&node);
        self.quorum_deadline.remove(&node);
        self.record(format!("t={} crash node={node}", self.now));
        Ok(())
    }

    pub fn restart(&mut self, node: NodeId) -> Result<(), SimError> {
        if !self.configuration.contains(node) {
            return Err(SimError::NodeUnavailable(node));
        }
        if self.nodes.contains_key(&node) {
            return Ok(());
        }
        let stable = self
            .disks
            .get(&node)
            .ok_or(SimError::NodeUnavailable(node))?
            .stable
            .clone();
        let raft = Raft::recover(node, self.configuration.clone(), stable).map_err(|error| {
            SimError::Recovery {
                node,
                message: error.to_string(),
            }
        })?;
        self.nodes.insert(node, raft);
        let deadline = self.random_election_deadline();
        self.election_deadline.insert(node, deadline);
        self.heartbeat_deadline
            .insert(node, self.now + HEARTBEAT_TICKS);
        self.quorum_deadline
            .insert(node, self.now + CHECK_QUORUM_TICKS);
        self.record(format!("t={} restart node={node}", self.now));
        self.assert_invariants()
    }

    pub fn partition(&mut self, from: NodeId, to: NodeId) {
        self.blocked.insert((from, to));
        self.record(format!("t={} partition {from}->{to}", self.now));
    }

    pub fn partition_bidirectional(&mut self, left: NodeId, right: NodeId) {
        self.partition(left, right);
        self.partition(right, left);
    }

    pub fn isolate(&mut self, node: NodeId) {
        for peer in self.configuration.voters() {
            if *peer != node {
                self.blocked.insert((node, *peer));
                self.blocked.insert((*peer, node));
            }
        }
    }

    pub fn heal(&mut self) {
        self.blocked.clear();
        self.record(format!("t={} heal-all", self.now));
    }

    /// Drops all in-flight traffic while preserving node and disk state. This
    /// models connection teardown and is useful for forcing snapshot catch-up.
    pub fn discard_queued_messages(&mut self) {
        self.messages.clear();
        self.queued_bytes = 0;
        self.record(format!("t={} discard-network", self.now));
    }

    /// Advances simulated time, fires due external clocks, then delivers up to
    /// four independently selected due packets. This intentionally creates
    /// reordering without relying on host scheduling.
    pub fn advance(&mut self) -> Result<(), SimError> {
        if self.step_number >= self.step_budget {
            return Err(SimError::StepBudget {
                seed: self.seed,
                budget: self.step_budget,
            });
        }
        self.now = self.now.saturating_add(1);
        self.step_number = self.step_number.saturating_add(1);

        let ids = self.nodes.keys().copied().collect::<Vec<_>>();
        for id in &ids {
            let role = self.nodes.get(id).map(Raft::role);
            if role != Some(Role::Leader)
                && self
                    .election_deadline
                    .get(id)
                    .is_some_and(|deadline| *deadline <= self.now)
            {
                self.input(*id, Input::Tick(Tick::Election))?;
            }
        }
        for id in &ids {
            if self
                .nodes
                .get(id)
                .is_some_and(|node| node.role() == Role::Leader)
                && self
                    .heartbeat_deadline
                    .get(id)
                    .is_some_and(|deadline| *deadline <= self.now)
            {
                self.input(*id, Input::Tick(Tick::Heartbeat))?;
            }
            if self
                .nodes
                .get(id)
                .is_some_and(|node| node.role() == Role::Leader)
                && self
                    .quorum_deadline
                    .get(id)
                    .is_some_and(|deadline| *deadline <= self.now)
            {
                self.quorum_deadline
                    .insert(*id, self.now + CHECK_QUORUM_TICKS);
                self.input(*id, Input::Tick(Tick::CheckQuorum))?;
            }
        }
        for _ in 0..4 {
            if !self.deliver_one()? {
                break;
            }
        }
        self.assert_invariants()
    }

    pub fn run(&mut self, ticks: usize) -> Result<(), SimError> {
        for _ in 0..ticks {
            self.advance()?;
        }
        Ok(())
    }

    pub fn run_until_leader(
        &mut self,
        max_ticks: usize,
    ) -> Result<Option<(NodeId, u64)>, SimError> {
        for _ in 0..max_ticks {
            let leaders = self.leaders();
            if leaders.len() == 1 {
                return Ok(leaders.into_iter().next());
            }
            self.advance()?;
        }
        Ok(None)
    }

    /// A bounded chaos workload used by CI and reusable by seed-replay tools.
    /// It perturbs links and crash state while continuously proposing commands
    /// whenever a leader is visible.
    pub fn run_chaos(&mut self, ticks: usize) -> Result<(), SimError> {
        let voters = self
            .configuration
            .voters()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut command_id = 1_u128;
        for tick in 0..ticks {
            let choice = self.rng.random_range(0..100_u8);
            match choice {
                0..=2 => {
                    let node = voters[self.rng.random_range(0..voters.len())];
                    if self.nodes.len() > self.configuration.voters().len() / 2 + 1
                        && self.nodes.contains_key(&node)
                    {
                        self.crash(node)?;
                    }
                }
                3..=6 => {
                    let node = voters[self.rng.random_range(0..voters.len())];
                    if !self.nodes.contains_key(&node) {
                        self.restart(node)?;
                    }
                }
                7..=11 => {
                    let from = voters[self.rng.random_range(0..voters.len())];
                    let to = voters[self.rng.random_range(0..voters.len())];
                    if from != to {
                        self.partition(from, to);
                    }
                }
                12..=15 => self.heal(),
                16..=35 => {
                    if let Some((leader, _)) = self.leaders().first().copied() {
                        self.propose(leader, command_id, tick.to_le_bytes().to_vec())?;
                        command_id += 1;
                    }
                }
                _ => {}
            }
            self.advance()?;
        }
        self.heal();
        for node in voters {
            if !self.nodes.contains_key(&node) {
                self.restart(node)?;
            }
        }
        self.run(80)
    }

    pub fn assert_invariants(&self) -> Result<(), SimError> {
        self.assert_indexes_and_applied_prefix()?;
        self.assert_log_matching()?;
        self.assert_committed_entries_unchanged()?;
        self.assert_state_hash_agreement()?;
        Ok(())
    }

    fn input(&mut self, node: NodeId, input: Input) -> Result<(), SimError> {
        self.record(format!(
            "t={} input node={node} {}",
            self.now,
            Self::describe_input(&input)
        ));
        let actions = self
            .nodes
            .get_mut(&node)
            .ok_or(SimError::NodeUnavailable(node))?
            .step(input);
        self.execute_actions(node, actions)?;
        self.assert_invariants()
    }

    fn execute_actions(&mut self, node: NodeId, actions: Vec<Action>) -> Result<(), SimError> {
        for action in actions {
            match action {
                Action::Persist(mutation) => self.persist(node, mutation)?,
                Action::Send { to, message } => self.enqueue(node, to, &message),
                Action::ResetElectionTimer => {
                    let deadline = self.random_election_deadline();
                    self.election_deadline.insert(node, deadline);
                }
                Action::ResetHeartbeatTimer => {
                    self.heartbeat_deadline
                        .insert(node, self.now + HEARTBEAT_TICKS);
                }
                Action::RoleChanged { role, term } => {
                    if role == Role::Leader {
                        if let Some(previous) = self.leaders_by_term.insert(term, node) {
                            if previous != node {
                                return Err(self.invariant(format!(
                                    "leaders {previous} and {node} both elected in term {term}"
                                )));
                            }
                        }
                        self.quorum_deadline
                            .insert(node, self.now + CHECK_QUORUM_TICKS);
                        self.events.push(SimEvent::Leader { node, term });
                    }
                }
                Action::Apply(entry) => self.apply(node, &entry)?,
                Action::ProposalCommitted { id, index } => {
                    self.events.push(SimEvent::Committed { node, id, index });
                }
                Action::ReadReady { context, index } => {
                    self.events.push(SimEvent::ReadReady {
                        node,
                        context,
                        index,
                    });
                }
                Action::Fatal { reason } => {
                    self.events.push(SimEvent::Fatal {
                        node,
                        reason: reason.clone(),
                    });
                    return Err(self.invariant(format!("node {node} fenced itself: {reason}")));
                }
                Action::ProposalRejected { .. }
                | Action::ReadRejected { .. }
                | Action::SnapshotRejected { .. } => {}
            }
        }
        Ok(())
    }

    fn persist(&mut self, node: NodeId, mutation: StorageMutation) -> Result<(), SimError> {
        match mutation {
            StorageMutation::HardState(next) => self.persist_hard_state(node, next)?,
            StorageMutation::Append(entries) => {
                let disk = self
                    .disks
                    .get_mut(&node)
                    .ok_or(SimError::NodeUnavailable(node))?;
                let expected = disk.last_index() + 1;
                if entries.first().is_some_and(|entry| entry.index != expected) {
                    return Err(self.invariant(format!(
                        "node {node} appended non-contiguous log at {expected}"
                    )));
                }
                disk.stable.entries.extend(entries);
            }
            StorageMutation::TruncateAndAppend { from, entries } => {
                let disk = self
                    .disks
                    .get_mut(&node)
                    .ok_or(SimError::NodeUnavailable(node))?;
                if from <= disk.stable.hard_state.commit_index {
                    return Err(
                        self.invariant(format!("node {node} truncated committed index {from}"))
                    );
                }
                let keep = usize::try_from(from.saturating_sub(disk.base_index() + 1))
                    .unwrap_or(usize::MAX);
                disk.stable.entries.truncate(keep);
                if entries.first().is_some_and(|entry| entry.index != from) {
                    return Err(
                        self.invariant(format!("node {node} replacement did not start at {from}"))
                    );
                }
                disk.stable.entries.extend(entries);
            }
            StorageMutation::CompactSnapshot {
                snapshot,
                retained_entries,
            } => {
                let disk = self
                    .disks
                    .get_mut(&node)
                    .ok_or(SimError::NodeUnavailable(node))?;
                if snapshot.metadata.last_included_index > disk.stable.applied_index {
                    return Err(self.invariant(format!(
                        "node {node} compacted unapplied index {}",
                        snapshot.metadata.last_included_index
                    )));
                }
                disk.stable.snapshot = Some(snapshot);
                disk.stable.entries = retained_entries;
            }
            StorageMutation::InstallSnapshot {
                snapshot,
                retained_entries,
                hard_state,
            } => {
                let disk = self
                    .disks
                    .get_mut(&node)
                    .ok_or(SimError::NodeUnavailable(node))?;
                if !snapshot.validate() {
                    return Err(self.invariant(format!(
                        "node {node} attempted to persist an invalid snapshot"
                    )));
                }
                let index = snapshot.metadata.last_included_index;
                disk.state_bytes.clone_from(&snapshot.data);
                disk.stable.snapshot = Some(snapshot);
                disk.stable.entries = retained_entries;
                disk.stable.hard_state = hard_state;
                disk.stable.applied_index = index;
                disk.applied.retain(|entry_index, _| *entry_index > index);
            }
        }
        self.capture_committed(node)?;
        Ok(())
    }

    fn persist_hard_state(&mut self, node: NodeId, next: HardState) -> Result<(), SimError> {
        let previous = self
            .disks
            .get(&node)
            .ok_or(SimError::NodeUnavailable(node))?
            .stable
            .hard_state;
        if next.current_term < previous.current_term {
            return Err(self.invariant(format!(
                "node {node} term regressed from {} to {}",
                previous.current_term, next.current_term
            )));
        }
        if next.commit_index < previous.commit_index {
            return Err(self.invariant(format!(
                "node {node} commit regressed from {} to {}",
                previous.commit_index, next.commit_index
            )));
        }
        if next.current_term == previous.current_term {
            if let (Some(old), Some(new)) = (previous.voted_for, next.voted_for) {
                if old != new {
                    return Err(self.invariant(format!(
                        "node {node} changed vote in term {} from {old} to {new}",
                        next.current_term
                    )));
                }
            }
        }
        if let Some(candidate) = next.voted_for {
            let key = (node, next.current_term);
            if let Some(previous_candidate) = self.votes_by_term.insert(key, candidate) {
                if previous_candidate != candidate {
                    return Err(self.invariant(format!(
                        "node {node} voted for {previous_candidate} and {candidate} in term {}",
                        next.current_term
                    )));
                }
            }
        }
        self.disks
            .get_mut(&node)
            .ok_or(SimError::NodeUnavailable(node))?
            .stable
            .hard_state = next;
        Ok(())
    }

    fn capture_committed(&mut self, node: NodeId) -> Result<(), SimError> {
        let disk = self
            .disks
            .get(&node)
            .ok_or(SimError::NodeUnavailable(node))?;
        let captured = self.captured_commit.get(&node).copied().unwrap_or(0);
        let start = captured.saturating_add(1).max(disk.base_index() + 1);
        let commit_index = disk.stable.hard_state.commit_index;
        for index in start..=commit_index {
            let Some(identity) = disk.known_identity(index) else {
                return Err(self.invariant(format!("node {node} committed missing index {index}")));
            };
            if let Some(previous) = self.committed.insert(index, identity.clone()) {
                if previous != identity {
                    return Err(self.invariant(format!("committed index {index} changed identity")));
                }
            }
        }
        self.captured_commit.insert(node, commit_index);
        Ok(())
    }

    fn apply(&mut self, node: NodeId, entry: &LogEntry) -> Result<(), SimError> {
        let disk = self
            .disks
            .get(&node)
            .ok_or(SimError::NodeUnavailable(node))?;
        let expected = disk.stable.applied_index + 1;
        let commit_index = disk.stable.hard_state.commit_index;
        if entry.index != expected || entry.index > commit_index {
            return Err(self.invariant(format!(
                "node {node} applied {} with expected {expected} and commit {commit_index}",
                entry.index
            )));
        }
        let disk = self
            .disks
            .get_mut(&node)
            .ok_or(SimError::NodeUnavailable(node))?;
        let identity = EntryIdentity::of(entry);
        if let EntryPayload::Command { id, bytes } = &entry.payload {
            disk.state_bytes
                .extend_from_slice(&entry.index.to_le_bytes());
            disk.state_bytes
                .extend_from_slice(&entry.term.to_le_bytes());
            disk.state_bytes.extend_from_slice(&id.to_le_bytes());
            disk.state_bytes
                .extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            disk.state_bytes.extend_from_slice(bytes);
        }
        disk.stable.applied_index = entry.index;
        disk.applied.insert(entry.index, identity);
        self.events.push(SimEvent::Applied {
            node,
            index: entry.index,
        });
        Ok(())
    }

    fn enqueue(&mut self, from: NodeId, to: NodeId, message: &Message) {
        if self.profile.drop_per_mille != 0
            && self
                .rng
                .random_ratio(u32::from(self.profile.drop_per_mille), 1_000)
        {
            return;
        }
        let copies = if self.profile.duplicate_per_mille != 0
            && self
                .rng
                .random_ratio(u32::from(self.profile.duplicate_per_mille), 1_000)
        {
            2
        } else {
            1
        };
        let byte_cost = Self::message_byte_cost(message);
        for _ in 0..copies {
            if self.messages.len() >= self.profile.max_queued_messages
                || self.queued_bytes.saturating_add(byte_cost) > self.profile.max_queued_bytes
            {
                // A bounded transport drops new packets under sustained
                // backpressure. Raft safety is independent of packet loss.
                continue;
            }
            let delay = if self.profile.max_delay_ticks == 0 {
                0
            } else {
                self.rng.random_range(0..=self.profile.max_delay_ticks)
            };
            self.next_sequence = self.next_sequence.saturating_add(1);
            self.messages.push(Envelope {
                sequence: self.next_sequence,
                from,
                to,
                deliver_at: self.now + delay,
                message: message.clone(),
                byte_cost,
            });
            self.queued_bytes = self.queued_bytes.saturating_add(byte_cost);
        }
    }

    fn deliver_one(&mut self) -> Result<bool, SimError> {
        if self.messages.is_empty() {
            return Ok(false);
        }
        // Sampling a bounded number keeps large retained partition queues from
        // turning each simulated tick into an O(queue) scan. Selection remains
        // deterministic for a seed and produces ample reordering.
        let attempts = self.messages.len().min(96);
        let mut selected = None;
        for _ in 0..attempts {
            let index = self.rng.random_range(0..self.messages.len());
            let envelope = &self.messages[index];
            if envelope.deliver_at <= self.now
                && self.nodes.contains_key(&envelope.to)
                && !self.blocked.contains(&(envelope.from, envelope.to))
            {
                selected = Some(index);
                break;
            }
        }
        let Some(selected) = selected else {
            return Ok(false);
        };
        let envelope = self.messages.swap_remove(selected);
        self.queued_bytes = self.queued_bytes.saturating_sub(envelope.byte_cost);
        self.record(format!(
            "t={} deliver seq={} {}->{}",
            self.now, envelope.sequence, envelope.from, envelope.to
        ));
        self.input(
            envelope.to,
            Input::Message {
                from: envelope.from,
                message: envelope.message,
            },
        )?;
        Ok(true)
    }

    fn assert_indexes_and_applied_prefix(&self) -> Result<(), SimError> {
        for (node, disk) in &self.disks {
            if disk.stable.applied_index > disk.stable.hard_state.commit_index {
                return Err(self.invariant(format!(
                    "node {node} applied {} beyond commit {}",
                    disk.stable.applied_index, disk.stable.hard_state.commit_index
                )));
            }
            if disk.stable.hard_state.commit_index > disk.last_index() {
                return Err(self.invariant(format!(
                    "node {node} committed {} beyond log end {}",
                    disk.stable.hard_state.commit_index,
                    disk.last_index()
                )));
            }
            for (expected, entry) in (disk.base_index() + 1..).zip(disk.stable.entries.iter()) {
                if entry.index != expected {
                    return Err(self.invariant(format!(
                        "node {node} log hole: expected {expected}, found {}",
                        entry.index
                    )));
                }
            }
            for (index, identity) in &disk.applied {
                if *index > disk.stable.applied_index {
                    return Err(
                        self.invariant(format!("node {node} has future applied record {index}"))
                    );
                }
                if let Some(committed) = self.committed.get(index) {
                    if committed != identity {
                        return Err(self.invariant(format!(
                            "node {node} applied a different entry at committed index {index}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn assert_log_matching(&self) -> Result<(), SimError> {
        let disks = self.disks.iter().collect::<Vec<_>>();
        for (position, (left_id, left)) in disks.iter().enumerate() {
            for (right_id, right) in disks.iter().skip(position + 1) {
                let start = left.base_index().max(right.base_index()) + 1;
                let end = left.last_index().min(right.last_index());
                let equal_boundary = (start..=end).rev().find(|index| {
                    left.entry(*index).map(|entry| entry.term)
                        == right.entry(*index).map(|entry| entry.term)
                });
                if let Some(index) = equal_boundary {
                    let term = left.entry(index).map_or(0, |entry| entry.term);
                    for prefix_index in start..=index {
                        let Some(left_prefix) = left.entry(prefix_index) else {
                            continue;
                        };
                        let Some(right_prefix) = right.entry(prefix_index) else {
                            continue;
                        };
                        if EntryIdentity::of(left_prefix) != EntryIdentity::of(right_prefix) {
                            return Err(self.invariant(format!(
                                "log matching failed for nodes {left_id}/{right_id}: equal ({index}, {term}) but divergent prefix at {prefix_index}"
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn assert_committed_entries_unchanged(&self) -> Result<(), SimError> {
        for (node, disk) in &self.disks {
            for (index, committed) in &self.committed {
                // A partitioned node may temporarily retain an uncommitted
                // suffix that conflicts with a globally committed entry. It
                // must repair that suffix before *locally* committing it.
                if *index <= disk.base_index()
                    || *index > disk.stable.hard_state.commit_index
                    || *index > disk.last_index()
                {
                    continue;
                }
                if let Some(identity) = disk.known_identity(*index) {
                    if &identity != committed {
                        return Err(
                            self.invariant(format!("node {node} replaced committed index {index}"))
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn assert_state_hash_agreement(&self) -> Result<(), SimError> {
        let disks = self.disks.iter().collect::<Vec<_>>();
        for (position, (left_id, left)) in disks.iter().enumerate() {
            for (right_id, right) in disks.iter().skip(position + 1) {
                if left.stable.applied_index == right.stable.applied_index
                    && left.state_hash() != right.state_hash()
                {
                    return Err(self.invariant(format!(
                        "nodes {left_id}/{right_id} disagree at applied index {}",
                        left.stable.applied_index
                    )));
                }
            }
        }
        Ok(())
    }

    fn random_election_deadline(&mut self) -> u64 {
        self.now
            + self
                .rng
                .random_range(ELECTION_MIN_TICKS..=ELECTION_MAX_TICKS)
    }

    fn invariant(&self, message: String) -> SimError {
        let trace = self.trace.iter().cloned().collect::<Vec<_>>().join(" | ");
        SimError::Invariant {
            seed: self.seed,
            step: self.step_number,
            message: if trace.is_empty() {
                message
            } else {
                format!("{message}; bounded replay trace: {trace}")
            },
        }
    }

    fn record(&mut self, event: String) {
        const TRACE_EVENTS: usize = 96;
        if self.trace.len() == TRACE_EVENTS {
            self.trace.pop_front();
        }
        self.trace.push_back(event);
    }

    fn describe_input(input: &Input) -> &'static str {
        match input {
            Input::Tick(Tick::Election) => "tick-election",
            Input::Tick(Tick::Heartbeat) => "tick-heartbeat",
            Input::Tick(Tick::CheckQuorum) => "tick-check-quorum",
            Input::Message {
                message: Message::PreVoteRequest { .. },
                ..
            } => "recv-prevote",
            Input::Message {
                message: Message::PreVoteResponse { .. },
                ..
            } => "recv-prevote-response",
            Input::Message {
                message: Message::RequestVote { .. },
                ..
            } => "recv-vote",
            Input::Message {
                message: Message::RequestVoteResponse { .. },
                ..
            } => "recv-vote-response",
            Input::Message {
                message: Message::AppendEntries { entries, .. },
                ..
            } if entries.is_empty() => "recv-heartbeat",
            Input::Message {
                message: Message::AppendEntries { .. },
                ..
            } => "recv-append",
            Input::Message {
                message: Message::AppendEntriesResponse { .. },
                ..
            } => "recv-append-response",
            Input::Message {
                message: Message::InstallSnapshot { .. },
                ..
            } => "recv-snapshot-chunk",
            Input::Message {
                message: Message::InstallSnapshotResponse { .. },
                ..
            } => "recv-snapshot-response",
            Input::Propose { .. } => "propose",
            Input::ReadIndex { .. } => "read-index",
            Input::SnapshotBuilt(_) => "snapshot-built",
        }
    }

    fn message_byte_cost(message: &Message) -> usize {
        const ENVELOPE_OVERHEAD: usize = 128;
        ENVELOPE_OVERHEAD
            + match message {
                Message::AppendEntries {
                    entries,
                    read_context,
                    ..
                } => {
                    read_context.as_ref().map_or(0, Vec::len)
                        + entries
                            .iter()
                            .map(|entry| match &entry.payload {
                                EntryPayload::Noop => 32,
                                EntryPayload::Command { bytes, .. } => 48 + bytes.len(),
                            })
                            .sum::<usize>()
                }
                Message::AppendEntriesResponse { read_context, .. } => {
                    read_context.as_ref().map_or(0, Vec::len)
                }
                Message::InstallSnapshot { data, .. } => data.len(),
                _ => 0,
            }
    }
}

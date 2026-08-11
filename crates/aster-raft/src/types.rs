use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::NodeId;

pub type CommandId = u128;

/// The immutable voters participating in this Raft process lifetime.
///
/// A configuration may be fixed or explicitly joint. Joint quorum decisions
/// require independent majorities of the incoming and outgoing voter sets;
/// merely reaching a majority of their union is insufficient. The core does
/// not expose unsafe direct voter replacement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Configuration {
    participants: BTreeSet<NodeId>,
    incoming: BTreeSet<NodeId>,
    outgoing: Option<BTreeSet<NodeId>>,
}

impl Configuration {
    /// Creates a validated fixed membership configuration.
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Result<Self, ConfigError> {
        let voters = voters.into_iter().collect::<BTreeSet<_>>();
        if voters.is_empty() {
            return Err(ConfigError::Empty);
        }
        Ok(Self {
            participants: voters.clone(),
            incoming: voters,
            outgoing: None,
        })
    }

    /// Constructs the joint phase of a Raft membership transition. A caller
    /// must first commit this joint configuration under the old configuration,
    /// and later commit a fixed incoming configuration under this joint quorum.
    pub fn joint(
        outgoing: impl IntoIterator<Item = NodeId>,
        incoming: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, ConfigError> {
        let outgoing = outgoing.into_iter().collect::<BTreeSet<_>>();
        let incoming = incoming.into_iter().collect::<BTreeSet<_>>();
        if outgoing.is_empty() || incoming.is_empty() {
            return Err(ConfigError::EmptyJointSide);
        }
        let participants = outgoing.union(&incoming).copied().collect();
        Ok(Self {
            participants,
            incoming,
            outgoing: Some(outgoing),
        })
    }

    #[must_use]
    pub const fn voters(&self) -> &BTreeSet<NodeId> {
        &self.participants
    }

    #[must_use]
    pub const fn incoming_voters(&self) -> &BTreeSet<NodeId> {
        &self.incoming
    }

    #[must_use]
    pub const fn outgoing_voters(&self) -> Option<&BTreeSet<NodeId>> {
        self.outgoing.as_ref()
    }

    #[must_use]
    pub const fn is_joint(&self) -> bool {
        self.outgoing.is_some()
    }

    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.participants.contains(&id)
    }

    #[must_use]
    pub fn quorum(&self, votes: &BTreeSet<NodeId>) -> bool {
        Self::has_majority(&self.incoming, votes)
            && self
                .outgoing
                .as_ref()
                .is_none_or(|outgoing| Self::has_majority(outgoing, votes))
    }

    #[must_use]
    pub fn quorum_index(&self, mut matched: impl FnMut(NodeId) -> u64) -> u64 {
        let matched = self
            .participants
            .iter()
            .copied()
            .map(|voter| (voter, matched(voter)))
            .collect::<BTreeMap<_, _>>();
        let mut indexes = matched.values().copied().collect::<Vec<_>>();
        indexes.sort_unstable();
        indexes.dedup();
        indexes
            .into_iter()
            .rev()
            .find(|index| {
                let acknowledgements = matched
                    .iter()
                    .filter_map(|(voter, matched)| (*matched >= *index).then_some(*voter))
                    .collect::<BTreeSet<_>>();
                self.quorum(&acknowledgements)
            })
            .unwrap_or(0)
    }

    fn has_majority(members: &BTreeSet<NodeId>, votes: &BTreeSet<NodeId>) -> bool {
        members.intersection(votes).count() > members.len() / 2
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigError {
    #[error("a Raft configuration must contain at least one voter")]
    Empty,
    #[error("both sides of a joint Raft configuration must contain a voter")]
    EmptyJointSide,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecoveryError {
    #[error("local node {0} is not a voter")]
    NotVoter(NodeId),
    #[error("invalid snapshot: {0}")]
    InvalidSnapshot(String),
    #[error("invalid stable log: {0}")]
    InvalidLog(String),
    #[error("invalid durable indexes: {0}")]
    InvalidIndexes(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    /// The highest entry known committed. Persisting this prevents a restart
    /// from applying less than a prefix previously exposed as committed.
    pub commit_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// Written by every newly elected leader before it serves linearizable
    /// reads. Committing this entry also commits prior-term prefixes.
    Noop,
    Command {
        id: CommandId,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: EntryPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub byte_len: u64,
    pub sha256: [u8; 32],
    pub configuration: Configuration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentSnapshot {
    pub metadata: SnapshotMetadata,
    pub data: Vec<u8>,
}

impl PersistentSnapshot {
    #[must_use]
    pub fn new(
        last_included_index: u64,
        last_included_term: u64,
        configuration: Configuration,
        data: Vec<u8>,
    ) -> Self {
        let sha256 = Sha256::digest(&data).into();
        Self {
            metadata: SnapshotMetadata {
                last_included_index,
                last_included_term,
                byte_len: data.len() as u64,
                sha256,
                configuration,
            },
            data,
        }
    }

    #[must_use]
    pub fn validate(&self) -> bool {
        self.metadata.byte_len == self.data.len() as u64
            && self.metadata.sha256 == <[u8; 32]>::from(Sha256::digest(&self.data))
    }
}

/// Exactly the bytes/state required to restart a Raft node safely. The applied
/// index belongs to the durable state machine, but is supplied here so replay
/// resumes at the correct entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StableState {
    pub hard_state: HardState,
    pub snapshot: Option<PersistentSnapshot>,
    pub entries: Vec<LogEntry>,
    pub applied_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tick {
    /// Fired after an externally randomized election timeout.
    Election,
    /// Leader heartbeat cadence.
    Heartbeat,
    /// One check-quorum window. The first window after election is a grace
    /// period; subsequent windows require responses from a quorum.
    CheckQuorum,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    PreVoteRequest {
        prospective_term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    PreVoteResponse {
        responder_term: u64,
        prospective_term: u64,
        granted: bool,
    },
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        /// An opaque token echoed by the follower. A leader completes the read
        /// only after a quorum has replied in its current term.
        read_context: Option<Vec<u8>>,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        conflict_term: Option<u64>,
        read_context: Option<Vec<u8>>,
    },
    InstallSnapshot {
        term: u64,
        leader_id: NodeId,
        metadata: SnapshotMetadata,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    InstallSnapshotResponse {
        term: u64,
        accepted: bool,
        next_offset: u64,
        done: bool,
        match_index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Tick(Tick),
    Message {
        from: NodeId,
        message: Message,
    },
    Propose {
        id: CommandId,
        command: Vec<u8>,
    },
    ReadIndex {
        context: Vec<u8>,
    },
    /// A state-machine checkpoint that is already durable locally. The core
    /// verifies its boundary before compacting the Raft log around it.
    SnapshotBuilt(PersistentSnapshot),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMutation {
    HardState(HardState),
    Append(Vec<LogEntry>),
    TruncateAndAppend {
        from: u64,
        entries: Vec<LogEntry>,
    },
    /// Atomically publish a local checkpoint and discard log entries through
    /// its included index. The database checkpoint is already installed.
    CompactSnapshot {
        snapshot: PersistentSnapshot,
        retained_entries: Vec<LogEntry>,
    },
    /// Atomically install a received database snapshot and its Raft metadata.
    /// A partial transfer must never execute this mutation.
    InstallSnapshot {
        snapshot: PersistentSnapshot,
        retained_entries: Vec<LogEntry>,
        hard_state: HardState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReadError {
    #[error("node is not the leader")]
    NotLeader,
    #[error("leader has not committed an entry in its current term")]
    LeaderNotReady,
    #[error("read context must be non-empty and unique")]
    InvalidContext,
    #[error("leadership was lost before the quorum barrier completed")]
    LeadershipLost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Must complete durably before the next action is executed.
    Persist(StorageMutation),
    Send {
        to: NodeId,
        message: Message,
    },
    ResetElectionTimer,
    ResetHeartbeatTimer,
    RoleChanged {
        role: Role,
        term: u64,
    },
    Apply(LogEntry),
    ProposalCommitted {
        id: CommandId,
        index: u64,
    },
    ProposalRejected {
        id: CommandId,
        leader_hint: Option<NodeId>,
    },
    ReadReady {
        context: Vec<u8>,
        index: u64,
    },
    ReadRejected {
        context: Vec<u8>,
        error: ReadError,
    },
    SnapshotRejected {
        reason: String,
    },
    /// Indicates a violated local precondition or an impossible safety state.
    /// Production adapters should fence the node instead of continuing.
    Fatal {
        reason: String,
    },
}

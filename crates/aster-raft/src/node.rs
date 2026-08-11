use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::log::RaftLog;
use crate::{
    Action, CommandId, Configuration, EntryPayload, HardState, Input, LogEntry, Message, NodeId,
    PersistentSnapshot, ReadError, RecoveryError, Role, SnapshotMetadata, StableState,
    StorageMutation, Tick,
};

const MAX_APPEND_ENTRIES: usize = 64;
const SNAPSHOT_CHUNK_BYTES: usize = 16 * 1024;
const MAX_SNAPSHOT_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone)]
struct Progress {
    match_index: u64,
    next_index: u64,
    snapshot_offset: Option<u64>,
}

#[derive(Debug, Clone)]
struct PendingRead {
    index: u64,
    acknowledgements: BTreeSet<NodeId>,
}

#[derive(Debug, Clone)]
struct IncomingSnapshot {
    metadata: SnapshotMetadata,
    data: Vec<u8>,
}

/// A deterministic Raft state machine. It is `Send` but intentionally owns no
/// sockets, tasks, mutexes, random generator, or clock.
#[derive(Debug, Clone)]
pub struct Raft {
    id: NodeId,
    configuration: Configuration,
    hard_state: HardState,
    log: RaftLog,
    snapshot: Option<PersistentSnapshot>,
    applied_index: u64,
    role: Role,
    leader_id: Option<NodeId>,
    campaign_term: Option<u64>,
    votes: BTreeSet<NodeId>,
    progress: BTreeMap<NodeId, Progress>,
    recent_active: BTreeSet<NodeId>,
    pending_reads: BTreeMap<Vec<u8>, PendingRead>,
    incoming_snapshot: Option<IncomingSnapshot>,
}

impl Raft {
    /// Recovers a node from durable consensus and state-machine state.
    pub fn recover(
        id: NodeId,
        configuration: Configuration,
        stable: StableState,
    ) -> Result<Self, RecoveryError> {
        if !configuration.contains(id) {
            return Err(RecoveryError::NotVoter(id));
        }
        if let Some(snapshot) = &stable.snapshot {
            if !snapshot.validate() {
                return Err(RecoveryError::InvalidSnapshot(
                    "length or SHA-256 mismatch".into(),
                ));
            }
            if snapshot.metadata.configuration != configuration {
                return Err(RecoveryError::InvalidSnapshot(
                    "snapshot voter set differs from local configuration".into(),
                ));
            }
        }

        let (base_index, base_term) = stable.snapshot.as_ref().map_or((0, 0), |snapshot| {
            (
                snapshot.metadata.last_included_index,
                snapshot.metadata.last_included_term,
            )
        });
        let mut expected = base_index.saturating_add(1);
        let mut previous_term = base_term;
        for entry in &stable.entries {
            if entry.index != expected {
                return Err(RecoveryError::InvalidLog(format!(
                    "expected index {expected}, found {}",
                    entry.index
                )));
            }
            if entry.term < previous_term {
                return Err(RecoveryError::InvalidLog(format!(
                    "term regressed from {previous_term} to {} at index {}",
                    entry.term, entry.index
                )));
            }
            if entry.term > stable.hard_state.current_term {
                return Err(RecoveryError::InvalidLog(format!(
                    "entry {} has future term {} beyond hard-state term {}",
                    entry.index, entry.term, stable.hard_state.current_term
                )));
            }
            expected = expected.saturating_add(1);
            previous_term = entry.term;
        }
        let last_index = stable
            .entries
            .last()
            .map_or(base_index, |entry| entry.index);
        if stable.hard_state.commit_index < base_index
            || stable.hard_state.commit_index > last_index
        {
            return Err(RecoveryError::InvalidIndexes(format!(
                "commit {} is outside [{base_index}, {last_index}]",
                stable.hard_state.commit_index
            )));
        }
        if stable.applied_index < base_index
            || stable.applied_index > stable.hard_state.commit_index
        {
            return Err(RecoveryError::InvalidIndexes(format!(
                "applied {} is outside [{base_index}, {}]",
                stable.applied_index, stable.hard_state.commit_index
            )));
        }

        let log = RaftLog::new(stable.snapshot.as_ref(), stable.entries);
        Ok(Self {
            id,
            configuration,
            hard_state: stable.hard_state,
            log,
            snapshot: stable.snapshot,
            applied_index: stable.applied_index,
            role: Role::Follower,
            leader_id: None,
            campaign_term: None,
            votes: BTreeSet::new(),
            progress: BTreeMap::new(),
            recent_active: BTreeSet::new(),
            pending_reads: BTreeMap::new(),
            incoming_snapshot: None,
        })
    }

    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    #[must_use]
    pub const fn term(&self) -> u64 {
        self.hard_state.current_term
    }

    #[must_use]
    pub const fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    #[must_use]
    pub const fn commit_index(&self) -> u64 {
        self.hard_state.commit_index
    }

    #[must_use]
    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub fn last_log_index(&self) -> u64 {
        self.log.last_index()
    }

    #[must_use]
    pub fn stable_state(&self) -> StableState {
        StableState {
            hard_state: self.hard_state,
            snapshot: self.snapshot.clone(),
            entries: self.log.all_entries().to_vec(),
            applied_index: self.applied_index,
        }
    }

    /// Advances the deterministic protocol by one external event.
    #[must_use]
    pub fn step(&mut self, input: Input) -> Vec<Action> {
        let mut actions = Vec::new();
        // A crash may occur after commit metadata is flushed but before the
        // external state machine applies the corresponding entries. Replay is
        // therefore the first ordered work after every recovery/event.
        self.apply_committed(&mut actions);
        match input {
            Input::Tick(tick) => self.on_tick(tick, &mut actions),
            Input::Message { from, message } => self.on_message(from, message, &mut actions),
            Input::Propose { id, command } => self.propose(id, command, &mut actions),
            Input::ReadIndex { context } => self.read_index(context, &mut actions),
            Input::SnapshotBuilt(snapshot) => self.snapshot_built(snapshot, &mut actions),
        }
        actions
    }

    fn on_tick(&mut self, tick: Tick, actions: &mut Vec<Action>) {
        match tick {
            Tick::Election if self.role != Role::Leader => self.start_pre_vote(actions),
            Tick::Heartbeat if self.role == Role::Leader => {
                self.broadcast_append(None, actions);
                actions.push(Action::ResetHeartbeatTimer);
            }
            Tick::CheckQuorum if self.role == Role::Leader => {
                if self.configuration.quorum(&self.recent_active) {
                    self.recent_active.clear();
                    self.recent_active.insert(self.id);
                    self.broadcast_append(None, actions);
                } else {
                    self.become_follower(self.hard_state.current_term, None, actions);
                    actions.push(Action::ResetElectionTimer);
                }
            }
            _ => {}
        }
    }

    #[allow(clippy::too_many_lines)]
    fn on_message(&mut self, from: NodeId, message: Message, actions: &mut Vec<Action>) {
        if !self.configuration.contains(from) || from == self.id {
            return;
        }
        match message {
            Message::PreVoteRequest {
                prospective_term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_pre_vote_request(
                from,
                prospective_term,
                candidate_id,
                last_log_index,
                last_log_term,
                actions,
            ),
            Message::PreVoteResponse {
                responder_term,
                prospective_term,
                granted,
            } => {
                self.on_pre_vote_response(from, responder_term, prospective_term, granted, actions);
            }
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_vote_request(
                from,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                actions,
            ),
            Message::RequestVoteResponse { term, granted } => {
                self.on_vote_response(from, term, granted, actions);
            }
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_context,
            } => self.on_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_context,
                actions,
            ),
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                read_context,
            } => self.on_append_response(
                from,
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                read_context,
                actions,
            ),
            Message::InstallSnapshot {
                term,
                leader_id,
                metadata,
                offset,
                data,
                done,
            } => self
                .on_install_snapshot(from, term, leader_id, metadata, offset, data, done, actions),
            Message::InstallSnapshotResponse {
                term,
                accepted,
                next_offset,
                done,
                match_index,
            } => self.on_snapshot_response(
                from,
                term,
                accepted,
                next_offset,
                done,
                match_index,
                actions,
            ),
        }
    }

    fn start_pre_vote(&mut self, actions: &mut Vec<Action>) {
        let Some(prospective_term) = self.hard_state.current_term.checked_add(1) else {
            actions.push(Action::Fatal {
                reason: "term counter exhausted".into(),
            });
            return;
        };
        self.role = Role::PreCandidate;
        self.leader_id = None;
        self.campaign_term = Some(prospective_term);
        self.votes.clear();
        self.votes.insert(self.id);
        actions.push(Action::RoleChanged {
            role: self.role,
            term: self.hard_state.current_term,
        });
        actions.push(Action::ResetElectionTimer);

        if self.configuration.quorum(&self.votes) {
            self.start_election(prospective_term, actions);
            return;
        }
        for peer in self.peers() {
            actions.push(Action::Send {
                to: peer,
                message: Message::PreVoteRequest {
                    prospective_term,
                    candidate_id: self.id,
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                },
            });
        }
    }

    fn start_election(&mut self, term: u64, actions: &mut Vec<Action>) {
        self.hard_state.current_term = term;
        self.hard_state.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.campaign_term = None;
        self.votes.clear();
        self.votes.insert(self.id);
        actions.push(Action::Persist(StorageMutation::HardState(self.hard_state)));
        actions.push(Action::RoleChanged {
            role: self.role,
            term,
        });
        actions.push(Action::ResetElectionTimer);

        if self.configuration.quorum(&self.votes) {
            self.become_leader(actions);
            return;
        }
        for peer in self.peers() {
            actions.push(Action::Send {
                to: peer,
                message: Message::RequestVote {
                    term,
                    candidate_id: self.id,
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                },
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_pre_vote_request(
        &mut self,
        from: NodeId,
        prospective_term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        actions: &mut Vec<Action>,
    ) {
        if candidate_id != from {
            return;
        }
        let granted = prospective_term > self.hard_state.current_term
            && self.candidate_log_is_up_to_date(last_log_index, last_log_term);
        actions.push(Action::Send {
            to: from,
            message: Message::PreVoteResponse {
                responder_term: self.hard_state.current_term,
                prospective_term,
                granted,
            },
        });
    }

    fn on_pre_vote_response(
        &mut self,
        from: NodeId,
        responder_term: u64,
        prospective_term: u64,
        granted: bool,
        actions: &mut Vec<Action>,
    ) {
        if responder_term > self.hard_state.current_term {
            self.become_follower(responder_term, None, actions);
            actions.push(Action::ResetElectionTimer);
            return;
        }
        if self.role != Role::PreCandidate
            || self.campaign_term != Some(prospective_term)
            || !granted
        {
            return;
        }
        self.votes.insert(from);
        if self.configuration.quorum(&self.votes) {
            self.start_election(prospective_term, actions);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_vote_request(
        &mut self,
        from: NodeId,
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        actions: &mut Vec<Action>,
    ) {
        if candidate_id != from {
            return;
        }
        if term > self.hard_state.current_term {
            self.become_follower(term, None, actions);
        }
        let granted = term == self.hard_state.current_term
            && (self.hard_state.voted_for.is_none()
                || self.hard_state.voted_for == Some(candidate_id))
            && self.candidate_log_is_up_to_date(last_log_index, last_log_term);
        if granted {
            if self.hard_state.voted_for != Some(candidate_id) {
                self.hard_state.voted_for = Some(candidate_id);
                actions.push(Action::Persist(StorageMutation::HardState(self.hard_state)));
            }
            self.leader_id = None;
            actions.push(Action::ResetElectionTimer);
        }
        actions.push(Action::Send {
            to: from,
            message: Message::RequestVoteResponse {
                term: self.hard_state.current_term,
                granted,
            },
        });
    }

    fn on_vote_response(
        &mut self,
        from: NodeId,
        term: u64,
        granted: bool,
        actions: &mut Vec<Action>,
    ) {
        if term > self.hard_state.current_term {
            self.become_follower(term, None, actions);
            actions.push(Action::ResetElectionTimer);
            return;
        }
        if self.role != Role::Candidate || term != self.hard_state.current_term || !granted {
            return;
        }
        self.votes.insert(from);
        if self.configuration.quorum(&self.votes) {
            self.become_leader(actions);
        }
    }

    fn become_leader(&mut self, actions: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.votes.clear();
        self.progress.clear();
        self.pending_reads.clear();

        let entry = LogEntry {
            index: self.log.last_index() + 1,
            term: self.hard_state.current_term,
            payload: EntryPayload::Noop,
        };
        self.log.append(std::slice::from_ref(&entry));
        actions.push(Action::Persist(StorageMutation::Append(vec![
            entry.clone(),
        ])));

        let next_index = entry.index + 1;
        for voter in self.configuration.voters() {
            self.progress.insert(
                *voter,
                Progress {
                    match_index: u64::from(*voter == self.id) * entry.index,
                    next_index,
                    snapshot_offset: None,
                },
            );
        }
        // One grace window prevents a leader from stepping down before its
        // first heartbeat responses have had a chance to arrive.
        self.recent_active = self.configuration.voters().clone();
        actions.push(Action::RoleChanged {
            role: self.role,
            term: self.hard_state.current_term,
        });
        actions.push(Action::ResetHeartbeatTimer);
        self.advance_leader_commit(actions);
        self.broadcast_append(None, actions);
    }

    fn become_follower(&mut self, term: u64, leader_id: Option<NodeId>, actions: &mut Vec<Action>) {
        let was_role = self.role;
        if term > self.hard_state.current_term {
            self.hard_state.current_term = term;
            self.hard_state.voted_for = None;
            actions.push(Action::Persist(StorageMutation::HardState(self.hard_state)));
        }
        if was_role == Role::Leader || !self.pending_reads.is_empty() {
            for (context, _) in std::mem::take(&mut self.pending_reads) {
                actions.push(Action::ReadRejected {
                    context,
                    error: ReadError::LeadershipLost,
                });
            }
        }
        self.role = Role::Follower;
        self.leader_id = leader_id;
        self.campaign_term = None;
        self.votes.clear();
        self.progress.clear();
        self.recent_active.clear();
        if was_role != Role::Follower {
            actions.push(Action::RoleChanged {
                role: self.role,
                term: self.hard_state.current_term,
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        from: NodeId,
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        mut entries: Vec<LogEntry>,
        leader_commit: u64,
        read_context: Option<Vec<u8>>,
        actions: &mut Vec<Action>,
    ) {
        if leader_id != from {
            return;
        }
        if term < self.hard_state.current_term {
            self.reject_append(from, read_context, self.log.last_index() + 1, None, actions);
            return;
        }
        self.become_follower(term, Some(from), actions);
        actions.push(Action::ResetElectionTimer);

        if prev_log_index < self.log.base_index() {
            self.reject_append(
                from,
                read_context,
                self.log.base_index() + 1,
                Some(self.log.base_term()),
                actions,
            );
            return;
        }
        let Some(local_prev_term) = self.log.term(prev_log_index) else {
            self.reject_append(from, read_context, self.log.last_index() + 1, None, actions);
            return;
        };
        if local_prev_term != prev_log_term {
            self.reject_append(
                from,
                read_context,
                self.log
                    .first_index_of_term(prev_log_index, local_prev_term),
                Some(local_prev_term),
                actions,
            );
            return;
        }
        if !Self::valid_entry_sequence(prev_log_index, prev_log_term, term, &entries) {
            self.reject_append(from, read_context, prev_log_index + 1, None, actions);
            return;
        }

        let mut changed_from = None;
        for (position, entry) in entries.iter().enumerate() {
            match self.log.term(entry.index) {
                Some(local_term) if local_term == entry.term => {}
                Some(_) => {
                    if entry.index <= self.hard_state.commit_index {
                        actions.push(Action::Fatal {
                            reason: format!(
                                "leader attempted to replace committed index {}",
                                entry.index
                            ),
                        });
                        return;
                    }
                    changed_from = Some((entry.index, position));
                    break;
                }
                None => {
                    if entry.index != self.log.last_index() + 1 {
                        self.reject_append(
                            from,
                            read_context,
                            self.log.last_index() + 1,
                            None,
                            actions,
                        );
                        return;
                    }
                    changed_from = Some((entry.index, position));
                    break;
                }
            }
        }
        let matched = prev_log_index + entries.len() as u64;
        if let Some((from_index, position)) = changed_from {
            let suffix = entries.split_off(position);
            let replacing = from_index <= self.log.last_index();
            self.log.truncate_and_append(from_index, &suffix);
            let mutation = if replacing {
                StorageMutation::TruncateAndAppend {
                    from: from_index,
                    entries: suffix,
                }
            } else {
                StorageMutation::Append(suffix)
            };
            actions.push(Action::Persist(mutation));
        }

        let new_commit = leader_commit.min(self.log.last_index());
        self.set_commit(new_commit, actions);
        actions.push(Action::Send {
            to: from,
            message: Message::AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: true,
                match_index: matched,
                conflict_index: 0,
                conflict_term: None,
                read_context,
            },
        });
    }

    fn reject_append(
        &self,
        to: NodeId,
        read_context: Option<Vec<u8>>,
        conflict_index: u64,
        conflict_term: Option<u64>,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Send {
            to,
            message: Message::AppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                match_index: 0,
                conflict_index,
                conflict_term,
                read_context,
            },
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_response(
        &mut self,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        conflict_term: Option<u64>,
        read_context: Option<Vec<u8>>,
        actions: &mut Vec<Action>,
    ) {
        if term > self.hard_state.current_term {
            self.become_follower(term, None, actions);
            actions.push(Action::ResetElectionTimer);
            return;
        }
        if self.role != Role::Leader || term != self.hard_state.current_term {
            return;
        }
        self.recent_active.insert(from);
        if let Some(context) = read_context {
            self.acknowledge_read(from, &context, actions);
        }

        if success {
            if let Some(progress) = self.progress.get_mut(&from) {
                progress.match_index = progress.match_index.max(match_index);
                progress.next_index = progress.next_index.max(match_index.saturating_add(1));
                progress.snapshot_offset = None;
            }
            self.advance_leader_commit(actions);
            if match_index < self.log.last_index() {
                self.send_append(from, None, actions);
            }
        } else {
            let next_index = conflict_term
                .and_then(|term| self.log.last_index_of_term(term))
                .map_or(conflict_index, |index| index + 1)
                .max(1);
            if let Some(progress) = self.progress.get_mut(&from) {
                progress.next_index = next_index.min(progress.next_index.saturating_sub(1).max(1));
                progress.snapshot_offset = None;
            }
            self.send_append(from, None, actions);
        }
    }

    fn propose(&mut self, id: CommandId, command: Vec<u8>, actions: &mut Vec<Action>) {
        if self.role != Role::Leader {
            actions.push(Action::ProposalRejected {
                id,
                leader_hint: self.leader_id,
            });
            return;
        }
        let entry = LogEntry {
            index: self.log.last_index() + 1,
            term: self.hard_state.current_term,
            payload: EntryPayload::Command { id, bytes: command },
        };
        self.log.append(std::slice::from_ref(&entry));
        if let Some(progress) = self.progress.get_mut(&self.id) {
            progress.match_index = entry.index;
            progress.next_index = entry.index + 1;
        }
        actions.push(Action::Persist(StorageMutation::Append(vec![entry])));
        self.advance_leader_commit(actions);
        self.broadcast_append(None, actions);
    }

    fn read_index(&mut self, context: Vec<u8>, actions: &mut Vec<Action>) {
        if self.role != Role::Leader {
            actions.push(Action::ReadRejected {
                context,
                error: ReadError::NotLeader,
            });
            return;
        }
        if context.is_empty() || self.pending_reads.contains_key(&context) {
            actions.push(Action::ReadRejected {
                context,
                error: ReadError::InvalidContext,
            });
            return;
        }
        if self.log.term(self.hard_state.commit_index) != Some(self.hard_state.current_term) {
            actions.push(Action::ReadRejected {
                context,
                error: ReadError::LeaderNotReady,
            });
            return;
        }
        let mut acknowledgements = BTreeSet::new();
        acknowledgements.insert(self.id);
        let pending = PendingRead {
            index: self.hard_state.commit_index,
            acknowledgements,
        };
        self.pending_reads.insert(context.clone(), pending);
        if self
            .pending_reads
            .get(&context)
            .is_some_and(|pending| self.configuration.quorum(&pending.acknowledgements))
        {
            self.complete_read(&context, actions);
            return;
        }
        for peer in self.peers() {
            self.send_append(peer, Some(context.clone()), actions);
        }
    }

    fn acknowledge_read(&mut self, from: NodeId, context: &[u8], actions: &mut Vec<Action>) {
        let Some(pending) = self.pending_reads.get_mut(context) else {
            return;
        };
        pending.acknowledgements.insert(from);
        if self.configuration.quorum(&pending.acknowledgements) {
            self.complete_read(context, actions);
        }
    }

    fn complete_read(&mut self, context: &[u8], actions: &mut Vec<Action>) {
        if let Some(pending) = self.pending_reads.remove(context) {
            actions.push(Action::ReadReady {
                context: context.to_vec(),
                index: pending.index,
            });
        }
    }

    fn snapshot_built(&mut self, snapshot: PersistentSnapshot, actions: &mut Vec<Action>) {
        let metadata = &snapshot.metadata;
        let valid = snapshot.validate()
            && metadata.configuration == self.configuration
            && metadata.last_included_index >= self.log.base_index()
            && metadata.last_included_index <= self.applied_index
            && self.log.term(metadata.last_included_index) == Some(metadata.last_included_term);
        if !valid {
            actions.push(Action::SnapshotRejected {
                reason: "snapshot hash, membership, or applied log boundary is invalid".into(),
            });
            return;
        }
        self.log.compact(
            metadata.last_included_index,
            metadata.last_included_term,
            true,
        );
        self.snapshot = Some(snapshot.clone());
        actions.push(Action::Persist(StorageMutation::CompactSnapshot {
            snapshot,
            retained_entries: self.log.all_entries().to_vec(),
        }));
    }

    // The transport hands ownership of one decoded snapshot message to this
    // boundary. Keeping its buffers owned prevents a second payload copy.
    #[allow(
        clippy::needless_pass_by_value,
        clippy::too_many_arguments,
        clippy::too_many_lines
    )]
    fn on_install_snapshot(
        &mut self,
        from: NodeId,
        term: u64,
        leader_id: NodeId,
        metadata: SnapshotMetadata,
        offset: u64,
        data: Vec<u8>,
        done: bool,
        actions: &mut Vec<Action>,
    ) {
        if leader_id != from {
            return;
        }
        if term < self.hard_state.current_term {
            self.snapshot_reply(from, false, 0, false, 0, actions);
            return;
        }
        self.become_follower(term, Some(from), actions);
        actions.push(Action::ResetElectionTimer);

        if metadata.configuration != self.configuration
            || metadata.byte_len > MAX_SNAPSHOT_BYTES
            || metadata.last_included_index == 0
        {
            self.incoming_snapshot = None;
            self.snapshot_reply(from, false, 0, false, 0, actions);
            return;
        }
        let Ok(snapshot_capacity) = usize::try_from(metadata.byte_len) else {
            self.incoming_snapshot = None;
            self.snapshot_reply(from, false, 0, false, 0, actions);
            return;
        };
        if metadata.last_included_index <= self.applied_index {
            self.snapshot_reply(
                from,
                true,
                metadata.byte_len,
                true,
                metadata.last_included_index,
                actions,
            );
            return;
        }

        let same_transfer = self
            .incoming_snapshot
            .as_ref()
            .is_some_and(|incoming| incoming.metadata == metadata);
        if offset == 0 && !same_transfer {
            self.incoming_snapshot = Some(IncomingSnapshot {
                metadata: metadata.clone(),
                data: Vec::with_capacity(snapshot_capacity),
            });
        }
        let Some(incoming) = self.incoming_snapshot.as_mut() else {
            self.snapshot_reply(from, false, 0, false, 0, actions);
            return;
        };
        if incoming.metadata != metadata {
            self.snapshot_reply(from, false, 0, false, 0, actions);
            return;
        }

        let expected = incoming.data.len() as u64;
        if offset < expected {
            let Ok(start) = usize::try_from(offset) else {
                self.snapshot_reply(from, false, expected, false, 0, actions);
                return;
            };
            let end = start.saturating_add(data.len());
            let duplicate_matches = incoming.data.get(start..end) == Some(data.as_slice());
            self.snapshot_reply(from, duplicate_matches, expected, false, 0, actions);
            return;
        }
        if offset != expected || expected.saturating_add(data.len() as u64) > metadata.byte_len {
            self.snapshot_reply(from, false, expected, false, 0, actions);
            return;
        }
        incoming.data.extend_from_slice(&data);
        let next_offset = incoming.data.len() as u64;
        if !done {
            self.snapshot_reply(from, true, next_offset, false, 0, actions);
            return;
        }
        if next_offset != metadata.byte_len
            || <[u8; 32]>::from(Sha256::digest(&incoming.data)) != metadata.sha256
        {
            self.incoming_snapshot = None;
            self.snapshot_reply(from, false, 0, false, 0, actions);
            return;
        }

        let Some(incoming) = self.incoming_snapshot.take() else {
            actions.push(Action::Fatal {
                reason: "completed snapshot transfer disappeared before install".into(),
            });
            return;
        };
        let snapshot = PersistentSnapshot {
            metadata: incoming.metadata,
            data: incoming.data,
        };
        let retain_suffix =
            self.log.term(metadata.last_included_index) == Some(metadata.last_included_term);
        self.log.compact(
            metadata.last_included_index,
            metadata.last_included_term,
            retain_suffix,
        );
        self.snapshot = Some(snapshot.clone());
        self.hard_state.commit_index = self
            .hard_state
            .commit_index
            .max(metadata.last_included_index);
        self.applied_index = metadata.last_included_index;
        let retained_entries = self.log.all_entries().to_vec();
        actions.push(Action::Persist(StorageMutation::InstallSnapshot {
            snapshot,
            retained_entries,
            hard_state: self.hard_state,
        }));
        self.snapshot_reply(
            from,
            true,
            metadata.byte_len,
            true,
            metadata.last_included_index,
            actions,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn on_snapshot_response(
        &mut self,
        from: NodeId,
        term: u64,
        accepted: bool,
        next_offset: u64,
        done: bool,
        match_index: u64,
        actions: &mut Vec<Action>,
    ) {
        if term > self.hard_state.current_term {
            self.become_follower(term, None, actions);
            actions.push(Action::ResetElectionTimer);
            return;
        }
        if self.role != Role::Leader || term != self.hard_state.current_term {
            return;
        }
        self.recent_active.insert(from);
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        if done && accepted {
            progress.match_index = progress.match_index.max(match_index);
            progress.next_index = progress.next_index.max(match_index + 1);
            progress.snapshot_offset = None;
            self.advance_leader_commit(actions);
        } else {
            progress.snapshot_offset = Some(if accepted { next_offset } else { 0 });
        }
        self.send_append(from, None, actions);
    }

    fn snapshot_reply(
        &self,
        to: NodeId,
        accepted: bool,
        next_offset: u64,
        done: bool,
        match_index: u64,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::Send {
            to,
            message: Message::InstallSnapshotResponse {
                term: self.hard_state.current_term,
                accepted,
                next_offset,
                done,
                match_index,
            },
        });
    }

    fn set_commit(&mut self, new_commit: u64, actions: &mut Vec<Action>) {
        if new_commit <= self.hard_state.commit_index {
            return;
        }
        if new_commit > self.log.last_index() {
            actions.push(Action::Fatal {
                reason: format!(
                    "attempted to commit {new_commit} beyond log end {}",
                    self.log.last_index()
                ),
            });
            return;
        }
        self.hard_state.commit_index = new_commit;
        actions.push(Action::Persist(StorageMutation::HardState(self.hard_state)));
        self.apply_committed(actions);
    }

    fn advance_leader_commit(&mut self, actions: &mut Vec<Action>) {
        if self.role != Role::Leader {
            return;
        }
        let quorum_index = self.configuration.quorum_index(|voter| {
            self.progress
                .get(&voter)
                .map_or(0, |progress| progress.match_index)
        });
        if quorum_index > self.hard_state.commit_index
            && self.log.term(quorum_index) == Some(self.hard_state.current_term)
        {
            self.set_commit(quorum_index, actions);
        }
    }

    fn apply_committed(&mut self, actions: &mut Vec<Action>) {
        while self.applied_index < self.hard_state.commit_index {
            let next = self.applied_index + 1;
            let Some(entry) = self.log.entry(next).cloned() else {
                actions.push(Action::Fatal {
                    reason: format!("committed entry {next} is unavailable for apply"),
                });
                return;
            };
            self.applied_index = next;
            actions.push(Action::Apply(entry.clone()));
            if self.role == Role::Leader {
                if let EntryPayload::Command { id, .. } = entry.payload {
                    actions.push(Action::ProposalCommitted {
                        id,
                        index: entry.index,
                    });
                }
            }
        }
    }

    // Each peer message owns its ReadIndex context, so this boundary accepts
    // ownership and clones only when broadcasting to multiple peers.
    #[allow(clippy::needless_pass_by_value)]
    fn broadcast_append(&mut self, read_context: Option<Vec<u8>>, actions: &mut Vec<Action>) {
        for peer in self.peers() {
            self.send_append(peer, read_context.clone(), actions);
        }
    }

    fn send_append(
        &mut self,
        peer: NodeId,
        read_context: Option<Vec<u8>>,
        actions: &mut Vec<Action>,
    ) {
        let Some(progress) = self.progress.get(&peer).cloned() else {
            return;
        };
        if progress.next_index <= self.log.base_index() {
            self.send_snapshot_chunk(peer, progress.snapshot_offset.unwrap_or(0), actions);
            return;
        }
        let next_index = progress.next_index.min(self.log.last_index() + 1);
        let prev_log_index = next_index - 1;
        let Some(prev_log_term) = self.log.term(prev_log_index) else {
            actions.push(Action::Fatal {
                reason: format!("missing previous log term at {prev_log_index}"),
            });
            return;
        };
        actions.push(Action::Send {
            to: peer,
            message: Message::AppendEntries {
                term: self.hard_state.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries: self.log.entries_from(next_index, MAX_APPEND_ENTRIES),
                leader_commit: self.hard_state.commit_index,
                read_context,
            },
        });
    }

    fn send_snapshot_chunk(&self, peer: NodeId, offset: u64, actions: &mut Vec<Action>) {
        let Some(snapshot) = &self.snapshot else {
            actions.push(Action::Fatal {
                reason: format!(
                    "peer {peer} requires index at/before compacted boundary, but no snapshot exists"
                ),
            });
            return;
        };
        let offset = offset.min(snapshot.data.len() as u64);
        let Ok(start) = usize::try_from(offset) else {
            actions.push(Action::Fatal {
                reason: format!("snapshot offset {offset} does not fit this platform"),
            });
            return;
        };
        let end = (start + SNAPSHOT_CHUNK_BYTES).min(snapshot.data.len());
        actions.push(Action::Send {
            to: peer,
            message: Message::InstallSnapshot {
                term: self.hard_state.current_term,
                leader_id: self.id,
                metadata: snapshot.metadata.clone(),
                offset,
                data: snapshot.data[start..end].to_vec(),
                done: end == snapshot.data.len(),
            },
        });
    }

    fn peers(&self) -> Vec<NodeId> {
        self.configuration
            .voters()
            .iter()
            .copied()
            .filter(|id| *id != self.id)
            .collect()
    }

    fn candidate_log_is_up_to_date(&self, index: u64, term: u64) -> bool {
        term > self.log.last_term()
            || (term == self.log.last_term() && index >= self.log.last_index())
    }

    fn valid_entry_sequence(
        prev_log_index: u64,
        prev_log_term: u64,
        leader_term: u64,
        entries: &[LogEntry],
    ) -> bool {
        let mut previous_term = prev_log_term;
        for (expected, entry) in (prev_log_index + 1..).zip(entries.iter()) {
            if entry.index != expected || entry.term > leader_term || entry.term < previous_term {
                return false;
            }
            previous_term = entry.term;
        }
        true
    }
}

//! Narrow runtime boundaries for driving the pure consensus core.
//!
//! [`execute`] preserves the order returned by [`crate::Raft::step`]. In
//! particular, a storage error aborts the batch before any later packet or
//! client acknowledgement can escape.

use std::fmt::Display;

use thiserror::Error;

use crate::{Action, CommandId, LogEntry, Message, NodeId, ReadError, Role, StorageMutation};

pub trait StableStorageAdapter {
    type Error: Display;

    /// Durably completes the mutation (including the required flush) before
    /// returning. Snapshot installation must be atomic with the state-machine
    /// checkpoint named by the mutation.
    fn persist(&mut self, mutation: &StorageMutation) -> Result<(), Self::Error>;
}

pub trait NetworkAdapter {
    type Error: Display;

    fn send(&mut self, to: NodeId, message: &Message) -> Result<(), Self::Error>;
}

pub trait ClockAdapter {
    type Error: Display;

    /// Picks and arms a fresh randomized election deadline.
    fn reset_election(&mut self) -> Result<(), Self::Error>;

    fn reset_heartbeat(&mut self) -> Result<(), Self::Error>;
}

pub trait StateMachineAdapter {
    type Error: Display;

    /// Applies one committed entry durably and idempotently by log index.
    fn apply(&mut self, entry: &LogEntry) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    RoleChanged {
        role: Role,
        term: u64,
    },
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
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AdapterError {
    #[error("stable storage failed: {0}")]
    Storage(String),
    #[error("network send failed: {0}")]
    Network(String),
    #[error("timer operation failed: {0}")]
    Clock(String),
    #[error("state-machine apply failed: {0}")]
    StateMachine(String),
    #[error("Raft fenced itself after an invariant failure: {0}")]
    Fatal(String),
}

/// Executes one ordered action batch and returns events intended for the
/// client/session layer.
pub fn execute<S, N, C, M>(
    actions: &[Action],
    storage: &mut S,
    network: &mut N,
    clock: &mut C,
    state_machine: &mut M,
) -> Result<Vec<ClientEvent>, AdapterError>
where
    S: StableStorageAdapter,
    N: NetworkAdapter,
    C: ClockAdapter,
    M: StateMachineAdapter,
{
    let mut events = Vec::new();
    for action in actions {
        match action {
            Action::Persist(mutation) => storage
                .persist(mutation)
                .map_err(|error| AdapterError::Storage(error.to_string()))?,
            Action::Send { to, message } => network
                .send(*to, message)
                .map_err(|error| AdapterError::Network(error.to_string()))?,
            Action::ResetElectionTimer => clock
                .reset_election()
                .map_err(|error| AdapterError::Clock(error.to_string()))?,
            Action::ResetHeartbeatTimer => clock
                .reset_heartbeat()
                .map_err(|error| AdapterError::Clock(error.to_string()))?,
            Action::Apply(entry) => state_machine
                .apply(entry)
                .map_err(|error| AdapterError::StateMachine(error.to_string()))?,
            Action::RoleChanged { role, term } => {
                events.push(ClientEvent::RoleChanged {
                    role: *role,
                    term: *term,
                });
            }
            Action::ProposalCommitted { id, index } => {
                events.push(ClientEvent::ProposalCommitted {
                    id: *id,
                    index: *index,
                });
            }
            Action::ProposalRejected { id, leader_hint } => {
                events.push(ClientEvent::ProposalRejected {
                    id: *id,
                    leader_hint: *leader_hint,
                });
            }
            Action::ReadReady { context, index } => events.push(ClientEvent::ReadReady {
                context: context.clone(),
                index: *index,
            }),
            Action::ReadRejected { context, error } => {
                events.push(ClientEvent::ReadRejected {
                    context: context.clone(),
                    error: error.clone(),
                });
            }
            Action::SnapshotRejected { reason } => {
                events.push(ClientEvent::SnapshotRejected {
                    reason: reason.clone(),
                });
            }
            Action::Fatal { reason } => return Err(AdapterError::Fatal(reason.clone())),
        }
    }
    Ok(events)
}

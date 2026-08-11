//! A deterministic implementation of the Raft consensus algorithm.
//!
//! [`Raft::step`] contains no I/O and reads no wall clock. Every durable write,
//! packet, timer reset, state-machine application, and client completion is an
//! ordered [`Action`]. An embedding must execute actions in returned order and
//! stop before later actions if persistence fails. This makes the critical
//! rule "persist before send" explicit and lets the same core run in production
//! and in the seeded adversarial [`simulator`] without mock timing hooks.
//!
//! The core implements pre-vote, election safety, conflict-term backtracking,
//! current-term-only leader commit, leader no-op entries, check-quorum,
//! quorum-barrier reads, ordered application, and chunked/hashed snapshots.
//! Membership is fixed for a node's lifetime, but that fixed value may be an
//! explicit joint old/new configuration requiring both majorities. Unsafe
//! one-step membership replacement is not exposed.

mod log;
mod node;
mod types;

pub mod adapter;
pub mod simulator;

pub use aster_core::NodeId;
pub use node::Raft;
pub use types::{
    Action, CommandId, Configuration, EntryPayload, HardState, Input, LogEntry, Message,
    PersistentSnapshot, ReadError, RecoveryError, Role, SnapshotMetadata, StableState,
    StorageMutation, Tick,
};

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use aster_raft::simulator::{FaultProfile, SimEvent, Simulator};
use aster_raft::{
    Action, Configuration, EntryPayload, HardState, Input, LogEntry, Message, PersistentSnapshot,
    Raft, ReadError, Role, StableState, StorageMutation, Tick,
};

fn config(voters: &[u64]) -> Configuration {
    Configuration::new(voters.iter().copied()).expect("non-empty voter set")
}

#[test]
fn single_node_persists_vote_and_noop_before_commit() {
    let mut node = Raft::recover(1, config(&[1]), StableState::default()).unwrap();
    let actions = node.step(Input::Tick(Tick::Election));

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.term(), 1);
    assert_eq!(node.commit_index(), 1);
    assert_eq!(node.applied_index(), 1);

    let hard_vote = actions
        .iter()
        .position(|action| {
            matches!(
                action,
                Action::Persist(StorageMutation::HardState(HardState {
                    current_term: 1,
                    voted_for: Some(1),
                    commit_index: 0,
                }))
            )
        })
        .unwrap();
    let noop = actions
        .iter()
        .position(|action| {
            matches!(
                action,
                Action::Persist(StorageMutation::Append(entries))
                    if entries.len() == 1 && entries[0].payload == EntryPayload::Noop
            )
        })
        .unwrap();
    let hard_commit = actions
        .iter()
        .position(|action| {
            matches!(
                action,
                Action::Persist(StorageMutation::HardState(HardState {
                    current_term: 1,
                    voted_for: Some(1),
                    commit_index: 1,
                }))
            )
        })
        .unwrap();
    let apply = actions
        .iter()
        .position(|action| matches!(action, Action::Apply(entry) if entry.index == 1))
        .unwrap();
    assert!(hard_vote < noop && noop < hard_commit && hard_commit < apply);

    let read = node.step(Input::ReadIndex {
        context: b"single-node-read".to_vec(),
    });
    assert!(read.iter().any(|action| matches!(
        action,
        Action::ReadReady { context, index: 1 } if context == b"single-node-read"
    )));
}

#[test]
fn recovering_commit_replays_before_any_protocol_response() {
    let entry = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            id: 9,
            bytes: b"durable".to_vec(),
        },
    };
    let stable = StableState {
        hard_state: HardState {
            current_term: 2,
            voted_for: None,
            commit_index: 1,
        },
        snapshot: None,
        entries: vec![entry],
        applied_index: 0,
    };
    let mut node = Raft::recover(1, config(&[1, 2, 3]), stable).unwrap();
    let actions = node.step(Input::Message {
        from: 2,
        message: Message::RequestVote {
            term: 2,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 1,
        },
    });
    assert!(matches!(actions.first(), Some(Action::Apply(entry)) if entry.index == 1));
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Send { to: 2, .. }))
    );
}

#[test]
fn vote_requires_an_up_to_date_log_and_is_stable() {
    let stable = StableState {
        hard_state: HardState {
            current_term: 2,
            voted_for: None,
            commit_index: 0,
        },
        snapshot: None,
        entries: vec![
            LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop,
            },
            LogEntry {
                index: 2,
                term: 2,
                payload: EntryPayload::Noop,
            },
        ],
        applied_index: 0,
    };
    let mut node = Raft::recover(1, config(&[1, 2, 3]), stable).unwrap();
    let stale_response = node.step(Input::Message {
        from: 2,
        message: Message::RequestVote {
            term: 3,
            candidate_id: 2,
            last_log_index: 99,
            last_log_term: 1,
        },
    });
    assert!(stale_response.iter().any(|action| matches!(
        action,
        Action::Send {
            to: 2,
            message: Message::RequestVoteResponse {
                term: 3,
                granted: false
            }
        }
    )));

    let granted = node.step(Input::Message {
        from: 3,
        message: Message::RequestVote {
            term: 3,
            candidate_id: 3,
            last_log_index: 2,
            last_log_term: 2,
        },
    });
    assert!(granted.iter().any(|action| matches!(
        action,
        Action::Persist(StorageMutation::HardState(HardState {
            current_term: 3,
            voted_for: Some(3),
            ..
        }))
    )));
}

#[test]
fn append_rejection_returns_first_index_of_conflicting_term() {
    let stable = StableState {
        hard_state: HardState {
            current_term: 3,
            voted_for: None,
            commit_index: 1,
        },
        snapshot: None,
        entries: vec![
            LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop,
            },
            LogEntry {
                index: 2,
                term: 2,
                payload: EntryPayload::Noop,
            },
            LogEntry {
                index: 3,
                term: 2,
                payload: EntryPayload::Noop,
            },
        ],
        applied_index: 1,
    };
    let mut node = Raft::recover(1, config(&[1, 2, 3]), stable).unwrap();
    let actions = node.step(Input::Message {
        from: 2,
        message: Message::AppendEntries {
            term: 3,
            leader_id: 2,
            prev_log_index: 3,
            prev_log_term: 1,
            entries: Vec::new(),
            leader_commit: 1,
            read_context: Some(b"barrier".to_vec()),
        },
    });
    assert!(actions.iter().any(|action| matches!(
        action,
        Action::Send {
            to: 2,
            message: Message::AppendEntriesResponse {
                success: false,
                conflict_index: 2,
                conflict_term: Some(2),
                read_context: Some(context),
                ..
            }
        } if context == b"barrier"
    )));
}

#[test]
fn corrupt_snapshot_chunk_never_reaches_persistence() {
    let configuration = config(&[1, 2, 3]);
    let mut node = Raft::recover(1, configuration.clone(), StableState::default()).unwrap();
    let valid = PersistentSnapshot::new(5, 2, configuration, b"correct bytes".to_vec());
    let actions = node.step(Input::Message {
        from: 2,
        message: Message::InstallSnapshot {
            term: 2,
            leader_id: 2,
            metadata: valid.metadata,
            offset: 0,
            data: b"tampered!!!!!".to_vec(),
            done: true,
        },
    });
    assert!(!actions.iter().any(|action| matches!(
        action,
        Action::Persist(StorageMutation::InstallSnapshot { .. })
    )));
    assert!(actions.iter().any(|action| matches!(
        action,
        Action::Send {
            message: Message::InstallSnapshotResponse {
                accepted: false,
                ..
            },
            ..
        }
    )));
}

#[test]
fn reliable_cluster_commits_and_agrees_on_state_hash() {
    let mut sim = Simulator::new(7, [1, 2, 3], FaultProfile::reliable()).unwrap();
    sim.campaign(1).unwrap();
    let (leader, _) = sim.run_until_leader(30).unwrap().expect("leader");
    sim.run(8).unwrap();
    sim.propose(leader, 42, b"alpha".to_vec()).unwrap();
    sim.run(16).unwrap();

    let applied = [1, 2, 3]
        .into_iter()
        .map(|node| sim.applied_index(node).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(applied.len(), 1);
    let hashes = [1, 2, 3]
        .into_iter()
        .map(|node| sim.state_hash(node).unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(hashes.len(), 1);
    assert!(
        sim.events().iter().any(
            |event| matches!(event, SimEvent::Committed { id: 42, node, .. } if *node == leader)
        )
    );
}

#[test]
fn isolated_leader_cannot_acknowledge_and_steps_down() {
    let mut sim = Simulator::new(11, [1, 2, 3], FaultProfile::reliable()).unwrap();
    sim.campaign(1).unwrap();
    let (leader, _) = sim.run_until_leader(30).unwrap().expect("leader");
    sim.run(8).unwrap();
    let baseline = sim.committed_index(leader).unwrap();
    sim.take_events();
    sim.isolate(leader);
    sim.propose(leader, 777, b"minority".to_vec()).unwrap();
    sim.read_index(leader, b"no-quorum".to_vec()).unwrap();
    sim.run(50).unwrap();

    assert_eq!(sim.committed_index(leader), Some(baseline));
    assert!(
        !sim.events()
            .iter()
            .any(|event| matches!(event, SimEvent::Committed { id: 777, .. }))
    );
    assert!(!sim.events().iter().any(
        |event| matches!(event, SimEvent::ReadReady { context, .. } if context == b"no-quorum")
    ));
    assert!(!sim.leaders().iter().any(|(node, _)| *node == leader));
}

#[test]
fn committed_entry_survives_leader_crash_and_failover() {
    let mut sim = Simulator::new(19, [1, 2, 3, 4, 5], FaultProfile::reliable()).unwrap();
    let (leader, _) = sim.run_until_leader(80).unwrap().expect("leader");
    sim.propose(leader, 501, b"survives".to_vec()).unwrap();
    sim.run(20).unwrap();
    assert!(sim.events().iter().any(
        |event| matches!(event, SimEvent::Committed { id: 501, node, .. } if *node == leader)
    ));
    sim.crash(leader).unwrap();
    let (replacement, _) = sim.run_until_leader(100).unwrap().expect("replacement");
    assert_ne!(replacement, leader);
    sim.restart(leader).unwrap();
    sim.run(50).unwrap();

    let live = sim.live_nodes();
    let max_applied = live
        .iter()
        .map(|node| sim.applied_index(*node).unwrap())
        .max()
        .unwrap();
    for node in live {
        assert_eq!(sim.applied_index(node), Some(max_applied));
    }
    sim.assert_invariants().unwrap();
}

#[test]
fn lagging_follower_catches_up_via_hashed_snapshot() {
    let mut sim = Simulator::new(23, [1, 2, 3], FaultProfile::reliable()).unwrap();
    let (leader, _) = sim.run_until_leader(60).unwrap().expect("leader");
    // Choose either non-leader deterministically without depending on leader ID.
    let lagger = [1, 2, 3].into_iter().find(|node| *node != leader).unwrap();
    sim.crash(lagger).unwrap();
    for id in 1..=12_u128 {
        sim.propose(leader, 10_000 + id, id.to_le_bytes().to_vec())
            .unwrap();
        sim.run(3).unwrap();
    }
    sim.run(10).unwrap();
    let snapshot_index = sim.applied_index(leader).unwrap();
    sim.snapshot(leader, snapshot_index).unwrap();
    sim.discard_queued_messages();
    sim.restart(lagger).unwrap();
    sim.run(100).unwrap();

    assert_eq!(sim.applied_index(lagger), sim.applied_index(leader));
    assert_eq!(sim.state_hash(lagger), sim.state_hash(leader));
    assert!(
        sim.durable_state(lagger)
            .unwrap()
            .snapshot
            .as_ref()
            .is_some_and(PersistentSnapshot::validate)
    );
}

#[test]
fn linearizable_read_needs_a_quorum_echo() {
    let mut sim = Simulator::new(31, [1, 2, 3], FaultProfile::reliable()).unwrap();
    let (leader, _) = sim.run_until_leader(60).unwrap().expect("leader");
    sim.run(10).unwrap();
    sim.take_events();
    sim.read_index(leader, b"read-ok".to_vec()).unwrap();
    sim.run(8).unwrap();
    assert!(sim.events().iter().any(|event| matches!(
        event,
        SimEvent::ReadReady { node, context, .. }
            if *node == leader && context == b"read-ok"
    )));
}

#[test]
fn many_hostile_seeds_preserve_all_safety_invariants() {
    let started = Instant::now();
    for seed in 0..8_u64 {
        let mut sim = Simulator::new(seed, [1, 2, 3, 4, 5], FaultProfile::hostile())
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
        sim.set_step_budget(240);
        sim.run_chaos(120)
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
        sim.assert_invariants()
            .unwrap_or_else(|error| panic!("seed {seed}: {error}"));
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "hostile seed sweep exceeded 15-second CI wall budget after seed {seed}"
        );
    }
}

#[test]
fn read_before_current_term_noop_commit_is_rejected() {
    let stable = StableState {
        hard_state: HardState {
            current_term: 4,
            voted_for: None,
            commit_index: 1,
        },
        snapshot: None,
        entries: vec![LogEntry {
            index: 1,
            term: 3,
            payload: EntryPayload::Noop,
        }],
        applied_index: 1,
    };
    let mut node = Raft::recover(1, config(&[1, 2, 3]), stable).unwrap();
    let actions = node.step(Input::Tick(Tick::Election));
    let prospective = actions.iter().find_map(|action| match action {
        Action::Send {
            message: Message::PreVoteRequest {
                prospective_term, ..
            },
            ..
        } => Some(*prospective_term),
        _ => None,
    });
    assert_eq!(prospective, Some(5));
    let _ = node.step(Input::Message {
        from: 2,
        message: Message::PreVoteResponse {
            responder_term: 4,
            prospective_term: 5,
            granted: true,
        },
    });
    let _ = node.step(Input::Message {
        from: 2,
        message: Message::RequestVoteResponse {
            term: 5,
            granted: true,
        },
    });
    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.commit_index(), 1);
    let rejected = node.step(Input::ReadIndex {
        context: b"not-ready".to_vec(),
    });
    assert!(rejected.iter().any(|action| matches!(
        action,
        Action::ReadRejected {
            error: ReadError::LeaderNotReady,
            ..
        }
    )));
}

#[test]
fn joint_configuration_requires_both_majorities_for_election_and_commit() {
    let joint = Configuration::joint([1, 2, 3], [3, 4, 5]).unwrap();
    let old_only = BTreeSet::from([1, 2, 3]);
    let both = BTreeSet::from([1, 3, 4]);
    assert!(!joint.quorum(&old_only));
    assert!(joint.quorum(&both));

    let mut no_new_majority =
        Simulator::with_configuration(41, joint.clone(), FaultProfile::reliable()).unwrap();
    no_new_majority.isolate(4);
    no_new_majority.isolate(5);
    no_new_majority.campaign(1).unwrap();
    no_new_majority.run(35).unwrap();
    assert!(no_new_majority.leaders().is_empty());

    let mut sim = Simulator::with_configuration(43, joint, FaultProfile::reliable()).unwrap();
    sim.campaign(1).unwrap();
    let (leader, _) = sim.run_until_leader(40).unwrap().expect("joint leader");
    sim.run(8).unwrap();
    let baseline = sim.committed_index(leader).unwrap();
    sim.isolate(4);
    sim.isolate(5);
    sim.propose(leader, 88_001, b"needs-new-majority".to_vec())
        .unwrap();
    sim.run(12).unwrap();
    assert_eq!(sim.committed_index(leader), Some(baseline));
    assert!(
        !sim.events()
            .iter()
            .any(|event| matches!(event, SimEvent::Committed { id: 88_001, .. }))
    );
}

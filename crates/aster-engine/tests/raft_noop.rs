use aster_engine::{ApplyOutcome, Engine};

#[test]
fn raft_noop_advances_snapshot_timestamp_and_is_idempotent() {
    let mut engine = Engine::new();
    let hash = [9; 32];
    assert_eq!(engine.apply_noop(1, hash).unwrap(), ApplyOutcome::Noop);
    assert_eq!(engine.last_applied(), 1);
    assert_eq!(engine.apply_noop(1, hash).unwrap(), ApplyOutcome::Noop);
    assert!(engine.apply_noop(1, [8; 32]).is_err());

    let mut reopened = Engine::from_snapshot(engine.snapshot()).unwrap();
    assert_eq!(reopened.apply_noop(1, hash).unwrap(), ApplyOutcome::Noop);
    assert_eq!(reopened.last_applied(), 1);
}

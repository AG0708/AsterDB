use aster_core::{ClientRequestId, Column, DataType, Row, Schema, Value};
use aster_engine::{
    AbortReason, ApplyOutcome, BinaryOp, CommitResult, Engine, EngineError, Expr, OrderBy, Query,
    RequestRejection, ScanSource, UnaryOp,
};

fn schema() -> Schema {
    Schema {
        columns: vec![
            Column {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                primary_key: true,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Text,
                nullable: false,
                primary_key: false,
            },
            Column {
                name: "active".into(),
                data_type: DataType::Bool,
                nullable: false,
                primary_key: false,
            },
        ],
    }
}

fn row(id: i64, name: &str, active: bool) -> Row {
    Row {
        values: vec![
            Value::Int64(id),
            Value::Text(name.into()),
            Value::Bool(active),
        ],
    }
}

fn request(client: u8, sequence: u64) -> ClientRequestId {
    ClientRequestId {
        client_id: [client; 16],
        sequence,
    }
}

fn apply(
    engine: &mut Engine,
    transaction: aster_engine::Transaction,
    client: u8,
    sequence: u64,
) -> (ApplyOutcome, aster_engine::TxnAttempt, [u8; 32]) {
    let attempt = transaction.into_attempt(request(client, sequence), 1);
    let hash = attempt.command_hash().unwrap();
    let outcome = engine
        .apply(engine.last_applied() + 1, hash, &attempt)
        .unwrap();
    (outcome, attempt, hash)
}

fn bootstrap() -> (Engine, u64) {
    let mut engine = Engine::new();
    let mut transaction = engine.begin().unwrap();
    let table_id = engine
        .create_table(&mut transaction, "accounts", schema())
        .unwrap();
    let (outcome, _, _) = apply(&mut engine, transaction, 1, 1);
    assert!(matches!(
        outcome,
        ApplyOutcome::Applied(CommitResult::Committed {
            commit_index: 1,
            ..
        })
    ));
    (engine, table_id)
}

fn seed(engine: &mut Engine, table_id: u64, client: u8, rows: &[Row]) {
    let mut transaction = engine.begin().unwrap();
    for row in rows {
        engine
            .insert(&mut transaction, table_id, row.clone())
            .unwrap();
    }
    let (outcome, _, _) = apply(engine, transaction, client, 1);
    assert!(matches!(
        outcome,
        ApplyOutcome::Applied(CommitResult::Committed { .. })
    ));
}

#[test]
fn read_your_writes_and_rollback_are_private() {
    let (mut engine, table_id) = bootstrap();
    let mut transaction = engine.begin().unwrap();
    engine
        .insert(&mut transaction, table_id, row(7, "private", true))
        .unwrap();
    assert_eq!(
        engine
            .get(&transaction, table_id, &Value::Int64(7))
            .unwrap(),
        Some(row(7, "private", true))
    );
    assert_eq!(engine.scan_table(&transaction, table_id).unwrap().len(), 1);
    transaction.rollback();

    let observer = engine.begin().unwrap();
    assert_eq!(
        engine.get(&observer, table_id, &Value::Int64(7)).unwrap(),
        None
    );
    assert!(engine.scan_table(&observer, table_id).unwrap().is_empty());
}

#[test]
fn fixed_snapshot_prevents_nonrepeatable_reads_and_phantoms() {
    let (mut engine, table_id) = bootstrap();
    seed(&mut engine, table_id, 2, &[row(1, "before", true)]);
    let reader = engine.begin().unwrap();
    assert_eq!(engine.scan_table(&reader, table_id).unwrap().len(), 1);

    let mut writer = engine.begin().unwrap();
    assert!(
        engine
            .update(
                &mut writer,
                table_id,
                &Value::Int64(1),
                row(1, "after", false),
            )
            .unwrap()
    );
    engine
        .insert(&mut writer, table_id, row(2, "phantom", true))
        .unwrap();
    apply(&mut engine, writer, 3, 1);

    assert_eq!(
        engine.get(&reader, table_id, &Value::Int64(1)).unwrap(),
        Some(row(1, "before", true))
    );
    assert_eq!(engine.scan_table(&reader, table_id).unwrap().len(), 1);
    let fresh = engine.begin().unwrap();
    assert_eq!(engine.scan_table(&fresh, table_id).unwrap().len(), 2);
}

#[test]
fn concurrent_same_key_update_aborts_loser() {
    let (mut engine, table_id) = bootstrap();
    seed(&mut engine, table_id, 2, &[row(1, "original", true)]);
    let mut first = engine.begin().unwrap();
    let mut second = engine.begin().unwrap();
    engine
        .update(
            &mut first,
            table_id,
            &Value::Int64(1),
            row(1, "winner", true),
        )
        .unwrap();
    engine
        .update(
            &mut second,
            table_id,
            &Value::Int64(1),
            row(1, "loser", true),
        )
        .unwrap();
    assert!(matches!(
        apply(&mut engine, first, 3, 1).0,
        ApplyOutcome::Applied(CommitResult::Committed { .. })
    ));
    assert!(matches!(
        apply(&mut engine, second, 4, 1).0,
        ApplyOutcome::Applied(CommitResult::Aborted {
            reason: AbortReason::WriteConflict {
                winning_commit: 3,
                ..
            },
            ..
        })
    ));

    let reader = engine.begin().unwrap();
    assert_eq!(
        engine.get(&reader, table_id, &Value::Int64(1)).unwrap(),
        Some(row(1, "winner", true))
    );
}

#[test]
fn write_skew_is_deliberately_allowed_under_snapshot_isolation() {
    let (mut engine, table_id) = bootstrap();
    seed(
        &mut engine,
        table_id,
        2,
        &[row(1, "doctor-a", true), row(2, "doctor-b", true)],
    );
    let mut first = engine.begin().unwrap();
    let mut second = engine.begin().unwrap();
    assert_eq!(
        engine
            .scan_table(&first, table_id)
            .unwrap()
            .iter()
            .filter(|row| row.values[2] == Value::Bool(true))
            .count(),
        2
    );
    assert_eq!(engine.scan_table(&second, table_id).unwrap().len(), 2);
    engine
        .update(
            &mut first,
            table_id,
            &Value::Int64(1),
            row(1, "doctor-a", false),
        )
        .unwrap();
    engine
        .update(
            &mut second,
            table_id,
            &Value::Int64(2),
            row(2, "doctor-b", false),
        )
        .unwrap();
    assert!(matches!(
        apply(&mut engine, first, 3, 1).0,
        ApplyOutcome::Applied(CommitResult::Committed { .. })
    ));
    assert!(matches!(
        apply(&mut engine, second, 4, 1).0,
        ApplyOutcome::Applied(CommitResult::Committed { .. })
    ));
    let reader = engine.begin().unwrap();
    assert!(
        engine
            .scan_table(&reader, table_id)
            .unwrap()
            .iter()
            .all(|row| row.values[2] == Value::Bool(false))
    );
}

#[test]
fn concurrent_schema_change_aborts_old_writer() {
    let (mut engine, table_id) = bootstrap();
    seed(&mut engine, table_id, 2, &[row(1, "one", true)]);
    let mut old_writer = engine.begin().unwrap();
    engine
        .update(
            &mut old_writer,
            table_id,
            &Value::Int64(1),
            row(1, "stale schema", false),
        )
        .unwrap();

    let mut ddl = engine.begin().unwrap();
    let index_id = engine
        .create_index(&mut ddl, "accounts_by_name", table_id, 1)
        .unwrap();
    assert!(matches!(
        apply(&mut engine, ddl, 3, 1).0,
        ApplyOutcome::Applied(CommitResult::Committed {
            schema_epoch: 2,
            ..
        })
    ));
    let index_reader = engine.begin().unwrap();
    assert_eq!(
        engine
            .scan_secondary(&index_reader, index_id, &Value::Text("one".into()))
            .unwrap(),
        vec![row(1, "one", true)]
    );
    assert!(matches!(
        apply(&mut engine, old_writer, 4, 1).0,
        ApplyOutcome::Applied(CommitResult::Aborted {
            reason: AbortReason::SchemaChanged {
                expected: 1,
                actual: 2,
            },
            ..
        })
    ));
}

#[test]
fn secondary_versions_respect_snapshot_and_own_overlay() {
    let (mut engine, table_id) = bootstrap();
    seed(&mut engine, table_id, 2, &[row(1, "old", true)]);
    let mut ddl = engine.begin().unwrap();
    let index_id = engine
        .create_index(&mut ddl, "accounts_by_name", table_id, 1)
        .unwrap();
    apply(&mut engine, ddl, 3, 1);
    let old_reader = engine.begin().unwrap();

    let mut writer = engine.begin().unwrap();
    engine
        .update(&mut writer, table_id, &Value::Int64(1), row(1, "new", true))
        .unwrap();
    assert!(
        engine
            .scan_secondary(&writer, index_id, &Value::Text("old".into()))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        engine
            .scan_secondary(&writer, index_id, &Value::Text("new".into()))
            .unwrap(),
        vec![row(1, "new", true)]
    );
    apply(&mut engine, writer, 4, 1);

    assert_eq!(
        engine
            .scan_secondary(&old_reader, index_id, &Value::Text("old".into()))
            .unwrap(),
        vec![row(1, "old", true)]
    );
    let fresh = engine.begin().unwrap();
    assert!(
        engine
            .scan_secondary(&fresh, index_id, &Value::Text("old".into()))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        engine
            .scan_secondary(&fresh, index_id, &Value::Text("new".into()))
            .unwrap(),
        vec![row(1, "new", true)]
    );
}

#[test]
fn latest_request_is_idempotent_and_sequence_rules_are_monotonic() {
    let (mut engine, table_id) = bootstrap();
    let mut transaction = engine.begin().unwrap();
    engine
        .insert(&mut transaction, table_id, row(1, "once", true))
        .unwrap();
    let (first, attempt, hash) = apply(&mut engine, transaction, 9, 1);
    let versions_after_first = engine.stats().primary_versions;

    assert_eq!(
        engine.apply(engine.last_applied(), hash, &attempt).unwrap(),
        first
    );
    assert!(matches!(
        engine
            .apply(engine.last_applied() + 1, hash, &attempt)
            .unwrap(),
        ApplyOutcome::Duplicate(CommitResult::Committed {
            commit_index: 2,
            ..
        })
    ));
    assert_eq!(engine.stats().primary_versions, versions_after_first);

    let gap = engine.begin().unwrap().into_attempt(request(9, 3), 1);
    assert_eq!(
        engine
            .apply(engine.last_applied() + 1, gap.command_hash().unwrap(), &gap)
            .unwrap(),
        ApplyOutcome::Rejected(RequestRejection::SequenceGap {
            expected: 2,
            actual: 3,
        })
    );

    let different = engine.begin().unwrap().into_attempt(request(9, 1), 2);
    assert_eq!(
        engine
            .apply(
                engine.last_applied() + 1,
                different.command_hash().unwrap(),
                &different,
            )
            .unwrap(),
        ApplyOutcome::Rejected(RequestRejection::SequenceHashMismatch { sequence: 1 })
    );

    let next = engine.begin().unwrap().into_attempt(request(9, 2), 1);
    assert!(matches!(
        engine
            .apply(
                engine.last_applied() + 1,
                next.command_hash().unwrap(),
                &next
            )
            .unwrap(),
        ApplyOutcome::Applied(CommitResult::Committed { .. })
    ));
    let old = engine.begin().unwrap().into_attempt(request(9, 1), 1);
    assert_eq!(
        engine
            .apply(engine.last_applied() + 1, old.command_hash().unwrap(), &old)
            .unwrap(),
        ApplyOutcome::Rejected(RequestRejection::SequenceTooOld {
            latest: 2,
            actual: 1,
        })
    );
}

#[test]
fn aborted_outcomes_are_also_durably_deduplicated() {
    let (mut engine, table_id) = bootstrap();
    seed(&mut engine, table_id, 2, &[row(1, "original", true)]);
    let mut winner = engine.begin().unwrap();
    let mut loser = engine.begin().unwrap();
    engine
        .update(
            &mut winner,
            table_id,
            &Value::Int64(1),
            row(1, "winner", true),
        )
        .unwrap();
    engine
        .update(
            &mut loser,
            table_id,
            &Value::Int64(1),
            row(1, "loser", true),
        )
        .unwrap();
    apply(&mut engine, winner, 3, 1);
    let (aborted, attempt, hash) = apply(&mut engine, loser, 8, 1);
    assert!(matches!(
        &aborted,
        ApplyOutcome::Applied(CommitResult::Aborted { .. })
    ));
    assert_eq!(
        engine
            .apply(engine.last_applied() + 1, hash, &attempt)
            .unwrap(),
        match aborted {
            ApplyOutcome::Applied(result) => ApplyOutcome::Duplicate(result),
            _ => unreachable!(),
        }
    );
}

#[test]
fn snapshot_round_trip_preserves_mvcc_and_client_dedup_state() {
    let (mut engine, table_id) = bootstrap();
    seed(&mut engine, table_id, 7, &[row(1, "durable", true)]);
    let bytes = engine.snapshot().to_bytes().unwrap();
    let snapshot = aster_engine::EngineSnapshot::from_bytes(&bytes).unwrap();
    let mut recovered = Engine::from_snapshot(snapshot).unwrap();
    assert_eq!(recovered.last_applied(), engine.last_applied());
    assert_eq!(
        recovered.client_record(&[7; 16]),
        engine.client_record(&[7; 16])
    );
    let reader = recovered.begin().unwrap();
    assert_eq!(
        recovered.get(&reader, table_id, &Value::Int64(1)).unwrap(),
        Some(row(1, "durable", true))
    );
}

#[test]
fn offline_vacuum_sets_an_enforced_snapshot_floor() {
    let (mut engine, table_id) = bootstrap();
    seed(&mut engine, table_id, 2, &[row(1, "v1", true)]);
    let stale_timestamp = engine.last_applied();
    let mut writer = engine.begin().unwrap();
    engine
        .update(&mut writer, table_id, &Value::Int64(1), row(1, "v2", true))
        .unwrap();
    apply(&mut engine, writer, 3, 1);
    let report = engine.vacuum_offline(engine.last_applied()).unwrap();
    assert!(report.primary_versions_removed >= 1);
    assert!(matches!(
        engine.begin_at(stale_timestamp, engine.catalog().schema_epoch()),
        Err(EngineError::SnapshotTooOld { .. })
    ));
    let fresh = engine.begin().unwrap();
    assert_eq!(
        engine.get(&fresh, table_id, &Value::Int64(1)).unwrap(),
        Some(row(1, "v2", true))
    );
}

#[test]
fn query_execution_filters_sorts_projects_and_limits_deterministically() {
    let (mut engine, table_id) = bootstrap();
    seed(
        &mut engine,
        table_id,
        2,
        &[row(1, "c", true), row(2, "a", false), row(3, "b", true)],
    );
    let transaction = engine.begin().unwrap();
    let query = Query {
        source: ScanSource::Table(table_id),
        filter: Some(Expr::Binary {
            left: Box::new(Expr::Column(2)),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Value::Bool(true))),
        }),
        projection: vec![Expr::Column(1)],
        order_by: vec![OrderBy {
            expression: Expr::Column(1),
            descending: false,
            nulls_first: false,
        }],
        offset: 0,
        limit: Some(1),
    };
    assert_eq!(
        engine.execute_query(&transaction, &query).unwrap(),
        vec![Row {
            values: vec![Value::Text("b".into())],
        }]
    );

    let null_check = Expr::Unary {
        op: UnaryOp::IsNull,
        expr: Box::new(Expr::Literal(Value::Null)),
    };
    assert_eq!(
        null_check.evaluate(&Row { values: vec![] }).unwrap(),
        Value::Bool(true)
    );
}

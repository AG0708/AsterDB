use aster_core::{ClientRequestId, Column, DataType, Row, Schema, Value};
use aster_engine::{Engine, EngineSnapshot, SnapshotRecord};

fn populated_engine() -> Engine {
    let mut engine = Engine::new();
    let mut transaction = engine.begin().unwrap();
    let table = engine
        .create_table(
            &mut transaction,
            "records",
            Schema {
                columns: vec![
                    Column {
                        name: "id".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        primary_key: true,
                    },
                    Column {
                        name: "value".into(),
                        data_type: DataType::Text,
                        nullable: false,
                        primary_key: false,
                    },
                ],
            },
        )
        .unwrap();
    engine
        .insert(
            &mut transaction,
            table,
            Row {
                values: vec![Value::Int64(7), Value::Text("seven".into())],
            },
        )
        .unwrap();
    let attempt = transaction.into_attempt(
        ClientRequestId {
            client_id: [9; 16],
            sequence: 1,
        },
        1,
    );
    let hash = attempt.command_hash().unwrap();
    engine.apply(1, hash, &attempt).unwrap();
    engine
}

#[test]
fn logical_records_round_trip_and_are_canonical() {
    let original = populated_engine().snapshot();
    let records = original.to_records().unwrap();
    assert!(records.windows(2).all(|pair| {
        (pair[0].kind, pair[0].key.as_slice()) < (pair[1].kind, pair[1].key.as_slice())
    }));
    let mut reversed = records;
    reversed.reverse();
    let restored = EngineSnapshot::from_records(&reversed).unwrap();
    assert_eq!(restored.to_bytes().unwrap(), original.to_bytes().unwrap());
}

#[test]
fn duplicate_missing_unknown_and_mismatched_records_are_rejected() {
    let mut records = populated_engine().snapshot().to_records().unwrap();
    records.push(records[0].clone());
    assert!(EngineSnapshot::from_records(&records).is_err());

    let records = populated_engine().snapshot().to_records().unwrap();
    let without_meta: Vec<_> = records
        .iter()
        .filter(|record| record.kind != 0)
        .cloned()
        .collect();
    assert!(EngineSnapshot::from_records(&without_meta).is_err());

    let mut unknown = records.clone();
    unknown.push(SnapshotRecord {
        kind: 250,
        key: b"unknown".to_vec(),
        value: b"{}".to_vec(),
    });
    assert!(EngineSnapshot::from_records(&unknown).is_err());

    let mut mismatched = records;
    let primary = mismatched
        .iter_mut()
        .find(|record| record.kind == 2)
        .unwrap();
    primary.key.push(0);
    assert!(EngineSnapshot::from_records(&mismatched).is_err());
}

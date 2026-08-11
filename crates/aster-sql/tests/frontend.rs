use aster_core::{Column, DataType, Row, Schema, Value};
use aster_sql::ast::Statement;
use aster_sql::binder::output_schema;
use aster_sql::eval::{
    EvaluationContext, TruthValue, evaluate, predicate_matches, validate_parameters,
};
use aster_sql::plan::{
    BoundExpr, BoundExprKind, BoundStatement, LogicalPlan, SqlType, explain_physical,
};
use aster_sql::{
    IndexDef, MemoryCatalog, SqlErrorKind, TableDef, bind, lex, optimize, parse_statement,
    parse_statements,
};

fn users_schema() -> Schema {
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
                nullable: true,
                primary_key: false,
            },
            Column {
                name: "active".into(),
                data_type: DataType::Bool,
                nullable: true,
                primary_key: false,
            },
        ],
    }
}

fn catalog() -> MemoryCatalog {
    let mut catalog = MemoryCatalog::new();
    catalog
        .add_table(TableDef {
            id: 1,
            name: "users".into(),
            schema: users_schema(),
        })
        .unwrap();
    catalog
        .add_table(TableDef {
            id: 2,
            name: "events".into(),
            schema: Schema {
                columns: vec![
                    Column {
                        name: "event_id".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        primary_key: true,
                    },
                    Column {
                        name: "user_id".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        primary_key: false,
                    },
                    Column {
                        name: "name".into(),
                        data_type: DataType::Text,
                        nullable: true,
                        primary_key: false,
                    },
                ],
            },
        })
        .unwrap();
    catalog
        .add_index(IndexDef {
            id: 10,
            name: "users_name_idx".into(),
            table_id: 1,
            column_index: 1,
        })
        .unwrap();
    catalog
        .add_index(IndexDef {
            id: 11,
            name: "events_user_idx".into(),
            table_id: 2,
            column_index: 1,
        })
        .unwrap();
    catalog
}

fn bind_sql(sql: &str) -> aster_sql::BindOutput {
    bind(&parse_statement(sql).unwrap(), &catalog()).unwrap()
}

fn query_plan(output: &aster_sql::BindOutput) -> &LogicalPlan {
    let BoundStatement::Query { plan, .. } = &output.statement else {
        panic!("expected query")
    };
    plan
}

fn find_filter(plan: &LogicalPlan) -> Option<&BoundExpr> {
    match plan {
        LogicalPlan::Filter { predicate, .. } => Some(predicate),
        LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Project { input, .. } => find_filter(input),
        LogicalPlan::Scan(_) | LogicalPlan::Join { .. } => None,
    }
}

#[test]
fn lexer_preserves_byte_spans_and_decodes_literals() {
    let sql = "SELECT 'it''s', X'00fF', \"na\"";
    let tokens = lex(sql).unwrap();
    assert_eq!(&sql[tokens[1].span.start..tokens[1].span.end], "'it''s'");
    assert_eq!(
        tokens[1].kind,
        aster_sql::TokenKind::Literal(Value::Text("it's".into()))
    );
    assert_eq!(
        tokens[3].kind,
        aster_sql::TokenKind::Literal(Value::Bytes(vec![0, 255]))
    );
    let error = lex("SELECT 'unterminated").unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Lex);
    assert_eq!(error.span.start, 7);
}

#[test]
fn comments_and_quoted_identifiers_are_accepted() {
    let parsed =
        parse_statement("/* heading */ SELECT \"select\".\"odd name\" FROM \"select\" -- tail\n;")
            .unwrap();
    let Statement::Select(select) = parsed.value else {
        panic!("expected select")
    };
    assert!(select.from.name.quoted);
    assert_eq!(select.from.name.value, "select");
}

#[test]
fn parser_covers_the_v1_statement_surface() {
    let sql = r"
        CREATE TABLE widgets (id INT64 PRIMARY KEY, label TEXT NULL, raw BYTES);
        CREATE INDEX widgets_label ON widgets(label);
        INSERT INTO widgets (id, label) VALUES (?, 'a');
        UPDATE widgets SET label = ? WHERE id = ?;
        DELETE FROM widgets WHERE id = ?;
        SELECT id, COUNT(*) AS n FROM widgets WHERE label IS NOT NULL
          GROUP BY id ORDER BY n DESC LIMIT 10;
        BEGIN; COMMIT TRANSACTION; ROLLBACK; EXPLAIN SELECT * FROM widgets;
    ";
    let statements = parse_statements(sql).unwrap();
    assert_eq!(statements.len(), 10);
}

#[test]
fn positional_parameters_restart_for_each_batched_statement() {
    let statements =
        parse_statements("SELECT * FROM users WHERE id = ?; SELECT * FROM users WHERE name = ?")
            .unwrap();
    let first = bind(&statements[0], &catalog()).unwrap();
    let second = bind(&statements[1], &catalog()).unwrap();
    assert_eq!(first.parameters[0].index, 0);
    assert_eq!(second.parameters[0].index, 0);
}

#[test]
fn create_table_requires_exactly_one_non_null_primary_key() {
    let no_key = parse_statement("CREATE TABLE t (id INT64, value TEXT)").unwrap();
    let error = bind(&no_key, &catalog()).unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Constraint);
    assert!(error.message.contains("exactly one primary-key"));

    let two_keys =
        parse_statement("CREATE TABLE t (id INT64 PRIMARY KEY, other INT64 PRIMARY KEY)").unwrap();
    assert_eq!(
        bind(&two_keys, &catalog()).unwrap_err().kind,
        SqlErrorKind::Constraint
    );
}

#[test]
fn insert_is_normalized_and_missing_required_columns_are_rejected() {
    let output = bind_sql("INSERT INTO users (name, id) VALUES ('Ada', 7)");
    let BoundStatement::Insert { values, .. } = output.statement else {
        panic!("expected insert")
    };
    assert!(matches!(
        values[0].value,
        BoundExprKind::Literal(Value::Int64(7))
    ));
    assert!(matches!(
        values[1].value,
        BoundExprKind::Literal(Value::Text(_))
    ));
    assert!(matches!(
        values[2].value,
        BoundExprKind::Literal(Value::Null)
    ));

    let parsed = parse_statement("INSERT INTO users (name) VALUES ('Ada')").unwrap();
    let error = bind(&parsed, &catalog()).unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Constraint);
    assert!(error.message.contains("omits required column `id`"));
}

#[test]
fn binder_infers_parameter_types_from_columns_and_limit() {
    let output = bind_sql("SELECT name FROM users WHERE id = ? AND active = ? LIMIT ?");
    let types: Vec<_> = output
        .parameters
        .iter()
        .map(|parameter| parameter.data_type)
        .collect();
    assert_eq!(
        types,
        vec![
            Some(DataType::Int64),
            Some(DataType::Bool),
            Some(DataType::Int64)
        ]
    );
    validate_parameters(
        &output.parameters,
        &[Value::Int64(2), Value::Bool(true), Value::Int64(5)],
    )
    .unwrap();
    assert_eq!(
        validate_parameters(
            &output.parameters,
            &[
                Value::Text("bad".into()),
                Value::Bool(true),
                Value::Int64(5)
            ]
        )
        .unwrap_err()
        .kind,
        SqlErrorKind::Type
    );
}

#[test]
fn non_null_parameter_contexts_are_validated_before_execution() {
    let output = bind_sql("INSERT INTO users (id) VALUES (?)");
    let error = validate_parameters(&output.parameters, &[Value::Null]).unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Constraint);
    assert!(error.message.contains("cannot be NULL"));

    let output = bind_sql("SELECT * FROM users LIMIT ?");
    assert!(validate_parameters(&output.parameters, &[Value::Null]).is_err());
}

#[test]
fn unconstrained_parameter_comparisons_fail_during_binding() {
    let parsed = parse_statement("SELECT * FROM users WHERE ? = ?").unwrap();
    let error = bind(&parsed, &catalog()).unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Type);
    assert!(error.message.contains("cannot be inferred"));
}

#[test]
fn type_mismatches_are_rejected_before_execution() {
    let parsed = parse_statement("SELECT * FROM users WHERE id = 'one'").unwrap();
    let error = bind(&parsed, &catalog()).unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Type);
    assert!(error.message.contains("expected INT64"));

    let parsed = parse_statement("UPDATE users SET active = 42").unwrap();
    assert_eq!(
        bind(&parsed, &catalog()).unwrap_err().kind,
        SqlErrorKind::Type
    );
}

#[test]
fn ambiguous_columns_are_never_silently_chosen() {
    let parsed =
        parse_statement("SELECT name FROM users u INNER JOIN events e ON u.id = e.user_id")
            .unwrap();
    let error = bind(&parsed, &catalog()).unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Bind);
    assert!(error.message.contains("ambiguous"));
}

#[test]
fn aggregate_queries_enforce_grouping_rules() {
    let valid = bind_sql(
        "SELECT active, COUNT(*), MIN(name) FROM users GROUP BY active ORDER BY COUNT(*) DESC",
    );
    let BoundStatement::Query { output, .. } = &valid.statement else {
        panic!("expected query")
    };
    assert_eq!(
        output_schema(output)[1].1,
        SqlType::known(DataType::Int64, false)
    );

    let parsed = parse_statement("SELECT name, COUNT(*) FROM users GROUP BY active").unwrap();
    let error = bind(&parsed, &catalog()).unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Type);
    assert!(error.message.contains("non-grouped"));

    let nested = parse_statement("SELECT MAX(MIN(id)) FROM users").unwrap();
    assert_eq!(
        bind(&nested, &catalog()).unwrap_err().kind,
        SqlErrorKind::Type
    );
}

#[test]
fn order_by_can_resolve_a_projection_alias() {
    bind_sql("SELECT active, COUNT(*) AS n FROM users GROUP BY active ORDER BY n DESC");
}

#[test]
fn sql_three_valued_logic_matches_truth_tables() {
    use TruthValue::{False, True, Unknown};
    let values = [False, True, Unknown];
    let and_table = [
        [False, False, False],
        [False, True, Unknown],
        [False, Unknown, Unknown],
    ];
    let or_table = [
        [False, True, Unknown],
        [True, True, True],
        [Unknown, True, Unknown],
    ];
    for (left_index, left) in values.iter().enumerate() {
        for (right_index, right) in values.iter().enumerate() {
            assert_eq!(left.and(*right), and_table[left_index][right_index]);
            assert_eq!(left.or(*right), or_table[left_index][right_index]);
        }
    }
    assert_eq!(Unknown.not(), Unknown);
}

#[test]
fn null_comparisons_produce_unknown_and_where_rejects_unknown() {
    let output = bind_sql("SELECT id FROM users WHERE active = TRUE AND name = ?");
    let predicate = find_filter(query_plan(&output)).unwrap();
    let row = Row {
        values: vec![Value::Int64(1), Value::Null, Value::Bool(true)],
    };
    let parameters = [Value::Text("Ada".into())];
    let context = EvaluationContext {
        row: &row,
        parameters: &parameters,
    };
    assert_eq!(evaluate(predicate, &context).unwrap(), Value::Null);
    assert!(!predicate_matches(predicate, &context).unwrap());
}

#[test]
fn optimizer_chooses_index_access_and_renders_stable_golden_plan() {
    let output =
        bind_sql("SELECT name FROM users WHERE name = ? AND active = TRUE ORDER BY name LIMIT 5");
    let physical = optimize(query_plan(&output), &catalog());
    assert_eq!(
        explain_physical(&physical),
        "Project\n  Limit\n    Sort\n      Filter\n        IndexScan table=users index=users_name_idx\n"
    );
}

#[test]
fn optimizer_searches_all_conjuncts_for_an_usable_index() {
    let output = bind_sql("SELECT name FROM users WHERE active = TRUE AND name = ?");
    let physical = optimize(query_plan(&output), &catalog());
    assert!(explain_physical(&physical).contains("IndexScan table=users index=users_name_idx"));
}

#[test]
fn optimizer_selects_index_nested_loop_for_indexed_join_key() {
    let output =
        bind_sql("SELECT u.id, e.event_id FROM users u INNER JOIN events e ON u.id = e.user_id");
    let physical = optimize(query_plan(&output), &catalog());
    let explanation = explain_physical(&physical);
    assert!(explanation.contains("IndexNestedLoop"));
    assert!(explanation.contains("SeqScan table=events"));
}

#[test]
fn unsupported_constructs_fail_explicitly() {
    let error = parse_statement("CREATE UNIQUE INDEX x ON users(name)").unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Unsupported);

    let error =
        parse_statement("INSERT INTO users VALUES (1, 'a', TRUE), (2, 'b', FALSE)").unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Unsupported);

    let error = parse_statement("SELECT LOWER(name) FROM users").unwrap_err();
    assert_eq!(error.kind, SqlErrorKind::Unsupported);
}

#[test]
fn whitespace_and_keyword_case_do_not_change_plan_shape() {
    let variants = [
        "SELECT name FROM users WHERE name = ? ORDER BY name LIMIT 3",
        "select name\nfrom users\nwhere name=? order by name limit 3",
        "SeLeCt name FrOm users WhErE name = ? OrDeR By name LiMiT 3",
        "/*a*/ SELECT name FROM users WHERE name = ? -- b\n ORDER BY name LIMIT 3",
    ];
    let plans: Vec<_> = variants
        .iter()
        .map(|sql| {
            let output = bind_sql(sql);
            explain_physical(&optimize(query_plan(&output), &catalog()))
        })
        .collect();
    assert!(plans.windows(2).all(|window| window[0] == window[1]));
}

#[test]
fn malformed_input_corpus_never_panics() {
    let atoms = ["'", "/*", "(", ")", "?", "SELECT", "CREATE", "X'0'", "\0"];
    for left in atoms {
        for right in atoms {
            let input = format!("{left} {right}");
            assert!(std::panic::catch_unwind(|| parse_statement(&input)).is_ok());
        }
    }
}

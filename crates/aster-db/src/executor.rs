use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use aster_core::codec::{encode_row, encode_value};
use aster_core::{Row, TableId, Value};
use aster_engine::{Engine, Transaction};
use aster_sql::ast::{AggregateFunction, BinaryOp, OrderDirection, UnaryOp};
use aster_sql::eval::{EvaluationContext, TruthValue, evaluate, predicate_matches};
use aster_sql::plan::{
    BoundExpr, BoundExprKind, BoundOrder, BoundStatement, JoinAlgorithm, PhysicalPlan,
};
use aster_sql::{SqlError, SqlErrorKind};

use crate::error::{DatabaseError, Result};
use crate::{MAX_INTERMEDIATE_ROWS, MAX_RESULT_ROWS};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatementOutput {
    Query {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
    Mutation {
        affected_rows: usize,
    },
    Explain(String),
}

#[derive(Clone)]
struct ExecRow {
    row: Row,
    aggregates: Vec<(BoundExpr, Value)>,
}

impl ExecRow {
    fn plain(row: Row) -> Self {
        Self {
            row,
            aggregates: Vec::new(),
        }
    }
}

// Keeping the complete finite statement dispatch in one match makes it easy
// to audit that every bound SQL variant is either executed or rejected.
#[allow(clippy::too_many_lines)]
pub(crate) fn execute_bound(
    engine: &Engine,
    transaction: &mut Transaction,
    statement: &BoundStatement,
    physical_plan: Option<&PhysicalPlan>,
    parameters: &[Value],
) -> Result<StatementOutput> {
    match statement {
        BoundStatement::CreateTable { name, schema } => {
            engine.create_table(transaction, name.clone(), schema.clone())?;
            Ok(StatementOutput::Mutation { affected_rows: 0 })
        }
        BoundStatement::CreateIndex {
            name,
            table,
            column_index,
        } => {
            engine.create_index(transaction, name.clone(), table.id, *column_index)?;
            Ok(StatementOutput::Mutation { affected_rows: 0 })
        }
        BoundStatement::Insert { table, values } => {
            let empty = ExecRow::plain(Row { values: Vec::new() });
            let values = values
                .iter()
                .map(|expression| evaluate_exec(expression, &empty, parameters))
                .collect::<Result<Vec<_>>>()?;
            engine.insert(transaction, table.id, Row { values })?;
            Ok(StatementOutput::Mutation { affected_rows: 1 })
        }
        BoundStatement::Update {
            table,
            assignments,
            selection,
        } => {
            let rows = engine.scan_table(transaction, table.id)?;
            let primary_key = primary_key_column(table)?;
            let mut affected_rows = 0;
            for original in rows {
                let exec = ExecRow::plain(original.clone());
                if selection.as_ref().is_some_and(|predicate| {
                    !predicate_exec(predicate, &exec, parameters).unwrap_or(false)
                }) {
                    if let Some(predicate) = selection {
                        predicate_exec(predicate, &exec, parameters)?;
                    }
                    continue;
                }
                let mut updated = original.clone();
                for (column, expression) in assignments {
                    let value = evaluate_exec(expression, &exec, parameters)?;
                    let slot = updated.values.get_mut(*column).ok_or_else(|| {
                        DatabaseError::Invariant(format!(
                            "UPDATE target column {column} is outside row"
                        ))
                    })?;
                    *slot = value;
                }
                let key = original.values.get(primary_key).ok_or_else(|| {
                    DatabaseError::Invariant("primary-key slot is outside row".into())
                })?;
                if engine.update(transaction, table.id, key, updated)? {
                    affected_rows += 1;
                }
            }
            Ok(StatementOutput::Mutation { affected_rows })
        }
        BoundStatement::Delete { table, selection } => {
            let rows = engine.scan_table(transaction, table.id)?;
            let primary_key = primary_key_column(table)?;
            let mut affected_rows = 0;
            for row in rows {
                let exec = ExecRow::plain(row.clone());
                if selection.as_ref().is_some_and(|predicate| {
                    !predicate_exec(predicate, &exec, parameters).unwrap_or(false)
                }) {
                    if let Some(predicate) = selection {
                        predicate_exec(predicate, &exec, parameters)?;
                    }
                    continue;
                }
                let key = row.values.get(primary_key).ok_or_else(|| {
                    DatabaseError::Invariant("primary-key slot is outside row".into())
                })?;
                if engine.delete(transaction, table.id, key)? {
                    affected_rows += 1;
                }
            }
            Ok(StatementOutput::Mutation { affected_rows })
        }
        BoundStatement::Query { output, .. } => {
            let plan = physical_plan.ok_or_else(|| {
                DatabaseError::Invariant("query has no optimized physical plan".into())
            })?;
            let rows = execute_plan(engine, transaction, plan, parameters)?;
            if rows.len() > MAX_RESULT_ROWS {
                return Err(DatabaseError::ResourceLimit(format!(
                    "query produced {} rows; maximum is {MAX_RESULT_ROWS}",
                    rows.len()
                )));
            }
            Ok(StatementOutput::Query {
                columns: output.iter().map(|item| item.name.clone()).collect(),
                rows: rows.into_iter().map(|row| row.row).collect(),
            })
        }
        BoundStatement::Explain(_) => Err(DatabaseError::Invariant(
            "EXPLAIN must be handled before execution".into(),
        )),
        BoundStatement::Transaction(_) => Err(DatabaseError::Invariant(
            "transaction control must be handled by Database".into(),
        )),
    }
}

fn primary_key_column(table: &aster_sql::TableDef) -> Result<usize> {
    table
        .schema
        .columns
        .iter()
        .position(|column| column.primary_key)
        .ok_or_else(|| {
            DatabaseError::Invariant(format!("table `{}` has no primary key", table.name))
        })
}

fn execute_plan(
    engine: &Engine,
    transaction: &Transaction,
    plan: &PhysicalPlan,
    parameters: &[Value],
) -> Result<Vec<ExecRow>> {
    let rows = match plan {
        PhysicalPlan::SeqScan(table) => engine
            .scan_table(transaction, table.table.id)?
            .into_iter()
            .map(ExecRow::plain)
            .collect(),
        PhysicalPlan::IndexScan {
            table,
            index,
            constraint,
        } => {
            let empty = ExecRow::plain(Row { values: Vec::new() });
            let value = evaluate_exec(&constraint.value, &empty, parameters)?;
            let rows = if constraint.operator == BinaryOp::Eq && !value.is_null() {
                engine.scan_secondary(transaction, index.id, &value)?
            } else {
                // The current storage engine exposes equality index probes.
                // Other sargable comparisons retain correct sequential-scan
                // semantics and are narrowed by the residual Filter node.
                engine.scan_table(transaction, table.table.id)?
            };
            rows.into_iter().map(ExecRow::plain).collect()
        }
        PhysicalPlan::Filter { input, predicate } => {
            execute_plan(engine, transaction, input, parameters)?
                .into_iter()
                .filter_map(|row| match predicate_exec(predicate, &row, parameters) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>>>()?
        }
        PhysicalPlan::Join {
            left,
            right,
            on,
            algorithm,
        } => execute_join(engine, transaction, left, right, on, algorithm, parameters)?,
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
        } => aggregate_rows(
            execute_plan(engine, transaction, input, parameters)?,
            group_by,
            aggregates,
            parameters,
        )?,
        PhysicalPlan::Sort { input, order_by } => {
            let input = execute_plan(engine, transaction, input, parameters)?;
            sort_rows(input, order_by, parameters)?
        }
        PhysicalPlan::Limit { input, limit } => {
            let mut input = execute_plan(engine, transaction, input, parameters)?;
            let empty = ExecRow::plain(Row { values: Vec::new() });
            let limit = match evaluate_exec(limit, &empty, parameters)? {
                Value::Int64(value) if value >= 0 => usize::try_from(value).map_err(|_| {
                    DatabaseError::ResourceLimit("LIMIT does not fit this platform".into())
                })?,
                _ => {
                    return Err(DatabaseError::Sql(SqlError::new(
                        SqlErrorKind::Constraint,
                        "LIMIT must evaluate to a non-negative INT64",
                        limit.span,
                    )));
                }
            };
            input.truncate(limit);
            input
        }
        PhysicalPlan::Project { input, expressions } => {
            execute_plan(engine, transaction, input, parameters)?
                .into_iter()
                .map(|row| {
                    expressions
                        .iter()
                        .map(|expression| evaluate_exec(&expression.expr, &row, parameters))
                        .collect::<Result<Vec<_>>>()
                        .map(|values| ExecRow::plain(Row { values }))
                })
                .collect::<Result<Vec<_>>>()?
        }
    };
    ensure_intermediate_bound(&rows)?;
    Ok(rows)
}

fn execute_join(
    engine: &Engine,
    transaction: &Transaction,
    left_plan: &PhysicalPlan,
    right_plan: &PhysicalPlan,
    on: &BoundExpr,
    algorithm: &JoinAlgorithm,
    parameters: &[Value],
) -> Result<Vec<ExecRow>> {
    let left = execute_plan(engine, transaction, left_plan, parameters)?;
    if let Some((left_key, right_column)) = hash_join_keys(on, right_plan) {
        if matches!(
            algorithm,
            JoinAlgorithm::Hash | JoinAlgorithm::IndexNestedLoop { .. }
        ) {
            let right = execute_plan(engine, transaction, right_plan, parameters)?;
            let mut buckets: BTreeMap<Vec<u8>, Vec<ExecRow>> = BTreeMap::new();
            for row in right {
                let Some(value) = row.row.values.get(right_column) else {
                    return Err(DatabaseError::Invariant(
                        "hash-join inner column is outside row".into(),
                    ));
                };
                if value.is_null() {
                    continue;
                }
                let mut key = Vec::new();
                encode_value(value, &mut key).map_err(aster_engine::EngineError::from)?;
                buckets.entry(key).or_default().push(row);
            }
            let mut output = Vec::new();
            for outer in left {
                let value = evaluate_exec(left_key, &outer, parameters)?;
                if value.is_null() {
                    continue;
                }
                let mut key = Vec::new();
                encode_value(&value, &mut key).map_err(aster_engine::EngineError::from)?;
                if let Some(matches) = buckets.get(&key) {
                    for inner in matches {
                        let joined = combine_rows(&outer, inner);
                        if predicate_exec(on, &joined, parameters)? {
                            output.push(joined);
                            ensure_intermediate_bound(&output)?;
                        }
                    }
                }
            }
            return Ok(output);
        }
    }

    let right = execute_plan(engine, transaction, right_plan, parameters)?;
    let mut output = Vec::new();
    for outer in &left {
        for inner in &right {
            let joined = combine_rows(outer, inner);
            if predicate_exec(on, &joined, parameters)? {
                output.push(joined);
                ensure_intermediate_bound(&output)?;
            }
        }
    }
    Ok(output)
}

fn combine_rows(left: &ExecRow, right: &ExecRow) -> ExecRow {
    let mut values = left.row.values.clone();
    values.extend(right.row.values.iter().cloned());
    let mut aggregates = left.aggregates.clone();
    aggregates.extend(right.aggregates.iter().cloned());
    ExecRow {
        row: Row { values },
        aggregates,
    }
}

fn hash_join_keys<'a>(
    on: &'a BoundExpr,
    right_plan: &PhysicalPlan,
) -> Option<(&'a BoundExpr, usize)> {
    let BoundExprKind::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = &on.value
    else {
        return None;
    };
    let right_tables = plan_tables(right_plan);
    match (&left.value, &right.value) {
        (BoundExprKind::Column(left_column), BoundExprKind::Column(right_column))
            if right_tables.contains(&right_column.table_id)
                && !right_tables.contains(&left_column.table_id) =>
        {
            Some((left, right_column.column_index))
        }
        (BoundExprKind::Column(left_column), BoundExprKind::Column(right_column))
            if right_tables.contains(&left_column.table_id)
                && !right_tables.contains(&right_column.table_id) =>
        {
            Some((right, left_column.column_index))
        }
        _ => None,
    }
}

fn plan_tables(plan: &PhysicalPlan) -> BTreeSet<TableId> {
    match plan {
        PhysicalPlan::SeqScan(table) | PhysicalPlan::IndexScan { table, .. } => {
            BTreeSet::from([table.table.id])
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Project { input, .. } => plan_tables(input),
        PhysicalPlan::Join { left, right, .. } => {
            let mut tables = plan_tables(left);
            tables.extend(plan_tables(right));
            tables
        }
    }
}

fn aggregate_rows(
    input: Vec<ExecRow>,
    group_by: &[BoundExpr],
    aggregates: &[BoundExpr],
    parameters: &[Value],
) -> Result<Vec<ExecRow>> {
    let mut groups: BTreeMap<Vec<u8>, Vec<ExecRow>> = BTreeMap::new();
    for row in input {
        let values = group_by
            .iter()
            .map(|expression| evaluate_exec(expression, &row, parameters))
            .collect::<Result<Vec<_>>>()?;
        let key = encode_row(&Row { values }).map_err(aster_engine::EngineError::from)?;
        groups.entry(key).or_default().push(row);
    }
    if groups.is_empty() && group_by.is_empty() {
        groups.insert(Vec::new(), Vec::new());
    }

    let mut output = Vec::with_capacity(groups.len());
    for rows in groups.into_values() {
        let representative = rows
            .first()
            .map_or_else(|| Row { values: Vec::new() }, |row| row.row.clone());
        let mut computed = Vec::with_capacity(aggregates.len());
        for aggregate in aggregates {
            computed.push((
                aggregate.clone(),
                compute_aggregate(aggregate, &rows, parameters)?,
            ));
        }
        output.push(ExecRow {
            row: representative,
            aggregates: computed,
        });
    }
    Ok(output)
}

fn compute_aggregate(
    expression: &BoundExpr,
    rows: &[ExecRow],
    parameters: &[Value],
) -> Result<Value> {
    let BoundExprKind::Aggregate { function, argument } = &expression.value else {
        return Err(DatabaseError::Invariant(
            "aggregate plan contains a scalar expression".into(),
        ));
    };
    match function {
        AggregateFunction::Count => {
            let count = if let Some(argument) = argument {
                rows.iter()
                    .map(|row| evaluate_exec(argument, row, parameters))
                    .collect::<Result<Vec<_>>>()?
                    .iter()
                    .filter(|value| !value.is_null())
                    .count()
            } else {
                rows.len()
            };
            Ok(Value::Int64(i64::try_from(count).map_err(|_| {
                DatabaseError::ResourceLimit("COUNT exceeds INT64".into())
            })?))
        }
        AggregateFunction::Sum => {
            let argument = argument
                .as_ref()
                .ok_or_else(|| DatabaseError::Invariant("SUM is missing its argument".into()))?;
            let mut sum: Option<i64> = None;
            for row in rows {
                match evaluate_exec(argument, row, parameters)? {
                    Value::Null => {}
                    Value::Int64(value) => {
                        sum = Some(sum.unwrap_or(0).checked_add(value).ok_or_else(|| {
                            DatabaseError::Sql(SqlError::new(
                                SqlErrorKind::Constraint,
                                "INT64 SUM overflow",
                                expression.span,
                            ))
                        })?);
                    }
                    other => return Err(type_error("SUM", &other, expression)),
                }
            }
            Ok(sum.map_or(Value::Null, Value::Int64))
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let argument = argument.as_ref().ok_or_else(|| {
                DatabaseError::Invariant("MIN/MAX is missing its argument".into())
            })?;
            let mut selected: Option<Value> = None;
            for row in rows {
                let value = evaluate_exec(argument, row, parameters)?;
                if value.is_null() {
                    continue;
                }
                let replace = selected.as_ref().is_none_or(|current| {
                    let order = value.checked_cmp(current).ok().flatten();
                    match function {
                        AggregateFunction::Min => order == Some(Ordering::Less),
                        AggregateFunction::Max => order == Some(Ordering::Greater),
                        _ => false,
                    }
                });
                if replace {
                    selected = Some(value);
                }
            }
            Ok(selected.unwrap_or(Value::Null))
        }
    }
}

fn sort_rows(
    input: Vec<ExecRow>,
    order_by: &[BoundOrder],
    parameters: &[Value],
) -> Result<Vec<ExecRow>> {
    let mut keyed = input
        .into_iter()
        .map(|row| {
            let keys = order_by
                .iter()
                .map(|order| evaluate_exec(&order.expr, &row, parameters))
                .collect::<Result<Vec<_>>>()?;
            Ok((row, keys))
        })
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|left, right| {
        for ((left, right), order) in left.1.iter().zip(&right.1).zip(order_by) {
            let comparison = compare_sort_values(left, right);
            let comparison = match order.direction {
                OrderDirection::Asc => comparison,
                OrderDirection::Desc => comparison.reverse(),
            };
            if comparison != Ordering::Equal {
                return comparison;
            }
        }
        Ordering::Equal
    });
    Ok(keyed.into_iter().map(|(row, _)| row).collect())
}

fn compare_sort_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => left
            .checked_cmp(right)
            .ok()
            .flatten()
            .unwrap_or(Ordering::Equal),
    }
}

fn evaluate_exec(expression: &BoundExpr, row: &ExecRow, parameters: &[Value]) -> Result<Value> {
    if !contains_aggregate(expression) {
        return evaluate(
            expression,
            &EvaluationContext {
                row: &row.row,
                parameters,
            },
        )
        .map_err(DatabaseError::from);
    }
    match &expression.value {
        BoundExprKind::Aggregate { .. } => row
            .aggregates
            .iter()
            .find(|(candidate, _)| same_expression(candidate, expression))
            .map(|(_, value)| value.clone())
            .ok_or_else(|| DatabaseError::Invariant("aggregate value was not computed".into())),
        BoundExprKind::Unary { op, expr } => {
            let value = evaluate_exec(expr, row, parameters)?;
            match op {
                UnaryOp::Not => Ok(truth(&value, expression)?.not().into_value()),
                UnaryOp::Negate => match value {
                    Value::Null => Ok(Value::Null),
                    Value::Int64(value) => value.checked_neg().map(Value::Int64).ok_or_else(|| {
                        DatabaseError::Sql(SqlError::new(
                            SqlErrorKind::Constraint,
                            "INT64 negation overflow",
                            expression.span,
                        ))
                    }),
                    other => Err(type_error("negate", &other, expression)),
                },
            }
        }
        BoundExprKind::Binary { left, op, right } => {
            let left = evaluate_exec(left, row, parameters)?;
            match op {
                BinaryOp::And => {
                    let left = truth(&left, expression)?;
                    if left == TruthValue::False {
                        return Ok(Value::Bool(false));
                    }
                    let right = evaluate_exec(right, row, parameters)?;
                    Ok(left.and(truth(&right, expression)?).into_value())
                }
                BinaryOp::Or => {
                    let left = truth(&left, expression)?;
                    if left == TruthValue::True {
                        return Ok(Value::Bool(true));
                    }
                    let right = evaluate_exec(right, row, parameters)?;
                    Ok(left.or(truth(&right, expression)?).into_value())
                }
                comparison => {
                    let right = evaluate_exec(right, row, parameters)?;
                    compare(&left, *comparison, &right, expression)
                }
            }
        }
        BoundExprKind::IsNull { expr, negated } => {
            let is_null = evaluate_exec(expr, row, parameters)?.is_null();
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        BoundExprKind::Literal(_) | BoundExprKind::Parameter { .. } | BoundExprKind::Column(_) => {
            Err(DatabaseError::Invariant(
                "aggregate expression traversal reached an unexpected scalar leaf".into(),
            ))
        }
    }
}

fn predicate_exec(expression: &BoundExpr, row: &ExecRow, parameters: &[Value]) -> Result<bool> {
    if !contains_aggregate(expression) {
        return predicate_matches(
            expression,
            &EvaluationContext {
                row: &row.row,
                parameters,
            },
        )
        .map_err(DatabaseError::from);
    }
    Ok(truth(&evaluate_exec(expression, row, parameters)?, expression)? == TruthValue::True)
}

fn truth(value: &Value, expression: &BoundExpr) -> Result<TruthValue> {
    match value {
        Value::Bool(false) => Ok(TruthValue::False),
        Value::Bool(true) => Ok(TruthValue::True),
        Value::Null => Ok(TruthValue::Unknown),
        other => Err(type_error("use as a predicate", other, expression)),
    }
}

fn compare(
    left: &Value,
    operator: BinaryOp,
    right: &Value,
    expression: &BoundExpr,
) -> Result<Value> {
    let Some(ordering) = left.checked_cmp(right).map_err(|error| {
        DatabaseError::Sql(SqlError::new(
            SqlErrorKind::Type,
            error.to_string(),
            expression.span,
        ))
    })?
    else {
        return Ok(Value::Null);
    };
    Ok(Value::Bool(match operator {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Lt => ordering == Ordering::Less,
        BinaryOp::LtEq => ordering != Ordering::Greater,
        BinaryOp::Gt => ordering == Ordering::Greater,
        BinaryOp::GtEq => ordering != Ordering::Less,
        BinaryOp::And | BinaryOp::Or => unreachable!("handled above"),
    }))
}

fn type_error(operation: &str, value: &Value, expression: &BoundExpr) -> DatabaseError {
    DatabaseError::Sql(SqlError::new(
        SqlErrorKind::Type,
        format!("cannot {operation} {}", value.type_name()),
        expression.span,
    ))
}

fn contains_aggregate(expression: &BoundExpr) -> bool {
    match &expression.value {
        BoundExprKind::Aggregate { .. } => true,
        BoundExprKind::Unary { expr, .. } | BoundExprKind::IsNull { expr, .. } => {
            contains_aggregate(expr)
        }
        BoundExprKind::Binary { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        BoundExprKind::Literal(_) | BoundExprKind::Parameter { .. } | BoundExprKind::Column(_) => {
            false
        }
    }
}

fn same_expression(left: &BoundExpr, right: &BoundExpr) -> bool {
    match (&left.value, &right.value) {
        (BoundExprKind::Literal(left), BoundExprKind::Literal(right)) => left == right,
        (
            BoundExprKind::Parameter { index: left, .. },
            BoundExprKind::Parameter { index: right, .. },
        ) => left == right,
        (BoundExprKind::Column(left), BoundExprKind::Column(right)) => {
            left.table_id == right.table_id && left.column_index == right.column_index
        }
        (
            BoundExprKind::Unary {
                op: left_op,
                expr: left,
            },
            BoundExprKind::Unary {
                op: right_op,
                expr: right,
            },
        ) => left_op == right_op && same_expression(left, right),
        (
            BoundExprKind::Binary {
                left: left_a,
                op: left_op,
                right: left_b,
            },
            BoundExprKind::Binary {
                left: right_a,
                op: right_op,
                right: right_b,
            },
        ) => {
            left_op == right_op
                && same_expression(left_a, right_a)
                && same_expression(left_b, right_b)
        }
        (
            BoundExprKind::IsNull {
                expr: left,
                negated: left_negated,
            },
            BoundExprKind::IsNull {
                expr: right,
                negated: right_negated,
            },
        ) => left_negated == right_negated && same_expression(left, right),
        (
            BoundExprKind::Aggregate {
                function: left_function,
                argument: left_argument,
            },
            BoundExprKind::Aggregate {
                function: right_function,
                argument: right_argument,
            },
        ) => {
            left_function == right_function
                && match (left_argument, right_argument) {
                    (None, None) => true,
                    (Some(left), Some(right)) => same_expression(left, right),
                    _ => false,
                }
        }
        _ => false,
    }
}

fn ensure_intermediate_bound(rows: &[ExecRow]) -> Result<()> {
    if rows.len() > MAX_INTERMEDIATE_ROWS {
        return Err(DatabaseError::ResourceLimit(format!(
            "physical plan produced {} intermediate rows; maximum is {MAX_INTERMEDIATE_ROWS}",
            rows.len()
        )));
    }
    Ok(())
}

pub(crate) fn explain_statement(
    statement: &BoundStatement,
    physical_plan: Option<&PhysicalPlan>,
) -> Result<StatementOutput> {
    let description = match statement {
        BoundStatement::Query { .. } => {
            aster_sql::plan::explain_physical(physical_plan.ok_or_else(|| {
                DatabaseError::Invariant("EXPLAIN query has no physical plan".into())
            })?)
        }
        BoundStatement::CreateTable { name, .. } => format!("CreateTable name={name}\n"),
        BoundStatement::CreateIndex { name, table, .. } => {
            format!("CreateIndex name={name} table={}\n", table.name)
        }
        BoundStatement::Insert { table, .. } => format!("Insert table={}\n", table.name),
        BoundStatement::Update { table, .. } => format!("Update table={}\n", table.name),
        BoundStatement::Delete { table, .. } => format!("Delete table={}\n", table.name),
        BoundStatement::Explain(_) | BoundStatement::Transaction(_) => {
            return Err(DatabaseError::Invariant(
                "invalid nested EXPLAIN target".into(),
            ));
        }
    };
    Ok(StatementOutput::Explain(description))
}

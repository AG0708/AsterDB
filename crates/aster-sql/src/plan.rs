use std::fmt::Write;

use aster_core::{Column, DataType, IndexId, Schema, TableId, Value};

use crate::ast::{AggregateFunction, BinaryOp, OrderDirection, Span, UnaryOp};
use crate::catalog::{Catalog, IndexDef, TableDef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlType {
    /// `None` denotes an untyped NULL or a parameter whose type is not inferred.
    pub data_type: Option<DataType>,
    pub nullable: bool,
}

impl SqlType {
    #[must_use]
    pub const fn known(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type: Some(data_type),
            nullable,
        }
    }

    pub const UNKNOWN: Self = Self {
        data_type: None,
        nullable: true,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterSpec {
    pub index: usize,
    pub data_type: Option<DataType>,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundTable {
    pub table: TableDef,
    pub alias: String,
    /// First flattened input slot occupied by this table.
    pub slot_offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundColumn {
    pub table_id: TableId,
    pub table_name: String,
    pub table_alias: String,
    pub column_index: usize,
    pub input_slot: usize,
    pub column: Column,
}

pub type BoundExpr = crate::ast::Spanned<BoundExprKind>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundExprKind {
    Literal(Value),
    Parameter {
        index: usize,
        data_type: Option<DataType>,
    },
    Column(BoundColumn),
    Unary {
        op: UnaryOp,
        expr: Box<BoundExpr>,
    },
    Binary {
        left: Box<BoundExpr>,
        op: BinaryOp,
        right: Box<BoundExpr>,
    },
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    Aggregate {
        function: AggregateFunction,
        argument: Option<Box<BoundExpr>>,
    },
}

impl BoundExpr {
    #[must_use]
    pub fn sql_type(&self) -> SqlType {
        match &self.value {
            BoundExprKind::Literal(value) => SqlType {
                data_type: value.data_type(),
                nullable: value.is_null(),
            },
            BoundExprKind::Parameter { data_type, .. } => SqlType {
                data_type: *data_type,
                nullable: true,
            },
            BoundExprKind::Column(column) => {
                SqlType::known(column.column.data_type, column.column.nullable)
            }
            BoundExprKind::Unary { op, expr } => match op {
                UnaryOp::Not => SqlType::known(DataType::Bool, expr.sql_type().nullable),
                UnaryOp::Negate => SqlType::known(DataType::Int64, expr.sql_type().nullable),
            },
            BoundExprKind::Binary { left, op, right } => {
                let nullable = left.sql_type().nullable || right.sql_type().nullable;
                let data_type = if matches!(op, BinaryOp::And | BinaryOp::Or) || op.is_comparison()
                {
                    DataType::Bool
                } else {
                    unreachable!("all binary operators are boolean")
                };
                SqlType::known(data_type, nullable)
            }
            BoundExprKind::IsNull { .. } => SqlType::known(DataType::Bool, false),
            BoundExprKind::Aggregate { function, argument } => match function {
                AggregateFunction::Count => SqlType::known(DataType::Int64, false),
                AggregateFunction::Sum => SqlType::known(DataType::Int64, true),
                AggregateFunction::Min | AggregateFunction::Max => {
                    argument
                        .as_ref()
                        .map_or(SqlType::UNKNOWN, |argument| SqlType {
                            nullable: true,
                            ..argument.sql_type()
                        })
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedExpr {
    pub expr: BoundExpr,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOrder {
    pub expr: BoundExpr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    Scan(BoundTable),
    Filter {
        input: Box<Self>,
        predicate: BoundExpr,
    },
    Join {
        left: Box<Self>,
        right: Box<Self>,
        on: BoundExpr,
    },
    Aggregate {
        input: Box<Self>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<BoundExpr>,
    },
    Sort {
        input: Box<Self>,
        order_by: Vec<BoundOrder>,
    },
    Limit {
        input: Box<Self>,
        limit: BoundExpr,
    },
    Project {
        input: Box<Self>,
        expressions: Vec<NamedExpr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundStatement {
    CreateTable {
        name: String,
        schema: Schema,
    },
    CreateIndex {
        name: String,
        table: TableDef,
        column_index: usize,
    },
    Insert {
        table: TableDef,
        /// Values normalized into schema column order.
        values: Vec<BoundExpr>,
    },
    Update {
        table: TableDef,
        assignments: Vec<(usize, BoundExpr)>,
        selection: Option<BoundExpr>,
    },
    Delete {
        table: TableDef,
        selection: Option<BoundExpr>,
    },
    Query {
        plan: LogicalPlan,
        output: Vec<NamedExpr>,
    },
    Transaction(TransactionStatement),
    Explain(Box<Self>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConstraint {
    pub column_index: usize,
    pub operator: BinaryOp,
    pub value: BoundExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinAlgorithm {
    NestedLoop,
    Hash,
    IndexNestedLoop {
        inner_index_id: IndexId,
        inner_column_index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    SeqScan(BoundTable),
    IndexScan {
        table: BoundTable,
        index: IndexDef,
        constraint: IndexConstraint,
    },
    Filter {
        input: Box<Self>,
        predicate: BoundExpr,
    },
    Join {
        left: Box<Self>,
        right: Box<Self>,
        on: BoundExpr,
        algorithm: JoinAlgorithm,
    },
    HashAggregate {
        input: Box<Self>,
        group_by: Vec<BoundExpr>,
        aggregates: Vec<BoundExpr>,
    },
    Sort {
        input: Box<Self>,
        order_by: Vec<BoundOrder>,
    },
    Limit {
        input: Box<Self>,
        limit: BoundExpr,
    },
    Project {
        input: Box<Self>,
        expressions: Vec<NamedExpr>,
    },
}

#[must_use]
pub fn optimize(plan: &LogicalPlan, catalog: &dyn Catalog) -> PhysicalPlan {
    match plan {
        LogicalPlan::Scan(table) => PhysicalPlan::SeqScan(table.clone()),
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::Scan(table) = input.as_ref()
                && let Some((index, constraint)) = choose_index(table, predicate, catalog)
            {
                // Keep the full predicate as a residual correctness check: one
                // selected conjunct may only narrow access to candidate rows.
                return PhysicalPlan::Filter {
                    input: Box::new(PhysicalPlan::IndexScan {
                        table: table.clone(),
                        index,
                        constraint,
                    }),
                    predicate: predicate.clone(),
                };
            }
            PhysicalPlan::Filter {
                input: Box::new(optimize(input, catalog)),
                predicate: predicate.clone(),
            }
        }
        LogicalPlan::Join { left, right, on } => {
            let algorithm = choose_join_algorithm(right, on, catalog);
            PhysicalPlan::Join {
                left: Box::new(optimize(left, catalog)),
                right: Box::new(optimize(right, catalog)),
                on: on.clone(),
                algorithm,
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(optimize(input, catalog)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        },
        LogicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(optimize(input, catalog)),
            order_by: order_by.clone(),
        },
        LogicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(optimize(input, catalog)),
            limit: limit.clone(),
        },
        LogicalPlan::Project { input, expressions } => PhysicalPlan::Project {
            input: Box::new(optimize(input, catalog)),
            expressions: expressions.clone(),
        },
    }
}

fn choose_index(
    table: &BoundTable,
    predicate: &BoundExpr,
    catalog: &dyn Catalog,
) -> Option<(IndexDef, IndexConstraint)> {
    if let BoundExprKind::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = &predicate.value
    {
        return choose_index(table, left, catalog).or_else(|| choose_index(table, right, catalog));
    }
    let (column, operator, value) = sargable(predicate)?;
    if column.table_id != table.table.id {
        return None;
    }
    let index = catalog
        .indexes_for_table(table.table.id)
        .into_iter()
        .find(|candidate| candidate.column_index == column.column_index)?;
    Some((
        index,
        IndexConstraint {
            column_index: column.column_index,
            operator,
            value: value.clone(),
        },
    ))
}

fn sargable(predicate: &BoundExpr) -> Option<(&BoundColumn, BinaryOp, &BoundExpr)> {
    match &predicate.value {
        BoundExprKind::Binary { left, op, right } if op.is_comparison() => {
            match (&left.value, &right.value) {
                (BoundExprKind::Column(column), BoundExprKind::Literal(value))
                    if !value.is_null() =>
                {
                    Some((column, *op, right))
                }
                (BoundExprKind::Column(column), BoundExprKind::Parameter { .. }) => {
                    Some((column, *op, right))
                }
                (BoundExprKind::Literal(value), BoundExprKind::Column(column))
                    if !value.is_null() =>
                {
                    Some((column, reverse_comparison(*op), left))
                }
                (BoundExprKind::Parameter { .. }, BoundExprKind::Column(column)) => {
                    Some((column, reverse_comparison(*op), left))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

const fn reverse_comparison(operator: BinaryOp) -> BinaryOp {
    match operator {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

fn choose_join_algorithm(
    right: &LogicalPlan,
    on: &BoundExpr,
    catalog: &dyn Catalog,
) -> JoinAlgorithm {
    let BoundExprKind::Binary {
        left,
        op: BinaryOp::Eq,
        right: comparison_right,
    } = &on.value
    else {
        return JoinAlgorithm::NestedLoop;
    };
    let (BoundExprKind::Column(left_column), BoundExprKind::Column(right_column)) =
        (&left.value, &comparison_right.value)
    else {
        return JoinAlgorithm::NestedLoop;
    };
    if let LogicalPlan::Scan(inner) = right {
        let inner_column = if right_column.table_id == inner.table.id {
            right_column
        } else if left_column.table_id == inner.table.id {
            left_column
        } else {
            return JoinAlgorithm::Hash;
        };
        if let Some(index) = catalog
            .indexes_for_table(inner.table.id)
            .into_iter()
            .find(|index| index.column_index == inner_column.column_index)
        {
            return JoinAlgorithm::IndexNestedLoop {
                inner_index_id: index.id,
                inner_column_index: inner_column.column_index,
            };
        }
    }
    JoinAlgorithm::Hash
}

/// Stable, intentionally compact plan rendering suitable for golden tests.
#[must_use]
pub fn explain_physical(plan: &PhysicalPlan) -> String {
    let mut output = String::new();
    explain_node(plan, 0, &mut output);
    output
}

fn explain_node(plan: &PhysicalPlan, depth: usize, output: &mut String) {
    let indent = "  ".repeat(depth);
    match plan {
        PhysicalPlan::SeqScan(table) => {
            let _ = writeln!(output, "{indent}SeqScan table={}", table.table.name);
        }
        PhysicalPlan::IndexScan { table, index, .. } => {
            let _ = writeln!(
                output,
                "{indent}IndexScan table={} index={}",
                table.table.name, index.name
            );
        }
        PhysicalPlan::Filter { input, .. } => {
            let _ = writeln!(output, "{indent}Filter");
            explain_node(input, depth + 1, output);
        }
        PhysicalPlan::Join {
            left,
            right,
            algorithm,
            ..
        } => {
            let _ = writeln!(output, "{indent}Join algorithm={algorithm:?}");
            explain_node(left, depth + 1, output);
            explain_node(right, depth + 1, output);
        }
        PhysicalPlan::HashAggregate { input, .. } => {
            let _ = writeln!(output, "{indent}HashAggregate");
            explain_node(input, depth + 1, output);
        }
        PhysicalPlan::Sort { input, .. } => {
            let _ = writeln!(output, "{indent}Sort");
            explain_node(input, depth + 1, output);
        }
        PhysicalPlan::Limit { input, .. } => {
            let _ = writeln!(output, "{indent}Limit");
            explain_node(input, depth + 1, output);
        }
        PhysicalPlan::Project { input, .. } => {
            let _ = writeln!(output, "{indent}Project");
            explain_node(input, depth + 1, output);
        }
    }
}

#[must_use]
pub const fn expression_span(expr: &BoundExpr) -> Span {
    expr.span
}

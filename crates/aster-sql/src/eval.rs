//! Scalar expression semantics shared by executors and conformance tests.

use std::cmp::Ordering;

use aster_core::{DataType, Row, Value};

use crate::ast::{BinaryOp, UnaryOp};
use crate::plan::{BoundExpr, BoundExprKind, ParameterSpec};
use crate::{Result, SqlError, SqlErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruthValue {
    False,
    True,
    Unknown,
}

impl TruthValue {
    #[must_use]
    pub const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn not(self) -> Self {
        match self {
            Self::False => Self::True,
            Self::True => Self::False,
            Self::Unknown => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn into_value(self) -> Value {
        match self {
            Self::False => Value::Bool(false),
            Self::True => Value::Bool(true),
            Self::Unknown => Value::Null,
        }
    }
}

pub struct EvaluationContext<'a> {
    /// Flattened row slots in the same order used by bound column references.
    pub row: &'a Row,
    pub parameters: &'a [Value],
}

pub fn evaluate(expression: &BoundExpr, context: &EvaluationContext<'_>) -> Result<Value> {
    match &expression.value {
        BoundExprKind::Literal(value) => Ok(value.clone()),
        BoundExprKind::Parameter { index, data_type } => {
            let value = context.parameters.get(*index).ok_or_else(|| {
                SqlError::new(
                    SqlErrorKind::Constraint,
                    format!("missing value for parameter ?{}", index + 1),
                    expression.span,
                )
            })?;
            ensure_runtime_type(value, *data_type, expression.span)?;
            Ok(value.clone())
        }
        BoundExprKind::Column(column) => {
            let value = context.row.values.get(column.input_slot).ok_or_else(|| {
                SqlError::new(
                    SqlErrorKind::Constraint,
                    format!(
                        "row has no slot {} for {}.{}",
                        column.input_slot, column.table_alias, column.column.name
                    ),
                    expression.span,
                )
            })?;
            ensure_runtime_type(value, Some(column.column.data_type), expression.span)?;
            Ok(value.clone())
        }
        BoundExprKind::Unary { op, expr } => {
            let value = evaluate(expr, context)?;
            match op {
                UnaryOp::Not => Ok(truth(&value, expression.span)?.not().into_value()),
                UnaryOp::Negate => match value {
                    Value::Null => Ok(Value::Null),
                    Value::Int64(value) => value.checked_neg().map(Value::Int64).ok_or_else(|| {
                        SqlError::new(
                            SqlErrorKind::Constraint,
                            "INT64 negation overflow",
                            expression.span,
                        )
                    }),
                    other => Err(type_error("negate", &other, expression.span)),
                },
            }
        }
        BoundExprKind::Binary { left, op, right } => {
            let left = evaluate(left, context)?;
            match op {
                BinaryOp::And => {
                    let left_truth = truth(&left, expression.span)?;
                    if left_truth == TruthValue::False {
                        return Ok(Value::Bool(false));
                    }
                    let right = evaluate(right, context)?;
                    Ok(left_truth.and(truth(&right, expression.span)?).into_value())
                }
                BinaryOp::Or => {
                    let left_truth = truth(&left, expression.span)?;
                    if left_truth == TruthValue::True {
                        return Ok(Value::Bool(true));
                    }
                    let right = evaluate(right, context)?;
                    Ok(left_truth.or(truth(&right, expression.span)?).into_value())
                }
                comparison if comparison.is_comparison() => {
                    let right = evaluate(right, context)?;
                    compare(&left, *comparison, &right, expression.span)
                }
                _ => unreachable!("AST has no other binary operators"),
            }
        }
        BoundExprKind::IsNull { expr, negated } => {
            let is_null = evaluate(expr, context)?.is_null();
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        BoundExprKind::Aggregate { .. } => Err(SqlError::unsupported(
            "aggregate expressions require an aggregate executor",
            expression.span,
        )),
    }
}

/// SQL WHERE keeps only TRUE; FALSE and UNKNOWN both reject a row.
pub fn predicate_matches(expression: &BoundExpr, context: &EvaluationContext<'_>) -> Result<bool> {
    Ok(truth(&evaluate(expression, context)?, expression.span)? == TruthValue::True)
}

pub fn validate_parameters(specifications: &[ParameterSpec], values: &[Value]) -> Result<()> {
    if specifications.len() != values.len() {
        return Err(SqlError::new(
            SqlErrorKind::Constraint,
            format!(
                "statement expects {} parameters, received {}",
                specifications.len(),
                values.len()
            ),
            crate::ast::Span::default(),
        ));
    }
    for specification in specifications {
        let value = values.get(specification.index).ok_or_else(|| {
            SqlError::new(
                SqlErrorKind::Constraint,
                format!("missing parameter ?{}", specification.index + 1),
                crate::ast::Span::default(),
            )
        })?;
        ensure_runtime_type(value, specification.data_type, crate::ast::Span::default())?;
        if value.is_null() && !specification.nullable {
            return Err(SqlError::new(
                SqlErrorKind::Constraint,
                format!("parameter ?{} cannot be NULL", specification.index + 1),
                crate::ast::Span::default(),
            ));
        }
    }
    Ok(())
}

fn truth(value: &Value, span: crate::ast::Span) -> Result<TruthValue> {
    match value {
        Value::Bool(false) => Ok(TruthValue::False),
        Value::Bool(true) => Ok(TruthValue::True),
        Value::Null => Ok(TruthValue::Unknown),
        other => Err(type_error("use as a predicate", other, span)),
    }
}

fn compare(
    left: &Value,
    operator: BinaryOp,
    right: &Value,
    span: crate::ast::Span,
) -> Result<Value> {
    let ordering = left
        .checked_cmp(right)
        .map_err(|error| SqlError::new(SqlErrorKind::Type, error.to_string(), span))?;
    let Some(ordering) = ordering else {
        return Ok(Value::Null);
    };
    let result = match operator {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Lt => ordering == Ordering::Less,
        BinaryOp::LtEq => ordering != Ordering::Greater,
        BinaryOp::Gt => ordering == Ordering::Greater,
        BinaryOp::GtEq => ordering != Ordering::Less,
        BinaryOp::And | BinaryOp::Or => unreachable!("comparison caller filters operators"),
    };
    Ok(Value::Bool(result))
}

fn ensure_runtime_type(
    value: &Value,
    expected: Option<DataType>,
    span: crate::ast::Span,
) -> Result<()> {
    if let (Some(expected), Some(actual)) = (expected, value.data_type())
        && expected != actual
    {
        return Err(SqlError::new(
            SqlErrorKind::Type,
            format!("expected {expected}, found {actual}"),
            span,
        ));
    }
    Ok(())
}

fn type_error(operation: &str, value: &Value, span: crate::ast::Span) -> SqlError {
    SqlError::new(
        SqlErrorKind::Type,
        format!("cannot {operation} {}", value.type_name()),
        span,
    )
}

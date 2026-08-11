use std::cmp::Ordering;

use aster_core::{Error as CoreError, Row, Value};
use serde::{Deserialize, Serialize};

use crate::{EngineError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    #[must_use]
    pub const fn is_true(self) -> bool {
        matches!(self, Self::True)
    }

    #[must_use]
    pub const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

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

    pub fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bool(true) => Ok(Self::True),
            Value::Bool(false) => Ok(Self::False),
            Value::Null => Ok(Self::Unknown),
            other => Err(CoreError::Type(format!(
                "expected BOOL predicate, got {}",
                other.type_name()
            ))
            .into()),
        }
    }

    #[must_use]
    pub const fn into_value(self) -> Value {
        match self {
            Self::True => Value::Bool(true),
            Self::False => Value::Bool(false),
            Self::Unknown => Value::Null,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Negate,
    IsNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Less,
    LessEq,
    Greater,
    GreaterEq,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    Column(usize),
    Literal(Value),
    Unary {
        op: UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        left: Box<Self>,
        op: BinaryOp,
        right: Box<Self>,
    },
}

impl Expr {
    pub fn evaluate(&self, row: &Row) -> Result<Value> {
        match self {
            Self::Column(index) => row.values.get(*index).cloned().ok_or_else(|| {
                EngineError::Expression(format!(
                    "column offset {index} is outside a {}-column row",
                    row.values.len()
                ))
            }),
            Self::Literal(value) => Ok(value.clone()),
            Self::Unary { op, expr } => evaluate_unary(*op, expr.evaluate(row)?),
            Self::Binary { left, op, right } => {
                let left = left.evaluate(row)?;
                match op {
                    BinaryOp::And if Truth::from_value(&left)? == Truth::False => {
                        Ok(Value::Bool(false))
                    }
                    BinaryOp::Or if Truth::from_value(&left)? == Truth::True => {
                        Ok(Value::Bool(true))
                    }
                    _ => evaluate_binary(left, *op, right.evaluate(row)?),
                }
            }
        }
    }

    pub fn evaluate_predicate(&self, row: &Row) -> Result<bool> {
        Ok(Truth::from_value(&self.evaluate(row)?)?.is_true())
    }
}

fn evaluate_unary(op: UnaryOp, value: Value) -> Result<Value> {
    match op {
        UnaryOp::Not => Ok(Truth::from_value(&value)?.not().into_value()),
        UnaryOp::IsNull => Ok(Value::Bool(value.is_null())),
        UnaryOp::Negate => match value {
            Value::Null => Ok(Value::Null),
            Value::Int64(value) => value
                .checked_neg()
                .map(Value::Int64)
                .ok_or_else(|| EngineError::Expression("integer overflow during negation".into())),
            other => Err(CoreError::Type(format!("cannot negate {}", other.type_name())).into()),
        },
    }
}

fn evaluate_binary(left: Value, op: BinaryOp, right: Value) -> Result<Value> {
    match op {
        BinaryOp::And => Ok(Truth::from_value(&left)?
            .and(Truth::from_value(&right)?)
            .into_value()),
        BinaryOp::Or => Ok(Truth::from_value(&left)?
            .or(Truth::from_value(&right)?)
            .into_value()),
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Less
        | BinaryOp::LessEq
        | BinaryOp::Greater
        | BinaryOp::GreaterEq => compare(&left, op, &right),
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
            arithmetic(left, op, right)
        }
    }
}

fn compare(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    let Some(ordering) = left.checked_cmp(right)? else {
        return Ok(Value::Null);
    };
    let result = match op {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Less => ordering == Ordering::Less,
        BinaryOp::LessEq => ordering != Ordering::Greater,
        BinaryOp::Greater => ordering == Ordering::Greater,
        BinaryOp::GreaterEq => ordering != Ordering::Less,
        _ => {
            return Err(EngineError::Expression(
                "invalid comparison operator".into(),
            ));
        }
    };
    Ok(Value::Bool(result))
}

fn arithmetic(left: Value, op: BinaryOp, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Int64(left), Value::Int64(right)) => {
            let result = match op {
                BinaryOp::Add => left.checked_add(right),
                BinaryOp::Subtract => left.checked_sub(right),
                BinaryOp::Multiply => left.checked_mul(right),
                BinaryOp::Divide => left.checked_div(right),
                _ => unreachable!("caller restricts arithmetic operators"),
            };
            result.map(Value::Int64).ok_or_else(|| {
                EngineError::Expression("integer overflow or division by zero".into())
            })
        }
        (left, right) => Err(CoreError::Type(format!(
            "arithmetic requires INT64, got {} and {}",
            left.type_name(),
            right.type_name()
        ))
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use aster_core::{Row, Value};

    use super::{BinaryOp, Expr, Truth, UnaryOp};

    #[test]
    fn sql_three_valued_logic_and_short_circuiting() {
        assert_eq!(Truth::Unknown.and(Truth::False), Truth::False);
        assert_eq!(Truth::Unknown.or(Truth::True), Truth::True);
        assert_eq!(Truth::Unknown.not(), Truth::Unknown);

        let dangerous = Expr::Binary {
            left: Box::new(Expr::Literal(Value::Int64(1))),
            op: BinaryOp::Divide,
            right: Box::new(Expr::Literal(Value::Int64(0))),
        };
        let expression = Expr::Binary {
            left: Box::new(Expr::Literal(Value::Bool(false))),
            op: BinaryOp::And,
            right: Box::new(dangerous),
        };
        assert_eq!(
            expression.evaluate(&Row { values: vec![] }).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn null_comparisons_are_unknown_and_is_null_is_boolean() {
        let comparison = Expr::Binary {
            left: Box::new(Expr::Column(0)),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Value::Int64(1))),
        };
        let row = Row {
            values: vec![Value::Null],
        };
        assert_eq!(comparison.evaluate(&row).unwrap(), Value::Null);
        assert!(!comparison.evaluate_predicate(&row).unwrap());
        let is_null = Expr::Unary {
            op: UnaryOp::IsNull,
            expr: Box::new(Expr::Column(0)),
        };
        assert_eq!(is_null.evaluate(&row).unwrap(), Value::Bool(true));
    }
}

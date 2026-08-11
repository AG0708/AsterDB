//! SQL parsing, semantic analysis, and planning for `AsterDB`.
//!
//! The crate deliberately stops at a typed physical plan. It has no dependency
//! on the storage or transaction implementations, which keeps SQL diagnostics
//! deterministic and makes plans easy to test in isolation.

#![forbid(unsafe_code)]

pub mod ast;
pub mod binder;
pub mod catalog;
pub mod eval;
pub mod lexer;
pub mod parser;
pub mod plan;

use std::error::Error;
use std::fmt;

pub use ast::{Ident, Span, Spanned, Statement};
pub use binder::{BindOutput, Binder, bind};
pub use catalog::{Catalog, IndexDef, MemoryCatalog, TableDef};
pub use lexer::{Token, TokenKind, lex};
pub use parser::{parse_statement, parse_statements};
pub use plan::{BoundStatement, LogicalPlan, PhysicalPlan, optimize};

/// Broad phase in which a SQL request was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorKind {
    Lex,
    Parse,
    Bind,
    Type,
    Constraint,
    Unsupported,
}

/// A deterministic user-facing diagnostic with an exact UTF-8 byte range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    pub kind: SqlErrorKind,
    pub message: String,
    pub span: Span,
}

impl SqlError {
    #[must_use]
    pub fn new(kind: SqlErrorKind, message: impl Into<String>, span: Span) -> Self {
        Self {
            kind,
            message: message.into(),
            span,
        }
    }

    #[must_use]
    pub fn unsupported(message: impl Into<String>, span: Span) -> Self {
        Self::new(SqlErrorKind::Unsupported, message, span)
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} error at bytes {}..{}: {}",
            self.kind, self.span.start, self.span.end, self.message
        )
    }
}

impl Error for SqlError {}

pub type Result<T> = std::result::Result<T, SqlError>;

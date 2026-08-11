use std::fmt;

use aster_core::{DataType, Value};

/// Half-open UTF-8 byte range in the original SQL source.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub const fn join(self, other: Self) -> Self {
        Self {
            start: self.start,
            end: other.end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spanned<T> {
    pub value: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    #[must_use]
    pub const fn new(value: T, span: Span) -> Self {
        Self { value, span }
    }
}

/// SQL identifier. Unquoted names are resolved case-insensitively.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    pub value: String,
    pub quoted: bool,
    pub span: Span,
}

impl Ident {
    #[must_use]
    pub fn matches(&self, candidate: &str) -> bool {
        if self.quoted {
            self.value == candidate
        } else {
            self.value.eq_ignore_ascii_case(candidate)
        }
    }

    #[must_use]
    pub fn canonical(&self) -> String {
        if self.quoted {
            self.value.clone()
        } else {
            self.value.to_ascii_lowercase()
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.quoted {
            write!(f, "\"{}\"", self.value.replace('"', "\"\""))
        } else {
            f.write_str(&self.value)
        }
    }
}

pub type Expr = Spanned<ExprKind>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprKind {
    Literal(Value),
    Parameter(usize),
    Column {
        qualifier: Option<Ident>,
        name: Ident,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Aggregate {
        function: AggregateFunction,
        argument: AggregateArgument,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Or,
    And,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl BinaryOp {
    #[must_use]
    pub const fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::NotEq | Self::Lt | Self::LtEq | Self::Gt | Self::GtEq
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateArgument {
    Star,
    Expr(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
// Keeping statement payloads inline makes the public AST substantially easier
// for executors and tooling to consume; statements are always passed by reference.
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Select(Select),
    Begin,
    Commit,
    Rollback,
    Explain(Box<Spanned<Statement>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub name: Ident,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: Ident,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndex {
    pub name: Ident,
    pub table: Ident,
    pub column: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    pub table: Ident,
    pub columns: Option<Vec<Ident>>,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub table: Ident,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: Ident,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub table: Ident,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    pub from: TableSource,
    pub joins: Vec<Join>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Wildcard(Span),
    Expr { expr: Expr, alias: Option<Ident> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSource {
    pub name: Ident,
    pub alias: Option<Ident>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub right: TableSource,
    pub on: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    pub expr: Expr,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

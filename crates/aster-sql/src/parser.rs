use aster_core::{DataType, Value};

use crate::ast::{
    AggregateArgument, AggregateFunction, Assignment, BinaryOp, ColumnDef, CreateIndex,
    CreateTable, Delete, Expr, ExprKind, Ident, Insert, Join, OrderBy, OrderDirection, Select,
    SelectItem, Span, Spanned, Statement, TableSource, UnaryOp, Update,
};
use crate::lexer::{Keyword, Token, TokenKind, lex};
use crate::{Result, SqlError, SqlErrorKind};

pub fn parse_statement(source: &str) -> Result<Spanned<Statement>> {
    let mut statements = parse_statements(source)?;
    if statements.len() != 1 {
        return Err(SqlError::new(
            SqlErrorKind::Parse,
            format!("expected exactly one statement, found {}", statements.len()),
            Span::new(0, source.len()),
        ));
    }
    Ok(statements.remove(0))
}

pub fn parse_statements(source: &str) -> Result<Vec<Spanned<Statement>>> {
    let tokens = lex(source)?;
    let mut parser = Parser::new(tokens);
    let mut statements = Vec::new();
    while !parser.at(&TokenKind::Eof) {
        if parser.consume(&TokenKind::Semicolon).is_some() {
            continue;
        }
        // Positional parameters are numbered per statement, not per batch.
        parser.next_parameter = 0;
        statements.push(parser.statement()?);
        if parser.consume(&TokenKind::Semicolon).is_none() && !parser.at(&TokenKind::Eof) {
            return Err(parser.error("expected `;` between statements"));
        }
    }
    if statements.is_empty() {
        return Err(SqlError::new(
            SqlErrorKind::Parse,
            "SQL input contains no statement",
            Span::new(0, source.len()),
        ));
    }
    Ok(statements)
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
    next_parameter: usize,
}

impl Parser {
    const fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
            next_parameter: 0,
        }
    }

    fn statement(&mut self) -> Result<Spanned<Statement>> {
        let start = self.current().span;
        let value = match self.current().kind {
            TokenKind::Keyword(Keyword::Create) => self.create()?,
            TokenKind::Keyword(Keyword::Insert) => self.insert()?,
            TokenKind::Keyword(Keyword::Update) => self.update()?,
            TokenKind::Keyword(Keyword::Delete) => self.delete()?,
            TokenKind::Keyword(Keyword::Select) => Statement::Select(self.select()?),
            TokenKind::Keyword(Keyword::Begin) => {
                self.advance();
                self.consume_keyword(Keyword::Transaction);
                Statement::Begin
            }
            TokenKind::Keyword(Keyword::Commit) => {
                self.advance();
                self.consume_keyword(Keyword::Transaction);
                Statement::Commit
            }
            TokenKind::Keyword(Keyword::Rollback) => {
                self.advance();
                self.consume_keyword(Keyword::Transaction);
                Statement::Rollback
            }
            TokenKind::Keyword(Keyword::Explain) => {
                self.advance();
                let inner = self.statement()?;
                if matches!(inner.value, Statement::Explain(_)) {
                    return Err(SqlError::unsupported(
                        "nested EXPLAIN is not supported",
                        inner.span,
                    ));
                }
                Statement::Explain(Box::new(inner))
            }
            _ => {
                return Err(SqlError::unsupported(
                    format!(
                        "unsupported statement beginning with {:?}",
                        self.current().kind
                    ),
                    self.current().span,
                ));
            }
        };
        let end = self.previous().span;
        Ok(Spanned::new(value, start.join(end)))
    }

    fn create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Create)?;
        if self.consume_keyword(Keyword::Unique).is_some() {
            return Err(SqlError::unsupported(
                "UNIQUE indexes are not part of the v1 SQL subset",
                self.previous().span,
            ));
        }
        if self.consume_keyword(Keyword::Table).is_some() {
            return self.create_table().map(Statement::CreateTable);
        }
        if self.consume_keyword(Keyword::Index).is_some() {
            return self.create_index().map(Statement::CreateIndex);
        }
        Err(self.error("expected TABLE or INDEX after CREATE"))
    }

    fn create_table(&mut self) -> Result<CreateTable> {
        let name = self.identifier()?;
        self.expect(&TokenKind::LeftParen, "expected `(` after table name")?;
        let mut columns = Vec::new();
        loop {
            let start = self.current().span;
            let column_name = self.identifier()?;
            let data_type = self.data_type()?;
            let mut nullable = true;
            let mut primary_key = false;
            let mut saw_nullability = false;
            loop {
                if self.consume_keyword(Keyword::Primary).is_some() {
                    if primary_key {
                        return Err(self.error("duplicate PRIMARY KEY clause"));
                    }
                    self.expect_keyword(Keyword::Key)?;
                    primary_key = true;
                    nullable = false;
                } else if self.consume_keyword(Keyword::Not).is_some() {
                    if saw_nullability {
                        return Err(self.error("duplicate NULL/NOT NULL clause"));
                    }
                    self.expect_keyword(Keyword::Null)?;
                    nullable = false;
                    saw_nullability = true;
                } else if self.consume_keyword(Keyword::Null).is_some() {
                    if saw_nullability || primary_key {
                        return Err(self.error("invalid or duplicate NULL clause"));
                    }
                    nullable = true;
                    saw_nullability = true;
                } else {
                    break;
                }
            }
            columns.push(ColumnDef {
                name: column_name,
                data_type,
                nullable,
                primary_key,
                span: start.join(self.previous().span),
            });
            if self.consume(&TokenKind::Comma).is_none() {
                break;
            }
        }
        self.expect(
            &TokenKind::RightParen,
            "expected `)` after column definitions",
        )?;
        Ok(CreateTable { name, columns })
    }

    fn create_index(&mut self) -> Result<CreateIndex> {
        let name = self.identifier()?;
        self.expect_keyword(Keyword::On)?;
        let table = self.identifier()?;
        self.expect(&TokenKind::LeftParen, "expected `(` before indexed column")?;
        let column = self.identifier()?;
        if self.consume(&TokenKind::Comma).is_some() {
            return Err(SqlError::unsupported(
                "multi-column indexes are not part of the v1 SQL subset",
                self.previous().span,
            ));
        }
        self.expect(&TokenKind::RightParen, "expected `)` after indexed column")?;
        Ok(CreateIndex {
            name,
            table,
            column,
        })
    }

    fn insert(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.identifier()?;
        let columns = if self.consume(&TokenKind::LeftParen).is_some() {
            let names = self.comma_separated_identifiers()?;
            self.expect(&TokenKind::RightParen, "expected `)` after insert columns")?;
            Some(names)
        } else {
            None
        };
        self.expect_keyword(Keyword::Values)?;
        self.expect(&TokenKind::LeftParen, "expected `(` after VALUES")?;
        let values = self.comma_separated_expressions()?;
        self.expect(&TokenKind::RightParen, "expected `)` after inserted values")?;
        if self.at(&TokenKind::Comma) {
            return Err(SqlError::unsupported(
                "multi-row INSERT is not part of the v1 SQL subset",
                self.current().span,
            ));
        }
        Ok(Statement::Insert(Insert {
            table,
            columns,
            values,
        }))
    }

    fn update(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Update)?;
        let table = self.identifier()?;
        self.expect_keyword(Keyword::Set)?;
        let mut assignments = Vec::new();
        loop {
            let column = self.identifier()?;
            self.expect(&TokenKind::Eq, "expected `=` in assignment")?;
            assignments.push(Assignment {
                column,
                value: self.expression()?,
            });
            if self.consume(&TokenKind::Comma).is_none() {
                break;
            }
        }
        let selection = if self.consume_keyword(Keyword::Where).is_some() {
            Some(self.expression()?)
        } else {
            None
        };
        Ok(Statement::Update(Update {
            table,
            assignments,
            selection,
        }))
    }

    fn delete(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table = self.identifier()?;
        let selection = if self.consume_keyword(Keyword::Where).is_some() {
            Some(self.expression()?)
        } else {
            None
        };
        Ok(Statement::Delete(Delete { table, selection }))
    }

    fn select(&mut self) -> Result<Select> {
        self.expect_keyword(Keyword::Select)?;
        let mut projection = Vec::new();
        loop {
            if let Some(star) = self.consume(&TokenKind::Star) {
                projection.push(SelectItem::Wildcard(star.span));
            } else {
                let expr = self.expression()?;
                let alias = if self.consume_keyword(Keyword::As).is_some() {
                    Some(self.identifier()?)
                } else {
                    None
                };
                projection.push(SelectItem::Expr { expr, alias });
            }
            if self.consume(&TokenKind::Comma).is_none() {
                break;
            }
        }
        self.expect_keyword(Keyword::From)?;
        let from = self.table_source()?;
        let mut joins = Vec::new();
        loop {
            let start = if let Some(inner) = self.consume_keyword(Keyword::Inner) {
                self.expect_keyword(Keyword::Join)?;
                inner.span
            } else if let Some(join) = self.consume_keyword(Keyword::Join) {
                join.span
            } else {
                break;
            };
            let right = self.table_source()?;
            self.expect_keyword(Keyword::On)?;
            let on = self.expression()?;
            let end = on.span;
            joins.push(Join {
                right,
                on,
                span: start.join(end),
            });
        }
        let selection = if self.consume_keyword(Keyword::Where).is_some() {
            Some(self.expression()?)
        } else {
            None
        };
        let group_by = if self.consume_keyword(Keyword::Group).is_some() {
            self.expect_keyword(Keyword::By)?;
            self.comma_separated_expressions()?
        } else {
            Vec::new()
        };
        let order_by = if self.consume_keyword(Keyword::Order).is_some() {
            self.expect_keyword(Keyword::By)?;
            let mut orders = Vec::new();
            loop {
                let expr = self.expression()?;
                let direction = if self.consume_keyword(Keyword::Desc).is_some() {
                    OrderDirection::Desc
                } else {
                    self.consume_keyword(Keyword::Asc);
                    OrderDirection::Asc
                };
                orders.push(OrderBy { expr, direction });
                if self.consume(&TokenKind::Comma).is_none() {
                    break;
                }
            }
            orders
        } else {
            Vec::new()
        };
        let limit = if self.consume_keyword(Keyword::Limit).is_some() {
            Some(self.expression()?)
        } else {
            None
        };
        Ok(Select {
            projection,
            from,
            joins,
            selection,
            group_by,
            order_by,
            limit,
        })
    }

    fn table_source(&mut self) -> Result<TableSource> {
        let name = self.identifier()?;
        let explicit_alias = self.consume_keyword(Keyword::As).is_some();
        let alias = if explicit_alias || matches!(self.current().kind, TokenKind::Identifier { .. })
        {
            Some(self.identifier()?)
        } else {
            None
        };
        let end = alias.as_ref().map_or(name.span, |alias| alias.span);
        let span = name.span.join(end);
        Ok(TableSource { name, alias, span })
    }

    fn expression(&mut self) -> Result<Expr> {
        self.or_expression()
    }

    fn or_expression(&mut self) -> Result<Expr> {
        let mut expr = self.and_expression()?;
        while self.consume_keyword(Keyword::Or).is_some() {
            let right = self.and_expression()?;
            let span = expr.span.join(right.span);
            expr = Spanned::new(
                ExprKind::Binary {
                    left: Box::new(expr),
                    op: BinaryOp::Or,
                    right: Box::new(right),
                },
                span,
            );
        }
        Ok(expr)
    }

    fn and_expression(&mut self) -> Result<Expr> {
        let mut expr = self.not_expression()?;
        while self.consume_keyword(Keyword::And).is_some() {
            let right = self.not_expression()?;
            let span = expr.span.join(right.span);
            expr = Spanned::new(
                ExprKind::Binary {
                    left: Box::new(expr),
                    op: BinaryOp::And,
                    right: Box::new(right),
                },
                span,
            );
        }
        Ok(expr)
    }

    fn not_expression(&mut self) -> Result<Expr> {
        if let Some(not) = self.consume_keyword(Keyword::Not) {
            let expr = self.not_expression()?;
            let span = not.span.join(expr.span);
            Ok(Spanned::new(
                ExprKind::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(expr),
                },
                span,
            ))
        } else {
            self.comparison_expression()
        }
    }

    fn comparison_expression(&mut self) -> Result<Expr> {
        let mut expr = self.unary_expression()?;
        if self.consume_keyword(Keyword::Is).is_some() {
            let negated = self.consume_keyword(Keyword::Not).is_some();
            self.expect_keyword(Keyword::Null)?;
            let span = expr.span.join(self.previous().span);
            return Ok(Spanned::new(
                ExprKind::IsNull {
                    expr: Box::new(expr),
                    negated,
                },
                span,
            ));
        }
        let operator = if self.consume(&TokenKind::Eq).is_some() {
            Some(BinaryOp::Eq)
        } else if self.consume(&TokenKind::NotEq).is_some() {
            Some(BinaryOp::NotEq)
        } else if self.consume(&TokenKind::Lt).is_some() {
            Some(BinaryOp::Lt)
        } else if self.consume(&TokenKind::LtEq).is_some() {
            Some(BinaryOp::LtEq)
        } else if self.consume(&TokenKind::Gt).is_some() {
            Some(BinaryOp::Gt)
        } else if self.consume(&TokenKind::GtEq).is_some() {
            Some(BinaryOp::GtEq)
        } else {
            None
        };
        if let Some(op) = operator {
            let right = self.unary_expression()?;
            let span = expr.span.join(right.span);
            expr = Spanned::new(
                ExprKind::Binary {
                    left: Box::new(expr),
                    op,
                    right: Box::new(right),
                },
                span,
            );
            if self.is_comparison_token() {
                return Err(self.error(
                    "chained comparisons are unsupported; combine predicates with AND or OR",
                ));
            }
        }
        Ok(expr)
    }

    fn unary_expression(&mut self) -> Result<Expr> {
        if let Some(minus) = self.consume(&TokenKind::Minus) {
            let expr = self.unary_expression()?;
            let span = minus.span.join(expr.span);
            return Ok(Spanned::new(
                ExprKind::Unary {
                    op: UnaryOp::Negate,
                    expr: Box::new(expr),
                },
                span,
            ));
        }
        self.primary_expression()
    }

    fn primary_expression(&mut self) -> Result<Expr> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Literal(value) => Ok(Spanned::new(ExprKind::Literal(value), token.span)),
            TokenKind::Keyword(Keyword::Null) => {
                Ok(Spanned::new(ExprKind::Literal(Value::Null), token.span))
            }
            TokenKind::Keyword(Keyword::True) => Ok(Spanned::new(
                ExprKind::Literal(Value::Bool(true)),
                token.span,
            )),
            TokenKind::Keyword(Keyword::False) => Ok(Spanned::new(
                ExprKind::Literal(Value::Bool(false)),
                token.span,
            )),
            TokenKind::Parameter => {
                let index = self.next_parameter;
                self.next_parameter += 1;
                Ok(Spanned::new(ExprKind::Parameter(index), token.span))
            }
            TokenKind::LeftParen => {
                let mut expr = self.expression()?;
                let right = self.expect(&TokenKind::RightParen, "expected `)`")?;
                expr.span = token.span.join(right.span);
                Ok(expr)
            }
            TokenKind::Keyword(
                keyword @ (Keyword::Count | Keyword::Sum | Keyword::Min | Keyword::Max),
            ) => self.aggregate(keyword, token.span),
            TokenKind::Identifier { value, quoted } => {
                let first = Ident {
                    value,
                    quoted,
                    span: token.span,
                };
                if self.at(&TokenKind::LeftParen) {
                    return Err(SqlError::unsupported(
                        format!("scalar function `{first}` is not supported"),
                        first.span,
                    ));
                }
                if self.consume(&TokenKind::Dot).is_some() {
                    let name = self.identifier()?;
                    let span = first.span.join(name.span);
                    Ok(Spanned::new(
                        ExprKind::Column {
                            qualifier: Some(first),
                            name,
                        },
                        span,
                    ))
                } else {
                    let span = first.span;
                    Ok(Spanned::new(
                        ExprKind::Column {
                            qualifier: None,
                            name: first,
                        },
                        span,
                    ))
                }
            }
            other => Err(SqlError::new(
                SqlErrorKind::Parse,
                format!("expected expression, found {other:?}"),
                token.span,
            )),
        }
    }

    fn aggregate(&mut self, keyword: Keyword, start: Span) -> Result<Expr> {
        let function = match keyword {
            Keyword::Count => AggregateFunction::Count,
            Keyword::Sum => AggregateFunction::Sum,
            Keyword::Min => AggregateFunction::Min,
            Keyword::Max => AggregateFunction::Max,
            _ => unreachable!("caller limits aggregate keywords"),
        };
        self.expect(&TokenKind::LeftParen, "expected `(` after aggregate")?;
        let argument = if self.consume(&TokenKind::Star).is_some() {
            if function != AggregateFunction::Count {
                return Err(SqlError::new(
                    SqlErrorKind::Type,
                    "only COUNT accepts `*`",
                    self.previous().span,
                ));
            }
            AggregateArgument::Star
        } else {
            AggregateArgument::Expr(Box::new(self.expression()?))
        };
        let right = self.expect(&TokenKind::RightParen, "expected `)` after aggregate")?;
        Ok(Spanned::new(
            ExprKind::Aggregate { function, argument },
            start.join(right.span),
        ))
    }

    fn data_type(&mut self) -> Result<DataType> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Keyword(Keyword::Int64) => Ok(DataType::Int64),
            TokenKind::Keyword(Keyword::Bool) => Ok(DataType::Bool),
            TokenKind::Keyword(Keyword::Text) => Ok(DataType::Text),
            TokenKind::Keyword(Keyword::Bytes) => Ok(DataType::Bytes),
            _ => Err(SqlError::unsupported(
                "v1 column types are INT64, BOOL, TEXT, and BYTES",
                token.span,
            )),
        }
    }

    fn comma_separated_identifiers(&mut self) -> Result<Vec<Ident>> {
        let mut values = vec![self.identifier()?];
        while self.consume(&TokenKind::Comma).is_some() {
            values.push(self.identifier()?);
        }
        Ok(values)
    }

    fn comma_separated_expressions(&mut self) -> Result<Vec<Expr>> {
        let mut values = vec![self.expression()?];
        while self.consume(&TokenKind::Comma).is_some() {
            values.push(self.expression()?);
        }
        Ok(values)
    }

    fn identifier(&mut self) -> Result<Ident> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Identifier { value, quoted } => Ok(Ident {
                value,
                quoted,
                span: token.span,
            }),
            _ => Err(SqlError::new(
                SqlErrorKind::Parse,
                "expected identifier",
                token.span,
            )),
        }
    }

    fn expect_keyword(&mut self, keyword: Keyword) -> Result<Token> {
        self.consume_keyword(keyword).ok_or_else(|| {
            self.error(format!(
                "expected keyword {keyword:?}, found {:?}",
                self.current().kind
            ))
        })
    }

    fn consume_keyword(&mut self, keyword: Keyword) -> Option<Token> {
        if self.current().kind == TokenKind::Keyword(keyword) {
            Some(self.advance().clone())
        } else {
            None
        }
    }

    fn expect(&mut self, kind: &TokenKind, message: &str) -> Result<Token> {
        self.consume(kind).ok_or_else(|| self.error(message))
    }

    fn consume(&mut self, kind: &TokenKind) -> Option<Token> {
        if self.at(kind) {
            Some(self.advance().clone())
        } else {
            None
        }
    }

    fn at(&self, kind: &TokenKind) -> bool {
        &self.current().kind == kind
    }

    fn is_comparison_token(&self) -> bool {
        matches!(
            self.current().kind,
            TokenKind::Eq
                | TokenKind::NotEq
                | TokenKind::Lt
                | TokenKind::LtEq
                | TokenKind::Gt
                | TokenKind::GtEq
        )
    }

    fn advance(&mut self) -> &Token {
        let current = self.position;
        if self.tokens[current].kind != TokenKind::Eof {
            self.position += 1;
        }
        &self.tokens[current]
    }

    fn current(&self) -> &Token {
        &self.tokens[self.position]
    }

    fn previous(&self) -> &Token {
        &self.tokens[self.position.saturating_sub(1)]
    }

    fn error(&self, message: impl Into<String>) -> SqlError {
        SqlError::new(SqlErrorKind::Parse, message, self.current().span)
    }
}

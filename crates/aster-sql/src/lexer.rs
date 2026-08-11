use aster_core::Value;

use crate::ast::Span;
use crate::{Result, SqlError, SqlErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    By,
    Commit,
    Count,
    Create,
    Delete,
    Desc,
    Explain,
    False,
    From,
    Group,
    Index,
    Inner,
    Insert,
    Int64,
    Into,
    Is,
    Join,
    Key,
    Limit,
    Max,
    Min,
    Not,
    Null,
    On,
    Or,
    Order,
    Primary,
    Rollback,
    Select,
    Set,
    Sum,
    Table,
    Text,
    Transaction,
    True,
    Unique,
    Update,
    Values,
    Where,
    Bytes,
}

impl Keyword {
    fn from_identifier(value: &str) -> Option<Self> {
        Some(match value.to_ascii_uppercase().as_str() {
            "AND" => Self::And,
            "AS" => Self::As,
            "ASC" => Self::Asc,
            "BEGIN" => Self::Begin,
            "BOOL" => Self::Bool,
            "BY" => Self::By,
            "COMMIT" => Self::Commit,
            "COUNT" => Self::Count,
            "CREATE" => Self::Create,
            "DELETE" => Self::Delete,
            "DESC" => Self::Desc,
            "EXPLAIN" => Self::Explain,
            "FALSE" => Self::False,
            "FROM" => Self::From,
            "GROUP" => Self::Group,
            "INDEX" => Self::Index,
            "INNER" => Self::Inner,
            "INSERT" => Self::Insert,
            "INT64" => Self::Int64,
            "INTO" => Self::Into,
            "IS" => Self::Is,
            "JOIN" => Self::Join,
            "KEY" => Self::Key,
            "LIMIT" => Self::Limit,
            "MAX" => Self::Max,
            "MIN" => Self::Min,
            "NOT" => Self::Not,
            "NULL" => Self::Null,
            "ON" => Self::On,
            "OR" => Self::Or,
            "ORDER" => Self::Order,
            "PRIMARY" => Self::Primary,
            "ROLLBACK" => Self::Rollback,
            "SELECT" => Self::Select,
            "SET" => Self::Set,
            "SUM" => Self::Sum,
            "TABLE" => Self::Table,
            "TEXT" => Self::Text,
            "TRANSACTION" => Self::Transaction,
            "TRUE" => Self::True,
            "UNIQUE" => Self::Unique,
            "UPDATE" => Self::Update,
            "VALUES" => Self::Values,
            "WHERE" => Self::Where,
            "BYTES" => Self::Bytes,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    Identifier { value: String, quoted: bool },
    Keyword(Keyword),
    Literal(Value),
    Parameter,
    LeftParen,
    RightParen,
    Comma,
    Dot,
    Star,
    Semicolon,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Minus,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

pub fn lex(source: &str) -> Result<Vec<Token>> {
    Lexer::new(source).lex_all()
}

struct Lexer<'a> {
    source: &'a str,
    position: usize,
}

impl<'a> Lexer<'a> {
    const fn new(source: &'a str) -> Self {
        Self {
            source,
            position: 0,
        }
    }

    fn lex_all(mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            self.skip_trivia()?;
            let token = self.next_token()?;
            let done = token.kind == TokenKind::Eof;
            tokens.push(token);
            if done {
                return Ok(tokens);
            }
        }
    }

    fn next_token(&mut self) -> Result<Token> {
        let start = self.position;
        let Some(character) = self.bump() else {
            return Ok(Token {
                kind: TokenKind::Eof,
                span: Span::new(start, start),
            });
        };
        let kind = match character {
            '(' => TokenKind::LeftParen,
            ')' => TokenKind::RightParen,
            ',' => TokenKind::Comma,
            '.' => TokenKind::Dot,
            '*' => TokenKind::Star,
            ';' => TokenKind::Semicolon,
            '?' => TokenKind::Parameter,
            '=' => TokenKind::Eq,
            '-' => TokenKind::Minus,
            '<' if self.consume('=') => TokenKind::LtEq,
            '<' if self.consume('>') => TokenKind::NotEq,
            '<' => TokenKind::Lt,
            '>' if self.consume('=') => TokenKind::GtEq,
            '>' => TokenKind::Gt,
            '!' if self.consume('=') => TokenKind::NotEq,
            '!' => return Err(self.error("expected `=` after `!`", start)),
            '\'' => TokenKind::Literal(Value::Text(self.quoted_string('\'', start)?)),
            '"' => TokenKind::Identifier {
                value: self.quoted_string('"', start)?,
                quoted: true,
            },
            c if (c == 'x' || c == 'X') && self.peek() == Some('\'') => {
                self.bump();
                TokenKind::Literal(Value::Bytes(self.bytes_literal(start)?))
            }
            c if c.is_ascii_digit() => {
                while self.peek().is_some_and(|next| next.is_ascii_digit()) {
                    self.bump();
                }
                let text = &self.source[start..self.position];
                let integer = text.parse::<i64>().map_err(|_| {
                    SqlError::new(
                        SqlErrorKind::Lex,
                        format!("integer literal `{text}` is outside INT64 range"),
                        Span::new(start, self.position),
                    )
                })?;
                TokenKind::Literal(Value::Int64(integer))
            }
            c if is_identifier_start(c) => {
                while self.peek().is_some_and(is_identifier_continue) {
                    self.bump();
                }
                let value = &self.source[start..self.position];
                Keyword::from_identifier(value).map_or_else(
                    || TokenKind::Identifier {
                        value: value.to_owned(),
                        quoted: false,
                    },
                    TokenKind::Keyword,
                )
            }
            other => {
                return Err(self.error(format!("unexpected character `{other}`"), start));
            }
        };
        Ok(Token {
            kind,
            span: Span::new(start, self.position),
        })
    }

    fn quoted_string(&mut self, delimiter: char, start: usize) -> Result<String> {
        let mut value = String::new();
        loop {
            let Some(character) = self.bump() else {
                return Err(self.error("unterminated quoted value", start));
            };
            if character == delimiter {
                if self.peek() == Some(delimiter) {
                    self.bump();
                    value.push(delimiter);
                } else {
                    return Ok(value);
                }
            } else {
                value.push(character);
            }
        }
    }

    fn bytes_literal(&mut self, start: usize) -> Result<Vec<u8>> {
        let digits_start = self.position;
        while self.peek().is_some_and(|c| c != '\'') {
            let Some(c) = self.bump() else {
                return Err(self.error("unterminated BYTES literal", start));
            };
            if !c.is_ascii_hexdigit() {
                return Err(self.error("BYTES literals contain hexadecimal digits", start));
            }
        }
        if !self.consume('\'') {
            return Err(self.error("unterminated BYTES literal", start));
        }
        let digits_end = self.position - 1;
        let digits = &self.source[digits_start..digits_end];
        if digits.len() % 2 != 0 {
            return Err(self.error("BYTES literals require an even number of digits", start));
        }
        let mut bytes = Vec::with_capacity(digits.len() / 2);
        for offset in (0..digits.len()).step_by(2) {
            let byte = u8::from_str_radix(&digits[offset..offset + 2], 16)
                .map_err(|_| self.error("invalid BYTES literal", start))?;
            bytes.push(byte);
        }
        Ok(bytes)
    }

    fn skip_trivia(&mut self) -> Result<()> {
        loop {
            while self.peek().is_some_and(char::is_whitespace) {
                self.bump();
            }
            if self.remaining().starts_with("--") {
                self.bump();
                self.bump();
                while self.peek().is_some_and(|c| c != '\n') {
                    self.bump();
                }
                continue;
            }
            if self.remaining().starts_with("/*") {
                let start = self.position;
                self.bump();
                self.bump();
                while !self.remaining().starts_with("*/") {
                    if self.bump().is_none() {
                        return Err(self.error("unterminated block comment", start));
                    }
                }
                self.bump();
                self.bump();
                continue;
            }
            return Ok(());
        }
    }

    fn remaining(&self) -> &'a str {
        &self.source[self.position..]
    }

    fn peek(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let character = self.peek()?;
        self.position += character.len_utf8();
        Some(character)
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn error(&self, message: impl Into<String>, start: usize) -> SqlError {
        SqlError::new(SqlErrorKind::Lex, message, Span::new(start, self.position))
    }
}

fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_identifier_continue(character: char) -> bool {
    character == '_' || character == '$' || character.is_alphanumeric()
}

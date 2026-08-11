//! Shared, deterministic data types used at `AsterDB` subsystem boundaries.
//!
//! Persistent encodings in this crate are explicit and versioned. They never
//! serialize Rust's in-memory representation.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ROW_COLUMNS: usize = 4_096;

pub type TableId = u64;
pub type IndexId = u64;
pub type TxnId = u64;
pub type Timestamp = u64;
pub type RaftIndex = u64;
pub type NodeId = u64;
pub type ShardId = u64;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),
    #[error("resource limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("type error: {0}")]
    Type(String),
    #[error("constraint violation: {0}")]
    Constraint(String),
    #[error("transaction conflict: {0}")]
    Conflict(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("not the leader{}", .0.as_ref().map(|h| format!("; leader hint: {h}")).unwrap_or_default())]
    NotLeader(Option<String>),
    #[error("corruption detected: {0}")]
    Corruption(String),
    #[error("I/O error: {0}")]
    Io(String),
    #[error("internal invariant failed: {0}")]
    Invariant(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Int64,
    Bool,
    Text,
    Bytes,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int64 => f.write_str("INT64"),
            Self::Bool => f.write_str("BOOL"),
            Self::Text => f.write_str("TEXT"),
            Self::Bytes => f.write_str("BYTES"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int64(i64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
}

impl Value {
    #[must_use]
    pub const fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Int64(_) => Some(DataType::Int64),
            Self::Bool(_) => Some(DataType::Bool),
            Self::Text(_) => Some(DataType::Text),
            Self::Bytes(_) => Some(DataType::Bytes),
        }
    }

    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn checked_cmp(&self, other: &Self) -> Result<Option<Ordering>> {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => Ok(None),
            (Self::Int64(a), Self::Int64(b)) => Ok(Some(a.cmp(b))),
            (Self::Bool(a), Self::Bool(b)) => Ok(Some(a.cmp(b))),
            (Self::Text(a), Self::Text(b)) => Ok(Some(a.cmp(b))),
            (Self::Bytes(a), Self::Bytes(b)) => Ok(Some(a.cmp(b))),
            _ => Err(Error::Type(format!(
                "cannot compare {} with {}",
                self.type_name(),
                other.type_name()
            ))),
        }
    }

    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Int64(_) => "INT64",
            Self::Bool(_) => "BOOL",
            Self::Text(_) => "TEXT",
            Self::Bytes(_) => "BYTES",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn validate(&self) -> Result<usize> {
        if self.columns.is_empty() {
            return Err(Error::Constraint(
                "a table must have at least one column".into(),
            ));
        }
        if self.columns.len() > MAX_ROW_COLUMNS {
            return Err(Error::LimitExceeded(format!(
                "schema has {} columns; maximum is {MAX_ROW_COLUMNS}",
                self.columns.len()
            )));
        }
        let primary_keys: Vec<_> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .collect();
        if primary_keys.len() != 1 {
            return Err(Error::Constraint(
                "exactly one primary-key column is required".into(),
            ));
        }
        let (primary_index, primary) = primary_keys[0];
        if primary.nullable {
            return Err(Error::Constraint("primary key cannot be nullable".into()));
        }
        for (index, left) in self.columns.iter().enumerate() {
            if left.name.is_empty() {
                return Err(Error::Constraint("column names cannot be empty".into()));
            }
            if self.columns[..index]
                .iter()
                .any(|right| right.name.eq_ignore_ascii_case(&left.name))
            {
                return Err(Error::Constraint(format!(
                    "duplicate column name `{}`",
                    left.name
                )));
            }
        }
        Ok(primary_index)
    }

    pub fn validate_row(&self, row: &Row) -> Result<()> {
        self.validate()?;
        if row.values.len() != self.columns.len() {
            return Err(Error::Constraint(format!(
                "row has {} values for {} columns",
                row.values.len(),
                self.columns.len()
            )));
        }
        for (column, value) in self.columns.iter().zip(&row.values) {
            match value.data_type() {
                None if !column.nullable => {
                    return Err(Error::Constraint(format!(
                        "column `{}` is not nullable",
                        column.name
                    )));
                }
                Some(actual) if actual != column.data_type => {
                    return Err(Error::Type(format!(
                        "column `{}` expects {}, got {}",
                        column.name,
                        column.data_type,
                        value.type_name()
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientRequestId {
    pub client_id: [u8; 16],
    pub sequence: u64,
}

pub mod codec {
    use super::{DataType, Error, MAX_ROW_COLUMNS, MAX_VALUE_BYTES, Result, Row, Value};

    const NULL: u8 = 0;
    const INT64: u8 = 1;
    const BOOL_FALSE: u8 = 2;
    const BOOL_TRUE: u8 = 3;
    const TEXT: u8 = 4;
    const BYTES: u8 = 5;

    pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
        if row.values.len() > MAX_ROW_COLUMNS {
            return Err(Error::LimitExceeded("too many row values".into()));
        }
        let mut output = Vec::new();
        let value_count = u32::try_from(row.values.len())
            .map_err(|_| Error::LimitExceeded("row value count does not fit encoding".into()))?;
        output.extend_from_slice(&value_count.to_le_bytes());
        for value in &row.values {
            encode_value(value, &mut output)?;
        }
        Ok(output)
    }

    pub fn decode_row(input: &[u8]) -> Result<Row> {
        let mut cursor = Cursor::new(input);
        let count = cursor.u32()? as usize;
        if count > MAX_ROW_COLUMNS {
            return Err(Error::LimitExceeded(format!(
                "encoded row contains {count} values"
            )));
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(decode_value(&mut cursor)?);
        }
        cursor.finish()?;
        Ok(Row { values })
    }

    pub fn encode_value(value: &Value, output: &mut Vec<u8>) -> Result<()> {
        match value {
            Value::Null => output.push(NULL),
            Value::Int64(value) => {
                output.push(INT64);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Value::Bool(false) => output.push(BOOL_FALSE),
            Value::Bool(true) => output.push(BOOL_TRUE),
            Value::Text(value) => {
                output.push(TEXT);
                encode_bytes(value.as_bytes(), output)?;
            }
            Value::Bytes(value) => {
                output.push(BYTES);
                encode_bytes(value, output)?;
            }
        }
        Ok(())
    }

    fn decode_value(cursor: &mut Cursor<'_>) -> Result<Value> {
        match cursor.byte()? {
            NULL => Ok(Value::Null),
            INT64 => Ok(Value::Int64(cursor.i64()?)),
            BOOL_FALSE => Ok(Value::Bool(false)),
            BOOL_TRUE => Ok(Value::Bool(true)),
            TEXT => String::from_utf8(cursor.bytes()?.to_vec())
                .map(Value::Text)
                .map_err(|error| Error::InvalidEncoding(error.to_string())),
            BYTES => Ok(Value::Bytes(cursor.bytes()?.to_vec())),
            tag => Err(Error::InvalidEncoding(format!("unknown value tag {tag}"))),
        }
    }

    fn encode_bytes(value: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if value.len() > MAX_VALUE_BYTES {
            return Err(Error::LimitExceeded(format!(
                "value contains {} bytes; maximum is {MAX_VALUE_BYTES}",
                value.len()
            )));
        }
        let byte_count = u32::try_from(value.len())
            .map_err(|_| Error::LimitExceeded("value length does not fit encoding".into()))?;
        output.extend_from_slice(&byte_count.to_le_bytes());
        output.extend_from_slice(value);
        Ok(())
    }

    /// Encodes a value so lexicographic byte ordering matches value ordering.
    pub fn encode_ordered(value: &Value, output: &mut Vec<u8>) {
        match value {
            Value::Null => output.push(NULL),
            Value::Int64(value) => {
                output.push(INT64);
                let sortable = u64::from_be_bytes(value.to_be_bytes()) ^ (1_u64 << 63);
                output.extend_from_slice(&sortable.to_be_bytes());
            }
            Value::Bool(false) => output.push(BOOL_FALSE),
            Value::Bool(true) => output.push(BOOL_TRUE),
            Value::Text(value) => {
                output.push(TEXT);
                encode_memcomparable(value.as_bytes(), output);
            }
            Value::Bytes(value) => {
                output.push(BYTES);
                encode_memcomparable(value, output);
            }
        }
    }

    pub fn encode_mvcc_key(
        table_id: u64,
        primary_key: &Value,
        commit_timestamp: u64,
    ) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        output.extend_from_slice(&table_id.to_be_bytes());
        encode_ordered(primary_key, &mut output);
        output.extend_from_slice(&(!commit_timestamp).to_be_bytes());
        Ok(output)
    }

    #[must_use]
    pub fn value_matches_type(value: &Value, expected: DataType, nullable: bool) -> bool {
        value.is_null() && nullable || value.data_type() == Some(expected)
    }

    fn encode_memcomparable(value: &[u8], output: &mut Vec<u8>) {
        for byte in value {
            if *byte == 0 {
                output.extend_from_slice(&[0, 0xff]);
            } else {
                output.push(*byte);
            }
        }
        output.extend_from_slice(&[0, 0]);
    }

    struct Cursor<'a> {
        input: &'a [u8],
        offset: usize,
    }

    impl<'a> Cursor<'a> {
        const fn new(input: &'a [u8]) -> Self {
            Self { input, offset: 0 }
        }

        fn take(&mut self, count: usize) -> Result<&'a [u8]> {
            let end = self
                .offset
                .checked_add(count)
                .ok_or_else(|| Error::InvalidEncoding("offset overflow".into()))?;
            if end > self.input.len() {
                return Err(Error::InvalidEncoding("truncated input".into()));
            }
            let bytes = &self.input[self.offset..end];
            self.offset = end;
            Ok(bytes)
        }

        fn byte(&mut self) -> Result<u8> {
            Ok(self.take(1)?[0])
        }

        fn u32(&mut self) -> Result<u32> {
            let mut bytes = [0; 4];
            bytes.copy_from_slice(self.take(4)?);
            Ok(u32::from_le_bytes(bytes))
        }

        fn i64(&mut self) -> Result<i64> {
            let mut bytes = [0; 8];
            bytes.copy_from_slice(self.take(8)?);
            Ok(i64::from_le_bytes(bytes))
        }

        fn bytes(&mut self) -> Result<&'a [u8]> {
            let count = self.u32()? as usize;
            if count > MAX_VALUE_BYTES {
                return Err(Error::LimitExceeded(format!(
                    "encoded value has {count} bytes"
                )));
            }
            self.take(count)
        }

        fn finish(self) -> Result<()> {
            if self.offset == self.input.len() {
                Ok(())
            } else {
                Err(Error::InvalidEncoding(format!(
                    "{} trailing bytes",
                    self.input.len() - self.offset
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::codec::{decode_row, encode_mvcc_key, encode_ordered, encode_row};
    use super::{Column, DataType, Row, Schema, Value};

    #[test]
    fn row_codec_round_trips_edge_values() {
        let row = Row {
            values: vec![
                Value::Null,
                Value::Int64(i64::MIN),
                Value::Int64(i64::MAX),
                Value::Bool(false),
                Value::Text("nul\0 snowman ☃".into()),
                Value::Bytes(vec![0, 1, 255]),
            ],
        };
        let encoded = encode_row(&row).unwrap();
        assert_eq!(decode_row(&encoded).unwrap(), row);
        assert!(decode_row(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn ordered_integer_encoding_preserves_order() {
        let values = [i64::MIN, -1, 0, 1, i64::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|value| {
                let mut bytes = Vec::new();
                encode_ordered(&Value::Int64(*value), &mut bytes);
                bytes
            })
            .collect();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn text_encoding_handles_zero_and_prefixes() {
        let values = ["", "a", "a\0", "aa", "b"];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|value| {
                let mut bytes = Vec::new();
                encode_ordered(&Value::Text((*value).into()), &mut bytes);
                bytes
            })
            .collect();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn descending_timestamp_puts_newest_version_first() {
        let old = encode_mvcc_key(7, &Value::Int64(42), 10).unwrap();
        let new = encode_mvcc_key(7, &Value::Int64(42), 11).unwrap();
        assert!(new < old);
    }

    #[test]
    fn schema_requires_exactly_one_non_null_primary_key() {
        let valid = Schema {
            columns: vec![Column {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
                primary_key: true,
            }],
        };
        assert_eq!(valid.validate().unwrap(), 0);

        let invalid = Schema {
            columns: vec![Column {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: true,
                primary_key: true,
            }],
        };
        assert!(invalid.validate().is_err());
    }
}

//! Bounded, versioned wire protocol shared by `AsterDB` clients and nodes.

use std::io;

use aster_core::{NodeId, Value};
use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const VERSION: u16 = 1;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SQL_BYTES: usize = 1024 * 1024;
pub const MAX_PARAMETERS: usize = 65_536;
pub const MAX_ROWS_PER_FRAME: usize = 16_384;
const MAGIC: [u8; 4] = *b"ASDB";
const HEADER_BYTES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRequest {
    pub request_id: u64,
    pub session: Option<SessionRequest>,
    pub operation: ClientOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRequest {
    pub client_id: [u8; 16],
    pub sequence: u64,
    pub transaction_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistency {
    Linearizable,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientOperation {
    Execute {
        sql: String,
        parameters: Vec<Value>,
        consistency: ReadConsistency,
    },
    Begin,
    Commit,
    Rollback,
    Status,
    Ping,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientResponse {
    pub request_id: u64,
    pub result: ResponseResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseResult {
    Query(QueryResult),
    Transaction { transaction_id: u64, read_ts: u64 },
    Committed { commit_index: u64 },
    RolledBack,
    Status(NodeStatus),
    Pong,
    Error(ProtocolError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub affected_rows: u64,
    pub applied_index: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node_id: NodeId,
    pub role: String,
    pub term: u64,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_log_index: u64,
    pub snapshot_index: u64,
    pub database_pages: u64,
    pub wal_durable_lsn: u64,
    pub active_transactions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
    pub leader_hint: Option<String>,
    pub retryable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    InvalidRequest,
    Unsupported,
    Constraint,
    Conflict,
    NotLeader,
    Timeout,
    ResourceExhausted,
    Corruption,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerEnvelope {
    pub from: NodeId,
    pub to: NodeId,
    pub term: u64,
    pub correlation_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    Request(ClientRequest),
    Response(ClientResponse),
    Peer(PeerEnvelope),
}

impl ClientRequest {
    pub fn validate(&self) -> Result<(), DecodeError> {
        if let ClientOperation::Execute {
            sql, parameters, ..
        } = &self.operation
        {
            if sql.len() > MAX_SQL_BYTES {
                return Err(DecodeError::Limit(format!(
                    "SQL is {} bytes; maximum is {MAX_SQL_BYTES}",
                    sql.len()
                )));
            }
            if parameters.len() > MAX_PARAMETERS {
                return Err(DecodeError::Limit(format!(
                    "request has {} parameters; maximum is {MAX_PARAMETERS}",
                    parameters.len()
                )));
            }
        }
        Ok(())
    }
}

impl ClientResponse {
    pub fn validate(&self) -> Result<(), DecodeError> {
        if let ResponseResult::Query(query) = &self.result {
            if query.rows.len() > MAX_ROWS_PER_FRAME {
                return Err(DecodeError::Limit(format!(
                    "response has {} rows; maximum is {MAX_ROWS_PER_FRAME}",
                    query.rows.len()
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("incomplete frame")]
    Incomplete,
    #[error("bad frame magic")]
    BadMagic,
    #[error("unsupported protocol version {0}")]
    Version(u16),
    #[error("unknown message kind {0}")]
    Kind(u8),
    #[error("frame checksum mismatch")]
    Checksum,
    #[error("resource limit exceeded: {0}")]
    Limit(String),
    #[error("malformed payload: {0}")]
    Payload(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

fn kind(message: &WireMessage) -> u8 {
    match message {
        WireMessage::Request(_) => 1,
        WireMessage::Response(_) => 2,
        WireMessage::Peer(_) => 3,
    }
}

pub fn encode(message: &WireMessage, max_frame_bytes: usize) -> Result<Vec<u8>, DecodeError> {
    match message {
        WireMessage::Request(request) => request.validate()?,
        WireMessage::Response(response) => response.validate()?,
        WireMessage::Peer(peer) if peer.payload.len() > max_frame_bytes => {
            return Err(DecodeError::Limit(
                "peer payload exceeds frame limit".into(),
            ));
        }
        WireMessage::Peer(_) => {}
    }
    let payload = match message {
        WireMessage::Request(value) => serde_json::to_vec(value),
        WireMessage::Response(value) => serde_json::to_vec(value),
        WireMessage::Peer(value) => serde_json::to_vec(value),
    }
    .map_err(|error| DecodeError::Payload(error.to_string()))?;
    if payload.len() > max_frame_bytes {
        return Err(DecodeError::Limit(format!(
            "frame has {} payload bytes; maximum is {max_frame_bytes}",
            payload.len()
        )));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| DecodeError::Limit("payload does not fit wire length".into()))?;
    let checksum = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len());
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&VERSION.to_be_bytes());
    frame.push(kind(message));
    frame.push(0);
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(&checksum.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub struct FrameDecoder {
    buffer: BytesMut,
    max_frame_bytes: usize,
}

impl FrameDecoder {
    #[must_use]
    pub fn new(max_frame_bytes: usize) -> Self {
        Self {
            buffer: BytesMut::new(),
            max_frame_bytes,
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    pub fn decode(&mut self) -> Result<Option<WireMessage>, DecodeError> {
        if self.buffer.len() < HEADER_BYTES {
            return Ok(None);
        }
        if self.buffer[..4] != MAGIC {
            return Err(DecodeError::BadMagic);
        }
        let version = u16::from_be_bytes([self.buffer[4], self.buffer[5]]);
        if version != VERSION {
            return Err(DecodeError::Version(version));
        }
        let message_kind = self.buffer[6];
        if self.buffer[7] != 0 {
            return Err(DecodeError::Payload("nonzero reserved frame flags".into()));
        }
        let payload_len = u32::from_be_bytes([
            self.buffer[8],
            self.buffer[9],
            self.buffer[10],
            self.buffer[11],
        ]) as usize;
        if payload_len > self.max_frame_bytes {
            return Err(DecodeError::Limit(format!(
                "declared payload is {payload_len} bytes; maximum is {}",
                self.max_frame_bytes
            )));
        }
        let total = HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| DecodeError::Limit("frame length overflow".into()))?;
        if self.buffer.len() < total {
            return Ok(None);
        }
        let checksum = u32::from_be_bytes([
            self.buffer[12],
            self.buffer[13],
            self.buffer[14],
            self.buffer[15],
        ]);
        let payload = &self.buffer[HEADER_BYTES..total];
        if crc32fast::hash(payload) != checksum {
            return Err(DecodeError::Checksum);
        }
        let message = match message_kind {
            1 => WireMessage::Request(
                serde_json::from_slice(payload)
                    .map_err(|error| DecodeError::Payload(error.to_string()))?,
            ),
            2 => WireMessage::Response(
                serde_json::from_slice(payload)
                    .map_err(|error| DecodeError::Payload(error.to_string()))?,
            ),
            3 => WireMessage::Peer(
                serde_json::from_slice(payload)
                    .map_err(|error| DecodeError::Payload(error.to_string()))?,
            ),
            other => return Err(DecodeError::Kind(other)),
        };
        self.buffer.advance(total);
        match &message {
            WireMessage::Request(request) => request.validate()?,
            WireMessage::Response(response) => response.validate()?,
            WireMessage::Peer(_) => {}
        }
        Ok(Some(message))
    }

    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }
}

pub async fn read_message<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<Option<WireMessage>, DecodeError> {
    let mut header = [0_u8; HEADER_BYTES];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(DecodeError::Io(error)),
    }
    if header[..4] != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let payload_len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    if payload_len > max_frame_bytes {
        return Err(DecodeError::Limit(format!(
            "declared payload is {payload_len} bytes; maximum is {max_frame_bytes}"
        )));
    }
    let mut payload = vec![0; payload_len];
    reader.read_exact(&mut payload).await?;
    let mut decoder = FrameDecoder::new(max_frame_bytes);
    decoder.feed(&header);
    decoder.feed(&payload);
    decoder.decode()?.ok_or(DecodeError::Incomplete).map(Some)
}

pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &WireMessage,
    max_frame_bytes: usize,
) -> Result<(), DecodeError> {
    let encoded = encode(message, max_frame_bytes)?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ping(id: u64) -> WireMessage {
        WireMessage::Request(ClientRequest {
            request_id: id,
            session: None,
            operation: ClientOperation::Ping,
        })
    }

    #[test]
    fn fragmented_and_coalesced_frames_decode() {
        let first = encode(&ping(1), 1024).unwrap();
        let second = encode(&ping(2), 1024).unwrap();
        let mut decoder = FrameDecoder::new(1024);
        for byte in &first[..first.len() - 1] {
            decoder.feed(&[*byte]);
            assert!(decoder.decode().unwrap().is_none());
        }
        decoder.feed(&first[first.len() - 1..]);
        decoder.feed(&second);
        assert_eq!(decoder.decode().unwrap(), Some(ping(1)));
        assert_eq!(decoder.decode().unwrap(), Some(ping(2)));
        assert_eq!(decoder.decode().unwrap(), None);
    }

    #[test]
    fn corruption_and_oversize_are_rejected_before_payload_allocation() {
        let mut frame = encode(&ping(1), 1024).unwrap();
        *frame.last_mut().unwrap() ^= 0xff;
        let mut decoder = FrameDecoder::new(1024);
        decoder.feed(&frame);
        assert!(matches!(decoder.decode(), Err(DecodeError::Checksum)));

        let mut header = [0_u8; HEADER_BYTES];
        header[..4].copy_from_slice(&MAGIC);
        header[4..6].copy_from_slice(&VERSION.to_be_bytes());
        header[6] = 1;
        header[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut decoder = FrameDecoder::new(1024);
        decoder.feed(&header);
        assert!(matches!(decoder.decode(), Err(DecodeError::Limit(_))));
    }

    #[tokio::test]
    async fn async_stream_round_trip_handles_chunking() {
        let (mut client, mut server) = tokio::io::duplex(32);
        let send = ping(9);
        let expected = send.clone();
        let writer = tokio::spawn(async move {
            write_message(&mut client, &send, 1024).await.unwrap();
        });
        let received = read_message(&mut server, 1024).await.unwrap().unwrap();
        writer.await.unwrap();
        assert_eq!(received, expected);
    }
}

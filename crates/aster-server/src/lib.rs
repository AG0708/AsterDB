//! Bounded TCP server and request-handler boundary for `AsterDB`.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aster_core::Error as CoreError;
use aster_db::{Database, DatabaseError, ExecutionResult};
use aster_engine::EngineError;
use aster_protocol::{
    ClientOperation, ClientRequest, ClientResponse, DEFAULT_MAX_FRAME_BYTES, ErrorCode, NodeStatus,
    ProtocolError, QueryResult, ReadConsistency, ResponseResult, SessionRequest, WireMessage,
    read_message, write_message,
};
use aster_runtime::{RuntimeError, RuntimeHandle};
use aster_sql::SqlErrorKind;
use aster_sql::ast::Statement;
use aster_storage::StorageError;
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub request_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_connections: 1_024,
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default)]
pub struct ServerMetrics {
    accepted_connections: AtomicU64,
    active_connections: AtomicU64,
    completed_requests: AtomicU64,
    rejected_connections: AtomicU64,
    protocol_errors: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub accepted_connections: u64,
    pub active_connections: u64,
    pub completed_requests: u64,
    pub rejected_connections: u64,
    pub protocol_errors: u64,
}

impl ServerMetrics {
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            accepted_connections: self.accepted_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            completed_requests: self.completed_requests.load(Ordering::Relaxed),
            rejected_connections: self.rejected_connections.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
pub trait RequestHandler: Send + Sync + 'static {
    async fn handle(&self, request: ClientRequest) -> ClientResponse;
}

pub struct Server<H> {
    handler: Arc<H>,
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
}

impl<H: RequestHandler> Server<H> {
    #[must_use]
    pub fn new(handler: H, config: ServerConfig) -> Self {
        Self {
            handler: Arc::new(handler),
            config,
            metrics: Arc::new(ServerMetrics::default()),
        }
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<ServerMetrics> {
        Arc::clone(&self.metrics)
    }

    pub async fn serve(
        self,
        listener: TcpListener,
        mut shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let permits = Arc::new(Semaphore::new(self.config.max_connections));
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        self.metrics.rejected_connections.fetch_add(1, Ordering::Relaxed);
                        drop(stream);
                        continue;
                    };
                    self.metrics.accepted_connections.fetch_add(1, Ordering::Relaxed);
                    self.metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                    let handler = Arc::clone(&self.handler);
                    let config = self.config.clone();
                    let metrics = Arc::clone(&self.metrics);
                    tokio::spawn(async move {
                        let _permit = permit;
                        let result = serve_connection(stream, handler, &config, &metrics).await;
                        if result.is_err() {
                            metrics.protocol_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            }
        }
    }
}

async fn serve_connection<H: RequestHandler>(
    mut stream: TcpStream,
    handler: Arc<H>,
    config: &ServerConfig,
    metrics: &ServerMetrics,
) -> io::Result<()> {
    loop {
        let incoming = timeout(
            config.request_timeout,
            read_message(&mut stream, config.max_frame_bytes),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "request read timed out"))?
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        let Some(message) = incoming else {
            return Ok(());
        };
        let WireMessage::Request(request) = message else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "non-client message on client listener",
            ));
        };
        let request_id = request.request_id;
        let mut response = timeout(config.request_timeout, handler.handle(request))
            .await
            .unwrap_or_else(|_| ClientResponse {
                request_id,
                result: ResponseResult::Error(ProtocolError {
                    code: ErrorCode::Timeout,
                    message: "request execution timed out".into(),
                    leader_hint: None,
                    retryable: true,
                }),
            });
        response.request_id = request_id;
        write_message(
            &mut stream,
            &WireMessage::Response(response),
            config.max_frame_bytes,
        )
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        metrics.completed_requests.fetch_add(1, Ordering::Relaxed);
    }
}

/// Minimal health/status handler used before a storage engine is configured.
pub struct DiagnosticHandler {
    status: NodeStatus,
}

impl DiagnosticHandler {
    #[must_use]
    pub const fn new(status: NodeStatus) -> Self {
        Self { status }
    }
}

#[async_trait]
impl RequestHandler for DiagnosticHandler {
    async fn handle(&self, request: ClientRequest) -> ClientResponse {
        let result = match request.operation {
            ClientOperation::Ping => ResponseResult::Pong,
            ClientOperation::Status => ResponseResult::Status(self.status.clone()),
            _ => ResponseResult::Error(ProtocolError {
                code: ErrorCode::Unsupported,
                message: "no database engine is configured".into(),
                leader_hint: None,
                retryable: false,
            }),
        };
        ClientResponse {
            request_id: request.request_id,
            result,
        }
    }
}

/// Real standalone SQL handler backed by the durable page/WAL database.
///
/// Filesystem work is dispatched to Tokio's blocking pool. Transaction owners
/// are tracked at the protocol boundary so one client cannot operate another
/// client's in-flight transaction.
pub struct DatabaseHandler {
    database: Arc<Database>,
    node_id: u64,
    transaction_owners: Arc<Mutex<BTreeMap<u64, [u8; 16]>>>,
}

impl DatabaseHandler {
    #[must_use]
    pub fn new(database: Database, node_id: u64) -> Self {
        Self {
            database: Arc::new(database),
            node_id,
            transaction_owners: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn open(directory: impl AsRef<Path>, node_id: u64) -> aster_db::Result<Self> {
        Database::open(directory).map(|database| Self::new(database, node_id))
    }
}

#[async_trait]
impl RequestHandler for DatabaseHandler {
    async fn handle(&self, request: ClientRequest) -> ClientResponse {
        let request_id = request.request_id;
        let database = Arc::clone(&self.database);
        let owners = Arc::clone(&self.transaction_owners);
        let node_id = self.node_id;
        let result = tokio::task::spawn_blocking(move || {
            dispatch_database(&database, &owners, node_id, request)
        })
        .await
        .unwrap_or_else(|error| {
            ResponseResult::Error(ProtocolError {
                code: ErrorCode::Internal,
                message: format!("database worker failed: {error}"),
                leader_hint: None,
                retryable: true,
            })
        });
        ClientResponse { request_id, result }
    }
}

/// Client-protocol adapter for the replicated Raft runtime.
///
/// The first replicated runtime deliberately exposes only autocommit DDL/DML
/// and leader-validated reads. Explicit multi-request transactions and stale
/// follower reads remain disabled until their failure semantics are designed
/// and tested end to end.
pub struct ReplicatedHandler {
    runtime: RuntimeHandle,
}

impl ReplicatedHandler {
    #[must_use]
    pub const fn new(runtime: RuntimeHandle) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl RequestHandler for ReplicatedHandler {
    async fn handle(&self, request: ClientRequest) -> ClientResponse {
        let request_id = request.request_id;
        let result = dispatch_replicated(&self.runtime, request).await;
        ClientResponse { request_id, result }
    }
}

async fn dispatch_replicated(runtime: &RuntimeHandle, request: ClientRequest) -> ResponseResult {
    let ClientRequest {
        session, operation, ..
    } = request;
    match operation {
        ClientOperation::Ping => ResponseResult::Pong,
        ClientOperation::Status => match runtime.status().await {
            Ok(status) => ResponseResult::Status(NodeStatus {
                node_id: status.node_id,
                role: format!("{:?}", status.role).to_ascii_lowercase(),
                term: status.term,
                leader_id: status.leader_id,
                commit_index: status.commit_index,
                applied_index: status.applied_index,
                last_log_index: status.last_log_index,
                snapshot_index: 0,
                database_pages: status.database_pages,
                // Protocol v1 retains this historical field name. The runtime
                // reports the physical durable WAL length in bytes.
                wal_durable_lsn: status.wal_bytes,
                active_transactions: status.active_transactions,
            }),
            Err(error) => ResponseResult::Error(map_runtime_error(&error)),
        },
        ClientOperation::Begin | ClientOperation::Commit | ClientOperation::Rollback => {
            ResponseResult::Error(ProtocolError {
                code: ErrorCode::Unsupported,
                message: "replicated mode currently supports autocommit SQL only".into(),
                leader_hint: None,
                retryable: false,
            })
        }
        ClientOperation::Execute {
            sql,
            parameters,
            consistency,
        } => {
            let session = match validate_replicated_session(session) {
                Ok(session) => session,
                Err(error) => return ResponseResult::Error(error),
            };
            let statement = match aster_sql::parse_statement(&sql) {
                Ok(statement) => statement,
                Err(error) => {
                    return ResponseResult::Error(map_database_error(&DatabaseError::Sql(error)));
                }
            };
            let result = match statement.value {
                Statement::CreateTable(_)
                | Statement::CreateIndex(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_) => {
                    runtime
                        .propose_sql(session.client_id, session.sequence, sql, parameters)
                        .await
                }
                Statement::Select(_) | Statement::Explain(_) => {
                    if consistency == ReadConsistency::Stale {
                        return ResponseResult::Error(ProtocolError {
                            code: ErrorCode::Unsupported,
                            message: "stale follower reads are not enabled in replicated mode"
                                .into(),
                            leader_hint: None,
                            retryable: false,
                        });
                    }
                    runtime
                        .linearizable_query(session.client_id, session.sequence, sql, parameters)
                        .await
                }
                Statement::Begin | Statement::Commit | Statement::Rollback => {
                    return ResponseResult::Error(ProtocolError {
                        code: ErrorCode::Unsupported,
                        message: "replicated mode currently supports autocommit SQL only".into(),
                        leader_hint: None,
                        retryable: false,
                    });
                }
            };
            match result {
                Ok(execution) => map_replicated_execution(execution),
                Err(error) => ResponseResult::Error(map_runtime_error(&error)),
            }
        }
    }
}

fn validate_replicated_session(
    session: Option<SessionRequest>,
) -> std::result::Result<SessionRequest, ProtocolError> {
    let Some(session) = session else {
        return Err(ProtocolError {
            code: ErrorCode::InvalidRequest,
            message: "replicated SQL requires a client session".into(),
            leader_hint: None,
            retryable: false,
        });
    };
    if session.sequence == 0 || session.transaction_id.is_some() {
        return Err(ProtocolError {
            code: ErrorCode::InvalidRequest,
            message:
                "replicated autocommit sessions require a positive sequence and no transaction id"
                    .into(),
            leader_hint: None,
            retryable: false,
        });
    }
    Ok(session)
}

fn map_replicated_execution(execution: ExecutionResult) -> ResponseResult {
    match execution {
        ExecutionResult::Query(query) => ResponseResult::Query(QueryResult {
            columns: query.columns,
            rows: query.rows.into_iter().map(|row| row.values).collect(),
            affected_rows: query.affected_rows,
            applied_index: query.applied_index,
            has_more: false,
        }),
        ExecutionResult::Committed(commit) => ResponseResult::Committed {
            commit_index: commit.commit_index,
        },
        ExecutionResult::Explain {
            plan,
            applied_index,
        } => ResponseResult::Query(QueryResult {
            columns: vec!["plan".into()],
            rows: vec![vec![aster_core::Value::Text(plan)]],
            affected_rows: 0,
            applied_index,
            has_more: false,
        }),
        ExecutionResult::Transaction(_) | ExecutionResult::RolledBack => {
            ResponseResult::Error(ProtocolError {
                code: ErrorCode::Internal,
                message: "replicated runtime returned an impossible transaction result".into(),
                leader_hint: None,
                retryable: false,
            })
        }
    }
}

fn map_runtime_error(error: &RuntimeError) -> ProtocolError {
    if let RuntimeError::Database(database) = error {
        return map_database_error(database);
    }
    let (code, retryable, leader_hint) = match error {
        RuntimeError::NotLeader { leader_hint } => (
            ErrorCode::NotLeader,
            true,
            leader_hint.map(|leader| leader.to_string()),
        ),
        RuntimeError::ProposalBusy => (ErrorCode::Conflict, true, None),
        RuntimeError::Timeout => (ErrorCode::Timeout, true, None),
        RuntimeError::Limit(_) => (ErrorCode::ResourceExhausted, true, None),
        RuntimeError::Unsupported(_) => (ErrorCode::Unsupported, false, None),
        RuntimeError::Configuration(_) => (ErrorCode::InvalidRequest, false, None),
        RuntimeError::Corruption(_) | RuntimeError::Recovery(_) => {
            (ErrorCode::Corruption, false, None)
        }
        RuntimeError::CommandRejected(_) => (ErrorCode::Conflict, false, None),
        RuntimeError::Io(_) | RuntimeError::Shutdown => (ErrorCode::Internal, true, None),
        RuntimeError::Fenced(_) => (ErrorCode::Internal, false, None),
        RuntimeError::Database(_) => unreachable!("database errors return above"),
    };
    ProtocolError {
        code,
        message: error.to_string(),
        leader_hint,
        retryable,
    }
}

fn dispatch_database(
    database: &Database,
    owners: &Mutex<BTreeMap<u64, [u8; 16]>>,
    node_id: u64,
    request: ClientRequest,
) -> ResponseResult {
    let ClientRequest {
        session, operation, ..
    } = request;
    let result = match operation {
        ClientOperation::Ping => return ResponseResult::Pong,
        ClientOperation::Status => database.status().map(|status| {
            ResponseResult::Status(NodeStatus {
                node_id,
                role: "standalone".into(),
                term: 1,
                leader_id: Some(node_id),
                commit_index: status.applied_index,
                applied_index: status.applied_index,
                last_log_index: status.applied_index,
                snapshot_index: 0,
                database_pages: status.database_pages,
                wal_durable_lsn: status.wal_bytes,
                active_transactions: status.active_transactions,
            })
        }),
        ClientOperation::Begin => require_session(session).and_then(|session| {
            if session.transaction_id.is_some() {
                return Err(DatabaseError::TransactionControl(
                    "BEGIN session already names a transaction".into(),
                ));
            }
            let info = database.begin()?;
            owners.lock().insert(info.transaction_id, session.client_id);
            Ok(ResponseResult::Transaction {
                transaction_id: info.transaction_id,
                read_ts: info.read_ts,
            })
        }),
        ClientOperation::Commit => require_session(session).and_then(|session| {
            let transaction_id = require_transaction(&session)?;
            verify_owner(owners, transaction_id, session.client_id)?;
            let committed = database.commit(session.client_id, session.sequence, transaction_id);
            if committed.is_ok()
                || matches!(
                    &committed,
                    Err(DatabaseError::TransactionAborted(_)
                        | DatabaseError::TransactionNotFound(_))
                )
            {
                owners.lock().remove(&transaction_id);
            }
            committed.map(|info| ResponseResult::Committed {
                commit_index: info.commit_index,
            })
        }),
        ClientOperation::Rollback => require_session(session).and_then(|session| {
            let transaction_id = require_transaction(&session)?;
            verify_owner(owners, transaction_id, session.client_id)?;
            let rolled_back = database.rollback(transaction_id);
            if rolled_back.is_ok()
                || matches!(&rolled_back, Err(DatabaseError::TransactionNotFound(_)))
            {
                owners.lock().remove(&transaction_id);
            }
            rolled_back.map(|()| ResponseResult::RolledBack)
        }),
        ClientOperation::Execute {
            sql,
            parameters,
            consistency: _,
        } => require_session(session).and_then(|session| {
            if let Some(transaction_id) = session.transaction_id {
                verify_owner(owners, transaction_id, session.client_id)?;
            }
            let execution = database.execute(
                session.client_id,
                session.sequence,
                session.transaction_id,
                &sql,
                &parameters,
            )?;
            Ok(map_execution(
                execution,
                owners,
                session.client_id,
                session.transaction_id,
            ))
        }),
    };
    result.unwrap_or_else(|error| ResponseResult::Error(map_database_error(&error)))
}

fn require_session(session: Option<SessionRequest>) -> aster_db::Result<SessionRequest> {
    let session = session.ok_or_else(|| {
        DatabaseError::TransactionControl(
            "SQL and transaction operations require a client session".into(),
        )
    })?;
    if session.sequence == 0 {
        return Err(DatabaseError::TransactionControl(
            "session sequence numbers start at one".into(),
        ));
    }
    Ok(session)
}

fn require_transaction(session: &SessionRequest) -> aster_db::Result<u64> {
    session.transaction_id.ok_or_else(|| {
        DatabaseError::TransactionControl("operation requires a transaction id".into())
    })
}

fn verify_owner(
    owners: &Mutex<BTreeMap<u64, [u8; 16]>>,
    transaction_id: u64,
    client_id: [u8; 16],
) -> aster_db::Result<()> {
    if let Some(owner) = owners.lock().get(&transaction_id)
        && *owner != client_id
    {
        return Err(DatabaseError::TransactionControl(
            "transaction belongs to a different client".into(),
        ));
    }
    Ok(())
}

fn map_execution(
    execution: ExecutionResult,
    owners: &Mutex<BTreeMap<u64, [u8; 16]>>,
    client_id: [u8; 16],
    active_transaction: Option<u64>,
) -> ResponseResult {
    match execution {
        ExecutionResult::Query(query) => ResponseResult::Query(QueryResult {
            columns: query.columns,
            rows: query.rows.into_iter().map(|row| row.values).collect(),
            affected_rows: query.affected_rows,
            applied_index: query.applied_index,
            has_more: false,
        }),
        ExecutionResult::Transaction(transaction) => {
            owners.lock().insert(transaction.transaction_id, client_id);
            ResponseResult::Transaction {
                transaction_id: transaction.transaction_id,
                read_ts: transaction.read_ts,
            }
        }
        ExecutionResult::Committed(commit) => {
            if let Some(transaction_id) = active_transaction {
                owners.lock().remove(&transaction_id);
            }
            ResponseResult::Committed {
                commit_index: commit.commit_index,
            }
        }
        ExecutionResult::RolledBack => {
            if let Some(transaction_id) = active_transaction {
                owners.lock().remove(&transaction_id);
            }
            ResponseResult::RolledBack
        }
        ExecutionResult::Explain {
            plan,
            applied_index,
        } => ResponseResult::Query(QueryResult {
            columns: vec!["plan".into()],
            rows: vec![vec![aster_core::Value::Text(plan)]],
            affected_rows: 0,
            applied_index,
            has_more: false,
        }),
    }
}

fn map_database_error(error: &DatabaseError) -> ProtocolError {
    let message = error.to_string();
    let (code, retryable, leader_hint) = match &error {
        DatabaseError::Sql(sql) => (
            match sql.kind {
                SqlErrorKind::Constraint => ErrorCode::Constraint,
                SqlErrorKind::Unsupported => ErrorCode::Unsupported,
                SqlErrorKind::Lex
                | SqlErrorKind::Parse
                | SqlErrorKind::Bind
                | SqlErrorKind::Type => ErrorCode::InvalidRequest,
            },
            false,
            None,
        ),
        DatabaseError::Engine(EngineError::Core(core)) => map_core_error(core),
        DatabaseError::Engine(EngineError::SnapshotTooOld { .. }) => {
            (ErrorCode::Conflict, true, None)
        }
        DatabaseError::Engine(
            EngineError::ApplyGap { .. }
            | EngineError::ApplyHashMismatch { .. }
            | EngineError::CommandHashMismatch
            | EngineError::InvalidSnapshot(_),
        )
        | DatabaseError::Corruption(_) => (ErrorCode::Corruption, false, None),
        DatabaseError::Engine(_) => (ErrorCode::InvalidRequest, false, None),
        DatabaseError::Storage(storage) => map_storage_error(storage),
        DatabaseError::Io(_) => (ErrorCode::Internal, true, None),
        DatabaseError::TransactionNotFound(_) | DatabaseError::TransactionControl(_) => {
            (ErrorCode::InvalidRequest, false, None)
        }
        DatabaseError::TransactionAborted(_) => (ErrorCode::Conflict, true, None),
        DatabaseError::RequestRejected(_) => (ErrorCode::Conflict, false, None),
        DatabaseError::ResourceLimit(_) => (ErrorCode::ResourceExhausted, false, None),
        DatabaseError::Invariant(_) => (ErrorCode::Internal, false, None),
    };
    ProtocolError {
        code,
        message,
        leader_hint,
        retryable,
    }
}

fn map_core_error(error: &CoreError) -> (ErrorCode, bool, Option<String>) {
    match error {
        CoreError::InvalidEncoding(_) | CoreError::Type(_) => {
            (ErrorCode::InvalidRequest, false, None)
        }
        CoreError::LimitExceeded(_) => (ErrorCode::ResourceExhausted, false, None),
        CoreError::Constraint(_) => (ErrorCode::Constraint, false, None),
        CoreError::Conflict(_) => (ErrorCode::Conflict, true, None),
        CoreError::Unsupported(_) => (ErrorCode::Unsupported, false, None),
        CoreError::NotLeader(hint) => (ErrorCode::NotLeader, true, hint.clone()),
        CoreError::Corruption(_) => (ErrorCode::Corruption, false, None),
        CoreError::Io(_) => (ErrorCode::Internal, true, None),
        CoreError::Invariant(_) => (ErrorCode::Internal, false, None),
    }
}

fn map_storage_error(error: &StorageError) -> (ErrorCode, bool, Option<String>) {
    match error {
        StorageError::InvalidPage(_)
        | StorageError::ChecksumMismatch { .. }
        | StorageError::CorruptWal { .. } => (ErrorCode::Corruption, false, None),
        StorageError::BufferPoolExhausted
        | StorageError::RecordTooLarge { .. }
        | StorageError::KeyTooLarge { .. } => (ErrorCode::ResourceExhausted, true, None),
        StorageError::Io(_) => (ErrorCode::Internal, true, None),
        StorageError::PagePinned(_)
        | StorageError::NotFound(_)
        | StorageError::Invariant(_)
        | StorageError::InjectedFault { .. } => (ErrorCode::Internal, false, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aster_core::Value;
    use aster_protocol::{
        ClientOperation, ClientRequest, ReadConsistency, SessionRequest, WireMessage,
    };
    use tempfile::TempDir;
    use tokio::net::TcpStream;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;

    fn status() -> NodeStatus {
        NodeStatus {
            node_id: 7,
            role: "standalone".into(),
            term: 0,
            leader_id: Some(7),
            commit_index: 0,
            applied_index: 0,
            last_log_index: 0,
            snapshot_index: 0,
            database_pages: 2,
            wal_durable_lsn: 0,
            active_transactions: 0,
        }
    }

    async fn round_trip(
        stream: &mut TcpStream,
        request_id: u64,
        session: Option<SessionRequest>,
        operation: ClientOperation,
    ) -> ResponseResult {
        write_message(
            stream,
            &WireMessage::Request(ClientRequest {
                request_id,
                session,
                operation,
            }),
            DEFAULT_MAX_FRAME_BYTES,
        )
        .await
        .unwrap();
        let WireMessage::Response(response) = read_message(stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("server returned a non-response message");
        };
        assert_eq!(response.request_id, request_id);
        response.result
    }

    fn session(client_id: [u8; 16], sequence: u64, transaction_id: Option<u64>) -> SessionRequest {
        SessionRequest {
            client_id,
            sequence,
            transaction_id,
        }
    }

    async fn start_database_server(
        directory: &std::path::Path,
    ) -> (
        std::net::SocketAddr,
        watch::Sender<bool>,
        Arc<ServerMetrics>,
        JoinHandle<io::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handler = DatabaseHandler::open(directory, 7).unwrap();
        let server = Server::new(handler, ServerConfig::default());
        let metrics = server.metrics();
        let task = tokio::spawn(server.serve(listener, shutdown_rx));
        (address, shutdown_tx, metrics, task)
    }

    async fn wait_until_idle(metrics: &ServerMetrics) {
        timeout(Duration::from_secs(2), async {
            while metrics.snapshot().active_connections != 0 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("connection task did not become idle");
    }

    fn execute(sql: &str) -> ClientOperation {
        ClientOperation::Execute {
            sql: sql.into(),
            parameters: Vec::new(),
            consistency: ReadConsistency::Linearizable,
        }
    }

    async fn seed_users(stream: &mut TcpStream, client: [u8; 16]) {
        assert!(matches!(
            round_trip(
                stream,
                1,
                Some(session(client, 1, None)),
                execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)"),
            )
            .await,
            ResponseResult::Query(QueryResult {
                affected_rows: 0,
                applied_index: 1,
                ..
            })
        ));
        assert!(matches!(
            round_trip(
                stream,
                2,
                Some(session(client, 2, None)),
                execute("INSERT INTO users VALUES (1, 'Ada')"),
            )
            .await,
            ResponseResult::Query(QueryResult {
                affected_rows: 1,
                applied_index: 2,
                ..
            })
        ));
    }

    async fn exercise_rollback(stream: &mut TcpStream, client: [u8; 16]) {
        let ResponseResult::Transaction { transaction_id, .. } = round_trip(
            stream,
            3,
            Some(session(client, 3, None)),
            ClientOperation::Begin,
        )
        .await
        else {
            panic!("BEGIN did not return a transaction");
        };
        assert!(matches!(
            round_trip(
                stream,
                4,
                Some(session(client, 3, Some(transaction_id))),
                execute("INSERT INTO users VALUES (99, 'rolled back')"),
            )
            .await,
            ResponseResult::Query(_)
        ));
        assert_eq!(
            round_trip(
                stream,
                5,
                Some(session(client, 3, Some(transaction_id))),
                ClientOperation::Rollback,
            )
            .await,
            ResponseResult::RolledBack
        );
    }

    async fn exercise_commit_and_ownership(
        stream: &mut TcpStream,
        client: [u8; 16],
        other_client: [u8; 16],
    ) {
        let ResponseResult::Transaction { transaction_id, .. } = round_trip(
            stream,
            6,
            Some(session(client, 3, None)),
            ClientOperation::Begin,
        )
        .await
        else {
            panic!("second BEGIN did not return a transaction");
        };
        assert!(matches!(
            round_trip(
                stream,
                7,
                Some(session(client, 3, Some(transaction_id))),
                execute("INSERT INTO users VALUES (2, 'Grace')"),
            )
            .await,
            ResponseResult::Query(_)
        ));
        assert!(matches!(
            round_trip(
                stream,
                8,
                Some(session(other_client, 1, Some(transaction_id))),
                ClientOperation::Commit,
            )
            .await,
            ResponseResult::Error(ProtocolError {
                code: ErrorCode::InvalidRequest,
                ..
            })
        ));
        assert_eq!(
            round_trip(
                stream,
                9,
                Some(session(client, 3, Some(transaction_id))),
                ClientOperation::Commit,
            )
            .await,
            ResponseResult::Committed { commit_index: 3 }
        );
    }

    async fn select_users(
        stream: &mut TcpStream,
        request_id: u64,
        client: [u8; 16],
    ) -> Vec<Vec<Value>> {
        let ResponseResult::Query(query) = round_trip(
            stream,
            request_id,
            Some(session(client, 4, None)),
            execute("SELECT id, name FROM users ORDER BY id"),
        )
        .await
        else {
            panic!("SELECT did not return rows");
        };
        query.rows
    }

    #[tokio::test]
    async fn real_tcp_ping_and_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let server = Server::new(DiagnosticHandler::new(status()), ServerConfig::default());
        let metrics = server.metrics();
        let task = tokio::spawn(server.serve(listener, shutdown_rx));

        let mut stream = TcpStream::connect(address).await.unwrap();
        let request = ClientRequest {
            request_id: 11,
            session: None,
            operation: ClientOperation::Ping,
        };
        write_message(
            &mut stream,
            &WireMessage::Request(request),
            DEFAULT_MAX_FRAME_BYTES,
        )
        .await
        .unwrap();
        let response = read_message(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            response,
            WireMessage::Response(ClientResponse {
                request_id: 11,
                result: ResponseResult::Pong
            })
        ));

        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(metrics.snapshot().completed_requests, 1);
    }

    #[tokio::test]
    async fn real_tcp_sql_transactions_and_restart_persistence() {
        let directory = TempDir::new().unwrap();
        let client = [9; 16];
        let other_client = [8; 16];
        let (address, shutdown_tx, metrics, task) = start_database_server(directory.path()).await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        seed_users(&mut stream, client).await;
        exercise_rollback(&mut stream, client).await;
        exercise_commit_and_ownership(&mut stream, client, other_client).await;
        let rows = select_users(&mut stream, 10, client).await;
        assert_eq!(
            rows,
            vec![
                vec![Value::Int64(1), Value::Text("Ada".into())],
                vec![Value::Int64(2), Value::Text("Grace".into())]
            ]
        );

        drop(stream);
        wait_until_idle(&metrics).await;
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();

        let (address, shutdown_tx, metrics, task) = start_database_server(directory.path()).await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        assert_eq!(select_users(&mut stream, 11, client).await, rows);
        let ResponseResult::Status(status) =
            round_trip(&mut stream, 12, None, ClientOperation::Status).await
        else {
            panic!("STATUS did not return node state");
        };
        assert_eq!(status.applied_index, 3);
        assert_eq!(status.commit_index, 3);
        assert!(status.database_pages > 2);
        assert!(status.wal_durable_lsn > 0);

        drop(stream);
        wait_until_idle(&metrics).await;
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }
}

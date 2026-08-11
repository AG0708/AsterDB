use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use aster_core::{Row, Value};
use aster_db::{Database, ExecutionResult};
use aster_protocol::{
    ClientOperation, ClientRequest, ClientResponse, DEFAULT_MAX_FRAME_BYTES, ErrorCode, NodeStatus,
    QueryResult, ReadConsistency, ResponseResult, SessionRequest, WireMessage, read_message,
    write_message,
};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const CLIENT: [u8; 16] = [0x6B; 16];
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

struct ProcessNode {
    node_id: u64,
    client_address: SocketAddr,
    child: Child,
}

impl ProcessNode {
    fn spawn(
        node_id: u64,
        client_address: SocketAddr,
        peer_addresses: &[SocketAddr],
        data_directory: &Path,
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_aster-server"));
        command
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--listen")
            .arg(client_address.to_string())
            .arg("--data-dir")
            .arg(data_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        for (offset, address) in peer_addresses.iter().enumerate() {
            command
                .arg("--peer")
                .arg(format!("{}={address}", offset + 1));
        }
        let child = command.spawn().expect("spawn replicated aster-server");
        Self {
            node_id,
            client_address,
            child,
        }
    }

    async fn crash(mut self) {
        self.child.kill().await.expect("kill replicated node");
        let status = self.child.wait().await.expect("wait for replicated node");
        assert!(!status.success(), "crashed node exited successfully");
    }
}

fn reserve_addresses(count: usize) -> Vec<SocketAddr> {
    let listeners = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("reserve local port"))
        .collect::<Vec<_>>();
    let addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("read reserved address"))
        .collect();
    drop(listeners);
    addresses
}

fn start_node(
    node_id: u64,
    client_addresses: &[SocketAddr],
    peer_addresses: &[SocketAddr],
    root: &Path,
) -> ProcessNode {
    let offset = usize::try_from(node_id - 1).expect("node id fits usize");
    ProcessNode::spawn(
        node_id,
        client_addresses[offset],
        peer_addresses,
        &root.join(format!("node-{node_id}")),
    )
}

async fn request(
    address: SocketAddr,
    request_id: u64,
    session: Option<SessionRequest>,
    operation: ClientOperation,
) -> Result<ClientResponse, String> {
    timeout(REQUEST_TIMEOUT, async {
        let mut stream = TcpStream::connect(address)
            .await
            .map_err(|error| error.to_string())?;
        write_message(
            &mut stream,
            &WireMessage::Request(ClientRequest {
                request_id,
                session,
                operation,
            }),
            DEFAULT_MAX_FRAME_BYTES,
        )
        .await
        .map_err(|error| error.to_string())?;
        let message = read_message(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .map_err(|error| error.to_string())?;
        let Some(WireMessage::Response(response)) = message else {
            return Err("server closed without a client response".into());
        };
        if response.request_id != request_id {
            return Err(format!(
                "response id {} does not match request {request_id}",
                response.request_id
            ));
        }
        Ok(response)
    })
    .await
    .map_err(|_| "client request timed out".to_string())?
}

async fn status(node: &ProcessNode) -> Option<NodeStatus> {
    let response = request(node.client_address, 1, None, ClientOperation::Status)
        .await
        .ok()?;
    let ResponseResult::Status(status) = response.result else {
        return None;
    };
    Some(status)
}

async fn wait_for_leader(nodes: &BTreeMap<u64, ProcessNode>, allowed: &[u64]) -> u64 {
    for _ in 0..160 {
        let mut leaders = Vec::new();
        for node_id in allowed {
            if let Some(node) = nodes.get(node_id)
                && let Some(status) = status(node).await
                && status.role == "leader"
            {
                leaders.push(*node_id);
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("public server cluster did not elect exactly one leader");
}

async fn wait_applied(nodes: &BTreeMap<u64, ProcessNode>, index: u64) {
    for _ in 0..160 {
        let mut caught_up = true;
        for node in nodes.values() {
            if status(node)
                .await
                .is_none_or(|status| status.applied_index < index)
            {
                caught_up = false;
                break;
            }
        }
        if caught_up {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("public server cluster did not apply index {index}");
}

fn execute(sql: &str) -> ClientOperation {
    ClientOperation::Execute {
        sql: sql.into(),
        parameters: Vec::new(),
        consistency: ReadConsistency::Linearizable,
    }
}

fn session(sequence: u64) -> SessionRequest {
    SessionRequest {
        client_id: CLIENT,
        sequence,
        transaction_id: None,
    }
}

async fn execute_on(node: &ProcessNode, sequence: u64, sql: &str) -> ResponseResult {
    request(
        node.client_address,
        sequence,
        Some(session(sequence)),
        execute(sql),
    )
    .await
    .unwrap_or_else(|error| panic!("node {} request failed: {error}", node.node_id))
    .result
}

fn applied_index(result: &ResponseResult) -> u64 {
    let ResponseResult::Query(QueryResult { applied_index, .. }) = result else {
        panic!("replicated mutation returned an unexpected response: {result:?}");
    };
    *applied_index
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn public_server_path_survives_leader_kill_and_restart() {
    let root = TempDir::new().expect("create cluster temp directory");
    let addresses = reserve_addresses(6);
    let client_addresses = &addresses[..3];
    let peer_addresses = &addresses[3..];
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(
            node_id,
            start_node(node_id, client_addresses, peer_addresses, root.path()),
        );
    }

    let all = [1, 2, 3];
    let leader = wait_for_leader(&nodes, &all).await;
    let follower = all
        .into_iter()
        .find(|node_id| *node_id != leader)
        .expect("cluster has a follower");
    let rejected = execute_on(
        &nodes[&follower],
        1,
        "CREATE TABLE events (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
    )
    .await;
    let ResponseResult::Error(error) = rejected else {
        panic!("follower unexpectedly accepted a mutation: {rejected:?}");
    };
    assert_eq!(error.code, ErrorCode::NotLeader);
    assert!(error.retryable);

    let created = execute_on(
        &nodes[&leader],
        1,
        "CREATE TABLE events (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
    )
    .await;
    let created_index = applied_index(&created);
    let inserted = execute_on(
        &nodes[&leader],
        2,
        "INSERT INTO events VALUES (1, 'before-failover')",
    )
    .await;
    let first_index = applied_index(&inserted);
    assert!(first_index > created_index);
    wait_applied(&nodes, first_index).await;

    nodes
        .remove(&leader)
        .expect("remove old leader")
        .crash()
        .await;
    let survivors = all
        .into_iter()
        .filter(|node_id| *node_id != leader)
        .collect::<Vec<_>>();
    let next_leader = wait_for_leader(&nodes, &survivors).await;
    let inserted = execute_on(
        &nodes[&next_leader],
        3,
        "INSERT INTO events VALUES (2, 'after-failover')",
    )
    .await;
    let second_index = applied_index(&inserted);

    nodes.insert(
        leader,
        start_node(leader, client_addresses, peer_addresses, root.path()),
    );
    wait_applied(&nodes, second_index).await;
    let current_leader = wait_for_leader(&nodes, &all).await;
    let queried = execute_on(
        &nodes[&current_leader],
        99,
        "SELECT id, value FROM events ORDER BY id",
    )
    .await;
    let ResponseResult::Query(query) = queried else {
        panic!("linearizable query returned an unexpected response: {queried:?}");
    };
    assert_eq!(
        query.rows,
        vec![
            vec![Value::Int64(1), Value::Text("before-failover".into())],
            vec![Value::Int64(2), Value::Text("after-failover".into())],
        ]
    );

    for (_, node) in nodes {
        node.crash().await;
    }
    for node_id in all {
        let database = Database::open(root.path().join(format!("node-{node_id}/database")))
            .expect("open converged database after process death");
        let result = database
            .execute_read_only(CLIENT, 100, "SELECT id, value FROM events ORDER BY id", &[])
            .expect("read durable converged database");
        let ExecutionResult::Query(query) = result else {
            panic!("expected durable query result");
        };
        assert_eq!(
            query.rows,
            vec![
                Row {
                    values: vec![Value::Int64(1), Value::Text("before-failover".into())],
                },
                Row {
                    values: vec![Value::Int64(2), Value::Text("after-failover".into())],
                },
            ]
        );
    }
}

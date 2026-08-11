use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use aster_core::{Row, Value};
use aster_db::{Database, ExecutionResult};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::{sleep, timeout};

const CLIENT: [u8; 16] = [0x5A; 16];
const CONTROL_TIMEOUT: Duration = Duration::from_secs(8);
const ELECTION_SEED_BASES: [u64; 3] = [0xA57E_3000, 0xA57E_4000, 0xA57E_5000];

struct ProcessNode {
    node_id: u64,
    seed_base: u64,
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    next_request_id: u64,
}

impl ProcessNode {
    fn spawn(
        node_id: u64,
        peers: &str,
        data_directory: &Path,
        seed_base: u64,
        snapshot_threshold_entries: usize,
    ) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_aster-runtime-node"))
            .arg("--node-id")
            .arg(node_id.to_string())
            .arg("--peers")
            .arg(peers)
            .arg("--data-dir")
            .arg(data_directory)
            .arg("--election-min-ms")
            .arg("120")
            .arg("--election-max-ms")
            .arg("280")
            .arg("--heartbeat-ms")
            .arg("30")
            .arg("--check-quorum-ms")
            .arg("350")
            .arg("--rng-seed")
            .arg((seed_base + node_id).to_string())
            .arg("--snapshot-threshold-entries")
            .arg(snapshot_threshold_entries.to_string())
            .arg("--snapshot-threshold-bytes")
            .arg((1024 * 1024).to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        Self {
            node_id,
            seed_base,
            child,
            input,
            output,
            next_request_id: 1,
        }
    }

    async fn request(&mut self, operation: JsonValue) -> JsonValue {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let mut request = operation;
        request
            .as_object_mut()
            .unwrap()
            .insert("request_id".into(), json!(request_id));
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        self.input.write_all(&encoded).await.unwrap();
        self.input.flush().await.unwrap();

        let mut line = String::new();
        let bytes = timeout(CONTROL_TIMEOUT, self.output.read_line(&mut line))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "node {} control request timed out; election seed base {:#x}",
                    self.node_id, self.seed_base
                )
            })
            .unwrap();
        assert_ne!(
            bytes, 0,
            "node {} closed its control stream; election seed base {:#x}",
            self.node_id, self.seed_base
        );
        let response: JsonValue = serde_json::from_str(&line).unwrap();
        assert_eq!(response["request_id"], request_id);
        response
    }

    async fn status(&mut self) -> Option<JsonValue> {
        let response = self.request(json!({ "operation": "status" })).await;
        response["error"]
            .is_null()
            .then(|| response["result"].clone())
    }

    async fn propose(&mut self, sequence: u64, sql: &str) -> JsonValue {
        self.request(json!({
            "operation": "propose",
            "client_id": CLIENT,
            "sequence": sequence,
            "sql": sql,
            "parameters": []
        }))
        .await
    }

    async fn query(&mut self, sql: &str) -> JsonValue {
        self.request(json!({
            "operation": "query",
            "client_id": CLIENT,
            "sequence": 99,
            "sql": sql,
            "parameters": []
        }))
        .await
    }

    async fn crash(mut self) {
        self.child.kill().await.unwrap();
        let status = self.child.wait().await.unwrap();
        assert!(!status.success(), "crashed node exited successfully");
    }

    async fn shutdown(mut self) {
        let response = self.request(json!({ "operation": "shutdown" })).await;
        assert!(response["error"].is_null(), "shutdown failed: {response}");
        let status = timeout(CONTROL_TIMEOUT, self.child.wait())
            .await
            .expect("node did not exit after shutdown")
            .unwrap();
        assert!(status.success(), "node shutdown status was {status}");
    }
}

fn reserve_addresses(count: usize) -> Vec<SocketAddr> {
    let listeners = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    drop(listeners);
    addresses
}

fn peer_argument(addresses: &[SocketAddr]) -> String {
    addresses
        .iter()
        .enumerate()
        .map(|(offset, address)| format!("{}={address}", offset + 1))
        .collect::<Vec<_>>()
        .join(",")
}

fn start_node(node_id: u64, peer_argument: &str, root: &Path, seed_base: u64) -> ProcessNode {
    ProcessNode::spawn(
        node_id,
        peer_argument,
        &root.join(format!("node-{node_id}")),
        seed_base,
        256,
    )
}

fn start_snapshot_node(
    node_id: u64,
    peer_argument: &str,
    root: &Path,
    seed_base: u64,
) -> ProcessNode {
    ProcessNode::spawn(
        node_id,
        peer_argument,
        &root.join(format!("node-{node_id}")),
        seed_base,
        4,
    )
}

async fn wait_for_leader(
    nodes: &mut BTreeMap<u64, ProcessNode>,
    allowed: &[u64],
    seed_base: u64,
) -> u64 {
    for _ in 0..120 {
        let mut leaders = Vec::new();
        for node_id in allowed {
            if let Some(node) = nodes.get_mut(node_id)
                && let Some(status) = node.status().await
                && status["role"] == "leader"
            {
                leaders.push(*node_id);
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("process cluster did not elect exactly one leader; election seed base {seed_base:#x}");
}

async fn wait_applied(nodes: &mut BTreeMap<u64, ProcessNode>, index: u64, seed_base: u64) {
    for _ in 0..120 {
        let mut ready = true;
        for node in nodes.values_mut() {
            let Some(status) = node.status().await else {
                ready = false;
                break;
            };
            if status["database_applied_index"].as_u64().unwrap() < index {
                ready = false;
                break;
            }
        }
        if ready {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("process cluster did not apply index {index}; election seed base {seed_base:#x}");
}

async fn wait_snapshot(
    nodes: &mut BTreeMap<u64, ProcessNode>,
    allowed: &[u64],
    minimum_index: u64,
    seed_base: u64,
) {
    for _ in 0..160 {
        let mut ready = true;
        for node_id in allowed {
            let Some(status) = nodes.get_mut(node_id).unwrap().status().await else {
                ready = false;
                break;
            };
            if status["snapshot_index"]
                .as_u64()
                .is_none_or(|index| index < minimum_index)
            {
                ready = false;
                break;
            }
        }
        if ready {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "process cluster did not compact through snapshot {minimum_index}; election seed base {seed_base:#x}"
    );
}

fn successful_index(response: &JsonValue, seed_base: u64) -> u64 {
    assert!(
        response["error"].is_null(),
        "replicated operation failed with election seed base {seed_base:#x}: {response}"
    );
    response["result"]["applied_index"].as_u64().unwrap()
}

async fn propose_until_success(
    nodes: &mut BTreeMap<u64, ProcessNode>,
    allowed: &[u64],
    sequence: u64,
    sql: &str,
    seed_base: u64,
) -> (u64, JsonValue) {
    let mut last_error = JsonValue::Null;
    for _ in 0..12 {
        let leader = wait_for_leader(nodes, allowed, seed_base).await;
        let response = nodes.get_mut(&leader).unwrap().propose(sequence, sql).await;
        if response["error"].is_null() {
            return (leader, response);
        }
        last_error = response;
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "replicated proposal did not succeed after election retries; election seed base {seed_base:#x}; last response {last_error}"
    );
}

async fn query_until_success(
    nodes: &mut BTreeMap<u64, ProcessNode>,
    allowed: &[u64],
    sql: &str,
    seed_base: u64,
) -> JsonValue {
    let mut last_error = JsonValue::Null;
    for _ in 0..12 {
        let leader = wait_for_leader(nodes, allowed, seed_base).await;
        let response = nodes.get_mut(&leader).unwrap().query(sql).await;
        if response["error"].is_null() {
            return response;
        }
        last_error = response;
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "linearizable query did not succeed after election retries; election seed base {seed_base:#x}; last response {last_error}"
    );
}

fn database_directory(root: &Path, node_id: u64) -> PathBuf {
    root.join(format!("node-{node_id}/database"))
}

#[allow(clippy::too_many_lines)]
async fn run_scenario(seed_base: u64) {
    let root = TempDir::new().unwrap();
    let addresses = reserve_addresses(3);
    let peers = peer_argument(&addresses);
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(node_id, start_node(node_id, &peers, root.path(), seed_base));
    }

    let all = [1, 2, 3];
    let (_, created) = propose_until_success(
        &mut nodes,
        &all,
        1,
        "CREATE TABLE events (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
        seed_base,
    )
    .await;
    let created_index = successful_index(&created, seed_base);
    let (leader, inserted) = propose_until_success(
        &mut nodes,
        &all,
        2,
        "INSERT INTO events VALUES (1, 'before-failover')",
        seed_base,
    )
    .await;
    let first_index = successful_index(&inserted, seed_base);
    assert!(first_index > created_index);
    wait_applied(&mut nodes, first_index, seed_base).await;

    nodes.remove(&leader).unwrap().crash().await;
    let survivors = all
        .into_iter()
        .filter(|node_id| *node_id != leader)
        .collect::<Vec<_>>();
    let (_, inserted) = propose_until_success(
        &mut nodes,
        &survivors,
        3,
        "INSERT INTO events VALUES (2, 'after-failover')",
        seed_base,
    )
    .await;
    let second_index = successful_index(&inserted, seed_base);

    nodes.insert(leader, start_node(leader, &peers, root.path(), seed_base));
    wait_applied(&mut nodes, second_index, seed_base).await;

    let current_leader = wait_for_leader(&mut nodes, &all, seed_base).await;
    let isolated_followers = all
        .into_iter()
        .filter(|node_id| *node_id != current_leader)
        .collect::<Vec<_>>();
    for node_id in &isolated_followers {
        nodes.remove(node_id).unwrap().crash().await;
    }
    let minority = nodes
        .get_mut(&current_leader)
        .unwrap()
        .propose(4, "INSERT INTO events VALUES (3, 'after-heal')")
        .await;
    assert!(
        minority["result"].is_null() && !minority["error"].is_null(),
        "minority leader acknowledged a write with election seed base {seed_base:#x}: {minority}"
    );

    for node_id in &isolated_followers {
        nodes.insert(
            *node_id,
            start_node(*node_id, &peers, root.path(), seed_base),
        );
    }
    let (_, healed) = propose_until_success(
        &mut nodes,
        &all,
        4,
        "INSERT INTO events VALUES (3, 'after-heal')",
        seed_base,
    )
    .await;
    let healed_index = successful_index(&healed, seed_base);
    wait_applied(&mut nodes, healed_index, seed_base).await;

    let query = query_until_success(
        &mut nodes,
        &all,
        "SELECT id, value FROM events ORDER BY id",
        seed_base,
    )
    .await;
    assert!(
        query["error"].is_null(),
        "linearizable read failed: {query}"
    );
    assert_eq!(query["result"]["rows"].as_array().unwrap().len(), 3);

    for (_, node) in nodes {
        node.shutdown().await;
    }
    for node_id in all {
        let database = Database::open(database_directory(root.path(), node_id)).unwrap();
        let result = database
            .execute_read_only(CLIENT, 100, "SELECT id, value FROM events ORDER BY id", &[])
            .unwrap();
        let ExecutionResult::Query(result) = result else {
            panic!("expected query result");
        };
        assert_eq!(
            result.rows,
            vec![
                Row {
                    values: vec![Value::Int64(1), Value::Text("before-failover".into())],
                },
                Row {
                    values: vec![Value::Int64(2), Value::Text("after-failover".into())],
                },
                Row {
                    values: vec![Value::Int64(3), Value::Text("after-heal".into())],
                },
            ]
        );
    }
}

// This single narrative test preserves the exact process-kill, failover,
// restart, minority, and healing sequence as a reproducible release gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_process_failover_catchup_minority_and_convergence() {
    for seed_base in ELECTION_SEED_BASES {
        run_scenario(seed_base).await;
    }
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_process_snapshot_catchup_and_restart_boundary() {
    const SEED_BASE: u64 = 0xA57E_6000;

    let root = TempDir::new().unwrap();
    let addresses = reserve_addresses(3);
    let peers = peer_argument(&addresses);
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(
            node_id,
            start_snapshot_node(node_id, &peers, root.path(), SEED_BASE),
        );
    }

    let all = [1, 2, 3];
    let (_, created) = propose_until_success(
        &mut nodes,
        &all,
        1,
        "CREATE TABLE snapshots (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
        SEED_BASE,
    )
    .await;
    let created_index = successful_index(&created, SEED_BASE);
    wait_applied(&mut nodes, created_index, SEED_BASE).await;

    let leader = wait_for_leader(&mut nodes, &all, SEED_BASE).await;
    let lagging = all.into_iter().find(|node_id| *node_id != leader).unwrap();
    nodes.remove(&lagging).unwrap().crash().await;
    let survivors = all
        .into_iter()
        .filter(|node_id| *node_id != lagging)
        .collect::<Vec<_>>();

    let mut final_index = created_index;
    for row_id in 1_u64..=16 {
        let value = format!("value-{row_id}-{}", "x".repeat(2_048));
        let (_, inserted) = propose_until_success(
            &mut nodes,
            &survivors,
            row_id + 1,
            &format!("INSERT INTO snapshots VALUES ({row_id}, '{value}')"),
            SEED_BASE,
        )
        .await;
        final_index = successful_index(&inserted, SEED_BASE);
    }
    wait_applied(&mut nodes, final_index, SEED_BASE).await;
    let compacted_through = final_index.saturating_sub(3);
    wait_snapshot(&mut nodes, &survivors, compacted_through, SEED_BASE).await;
    for node_id in &survivors {
        let status = nodes.get_mut(node_id).unwrap().status().await.unwrap();
        assert!(status["snapshot_index"].as_u64().unwrap() > created_index);
        assert!(status["snapshot_bytes"].as_u64().unwrap() > 16 * 1024);
        assert!(status["retained_log_entries"].as_u64().unwrap() < 4);
    }

    nodes.insert(
        lagging,
        start_snapshot_node(lagging, &peers, root.path(), SEED_BASE),
    );
    wait_applied(&mut nodes, final_index, SEED_BASE).await;
    wait_snapshot(&mut nodes, &[lagging], compacted_through, SEED_BASE).await;
    let installed_status = nodes.get_mut(&lagging).unwrap().status().await.unwrap();
    let installed_boundary = installed_status["snapshot_index"].as_u64().unwrap();
    assert!(installed_status["snapshot_bytes"].as_u64().unwrap() > 16 * 1024);

    nodes.remove(&lagging).unwrap().crash().await;
    nodes.insert(
        lagging,
        start_snapshot_node(lagging, &peers, root.path(), SEED_BASE),
    );
    wait_applied(&mut nodes, final_index, SEED_BASE).await;
    let reopened = nodes.get_mut(&lagging).unwrap().status().await.unwrap();
    assert_eq!(
        reopened["snapshot_index"].as_u64(),
        Some(installed_boundary)
    );

    let query = query_until_success(
        &mut nodes,
        &all,
        "SELECT id, value FROM snapshots ORDER BY id",
        SEED_BASE,
    )
    .await;
    assert_eq!(query["result"]["rows"].as_array().unwrap().len(), 16);

    let (_, suffix) = propose_until_success(
        &mut nodes,
        &all,
        18,
        "INSERT INTO snapshots VALUES (17, 'after-snapshot-restart')",
        SEED_BASE,
    )
    .await;
    let suffix_index = successful_index(&suffix, SEED_BASE);
    wait_applied(&mut nodes, suffix_index, SEED_BASE).await;

    for (_, node) in nodes {
        node.shutdown().await;
    }
    for node_id in all {
        let database = Database::open(database_directory(root.path(), node_id)).unwrap();
        let result = database
            .execute_read_only([0x6B; 16], 1, "SELECT id FROM snapshots ORDER BY id", &[])
            .unwrap();
        let ExecutionResult::Query(result) = result else {
            panic!("expected query result");
        };
        assert_eq!(result.rows.len(), 17);
    }
}

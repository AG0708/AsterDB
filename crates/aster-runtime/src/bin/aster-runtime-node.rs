use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use aster_core::Value;
use aster_db::ExecutionResult;
use aster_runtime::{RuntimeConfig, RuntimeHandle, RuntimeStatus, start};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Debug, Parser)]
#[command(
    name = "aster-runtime-node",
    about = "AsterDB replicated-runtime process harness"
)]
struct Arguments {
    #[arg(long)]
    node_id: u64,
    /// Comma-separated fixed voters, for example
    /// `1=127.0.0.1:7601,2=127.0.0.1:7602,3=127.0.0.1:7603`.
    #[arg(long, value_parser = parse_peers)]
    peers: BTreeMap<u64, SocketAddr>,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 350)]
    election_min_ms: u64,
    #[arg(long, default_value_t = 700)]
    election_max_ms: u64,
    #[arg(long, default_value_t = 75)]
    heartbeat_ms: u64,
    #[arg(long, default_value_t = 900)]
    check_quorum_ms: u64,
    #[arg(long, default_value_t = 256)]
    snapshot_threshold_entries: usize,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    snapshot_threshold_bytes: usize,
    /// Fixed election-jitter seed for a replayable test run. Omit to use OS
    /// entropy.
    #[arg(long)]
    rng_seed: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ControlRequest {
    request_id: u64,
    #[serde(flatten)]
    operation: ControlOperation,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum ControlOperation {
    Status,
    Propose {
        client_id: [u8; 16],
        sequence: u64,
        sql: String,
        #[serde(default)]
        parameters: Vec<Value>,
    },
    Query {
        client_id: [u8; 16],
        sequence: u64,
        sql: String,
        #[serde(default)]
        parameters: Vec<Value>,
    },
    Shutdown,
}

#[derive(Debug, Serialize)]
struct ControlResponse {
    request_id: u64,
    result: Option<ControlResult>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ControlResult {
    Query {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        affected_rows: u64,
        applied_index: u64,
    },
    Explain {
        plan: String,
        applied_index: u64,
    },
    Status {
        node_id: u64,
        role: String,
        term: u64,
        leader_id: Option<u64>,
        commit_index: u64,
        applied_index: u64,
        last_log_index: u64,
        database_applied_index: u64,
        database_pages: u64,
        wal_bytes: u64,
        active_transactions: u64,
        snapshot_index: Option<u64>,
        snapshot_bytes: Option<u64>,
        retained_log_entries: u64,
        retained_log_bytes: u64,
    },
    Shutdown,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    let mut config =
        RuntimeConfig::localhost(arguments.node_id, arguments.peers, arguments.data_dir);
    config.election_timeout_min = std::time::Duration::from_millis(arguments.election_min_ms);
    config.election_timeout_max = std::time::Duration::from_millis(arguments.election_max_ms);
    config.heartbeat_interval = std::time::Duration::from_millis(arguments.heartbeat_ms);
    config.check_quorum_interval = std::time::Duration::from_millis(arguments.check_quorum_ms);
    config.snapshot_threshold_entries = arguments.snapshot_threshold_entries;
    config.snapshot_threshold_bytes = arguments.snapshot_threshold_bytes;
    config.rng_seed = arguments.rng_seed;
    let node = start(config).await?;
    let handle = node.handle.clone();

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut output = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        let parsed = serde_json::from_str::<ControlRequest>(&line);
        let (response, shutdown) = match parsed {
            Ok(request) => dispatch(&handle, request).await,
            Err(error) => (
                ControlResponse {
                    request_id: 0,
                    result: None,
                    error: Some(format!("invalid control request: {error}")),
                },
                false,
            ),
        };
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        output.write_all(&encoded).await?;
        output.flush().await?;
        if shutdown {
            break;
        }
    }
    node.shutdown().await?;
    Ok(())
}

async fn dispatch(handle: &RuntimeHandle, request: ControlRequest) -> (ControlResponse, bool) {
    let request_id = request.request_id;
    let shutdown = matches!(request.operation, ControlOperation::Shutdown);
    let result = match request.operation {
        ControlOperation::Status => handle.status().await.map(map_status),
        ControlOperation::Propose {
            client_id,
            sequence,
            sql,
            parameters,
        } => handle
            .propose_sql(client_id, sequence, sql, parameters)
            .await
            .and_then(map_execution),
        ControlOperation::Query {
            client_id,
            sequence,
            sql,
            parameters,
        } => handle
            .linearizable_query(client_id, sequence, sql, parameters)
            .await
            .and_then(map_execution),
        ControlOperation::Shutdown => Ok(ControlResult::Shutdown),
    };
    match result {
        Ok(result) => (
            ControlResponse {
                request_id,
                result: Some(result),
                error: None,
            },
            shutdown,
        ),
        Err(error) => (
            ControlResponse {
                request_id,
                result: None,
                error: Some(error.to_string()),
            },
            shutdown,
        ),
    }
}

fn map_execution(result: ExecutionResult) -> aster_runtime::Result<ControlResult> {
    match result {
        ExecutionResult::Query(query) => Ok(ControlResult::Query {
            columns: query.columns,
            rows: query.rows.into_iter().map(|row| row.values).collect(),
            affected_rows: query.affected_rows,
            applied_index: query.applied_index,
        }),
        ExecutionResult::Explain {
            plan,
            applied_index,
        } => Ok(ControlResult::Explain {
            plan,
            applied_index,
        }),
        ExecutionResult::Transaction(_)
        | ExecutionResult::Committed(_)
        | ExecutionResult::RolledBack => Err(aster_runtime::RuntimeError::Unsupported(
            "replicated mode does not expose multi-request transactions".into(),
        )),
    }
}

fn map_status(status: RuntimeStatus) -> ControlResult {
    ControlResult::Status {
        node_id: status.node_id,
        role: format!("{:?}", status.role).to_ascii_lowercase(),
        term: status.term,
        leader_id: status.leader_id,
        commit_index: status.commit_index,
        applied_index: status.applied_index,
        last_log_index: status.last_log_index,
        database_applied_index: status.database_applied_index,
        database_pages: status.database_pages,
        wal_bytes: status.wal_bytes,
        active_transactions: status.active_transactions,
        snapshot_index: status.snapshot_index,
        snapshot_bytes: status.snapshot_bytes,
        retained_log_entries: status.retained_log_entries,
        retained_log_bytes: status.retained_log_bytes,
    }
}

fn parse_peers(input: &str) -> Result<BTreeMap<u64, SocketAddr>, String> {
    let mut peers = BTreeMap::new();
    for item in input.split(',') {
        let (node, address) = item
            .split_once('=')
            .ok_or_else(|| format!("peer `{item}` must be NODE=ADDRESS"))?;
        let node = node
            .parse::<u64>()
            .map_err(|error| format!("invalid peer node `{node}`: {error}"))?;
        let address = address
            .parse::<SocketAddr>()
            .map_err(|error| format!("invalid peer address `{address}`: {error}"))?;
        if peers.insert(node, address).is_some() {
            return Err(format!("duplicate peer node {node}"));
        }
    }
    if peers.is_empty() {
        return Err("peer map cannot be empty".into());
    }
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::parse_peers;

    #[test]
    fn peer_map_parser_is_strict() {
        let peers = parse_peers("1=127.0.0.1:7001,2=127.0.0.1:7002").unwrap();
        assert_eq!(peers.len(), 2);
        assert!(parse_peers("1:127.0.0.1:7001").is_err());
        assert!(parse_peers("1=127.0.0.1:1,1=127.0.0.1:2").is_err());
    }
}

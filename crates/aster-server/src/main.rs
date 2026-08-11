use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, bail};
use aster_runtime::{RuntimeConfig, start};
use aster_server::{DatabaseHandler, ReplicatedHandler, Server, ServerConfig};
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(name = "aster-server", about = "AsterDB database node")]
struct Arguments {
    #[arg(long, default_value = "127.0.0.1:7442")]
    listen: SocketAddr,
    #[arg(long, default_value_t = 1)]
    node_id: u64,
    #[arg(long, default_value = "aster-data")]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 1_024)]
    max_connections: usize,
    /// Fixed Raft voter mapping as `NODE_ID=HOST:PORT`. Supplying any peers
    /// enables replicated mode; every voter, including this node, is required.
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<(u64, SocketAddr)>,
}

fn parse_peer(value: &str) -> Result<(u64, SocketAddr), String> {
    let (node, address) = value
        .split_once('=')
        .ok_or_else(|| "peer must be NODE_ID=HOST:PORT".to_string())?;
    let node_id = node
        .parse::<u64>()
        .map_err(|error| format!("invalid peer node id: {error}"))?;
    if node_id == 0 {
        return Err("peer node id must be positive".into());
    }
    let address = address
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid peer address: {error}"))?;
    Ok((node_id, address))
}

fn peer_map(peers: Vec<(u64, SocketAddr)>) -> anyhow::Result<BTreeMap<u64, SocketAddr>> {
    let expected = peers.len();
    let peers: BTreeMap<_, _> = peers.into_iter().collect();
    if peers.len() != expected {
        bail!("peer node ids must be unique");
    }
    Ok(peers)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();
    let arguments = Arguments::parse();
    let listener = TcpListener::bind(arguments.listen)
        .await
        .with_context(|| format!("bind {}", arguments.listen))?;
    let config = ServerConfig {
        max_connections: arguments.max_connections,
        ..ServerConfig::default()
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(true);
        }
    });
    if arguments.peers.is_empty() {
        let handler = DatabaseHandler::open(&arguments.data_dir, arguments.node_id)
            .with_context(|| format!("open database at {}", arguments.data_dir.display()))?;
        Server::new(handler, config)
            .serve(listener, shutdown_rx)
            .await?;
        return Ok(());
    }

    let peers = peer_map(arguments.peers)?;
    let runtime = start(RuntimeConfig::localhost(
        arguments.node_id,
        peers,
        arguments.data_dir.clone(),
    ))
    .await
    .with_context(|| {
        format!(
            "start replicated node {} at {}",
            arguments.node_id,
            arguments.data_dir.display()
        )
    })?;
    let handler = ReplicatedHandler::new(runtime.handle.clone());
    let server_result = Server::new(handler, config)
        .serve(listener, shutdown_rx)
        .await;
    let runtime_result = runtime.shutdown().await;
    server_result?;
    runtime_result.context("shut down replicated runtime")?;
    Ok(())
}

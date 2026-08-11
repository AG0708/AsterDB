# AsterDB

AsterDB is a replicated SQL database written in Rust. One Raft group replicates
the full database. It provides snapshot isolation and is not production-ready.

## Features

- SQL tables, primary and secondary indexes, joins, aggregates, ordering, and
  explicit transactions
- Checksummed 4 KiB pages, B+ tree storage, redo WAL, checkpoints, and recovery
- Snapshot-isolated transactions with deterministic conflict checks
- Raft replication with leader failover, consistent reads, snapshots, and follower
  catch-up
- Standalone and replicated TCP servers with a command-line client
- Limits on protocol frames, queues, retries, and duplicate requests

## Verification

- 113 Rust tests plus strict Clippy checks
- 240 differential SQL operations checked against SQLite
- Three-process tests for leader failure, minority refusal, restart, and
  follower catch-up
- External Porcupine checks for concurrent read/write histories
- TLA+ model run over 21.6 million generated states with no invariant failure
- Crash injection for WAL, checkpoints, page writes, and snapshot installation

Run the standard checks with:

```sh
make verify
```

The release gate also runs Porcupine, TLA+, SBOM generation, and release builds:

```sh
make release-gate
```

Reference tools: Rust 1.97.1, Go 1.26.5, Java 21, `cargo-deny` 0.20.2, and
`cargo-audit` 0.22.2.

## Run

Start a standalone server and issue SQL through the CLI:

```sh
cargo run -p aster-server -- --listen 127.0.0.1:7442 --data-dir aster-data
cargo run -p aster-cli -- --address 127.0.0.1:7442 ping
cargo run -p aster-cli -- --address 127.0.0.1:7442 sql \
  "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)"
```

See [`docs/VERIFICATION.md`](docs/VERIFICATION.md) for the full test commands and
[`docs/CONSISTENCY.md`](docs/CONSISTENCY.md) for the commit and read guarantees.

## Layout

```text
crates/aster-core       values, schemas, and key encoding
crates/aster-storage    pages, B+ tree, WAL, and recovery
crates/aster-sql        parser, binder, and query plans
crates/aster-engine     catalog, MVCC, and execution
crates/aster-db         SQL and durable storage integration
crates/aster-raft       Raft and deterministic simulation
crates/aster-runtime    persistent Raft/TCP runtime
crates/aster-protocol   client and peer protocol
crates/aster-server     standalone and replicated server
crates/aster-cli        command-line client
tools/                  differential, history, fault, and release tools
model/                  TLA+ model
docs/                   design and verification notes
```

## Scope

Supported values are `INT64`, `BOOL`, `TEXT`, and `BYTES`. Tables require a
primary key. The SQL layer supports inserts, updates, deletes, filters, joins,
aggregates, ordering, limits, secondary indexes, transactions, and `EXPLAIN`.

- Standalone mode supports multi-statement transactions.
- Replicated mode supports autocommit writes and leader ReadIndex queries.
- One Raft group stores the full database; there is no sharding or cross-shard
  transaction protocol.
- Isolation is snapshot isolation, not serializability.
- Authentication, TLS, and online membership changes are not implemented.
- The server binds to localhost by default.

See [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md) for the complete list.

## Benchmark

`make benchmark` records end-to-end standalone latency, raw samples, workload
settings, durability mode, hashes, and p50/p95/p99. It does not claim a
cross-database performance win.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

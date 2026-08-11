# AsterDB

AsterDB is a from-scratch replicated relational database written in Rust. Its
focus is not the size of the SQL surface; it is making the difficult guarantees
inspectable: page-format invariants, crash recovery, snapshot visibility, leader
failover, quorum commits, and bounded wire/resource handling.

The project is an engineering and research system, not a production database.
It binds to localhost by default and does not yet provide authentication or TLS.

## Design

- Explicit 4 KiB checksummed pages and a persistent B+ tree containing the
  canonical logical-state records used to rebuild the in-memory MVCC view.
- A tested bounded clock-buffer component, plus an integrated copy-on-write
  page path with redo-only full-page WAL apply groups.
- Checkpoints with two independently checksummed superblock pages.
- A typed SQL frontend with source-span diagnostics and a finite documented
  grammar.
- MVCC snapshot isolation using Raft log indexes as commit timestamps.
- Deterministic first-committer-wins conflict decisions during state-machine
  apply.
- A pure Raft core with deterministic simulation and snapshot logic, plus a
  fixed-voter persistent runtime that exchanges Raft messages over TCP.
- Replayable history checking for autocommit linearizability and multi-statement
  snapshot isolation.

The acknowledgement path and exact non-guarantees are documented in
[`docs/CONSISTENCY.md`](docs/CONSISTENCY.md). A single Raft group is described as
*replicated SQL*. Scale-out sharding and durable cross-shard two-phase commit are
an extended gate; the project does not call itself distributed SQL until that
gate passes.

## Build and test

Rust 1.97.1, Go 1.26.5, Java 21, `cargo-deny` 0.20.2, and `cargo-audit`
0.22.2 are the reference verification tools. Go is used only by the independent
Porcupine consistency checker, and Java runs the checksum-pinned TLC release.

The complete local gate is one command:

```sh
make verify
```

For a release candidate, `make release-gate` reruns the complete gate, the
external Porcupine history, the bounded TLC model, reproducible SPDX generation,
and release builds while retaining content-addressed logs and binaries under
`artifacts/release/`.

The individual commands are:

```sh
cargo build --workspace --locked
cargo test --workspace --locked --all-targets
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p aster-runtime --test process_cluster --locked
python3 tools/sql_differential.py
python3 -m unittest discover -s tools -p 'test_*.py' -v
(cd tools/porcupine-check && go test ./...)
python3 tools/cluster_history.py
tools/check_tla.sh
```

Run a durable standalone server and drive its SQL/transaction protocol:

```sh
cargo run -p aster-server -- --listen 127.0.0.1:7442 --data-dir aster-data
cargo run -p aster-cli -- --address 127.0.0.1:7442 ping
cargo run -p aster-cli -- --address 127.0.0.1:7442 sql \
  "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)"
cargo run -p aster-cli -- --address 127.0.0.1:7442 transaction \
  --statement "INSERT INTO users VALUES (1, 'Ada')" \
  --statement "INSERT INTO users VALUES (2, 'Grace')"
cargo run -p aster-cli -- --address 127.0.0.1:7442 sql \
  "SELECT id, name FROM users ORDER BY id"
```

The release gate is stricter than a successful demo. See
[`docs/COMPLETION_GATES.md`](docs/COMPLETION_GATES.md) and
[`docs/VERIFICATION.md`](docs/VERIFICATION.md) for claim-to-test mapping.

## Repository map

```text
crates/aster-core       stable value/schema/ordered-key codecs
crates/aster-storage    pages, heap, B+ tree, buffer pool, WAL, recovery
crates/aster-sql        lexer, parser, binder, evaluator, plans
crates/aster-engine     catalog, MVCC, query execution, replicated apply
crates/aster-db         SQL-to-engine snapshot records in a B+tree/WAL facade
crates/aster-raft       consensus core, persistence actions, simulator
crates/aster-protocol   bounded client/peer frame codec
crates/aster-runtime    fixed-voter persistent Raft/TCP peer runtime
crates/aster-server     standalone or replicated SQL TCP service
crates/aster-cli        typed command-line client
tools/                  history, differential, fault, and benchmark tools
docs/                   architecture, contracts, evidence, and decisions
```

## Scope

The core SQL surface covers tables with one mandatory primary key; typed
`INT64`, `BOOL`, `TEXT`, and `BYTES` values; non-unique secondary indexes;
mutations; projection/filtering; joins; aggregation; ordering; limits; explicit
transactions; and `EXPLAIN`. Unsupported syntax returns a structured error
rather than silently changing semantics.

See [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md) before using or evaluating the
system.

With no `--peer` flags, `aster-server` runs in standalone mode and supports
explicit multi-request transactions. Supplying the complete fixed voter map as
repeated `--peer NODE=ADDRESS` flags starts replicated mode; each node uses a
separate `--listen` client address and peer address. Replicated mode currently
supports autocommit DDL/DML and leader ReadIndex queries and rejects
multi-request transactions and stale reads. Its process-level gate starts
three real child processes, kills a leader, writes through the replacement,
restarts and catches up the old leader, verifies minority refusal, heals, and
compares all durable database states. The persistent runtime creates a
checksummed database/Raft snapshot after either a retained-entry or byte
threshold, compacts the durable prefix, and transfers snapshots to followers
that have fallen behind that boundary. Snapshot installation uses a durable
cross-file intent so restart can finish either side of database publication;
the process gate also restarts a caught-up follower across the compacted
boundary and verifies the retained suffix and SQL state.

`tools/cluster_history.py` separately drives three public server processes with
concurrent SQL register reads/writes, kills and restarts the active leader, and
exports invocation/return intervals to Porcupine v1.0.3. A timeout is a failed
gate, not an assumed success. This is bounded evidence for that operation and
fault model; it is not a proof that every SQL history is linearizable.

`model/AsterCommit.tla` independently checks the bounded design contract among
quorum durability, leader completeness, apply indexes, ReadIndex barriers, and
MVCC commit timestamps. Its assumptions and abstraction boundary are explicit
in [`model/README.md`](model/README.md); it does not claim to prove the Rust
implementation.

## Benchmarking

`make benchmark` drives the release server through persistent public-protocol
TCP connections and records every measured latency, seeded workload settings,
durability mode, source/binary hashes, machine metadata, nearest-rank
p50/p95/p99, and a verified final row count. It is deliberately a local,
standalone end-to-end measurement; it includes Python client overhead and makes
no comparison to another database. Correctness gates run before this benchmark
in `make release-gate`.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

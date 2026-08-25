# Replicated SQL Database in Rust

A replicated SQL database using one Raft group and snapshot isolation. It is
not production-ready.

## Features

- SQL tables, indexes, joins, aggregates, ordering, and transactions
- Checksummed 4 KiB pages, B+ trees, WAL, checkpoints, and crash recovery
- Raft replication, leader failover, snapshots, and follower catch-up
- Standalone and replicated TCP servers with a command-line client

## Run

```sh
cargo run -p aster-server -- --listen 127.0.0.1:7442 --data-dir aster-data
cargo run -p aster-cli -- --address 127.0.0.1:7442 ping
cargo run -p aster-cli -- --address 127.0.0.1:7442 sql \
  "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)"
```

## Test

Requires Rust, Go, Java 21, `cargo-deny`, and `cargo-audit`.

```sh
make verify
```

This runs Rust tests and lints, SQLite differential tests, process-cluster
tests, Porcupine checks, the TLA+ model, and crash-injection tests.

## Limits

- Standalone mode supports multi-statement transactions.
- Replicated mode supports autocommit writes and leader ReadIndex queries.
- One Raft group stores the full database. There is no sharding.
- Isolation is snapshot isolation, not serializability.
- Authentication, TLS, and online membership changes are not implemented.

See [`docs/VERIFICATION.md`](docs/VERIFICATION.md),
[`docs/CONSISTENCY.md`](docs/CONSISTENCY.md), and
[`docs/LIMITATIONS.md`](docs/LIMITATIONS.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).

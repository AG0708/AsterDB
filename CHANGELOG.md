# Changelog

All notable changes to AsterDB are documented here. The project uses semantic
versioning for tagged source releases, but makes no wire- or disk-format
stability promise before 1.0.

## [0.1.0] - 2026-08-11

### Added

- A finite typed SQL frontend and snapshot-isolated MVCC execution engine.
- Checksummed 4 KiB pages, a persistent B+ tree, full-page redo WAL groups,
  dual-superblock checkpoints, and crash-injection tests.
- A pure Raft core, deterministic fault simulator, and a fixed-voter persistent
  three-node TCP runtime with ReadIndex queries, failover, restart catch-up,
  snapshot installation, and bounded log compaction.
- A bounded public protocol, standalone and replicated server modes, and a
  typed command-line client.
- SQLite differential tests, real-process Porcupine histories, a bounded TLA+
  safety model, dependency policy/advisory gates, reproducible SPDX generation,
  and an end-to-end benchmark harness that retains raw samples.

### Known limitations

- One Raft group replicates the entire database; there is no sharding or
  cross-shard transaction protocol.
- The implemented guarantee is snapshot isolation, not serializability.
- Replicated mode supports autocommit statements and linearizable leader reads,
  not multi-request transactions.
- Authentication, TLS, online membership changes, database-WAL truncation, and
  reclamation of snapshot-installed copy-on-write pages are not implemented.
- The system is an engineering and research project, not a production database.

[0.1.0]: https://github.com/AG0708/AsterDB/releases/tag/v0.1.0

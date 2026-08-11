# Changelog

## [0.1.0] - 2026-08-11

- Typed SQL, indexes, joins, aggregates, and snapshot-isolated transactions
- Checksummed pages, B+ tree storage, redo WAL, checkpoints, and recovery
- Three-node Raft with failover, consistent reads, restart catch-up, snapshots,
  and log compaction
- Standalone and replicated servers plus a CLI
- Differential SQL tests, Porcupine history checks, a TLA+ model, an SPDX
  SBOM, and an end-to-end benchmark.

Limits: one Raft group, no sharding, snapshot isolation rather than
serializability, autocommit-only replicated writes, and no authentication or
TLS.

[0.1.0]: https://github.com/AG0708/AsterDB/releases/tag/v0.1.0

# Known Limitations

AsterDB is not production-ready. The following limits are intentional and part
of its public contract.

- Transactions provide snapshot isolation. Write skew across distinct primary
  keys is possible; there is no serializable or predicate-locking mode.
- The core release uses one Raft group for the database. That is replication,
  not horizontal scale-out. Sharding and cross-shard 2PC are extended work.
- The persistent replicated runtime has a fixed voter set. Snapshot install and
  log compaction are enabled and tested in real processes; online membership
  changes remain pure-core/simulator behavior and are not a runtime surface.
- Replicated SQL currently supports autocommit DDL/DML and leader ReadIndex
  queries. Multi-request transactions and follower stale reads are standalone
  or future surfaces, not runtime guarantees.
- Replicated peers accept localhost addresses only. The diagnostic process
  control channel is a test/inspection harness, not a production client API.
- Secondary indexes are non-unique. Unique secondary conflict checking is not
  silently approximated.
- SQL is a finite subset, not PostgreSQL/MySQL compatibility. There are no
  subqueries, outer joins, window functions, triggers, stored procedures, or
  online schema migrations in the core release.
- The integrated executor materializes catalog, MVCC, and secondary-index state
  in memory. Persistence stores canonical logical records in a real page-backed
  B+ tree and WAL, but SQL lookups do not directly traverse separate on-disk
  primary/secondary trees. The active database must fit in memory, and no
  larger-than-RAM or high-throughput claim is made.
- Delete/vacuum behavior follows the storage implementation's documented gate;
  no online garbage-collection claim is made without snapshot leases.
- Authentication, authorization, TLS, tenant isolation, encryption at rest,
  audit logging, and adversarial-network hardening are absent.
- Raft tolerates crash/omission faults, not Byzantine peers or disk rollback of
  a majority.
- Durability assumes successful operating-system flushes reflect the storage
  device's contract. Correlated media loss and faulty hardware are outside the
  model.
- Exactly-once behavior is limited to the latest monotonic sequence per client;
  external side effects and arbitrary historical retries are not covered.
- The included benchmark is a local standalone public-TCP measurement with
  Python client overhead. It is not a replicated result, and no superiority to
  an established database is claimed without equivalent durability,
  consistency, workload, and hardware.

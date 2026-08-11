# Completion Gates

The project is called complete only when every core gate below passes from a
clean checkout. A feature is not credited by source-code presence alone.

## Core release gates

- **G1 — Reproducible build:** pinned stable Rust toolchain, formatting,
  warnings-as-errors, unit tests, and documentation build pass on macOS and
  Linux CI.
- **G2 — Persistent pages:** create/reopen a database containing records that
  span pages; page checksums and format/version rejection are tested.
- **G3 — B+ tree:** randomized insert/search/range/delete traces match an
  ordered-map model; reopen preserves results; structural validation passes
  after each mutation and root/internal/leaf splits are covered.
- **G4 — WAL recovery:** a deterministic crash is injected at every logged I/O
  boundary for generated transactions. Recovered state equals the committed
  reference model; repeated recovery reaches the same bytes/logical state.
- **G5 — Checkpoints:** recovery from a quiesced-apply checkpoint plus subsequent
  WAL yields the same state as full replay, including crashes before and after
  each data flush, checkpoint-marker flush, alternate-superblock publication,
  and WAL retirement.
- **G6 — SQL coherence:** documented SQL grammar parses and executes end to end.
  Golden error tests carry source spans; differential query tests agree with
  SQLite for the shared semantic subset.
- **G7 — Snapshot isolation:** dirty/non-repeatable reads are absent,
  read-your-writes works and same-primary-key conflicts abort one writer,
  rollback leaves no visible effects, and a test documents allowed write skew.
- **G8 — Real TCP:** CLI connects to a separately running server, initializes a
  schema, executes parameterized transactions, streams bounded results, and
  survives fragmented/coalesced frames and malformed clients.
- **G9 — Raft safety:** deterministic simulations across many seeds exercise
  elections, duplicated/reordered messages, partitions, crashes, and restarts
  while checking election safety, log matching, leader completeness, and state
  machine agreement.
- **G10 — Three-node failover:** a black-box three-process test writes through a
  leader, kills it, observes a replacement, reads every acknowledged write,
  restarts the old leader, and verifies catch-up and equal committed state.
- **G11 — Snapshot catch-up:** a follower offline beyond log compaction installs
  a checksummed snapshot, replays the suffix, and reaches the leader's state.
- **G12 — Client semantics:** redirect hints work; latest duplicate request
  sequences apply once; minority partitions cannot acknowledge writes; bounded
  single-register SQL histories pass an independent Porcupine check across
  leader kill/restart; stale reads identify their applied index.
- **G13 — Observable evidence:** cluster-state output exposes term/role/indexes
  and storage/MVCC counters; benchmark and chaos runs emit machine-readable
  metadata, raw results, and reproducible commands without hard-coded claims.
- **G14 — Resource discipline:** fuzz/property tests cover codecs/parser; frame,
  result, queue, connection, cache, and request limits are enforced; sanitizers
  or equivalent concurrency/model checking runs where supported.
- **G15 — Claim boundary:** the core release is labeled a fault-tolerant
  replicated SQL database. It is labeled distributed SQL only after independent
  range/hash shards, routing epochs, durable intents, replicated 2PC decisions,
  coordinator recovery, cross-shard snapshot/conflict tests, and range movement
  under failure pass end to end.

## Extended gates

These improve the project but are not silently included in the core claim:

- safe joint-consensus membership changes;
- secondary-index-only and covering scans;
- hash join and cost-based plan selection;
- online backup/restore tooling;
- TLS and authenticated clients/peers;
- long-running performance regression dashboard.

The built-in linearizability checker covers bounded autocommit map histories.
The external gate covers a single-row SQL register using invocation/completion
intervals, concurrent clients, stable retry identities, and leader
kill/restart. Multi-statement snapshot-isolation histories use a separate
oracle for fixed `read_ts`, read-your-writes, atomic commit indexes,
first-committer-wins, and abort invisibility; they are not called linearizable.

## Evidence policy

Benchmarks are reported only with exact commit, build profile, machine, OS,
dataset, concurrency, duration, warmup, and command. Throughput without a
correctness gate is not a result. Simulated tests are labeled simulated; local
three-process tests are not described as geo-distributed or production scale.
Known limitations remain visible in `docs/LIMITATIONS.md` and are never converted
into implied guarantees.

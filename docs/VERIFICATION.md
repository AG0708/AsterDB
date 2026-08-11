# Verification Matrix

This matrix is the authority for public claims. A row is complete only when the
listed command passes from a clean checkout and the referenced artifact exists.
Source code that merely resembles a feature is not evidence.

`make release-gate` executes the rows below in a fail-closed sequence and writes
`asterdb.release-gate.v1` evidence with source/tool metadata, per-step log
hashes, reproducibly generated SPDX inventory, and release-binary hashes. It is
an evidence pack, not a substitute for the individual independent oracles.

| Claim | Primary evidence | Command | Release gate |
|---|---|---|---|
| Explicit page/row codecs reject truncation and corruption | storage/core unit and golden tests | `cargo test -p aster-core -p aster-storage` | G2 |
| B+ tree preserves ordering through splits, lazy-underflow deletes, root contraction, and reopen | randomized ordered-map model and invariant walk | `cargo test -p aster-storage btree` | G3 |
| Integrated logical state persists through the page B+ tree and full-page WAL | database restart, multi-page/chunk, checkpoint, and snapshot-record tests | `cargo test -p aster-db --locked` | G3–G5 |
| A complete apply group survives each modeled crash boundary | durable/volatile `FaultyFile` matrix | `cargo test -p aster-storage recovery` | G4–G5 |
| SQL parser/binder has deterministic supported semantics | parser/binder/evaluator suites | `cargo test -p aster-sql` | G6 |
| Shared SQL subset agrees with SQLite | seeded differential harness and saved counterexamples | `python3 tools/sql_differential.py` | G6 |
| Transactions provide snapshot isolation, not serializability | directed MVCC litmus tests plus history oracle | `cargo test -p aster-engine` and `python3 -m unittest discover -s tools` | G7 |
| Wire decoder is framed, bounded, and stream-correct | byte-at-a-time, coalesced, CRC, oversize tests | `cargo test -p aster-protocol` | G8, G14 |
| CLI reaches a separately listening process | loopback integration test | `cargo test -p aster-server real_tcp` | G8 |
| Raft safety survives deterministic faults | seeded simulator with invariants and replay trace | `cargo test -p aster-raft` | G9 |
| Replicated commit, ReadIndex, and MVCC timestamp invariants hold in the bounded abstract design | TLC exhaustively checks the finite configuration in `model/AsterCommit.cfg` | `tools/check_tla.sh` | G9, G12 supporting evidence |
| Acknowledged writes survive leader loss and minority isolation | real three-child-process kill/restart/heal harness | `cargo test -p aster-runtime --test process_cluster --locked` | G10, G12 |
| Lagging follower installs a compacted snapshot, replays the suffix, and survives another restart | in-process TCP cluster plus real three-child-process runtime test; durable-intent failpoint unit tests | `cargo test -p aster-runtime --locked --all-targets` | G11 |
| Built-in autocommit map histories satisfy their reference model | interval history and bounded backtracking checker with positive/negative self-tests | `python3 -m unittest discover -s tools -p 'test_*.py' -v` | G12 supporting evidence |
| A single-row SQL register remains linearizable across concurrent clients and leader kill/restart | public server/CLI processes export intervals to independently versioned Porcupine v1.0.3; `Unknown`/timeout fails closed | `(cd tools/porcupine-check && go test ./...) && python3 tools/cluster_history.py` | G12 |
| Status reflects live consensus and durable storage state | runtime and server assertions | `cargo test -p aster-runtime -p aster-server --locked` | G13 |
| Standalone measurements retain raw samples and exact context | public TCP benchmark, verified final row count, source/binary hashes, machine and durability metadata | `make benchmark` | G13 measurement evidence |

No row is converted into a performance claim. Performance reports additionally
require the exact source revision, release build, CPU, OS, dataset, concurrency,
durability/replication settings, warmup, repetitions, raw samples, and percentile
calculation.

## Failure artifact format

Randomized tests print and save:

- seed and generator version;
- ordered input operations;
- fault schedule and persistence events;
- invocation/completion intervals;
- expected and observed state hashes;
- exact replay command.

A test that cannot reproduce its counterexample is treated as a harness bug.

## Independent validation

The built-in checker remains useful as an inspectable reference model and has
positive and negative self-tests. The release gate also exports histories from
the public SQL/TCP surface to Porcupine v1.0.3, whose wrapper has legal and stale
read negative controls. The default run uses three deterministic workload seeds
and treats `Unknown`, timeout, process failure, or malformed output as failure.
This is independent, bounded validation of a single-register autocommit scope;
it is not a formal proof and does not make multi-row SQL transactions
linearizable.

The TLA+ model is a separate design-level safety check. It bounds the log,
terms, and recorded reads, and abstracts message queues, disk bytes, SQL, and
code. Its invariants are traceable to the executable tests, but TLC success is
never described as verification of the Rust implementation.

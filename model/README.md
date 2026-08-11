# Replicated commit model

`AsterCommit.tla` is a bounded safety model of the contract between Raft
commit, ReadIndex, and MVCC timestamps. TLC explores three symmetric nodes, two
log entries with two possible values, up to three election terms and two
recorded reads, crashes,
restarts, isolation/healing, divergent uncommitted suffixes, replication,
current-term commit, apply lag, and linearizable single-register reads.

The checked invariants are:

- every elected leader contains the acknowledged prefix;
- every acknowledged prefix remains on a durable quorum;
- a node applies only acknowledged entries and never past commit;
- a ReadIndex result is the value at its recorded committed barrier;
- commit timestamps are exactly committed log indexes.

The two-entry bound still admits divergent uncommitted suffixes, replacement by
a newer leader, and committing an older prefix through a current-term entry.
This model deliberately abstracts message queues, timeouts, bytes on disk, SQL
execution, and the Rust implementation. It is a design-level bounded check,
not a proof of the executable. Deterministic Raft simulation, crash recovery,
real-process failure tests, and the external Porcupine history gate cover those
separate layers.

Run the pinned TLC release with:

```sh
tools/check_tla.sh
```

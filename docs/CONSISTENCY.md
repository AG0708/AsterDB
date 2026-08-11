# Consistency Contract

This document distinguishes behavior AsterDB guarantees from behavior it does
not implement. Tests reference these clauses by name.

## Single-node durability

After a client receives a successful commit, that transaction survives process
termination and restart, assuming the operating system and storage device honor
successful `fsync` calls. An incomplete local apply group is not visible after
recovery. Recovery tolerates a truncated final WAL frame and is idempotent across
crashes during recovery.

The project does not claim durability against lost `fsync` writes, filesystem
corruption, media failure, or simultaneous loss of all replicas.

## Snapshot isolation

Each transaction observes:

1. all versions committed before its snapshot was taken;
2. its own writes;
3. none of another transaction's uncommitted writes; and
4. none of the versions committed after its snapshot was taken.

Statement execution inside a transaction uses the same snapshot. Autocommit
statements each receive a fresh snapshot.

Two concurrent transactions that update or delete the same logical primary key
cannot both commit. The later committer aborts. Reads never observe dirty,
intermediate, or rolled-back values. The core release supports non-unique
secondary indexes only.

Snapshot isolation permits write skew across distinct rows. AsterDB does not
claim serializable transactions or predicate locking. This limitation is
deliberate and is demonstrated by a named test.

## Replicated writes

A clustered write is successful only when:

- its entry is stored by a majority of the active configuration;
- the leader has committed it according to Raft's current-term rule;
- the local state machine has durably applied it; and
- the result is associated with the request's client/session sequence number.

Once acknowledged, a write remains in the committed prefix of every future
leader elected by a quorum containing a majority of the configuration. A
minority partition cannot acknowledge writes. An isolated former leader rejects
or times out writes once it cannot obtain quorum progress.

An exact retry of a client's latest sequence returns its first durable outcome
and does not apply the mutation twice, even when a new leader re-prepares it
under a different term or snapshot. Exact log-entry hashes protect same-index
replay, while a separate SQL-and-parameter fingerprint identifies the logical
client request. Reuse of that sequence with a different logical fingerprint,
sequence gaps, and older-than-latest requests are rejected. This is a
monotonic-session contract, not arbitrary historical deduplication.

## Reads

`LINEARIZABLE` is the default for autocommit reads in cluster mode. Such reads
run only on the leader after a quorum-confirmed read-index barrier and after the
local applied index reaches that barrier. A successful response therefore
reflects all writes completed before the read began.

Follower stale reads are not exposed by the current persistent runtime. The
database applied index is available in status for inspection, but it is not a
SQL read consistency mode.

In standalone mode, an explicit transaction's snapshot remains fixed.
Consequently, later reads in
that transaction need not include commits completed after `BEGIN`; this is the
documented snapshot-isolation behavior rather than a linearizable read claim.

## Ordering and time

Raft log order defines the global order of replicated state-machine commands.
Wall-clock timestamps do not establish correctness and are not persisted as a
source of truth. Election and request timers affect liveness only.

There is no availability guarantee without a reachable quorum. Under fair
delivery and bounded processing, a stable majority elects a leader and makes
progress. During arbitrary delay or repeated partitions, safe unavailability is
allowed.

## Membership and snapshots

The persistent runtime supports a fixed, configured voter set only. The pure
Raft core implements and simulator-tests joint consensus, but the process
runtime does not expose membership changes.

An installed snapshot represents the state machine exactly through its included
log index and term. Entries after that index replay in order. A snapshot is
checksummed and content-addressed; a partial sidecar never replaces the last
valid state. Log compaction through index `N` is allowed only after a database
checkpoint and logical snapshot at applied index `N` are durable and the Raft
boundary term and retained suffix match the durable log. Runtime follower
installation uses a durable intent spanning the Raft state file and database
publication, and restart completes that intent idempotently. The current gates
exercise leader-side compaction, snapshot transfer to a lagging real process,
suffix replay, and restart across the installed boundary.

## Extended cross-shard transactions

When experimental sharding is enabled, a successful cross-shard commit means
every participant durably prepared and the replicated transaction coordinator
durably decided commit. Participants retain intents until they learn a durable
decision; coordinator recovery re-drives unresolved decisions. Failure before a
durable commit decision resolves to abort. Failure after it resolves to commit.

The sharded system claims snapshot isolation only after a shared timestamp and
routing-epoch protocol passes the cross-shard gates. Without those gates, only
per-shard snapshot isolation is claimed and the project is labeled replicated
rather than distributed SQL.

## Non-guarantees

AsterDB does not claim:

- serializable SQL transactions;
- follower linearizable reads without a leader barrier;
- exactly-once effects for external systems;
- synchronous geo-replication or bounded replication lag;
- Byzantine fault tolerance;
- online schema migration compatibility across arbitrary versions;
- distributed transactions across independent AsterDB clusters.

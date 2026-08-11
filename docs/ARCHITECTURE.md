# AsterDB Architecture

## Purpose and scope

AsterDB is a from-scratch, educational replicated relational database. It is
designed to make its durability and consistency arguments inspectable: the
storage format is documented, state transitions are explicit, and every major
claim has a deterministic test gate.

It is not intended to replace a production database. The first complete release
targets a small, coherent SQL surface and a three-node replicated cluster while
preserving the hard parts: pages, indexes, write-ahead recovery, multiversion
transactions, consensus, failure recovery, and observable evidence.

## Repository layout

The implementation is a Cargo workspace:

- `crates/aster-core`: shared identifiers, values, schemas, binary codecs, and
  errors.
- `crates/aster-storage`: database files, fixed-size pages, a bounded buffer
  pool, slotted heap pages, a persistent B+ tree, WAL, checkpoints, and recovery.
- `crates/aster-sql`: lexer, parser, typed AST, logical plans, and plan
  construction for the supported SQL grammar.
- `crates/aster-engine`: catalog, MVCC transaction manager, row encoding,
  execution, and the replicated state-machine command boundary.
- `crates/aster-db`: the integrated SQL/MVCC/storage facade, including private
  mutation preparation, exact-index replicated apply, and restart recovery.
- `crates/aster-raft`: Raft state machine, persistent hard state/log, snapshots,
  transport messages, and deterministic simulation hooks.
- `crates/aster-protocol`: versioned, length-delimited client and peer messages.
- `crates/aster-runtime`: persistent Raft hard-state/log storage, localhost TCP
  peer transport, timers, ReadIndex, and ordered durable database apply.
- `crates/aster-server`: the client-facing TCP SQL and node-status API, in
  standalone mode or backed by `aster-runtime` when a fixed peer map is given.
- `crates/aster-cli`: interactive SQL client and administrative commands.
- `tests/`: black-box crash, differential, cluster, and history tests.
- `tools/`: fault-injection, benchmark, and report-generation utilities.

The primary integration boundaries are:

```text
runtime -> {db, raft, protocol, core}
server  -> {db, protocol}
db      -> {storage, sql, engine, core}
storage -> core
sql     -> core
engine  -> core
raft    -> core
protocol-> core
```

`aster-raft` replicates opaque deterministic commands and never imports SQL or
storage internals. `aster-engine` owns the mapping from a committed command to a
database transition. This prevents a consensus test from accidentally relying
on database behavior and permits the Raft core to run under a deterministic
simulator.

## Storage format

### Database file

The primary file is a sequence of 4 KiB pages. Pages 0 and 1 are independent,
alternating checksummed superblocks containing:

- format magic and version;
- page size;
- database and cluster identifiers;
- the fixed root-directory page id;
- the last checkpoint Raft index and LSN;
- a monotonically increasing generation.

Recovery chooses the valid superblock page with the highest generation. Keeping
the copies on distinct pages prevents one torn page write from destroying both.
Changing catalog roots and allocator state live in ordinary, WAL-protected pages
reachable from the fixed root directory. All integers have an explicit
little-endian encoding; Rust struct layout is never written directly to disk.

Every non-superblock page has a common header with page id, page kind, WAL byte
LSN, replicated apply-index epoch, lower/upper free-space bounds, slot count,
generation, and checksum. The two units are never compared. Heap pages
use a slotted layout: stable slot identifiers point to variable-length records
packed from the end of the page. Compaction can move record bytes without
changing a record identifier `(page_id, slot_id)`.

### Buffer-pool component

The storage library's buffer pool is bounded and uses pin guards to prevent eviction while a page
is in use. Dirty pages carry the LSN of the update that dirtied them. Eviction
uses a clock replacement policy and obeys write-ahead ordering: the WAL is
flushed through a dirty page's page LSN before that page reaches the database
file. A test-only I/O backend records reads, writes, flushes, and injected
failures. The current integrated database facade instead stages copy-on-write
B+ tree pages for one apply group and does not claim SQL execution through this
buffer pool.

### Write-ahead log and recovery

SQL writes remain in a transaction-private overlay until the replicated state
machine decides commit or abort. State-machine application is serialized and
uses private page copies, so an aborted SQL transaction never dirties shared
pages and the storage WAL needs redo, not undo.

The WAL is an append-only sequence of aligned, framed, checksummed records. One
applied Raft entry is a group: `BEGIN_APPLY(index, command_hash)`, zero or more
`PAGE_IMAGE(index, page_id, 4096_bytes)` records, and
`COMMIT_APPLY(index, image_count, group_digest)`. The complete group is flushed
before its private page copies enter the shared buffer.

Recovery accepts only a complete group with a matching count and digest. It
truncates a torn or invalid tail and redoes a page when its epoch is older than
the group index. Reapplying the same `(index, command_hash)` is a no-op; seeing
the same index with a different command hash is fatal corruption. A crash after
WAL flush but before page publication is therefore recovered, while a crash
before the commit frame has no visible effect.

Checkpointing briefly excludes state-machine apply, first flushes WAL required
by dirty pages, writes those pages, flushes the data file, appends and flushes a
`CHECKPOINT(index, manifest_digest)` record, and then writes and flushes the
alternate superblock generation pointing to that record. The superblock is the
publication point. Old WAL segments are retired only afterward. A crash before
publication retains the old superblock and WAL; a crash afterward has a complete
new checkpoint.

### B+ tree and integrated state layout

`aster-storage` implements a general persistent B+ tree. Leaf pages contain
ordered key/value entries and sibling links; internal pages contain separator
keys and child page ids. Insert implements root-to-leaf descent,
leaf/internal splits, separator propagation, and stable root identity: a root
split rewrites the root as an internal page pointing to two new children.
Version one uses an explicit lazy-underflow policy on delete: it retains
non-empty underfull nodes, unlinks empty leaves, and contracts a unary stable
root. Tree validation checks ordering, separator bounds, equal leaf depth,
sibling traversal, and reachability.

The integrated `aster-db` facade uses one such tree as a durable directory of
canonical logical-state records. Catalog records, MVCC primary/secondary
version-chain records, and client-deduplication records are independently
encoded, chunked, checksummed by digest, and diffed against the current tree.
Changed tree pages enter the full-page redo apply group. On open, the tree is
validated and decoded into the engine's ordered in-memory maps.

Consequently, the current SQL executor does not perform each lookup against a
separate on-disk primary or secondary tree, and the active logical state must
fit in memory. The physical tree and WAL provide real page-level persistence
and recovery, but this release is not described as a larger-than-memory or
row-buffer-pool execution engine.

## Relational and SQL layer

The supported grammar is intentionally finite and testable:

- `CREATE TABLE` with `INT`, `BOOL`, `TEXT`, nullable columns, and a single
  primary key;
- `CREATE INDEX` on one column;
- `INSERT`, `UPDATE`, and `DELETE`;
- `SELECT` with projection, aliases, `WHERE`, `INNER JOIN`, `GROUP BY`,
  `COUNT`/`SUM`/`MIN`/`MAX`, `ORDER BY`, and `LIMIT`;
- `BEGIN`, `COMMIT`, and `ROLLBACK`;
- `EXPLAIN` for the logical/physical plan.

The parser produces a span-carrying AST with actionable errors. Binding resolves
tables and columns against a transaction-visible catalog and performs type
checks. The planner selects a primary/secondary index scan for compatible
equality/range predicates and otherwise uses a table scan. Execution uses
iterators for scans, filters, projections, nested-loop/index joins, hash
aggregation, sorting, and limits. Plans and expression evaluation are
deterministic so committed Raft commands replay identically.

## MVCC and transactions

The engine's ordered primary MVCC key is `(table_id, encoded_primary_key)`,
whose version chain is ordered by descending commit index. Each immutable
version is either a row or tombstone. A
transaction uses the engine's last-applied index as `read_ts`. Standalone
`BEGIN` captures that local prefix; a future clustered `BEGIN` would first need
a leader ReadIndex barrier, and is currently rejected. Lookup selects the newest version with
`commit_index <= read_ts`, overlaid with the transaction's own staged writes.

Commit proposes a deterministic command containing client sequence, leader
term, `read_ts`, schema epoch, and canonical ordered writes. Serialized
state-machine apply rechecks first-committer-wins: if any written logical primary
key has a newer version than `read_ts`, it deterministically records an abort;
otherwise every new version receives the command's Raft index as its commit
timestamp. Abort discards the private overlay. DDL increments the schema epoch,
causing an older writer to abort.

Secondary entries are versioned, ordered in memory, and non-unique in the core release. Reads
collapse each secondary version chain, fetch the primary row, and recheck the
predicate, preventing stale-index false positives. Unique secondary indexes are
not claimed until their cross-row conflict rule is implemented.

Catalog changes use the same transactional machinery as data changes. The first
release does not silently claim serializability: write skew is permitted unless
an explicit write conflict or uniqueness constraint prevents it.

## Replication and node architecture

Each replicated-runtime process has a bounded peer listener and a local control
handle. Peer traffic uses the versioned length-prefixed protocol. The diagnostic
process harness accepts newline-delimited local JSON control requests.
`aster-server` exposes the public SQL protocol in either standalone mode or
replicated mode when its complete voter map is supplied with repeated `--peer`
flags. A follower returns a leader hint through the runtime/server boundary for
writes and ReadIndex queries.

The Raft core implements:

- follower, candidate, and leader roles;
- randomized election timeouts and periodic heartbeats;
- persistent current term and voted-for state;
- append consistency checks and conflict backtracking;
- quorum commit restricted to entries from the leader's current term;
- ordered, at-most-once application by log index;
- snapshot creation/install and log prefix truncation in both the pure core and
  persistent runtime;
- follower catch-up and restart from persisted state;
- joint-consensus membership transitions in the extended completion gate.

Committed log entries contain deterministic transaction batches. A leader does
not expose a successful write before both Raft quorum commit and local durable
application. Client requests carry a stable client/session sequence pair. The
replicated state machine durably stores each client's latest sequence, logical
SQL-and-parameter fingerprint, and result. This fingerprint is intentionally
separate from the exact Raft-entry hash because a retry may be re-prepared under
a different term or snapshot. An exact retry of the latest sequence returns the
first durable result; sequence gaps, older requests, or the same sequence with a
different logical fingerprint are rejected.

Linearizable reads use a read-index barrier: the leader confirms authority with
a quorum in its current term and waits until its applied index reaches the
barrier. The persistent runtime uses a fixed voter configuration. It builds a
database snapshot only at an exact durably applied Raft boundary, publishes a
checksummed content-addressed snapshot sidecar, and then compacts the matching
log prefix while preserving the exact suffix. A lagging follower first durably
publishes a cross-file install intent, atomically installs the database through
copy-on-write pages and an alternate superblock, publishes the matching Raft
state, and finally removes the intent; startup idempotently completes an
interruption at either publication boundary. Membership changes remain a
pure-core/simulator capability and are not exposed by the runtime. Runtime SQL
is autocommit DDL/DML plus leader ReadIndex queries; multi-request transactions
are rejected. Explicit snapshot transactions remain available in standalone
mode.

## Extended sharding architecture

A single Raft group is a fault-tolerant replicated SQL database, not a scale-out
distributed SQL database. AsterDB uses the stronger label only after the
extended sharding gates pass.

In the extended design, ranges of encoded primary keys are assigned to
independent Raft groups through a replicated range directory. A transaction
touching one range uses that range's normal commit path. A cross-range
transaction uses durable two-phase commit: participant groups replicate prepare
intents and conflict checks; a replicated coordinator record decides commit or
abort; participants resolve only from that durable decision, including after
coordinator or leader failure. Retries are idempotent. Snapshot timestamps come
from a monotonic timestamp service backed by a Raft group, never wall clocks.

Range movement and split are admitted only after tests prove that ownership,
prepared intents, data, and routing epochs transfer safely under failure. Until
those gates pass, the project remains accurately described as replicated SQL
with experimental sharding.

## Observability

The runtime status API and diagnostic process harness expose:

- node id, role, term, leader id, and commit/applied/log indexes;
- the database applied index, page count, durable WAL byte length, and active
  transaction count.

The standalone server exposes the corresponding storage fields through its
status response. Peer progress maps and latency histograms are not yet a public
runtime API. Fault injection is test-only.

## Security and resource boundaries

The wire decoder rejects oversized or malformed frames before allocation. SQL
parameters are typed protocol values rather than interpolated strings. File
paths are rooted below a configured data directory. Every queue, frame, batch,
result set, page cache, and connection has an explicit bound or timeout. The
initial release is a trusted-network system and does not claim authentication,
encryption, tenant isolation, or adversarial SQL safety.

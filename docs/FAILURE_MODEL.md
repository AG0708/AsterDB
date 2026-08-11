# Failure Model

## Process and storage failures

Tests may terminate a node at any instrumented storage boundary, including
before/after WAL append, WAL flush, data-page write, data-file flush, checkpoint
end, state-machine apply, snapshot rename, and client response. Restart retains
only bytes whose modeled flush completed.

The file backend can inject:

- short reads/writes and I/O errors;
- a torn/truncated final WAL frame;
- corruption detected by a checksum;
- failure between temporary-file flush and atomic rename;
- process death with dirty buffer-pool pages.

The supported response to detectable corruption outside a recoverable WAL tail
is a clear startup error, not silent repair.

## Network failures

The deterministic simulator can independently:

- drop, delay, duplicate, and reorder messages;
- partition arbitrary directional links;
- disconnect and reconnect peers;
- schedule messages under bounded queues;
- deliver stale messages from old terms after healing.

The real TCP process gates separately cover leader process death, restart and
catch-up, minority refusal, healing, and an offline follower that must install a
compacted snapshot before replaying the suffix and surviving another restart.
They are not yet a general-purpose network proxy. The protocol must remain safe
under the modeled behaviors. Liveness is tested only after the network becomes
fair and a stable majority can communicate.

TCP itself is treated as an ordered byte stream, not a message transport. Frame
decoding must handle split headers, split payloads, multiple coalesced frames,
EOF mid-frame, and oversized length prefixes.

## Timing and scheduling

No correctness decision may depend on synchronized clocks. The deterministic
simulator controls election ticks, heartbeat ticks, message delivery, node
steps, and crash/restart order from a seed. Every randomized failure must print
a replayable seed and a minimized or saved trace when it fails.

Production timers may fire late, concurrently, or in bursts. Lock ordering and
bounded channels must prevent a slow client or peer from blocking consensus
progress indefinitely.

## Client failures

Clients can disconnect before receiving a commit result and retry the same
request at another node. They can send malformed, truncated, unsupported-version,
or oversized frames. A client's loss of a response makes the outcome unknown;
the idempotency key allows it to resolve the outcome safely.

## Fault threshold

For `N` voters, AsterDB remains safe with any number of crash failures and makes
progress when at least `floor(N/2)+1` voters are alive and mutually reachable.
It tolerates crash/omission faults, not malicious or Byzantine peers.

Disk failure on one node is treated as that node failing. Durability of an
acknowledged write assumes at least one member of every future quorum retains
its durable state; correlated loss or rollback of a majority's storage is
outside the model.

## Required invariants under failure

- At most one leader can be elected in a term.
- A node grants at most one vote per term, including across restart.
- Log matching: equal `(index, term)` implies identical prefixes.
- A committed entry is never replaced.
- Applied indexes advance monotonically and never exceed commit indexes.
- Each committed client sequence changes database state at most once.
- WAL-before-data ordering is never violated.
- Recovery exposes all and only committed transactions.
- No checksum failure is interpreted as valid state.

# ADR 0002: Commit-Index Snapshot Isolation

Status: accepted

## Decision

Transactions begin after a leader ReadIndex barrier and use the applied Raft
index as `read_ts`. Committed row versions use their apply index as `commit_ts`.
The state machine rechecks write-write conflicts and schema epoch at apply time.

## Consequences

All replicas make the same commit/abort decision without wall clocks. Reads are
repeatable and overlapping writes to one primary key cannot both commit. Write
skew across separate keys remains possible, so the system does not claim
serializability.

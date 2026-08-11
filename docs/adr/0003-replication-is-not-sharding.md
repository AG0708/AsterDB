# ADR 0003: Replication Is Not Sharding

Status: accepted

## Decision

The core system is described as a fault-tolerant replicated SQL database. It is
described as distributed SQL only after independent key ranges use separate
Raft groups and recoverable cross-range snapshot transactions pass their failure
gates.

## Rationale

Three replicas of one state machine improve availability and durability but do
not distribute storage or write throughput. Precise language is part of the
project's evidence policy.

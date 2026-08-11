# ADR 0001: Serialized Apply with Full-Page Redo

Status: accepted

## Decision

Uncommitted SQL writes remain in a transaction-private overlay. Raft orders a
deterministic transaction attempt; serialized state-machine apply decides its
outcome and constructs private page copies. The local WAL stores a checksummed
begin record, full-page after-images, and a matching commit digest. The group is
flushed before page publication.

## Rationale

This creates one deterministic commit path for standalone and replicated modes,
keeps aborted SQL transactions out of shared pages, and makes crash recovery an
idempotent redo problem. It avoids mixing ARIES loser-undo semantics with a
replicated state machine whose command has already committed.

## Rejected alternative

Steal/no-force pages with undo, compensation records, and fuzzy checkpoints can
be correct but add a second transaction-outcome protocol. Combining pieces of
both designs would make replay and acknowledgement reasoning unsound.

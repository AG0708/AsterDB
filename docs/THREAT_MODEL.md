# Threat Model

The core system assumes trusted operators, trusted binaries, and isolated
private hosts. Its security work focuses on memory/resource safety at accidental
or malformed-input boundaries, not hostile multi-tenant deployment.

## In scope

- malformed, truncated, coalesced, fragmented, and oversized protocol frames;
- invalid SQL and type/constraint errors;
- disk truncation, torn tails, detectable checksum corruption, and I/O errors;
- slow/disconnected clients and peers;
- crash/restart, stale leaders, partitions, message duplication/reordering, and
  minority isolation;
- bounded memory, connection, queue, frame, result, and request-time behavior.

## Out of scope

- compromised or Byzantine peers;
- credential theft (the core release has no authentication);
- traffic confidentiality or integrity on an untrusted network;
- denial of service by an attacker with network access;
- malicious filesystem/kernel/hardware behavior after successful flush;
- SQL access control, tenant isolation, or row-level policies;
- side-channel resistance and cryptographic key management.

Consequently, the server binds to loopback by default and should not be exposed
to the public internet.

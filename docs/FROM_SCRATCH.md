# From-Scratch Boundary

"From scratch" describes the database algorithms and formats, not the absence
of libraries or development tools.

Implemented in this repository:

- page, superblock, heap-record, ordered-key, WAL, and protocol formats;
- buffer-pool and B+ tree algorithms;
- recovery/checkpoint state transitions;
- SQL lexing, parsing, binding, planning, and execution for the supported subset;
- MVCC visibility/conflict/idempotency rules;
- Raft consensus state machine and deterministic simulator;
- TCP database protocol, CLI, fault harnesses, and the built-in history model.

Third-party crates provide general-purpose building blocks such as async I/O,
serialization of bounded protocol payloads, checksums/hashes, error derivation,
logging, CLI parsing, and test temporary directories. They do not provide a
storage engine, SQL parser, MVCC implementation, consensus library, or database
server. Porcupine is deliberately used as a separately versioned external
linearizability oracle for exported test histories; it is not linked into the
database or used to implement its guarantees.

Rust, Cargo, the operating system, SQLite differential oracle, and external
verification tools are toolchain/reference dependencies. Their use is reported
in evidence metadata.

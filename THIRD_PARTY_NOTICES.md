# Third-party notices

AsterDB's database, storage, SQL, MVCC, Raft, runtime, and protocol algorithms
are implemented in this repository. General-purpose libraries and verification
tools remain the work of their respective authors.

## Rust dependencies

The exact dependency graph and versions are fixed by `Cargo.lock`. Direct
third-party crates are `anyhow`, `async-trait`, `bytes`, `clap`, `crc32fast`,
`parking_lot`, `rand`, `serde`, `serde_json`, `sha2`, `tempfile`, `thiserror`,
`tokio`, `tracing`, and `tracing-subscriber`. Their package metadata declares
MIT, Apache-2.0, or dual MIT/Apache-2.0 terms. Transitive packages and their
declared SPDX expressions can be reproduced in an SPDX 2.3 inventory with:

```sh
make sbom
```

No third-party Rust storage engine, SQL parser/planner, MVCC engine, or Raft
implementation is used.

## Porcupine v1.0.3

The host-only consistency oracle under `tools/porcupine-check` uses
[Porcupine](https://github.com/anishathalye/porcupine), copyright Anish Athalye,
under the MIT License. Porcupine is not linked into AsterDB binaries.

> Permission is hereby granted, free of charge, to any person obtaining a copy
> of this software and associated documentation files (the "Software"), to deal
> in the Software without restriction, including without limitation the rights
> to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
> copies of the Software, and to permit persons to whom the Software is
> furnished to do so, subject to the following conditions: The above copyright
> notice and this permission notice shall be included in all copies or
> substantial portions of the Software. THE SOFTWARE IS PROVIDED "AS IS",
> WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
> TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
> NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
> FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
> TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
> THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## TLA+ tools v1.8.0

`tools/check_tla.sh` downloads the checksum-pinned `tla2tools.jar` release from
the [TLA+ project](https://github.com/tlaplus/tlaplus). It is a host-only model
checker and is not redistributed in this repository. TLA+ tools are available
under the MIT License, copyright HP Corporation, Microsoft Corporation, and the
Linux Foundation.

## Reference oracles

The SQL differential harness invokes the host's SQLite installation as an
independent behavioral oracle. No SQLite source or binary is redistributed.

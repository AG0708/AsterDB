# Benchmark contract

The bundled benchmark measures AsterDB's default durable standalone server
through its public TCP protocol. It uses persistent client connections, a
seeded read/write mix, disjoint inserted keys, and a final SQL row-count check.
It records every measured latency rather than only a best run.

Run:

```sh
make benchmark
```

Results are written below `benchmarks/results/` and are intentionally ignored
by Git. Release results belong in the content-addressed release evidence pack,
where they are tied to a clean source commit and binary hash. A result must
retain the machine, OS, CPU, concurrency, warmup, workload, durability mode,
raw samples, percentile method, and command before it can support a public
performance statement.

This harness is not a PostgreSQL/SQLite comparison and does not measure the
three-node replicated path. It includes Python client overhead and makes no
performance-superiority claim.

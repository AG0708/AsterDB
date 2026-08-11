#!/usr/bin/env python3
"""Seeded black-box SQL differential test against SQLite.

The harness uses the shipped AsterDB server and CLI binaries. It deliberately
does not import AsterDB implementation code, so parser, wire protocol, server,
durability, and result encoding remain inside the system under test.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "debug" / "aster-server"
CLI = ROOT / "target" / "debug" / "aster-cli"


class DifferentialFailure(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="compare AsterDB's supported SQL subset with SQLite"
    )
    parser.add_argument("--seeds", default="0,1,2", help="comma-separated integer seeds")
    parser.add_argument("--operations", type=int, default=80, help="mutations per seed")
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=ROOT / "artifacts" / "differential",
    )
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if args.operations < 1:
        parser.error("--operations must be positive")
    try:
        args.seeds = [int(value, 0) for value in args.seeds.split(",") if value]
    except ValueError as error:
        parser.error(f"invalid seed: {error}")
    if not args.seeds:
        parser.error("at least one seed is required")
    return args


def build_binaries() -> None:
    subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "-p",
            "aster-server",
            "-p",
            "aster-cli",
        ],
        cwd=ROOT,
        check=True,
    )


def reserve_address() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    return f"127.0.0.1:{port}"


class AsterProcess:
    def __init__(self, data_dir: Path, log_path: Path):
        self.data_dir = data_dir
        self.log_path = log_path
        self.address = reserve_address()
        self.process: subprocess.Popen[bytes] | None = None
        self.log_file: Any = None

    def start(self) -> None:
        self.log_file = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [
                str(SERVER),
                "--listen",
                self.address,
                "--data-dir",
                str(self.data_dir),
            ],
            cwd=ROOT,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise DifferentialFailure(
                    f"server exited during startup with {self.process.returncode}"
                )
            ping = subprocess.run(
                [str(CLI), "--address", self.address, "ping"],
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if ping.returncode == 0:
                return
            time.sleep(0.025)
        raise DifferentialFailure("server did not accept connections within 10 seconds")

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log_file is not None:
            self.log_file.close()
        self.process = None
        self.log_file = None

    def restart(self) -> None:
        self.stop()
        self.address = reserve_address()
        self.start()

    def sql(self, statement: str, expect_success: bool = True) -> dict[str, Any]:
        completed = subprocess.run(
            [str(CLI), "--address", self.address, "sql", statement],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if expect_success and completed.returncode != 0:
            raise DifferentialFailure(
                f"Aster rejected `{statement}`: {completed.stderr.strip()}"
            )
        if not expect_success:
            if completed.returncode == 0:
                raise DifferentialFailure(f"Aster unexpectedly accepted `{statement}`")
            return {"error": completed.stderr.strip()}
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise DifferentialFailure(
                f"Aster emitted invalid JSON for `{statement}`: {error}"
            ) from error
        if "Query" not in payload:
            raise DifferentialFailure(f"Aster returned a non-query response: {payload}")
        return payload["Query"]

    def __enter__(self) -> "AsterProcess":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


def decode_value(value: Any) -> Any:
    if value == "Null":
        return None
    if not isinstance(value, dict) or len(value) != 1:
        raise DifferentialFailure(f"unexpected Aster value encoding: {value!r}")
    kind, payload = next(iter(value.items()))
    if kind in {"Int64", "Bool", "Text"}:
        return payload
    if kind == "Bytes":
        return bytes(payload)
    raise DifferentialFailure(f"unknown Aster value kind: {kind}")


def aster_rows(process: AsterProcess, sql: str) -> list[tuple[Any, ...]]:
    result = process.sql(sql)
    return [tuple(decode_value(value) for value in row) for row in result["rows"]]


def sqlite_rows(connection: sqlite3.Connection, sql: str) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in connection.execute(sql).fetchall()]


def compare(
    process: AsterProcess,
    connection: sqlite3.Connection,
    aster_sql: str,
    sqlite_sql: str,
    boolean_columns: Iterable[int] = (),
) -> None:
    actual = aster_rows(process, aster_sql)
    expected = sqlite_rows(connection, sqlite_sql)
    boolean_columns = tuple(boolean_columns)
    normalized = []
    for row in expected:
        values = list(row)
        for column in boolean_columns:
            values[column] = bool(values[column])
        normalized.append(tuple(values))
    if actual != normalized:
        raise DifferentialFailure(
            "query mismatch\n"
            f"Aster SQL: {aster_sql}\n"
            f"SQLite SQL: {sqlite_sql}\n"
            f"Aster: {actual!r}\n"
            f"SQLite: {normalized!r}"
        )


def compare_database(process: AsterProcess, connection: sqlite3.Connection) -> None:
    compare(
        process,
        connection,
        "SELECT id, value, label, active FROM items ORDER BY id",
        "SELECT id, value, label, active FROM items ORDER BY id",
        boolean_columns=(3,),
    )
    compare(
        process,
        connection,
        "SELECT id, value FROM items WHERE active = true AND value >= 0 "
        "ORDER BY value DESC, id LIMIT 10",
        "SELECT id, value FROM items WHERE active = 1 AND value >= 0 "
        "ORDER BY value DESC, id LIMIT 10",
    )
    compare(
        process,
        connection,
        "SELECT active, COUNT(*) AS row_count, SUM(value) AS total FROM items "
        "GROUP BY active ORDER BY active",
        "SELECT active, COUNT(*) AS row_count, SUM(value) AS total FROM items "
        "GROUP BY active ORDER BY active",
        boolean_columns=(0,),
    )


def run_seed(seed: int, operations: int, run_dir: Path) -> dict[str, Any]:
    seed_dir = run_dir / f"seed-{seed}"
    seed_dir.mkdir(parents=True)
    trace: list[dict[str, Any]] = []
    rng = random.Random(seed)
    sqlite_db = sqlite3.connect(":memory:")
    schema = (
        "CREATE TABLE items (id INT64 PRIMARY KEY, value INT64 NOT NULL, "
        "label TEXT, active BOOL NOT NULL)"
    )
    sqlite_db.execute(schema)
    rows: dict[int, tuple[int, str, bool]] = {}
    process = AsterProcess(seed_dir / "aster-data", seed_dir / "server.log")
    try:
        process.start()
        process.sql(schema)
        trace.append({"step": -1, "sql": schema})
        next_id = 1
        for step in range(operations):
            choice = rng.randrange(100)
            if not rows or choice < 45:
                row_id = next_id
                next_id += 1
                value = rng.randint(-500, 500)
                label = f"label_{rng.randrange(64)}"
                active = bool(rng.randrange(2))
                sql = (
                    f"INSERT INTO items VALUES ({row_id}, {value}, '{label}', "
                    f"{'true' if active else 'false'})"
                )
                rows[row_id] = (value, label, active)
            elif choice < 78:
                row_id = rng.choice(sorted(rows))
                value = rng.randint(-500, 500)
                label = f"label_{rng.randrange(64)}"
                active = bool(rng.randrange(2))
                sql = (
                    f"UPDATE items SET value = {value}, label = '{label}', "
                    f"active = {'true' if active else 'false'} WHERE id = {row_id}"
                )
                rows[row_id] = (value, label, active)
            else:
                row_id = rng.choice(sorted(rows))
                sql = f"DELETE FROM items WHERE id = {row_id}"
                del rows[row_id]

            process.sql(sql)
            sqlite_db.execute(sql)
            sqlite_db.commit()
            trace.append({"step": step, "sql": sql})
            if step % 10 == 0:
                compare_database(process, sqlite_db)
            if step == operations // 2:
                process.restart()
                trace.append({"step": step, "event": "server_restart"})
                compare_database(process, sqlite_db)

        compare_database(process, sqlite_db)
        if rows:
            row_id = min(rows)
            value, label, active = rows[row_id]
            duplicate = (
                f"INSERT INTO items VALUES ({row_id}, {value}, '{label}', "
                f"{'true' if active else 'false'})"
            )
            process.sql(duplicate, expect_success=False)
            try:
                sqlite_db.execute(duplicate)
            except sqlite3.IntegrityError:
                pass
            else:
                raise DifferentialFailure("SQLite unexpectedly accepted duplicate primary key")
            trace.append({"step": operations, "sql": duplicate, "expected": "constraint"})
    except Exception as error:
        (seed_dir / "counterexample.json").write_text(
            json.dumps(
                {"seed": seed, "operations": operations, "trace": trace, "error": str(error)},
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        raise
    finally:
        process.stop()
        sqlite_db.close()

    summary = {
        "seed": seed,
        "operations": operations,
        "trace_entries": len(trace),
        "final_rows": len(rows),
        "status": "pass",
    }
    (seed_dir / "trace.json").write_text(
        json.dumps({"summary": summary, "trace": trace}, indent=2, sort_keys=True) + "\n"
    )
    return summary


def main() -> int:
    args = parse_args()
    if not args.skip_build:
        build_binaries()
    if not SERVER.is_file() or not CLI.is_file():
        raise DifferentialFailure("AsterDB binaries are missing; run without --skip-build")
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_dir = args.artifacts_dir / f"run-{timestamp}-{int(time.time_ns() % 1_000_000):06d}"
    run_dir.mkdir(parents=True)
    summaries = []
    started = time.monotonic()
    try:
        for seed in args.seeds:
            summary = run_seed(seed, args.operations, run_dir)
            summaries.append(summary)
            print(
                f"PASS seed={seed} operations={args.operations} "
                f"final_rows={summary['final_rows']}"
            )
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        print(f"artifacts: {run_dir}", file=sys.stderr)
        return 1
    aggregate = {
        "status": "pass",
        "seeds": args.seeds,
        "operations_per_seed": args.operations,
        "total_operations": len(args.seeds) * args.operations,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "runs": summaries,
    }
    (run_dir / "summary.json").write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n"
    )
    print(f"PASS total_operations={aggregate['total_operations']}")
    print(f"artifacts: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

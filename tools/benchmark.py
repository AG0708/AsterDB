#!/usr/bin/env python3
"""Benchmark AsterDB's durable standalone TCP path without competitor claims."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import pathlib
import platform
import random
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import zlib
from typing import Any, Dict, Iterable, List, Optional


ROOT = pathlib.Path(__file__).resolve().parents[1]
SERVER = ROOT / "target/release/aster-server"
MAX_FRAME_BYTES = 8 * 1024 * 1024
PROTOCOL_VERSION = 1
HEADER = struct.Struct(">4sHBBII")


class BenchmarkError(RuntimeError):
    pass


def read_exact(stream: socket.socket, length: int) -> bytes:
    output = bytearray()
    while len(output) < length:
        chunk = stream.recv(length - len(output))
        if not chunk:
            raise BenchmarkError("server closed mid-frame")
        output.extend(chunk)
    return bytes(output)


def encode_request(payload: Dict[str, Any]) -> bytes:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(body) > MAX_FRAME_BYTES:
        raise BenchmarkError("request exceeds protocol frame limit")
    return HEADER.pack(
        b"ASDB",
        PROTOCOL_VERSION,
        1,
        0,
        len(body),
        zlib.crc32(body) & 0xFFFFFFFF,
    ) + body


def decode_response(stream: socket.socket) -> Dict[str, Any]:
    magic, version, kind, flags, length, checksum = HEADER.unpack(
        read_exact(stream, HEADER.size)
    )
    if magic != b"ASDB" or version != PROTOCOL_VERSION or kind != 2 or flags != 0:
        raise BenchmarkError("server returned an invalid protocol header")
    if length > MAX_FRAME_BYTES:
        raise BenchmarkError("server response exceeds protocol frame limit")
    body = read_exact(stream, length)
    if zlib.crc32(body) & 0xFFFFFFFF != checksum:
        raise BenchmarkError("server response checksum mismatch")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise BenchmarkError("server response is not an object")
    return value


def session(client_id: bytes, sequence: int) -> Dict[str, Any]:
    return {
        "client_id": list(client_id),
        "sequence": sequence,
        "transaction_id": None,
    }


def execute_operation(
    sql: str, parameters: Iterable[Dict[str, Any]]
) -> Dict[str, Any]:
    return {
        "Execute": {
            "sql": sql,
            "parameters": list(parameters),
            "consistency": "Linearizable",
        }
    }


class WireClient:
    def __init__(self, address: str, timeout: float = 10.0):
        host, port_text = address.rsplit(":", 1)
        self.stream = socket.create_connection((host, int(port_text)), timeout=timeout)
        self.stream.settimeout(timeout)
        self.request_id = 1

    def close(self) -> None:
        self.stream.close()

    def request(
        self, operation: Any, request_session: Optional[Dict[str, Any]] = None
    ) -> Any:
        request_id = self.request_id
        self.request_id += 1
        frame = encode_request(
            {
                "request_id": request_id,
                "session": request_session,
                "operation": operation,
            }
        )
        self.stream.sendall(frame)
        response = decode_response(self.stream)
        if response.get("request_id") != request_id:
            raise BenchmarkError("response request id mismatch")
        result = response.get("result")
        if isinstance(result, dict) and "Error" in result:
            error = result["Error"]
            raise BenchmarkError(
                "server error {}: {}".format(error.get("code"), error.get("message"))
            )
        return result

    def execute(
        self,
        sql: str,
        parameters: Iterable[Dict[str, Any]],
        client_id: bytes,
        sequence: int,
    ) -> Any:
        return self.request(
            execute_operation(sql, parameters), session(client_id, sequence)
        )


def reserve_address() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return "127.0.0.1:{}".format(listener.getsockname()[1])


class ServerProcess:
    def __init__(self, address: str, data_dir: pathlib.Path, log_path: pathlib.Path):
        self.address = address
        self.data_dir = data_dir
        self.log_path = log_path
        self.process: Optional[subprocess.Popen[bytes]] = None
        self.log: Any = None

    def start(self) -> None:
        self.log = self.log_path.open("wb")
        self.process = subprocess.Popen(
            [
                str(SERVER),
                "--listen",
                self.address,
                "--data-dir",
                str(self.data_dir),
            ],
            cwd=str(ROOT),
            stdin=subprocess.DEVNULL,
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise BenchmarkError(
                    "server exited during startup with {}".format(self.process.returncode)
                )
            try:
                client = WireClient(self.address, timeout=0.5)
                try:
                    if client.request("Ping") == "Pong":
                        return
                finally:
                    client.close()
            except (OSError, BenchmarkError):
                time.sleep(0.025)
        raise BenchmarkError("server did not accept connections within 15 seconds")

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log is not None:
            self.log.close()
        self.process = None
        self.log = None


def stable_client_id(label: str) -> bytes:
    value = hashlib.sha256(label.encode("utf-8")).digest()[:16]
    return value if value != bytes(16) else bytes([1]) + bytes(15)


def setup_schema(address: str) -> None:
    client = WireClient(address)
    client_id = stable_client_id("asterdb-benchmark-setup")
    try:
        result = client.execute(
            "CREATE TABLE bench (id INT64 PRIMARY KEY, value TEXT NOT NULL)",
            [],
            client_id,
            1,
        )
        if not isinstance(result, dict) or "Query" not in result:
            raise BenchmarkError("benchmark schema creation returned an unexpected result")
        result = client.execute(
            "INSERT INTO bench VALUES (?, ?)",
            [{"Int64": 0}, {"Text": "fixture"}],
            client_id,
            2,
        )
        if not isinstance(result, dict) or "Query" not in result:
            raise BenchmarkError("benchmark fixture insert returned an unexpected result")
    finally:
        client.close()


def run_worker(
    address: str,
    worker: int,
    seed: int,
    warmup: int,
    operations: int,
    write_ratio: float,
) -> Dict[str, Any]:
    client = WireClient(address, timeout=30)
    client_id = stable_client_id("asterdb-benchmark-worker-{}".format(worker))
    rng = random.Random(seed ^ (worker * 0x9E3779B1))
    sequence = 0
    samples: List[Dict[str, Any]] = []
    writes = 0
    try:
        for step in range(warmup + operations):
            is_write = rng.random() < write_ratio
            measured = step >= warmup
            if is_write:
                sequence += 1
                writes += 1
                row_id = 1 + worker * (warmup + operations + 1) + step
                sql = "INSERT INTO bench VALUES (?, ?)"
                parameters = [
                    {"Int64": row_id},
                    {"Text": "worker-{}-step-{}".format(worker, step)},
                ]
                operation_kind = "write"
                request_sequence = sequence
            else:
                sql = "SELECT value FROM bench WHERE id = ?"
                parameters = [{"Int64": 0}]
                operation_kind = "read"
                request_sequence = max(sequence, 1)
            started = time.perf_counter_ns()
            result = client.execute(
                sql, parameters, client_id, request_sequence
            )
            elapsed = time.perf_counter_ns() - started
            if not isinstance(result, dict) or "Query" not in result:
                raise BenchmarkError("worker received an unexpected query result")
            if measured:
                samples.append(
                    {
                        "worker": worker,
                        "kind": operation_kind,
                        "latency_ns": elapsed,
                    }
                )
    finally:
        client.close()
    return {"samples": samples, "writes": writes}


def verify_row_count(address: str, expected: int) -> None:
    client = WireClient(address)
    try:
        result = client.execute(
            "SELECT COUNT(*) FROM bench",
            [],
            stable_client_id("asterdb-benchmark-verifier"),
            1,
        )
    finally:
        client.close()
    try:
        actual = result["Query"]["rows"][0][0]["Int64"]
    except (KeyError, IndexError, TypeError) as error:
        raise BenchmarkError("row-count verification returned an invalid result") from error
    if actual != expected:
        raise BenchmarkError(
            "row-count verification expected {}, observed {}".format(expected, actual)
        )


def percentile(values: List[int], quantile: float) -> Optional[int]:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[rank - 1]


def distribution(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    values = [int(sample["latency_ns"]) for sample in samples]
    return {
        "operations": len(values),
        "min_ns": min(values) if values else None,
        "p50_ns": percentile(values, 0.50),
        "p95_ns": percentile(values, 0.95),
        "p99_ns": percentile(values, 0.99),
        "max_ns": max(values) if values else None,
        "mean_ns": round(sum(values) / len(values), 1) if values else None,
    }


def git_commit() -> Optional[str]:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def cpu_description() -> str:
    if platform.system() == "Darwin":
        completed = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
        if completed.returncode == 0 and completed.stdout.strip():
            return completed.stdout.strip()
    return platform.processor() or "unknown"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--warmup-operations", type=int, default=20)
    parser.add_argument("--operations-per-client", type=int, default=100)
    parser.add_argument("--write-ratio", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=20260811)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path)
    arguments = parser.parse_args()
    if arguments.clients < 1 or arguments.clients > 32:
        parser.error("--clients must be between 1 and 32")
    if arguments.warmup_operations < 0:
        parser.error("--warmup-operations must be non-negative")
    if arguments.operations_per_client < 1:
        parser.error("--operations-per-client must be positive")
    if arguments.write_ratio < 0.0 or arguments.write_ratio > 1.0:
        parser.error("--write-ratio must be between zero and one")
    return arguments


def main() -> int:
    arguments = parse_args()
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output = (
        arguments.output
        if arguments.output is not None
        else ROOT / "benchmarks/results" / ("standalone-{}.json".format(timestamp))
    ).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    server_log = output.with_suffix(".server.log")
    if not arguments.skip_build:
        subprocess.run(
            ["cargo", "build", "--release", "--locked", "-p", "aster-server"],
            cwd=str(ROOT),
            check=True,
        )
    if not SERVER.is_file():
        raise BenchmarkError("release server binary does not exist")

    address = reserve_address()
    with tempfile.TemporaryDirectory(prefix="asterdb-benchmark-") as temporary:
        server = ServerProcess(address, pathlib.Path(temporary) / "data", server_log)
        try:
            server.start()
            setup_schema(address)
            started = time.perf_counter_ns()
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=arguments.clients
            ) as pool:
                futures = [
                    pool.submit(
                        run_worker,
                        address,
                        worker,
                        arguments.seed,
                        arguments.warmup_operations,
                        arguments.operations_per_client,
                        arguments.write_ratio,
                    )
                    for worker in range(arguments.clients)
                ]
                worker_results = [future.result() for future in futures]
                samples = [
                    sample
                    for result in worker_results
                    for sample in result["samples"]
                ]
                expected_rows = 1 + sum(
                    int(result["writes"]) for result in worker_results
                )
            duration_ns = time.perf_counter_ns() - started
            verify_row_count(address, expected_rows)
        finally:
            server.stop()

    reads = [sample for sample in samples if sample["kind"] == "read"]
    writes = [sample for sample in samples if sample["kind"] == "write"]
    report = {
        "schema": "asterdb.standalone-benchmark.v1",
        "source": {
            "git_commit": git_commit(),
            "server_sha256": hashlib.sha256(SERVER.read_bytes()).hexdigest(),
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "cpu": cpu_description(),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
        },
        "workload": {
            "mode": "standalone durable public TCP protocol",
            "connections": arguments.clients,
            "warmup_operations_per_client": arguments.warmup_operations,
            "measured_operations_per_client": arguments.operations_per_client,
            "requested_write_ratio": arguments.write_ratio,
            "seed": arguments.seed,
            "batching": False,
            "dataset": "one fixture row plus unique inserted rows",
            "durability": "default full-page WAL acknowledgement path",
            "client": "persistent Python TCP connections on the server host",
            "verified_final_rows": expected_rows,
        },
        "results": {
            "duration_ns": duration_ns,
            "throughput_ops_per_second": round(
                len(samples) * 1_000_000_000 / duration_ns, 3
            ),
            "all": distribution(samples),
            "reads": distribution(reads),
            "writes": distribution(writes),
        },
        "raw_samples": samples,
        "limitations": [
            "This is a local standalone result, not a replicated-cluster result.",
            "The client and server share one machine; Python client overhead is included.",
            "No comparison or superiority claim is made against another database.",
        ],
    }
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_hash = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix + ".sha256").write_text(
        "{}  {}\n".format(report_hash, output.name), encoding="ascii"
    )
    print(output)
    print(
        "ops={} throughput={} p50_ns={} p99_ns={}".format(
            len(samples),
            report["results"]["throughput_ops_per_second"],
            report["results"]["all"]["p50_ns"],
            report["results"]["all"]["p99_ns"],
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

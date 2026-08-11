#!/usr/bin/env python3
"""Generate and independently check concurrent AsterDB register histories.

The harness drives the public ``aster-server`` and ``aster-cli`` binaries. It
uses three OS processes, retries writes with stable client identities across a
leader kill, records invocation/return intervals, and passes the resulting
history to the separately versioned Porcupine checker under
``tools/porcupine-check``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "target" / "debug" / "aster-server"
CLI = ROOT / "target" / "debug" / "aster-cli"
CHECKER = ROOT / "tools" / "porcupine-check"


class HistoryFailure(RuntimeError):
    pass


class LogicalClock:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value


def reserve_addresses(count: int) -> list[str]:
    listeners: list[socket.socket] = []
    try:
        for _ in range(count):
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
        return [f"127.0.0.1:{listener.getsockname()[1]}" for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def run_cli(
    address: str,
    command: list[str],
    *,
    client_id: str | None = None,
    sequence: int = 1,
    timeout_seconds: float = 3,
) -> dict[str, Any]:
    arguments = [str(CLI), "--address", address]
    if client_id is not None:
        arguments.extend(["--client-id", client_id, "--sequence", str(sequence)])
    arguments.extend(command)
    completed = subprocess.run(
        arguments,
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        raise HistoryFailure(completed.stderr.strip() or "CLI request failed")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise HistoryFailure(f"CLI emitted invalid JSON: {error}") from error


class ClusterNode:
    def __init__(
        self,
        node_id: int,
        client_address: str,
        peer_addresses: list[str],
        directory: Path,
    ) -> None:
        self.node_id = node_id
        self.client_address = client_address
        self.peer_addresses = peer_addresses
        self.directory = directory
        self.process: subprocess.Popen[bytes] | None = None
        self.log_file: Any = None

    def start(self) -> None:
        if self.process is not None and self.process.poll() is None:
            raise HistoryFailure(f"node {self.node_id} is already running")
        self.directory.mkdir(parents=True, exist_ok=True)
        self.log_file = (self.directory / "server.log").open("ab")
        arguments = [
            str(SERVER),
            "--node-id",
            str(self.node_id),
            "--listen",
            self.client_address,
            "--data-dir",
            str(self.directory / "data"),
        ]
        for offset, address in enumerate(self.peer_addresses, start=1):
            arguments.extend(["--peer", f"{offset}={address}"])
        self.process = subprocess.Popen(
            arguments,
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise HistoryFailure(
                    f"node {self.node_id} exited during startup with {self.process.returncode}"
                )
            try:
                run_cli(self.client_address, ["ping"], timeout_seconds=0.5)
                return
            except (HistoryFailure, subprocess.TimeoutExpired, OSError):
                time.sleep(0.025)
        raise HistoryFailure(f"node {self.node_id} did not accept clients")

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def crash(self) -> None:
        if self.process is None or self.process.poll() is not None:
            raise HistoryFailure(f"node {self.node_id} is not running")
        self.process.kill()
        self.process.wait(timeout=5)
        self._close_log()

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self._close_log()

    def _close_log(self) -> None:
        if self.log_file is not None:
            self.log_file.close()
        self.log_file = None
        self.process = None


def node_status(node: ClusterNode) -> dict[str, Any] | None:
    if not node.alive():
        return None
    try:
        payload = run_cli(node.client_address, ["status"], timeout_seconds=0.75)
    except (HistoryFailure, subprocess.TimeoutExpired, OSError):
        return None
    status = payload.get("Status")
    return status if isinstance(status, dict) else None


def find_leader(nodes: dict[int, ClusterNode], timeout_seconds: float = 10) -> ClusterNode:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        leaders = [
            node
            for node in list(nodes.values())
            if (node_status(node) or {}).get("role") == "leader"
        ]
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(0.025)
    raise HistoryFailure("cluster did not expose exactly one leader")


def query_value(payload: dict[str, Any]) -> int:
    query = payload.get("Query")
    if not isinstance(query, dict) or len(query.get("rows", [])) != 1:
        raise HistoryFailure(f"unexpected read response: {payload}")
    encoded = query["rows"][0][0]
    if not isinstance(encoded, dict) or "Int64" not in encoded:
        raise HistoryFailure(f"unexpected register value: {encoded}")
    return int(encoded["Int64"])


def mutation_index(payload: dict[str, Any]) -> int:
    query = payload.get("Query")
    if not isinstance(query, dict) or query.get("affected_rows") != 1:
        raise HistoryFailure(f"unexpected mutation response: {payload}")
    return int(query["applied_index"])


def execute_with_retry(
    nodes: dict[int, ClusterNode],
    sql: str,
    client_id: str,
    sequence: int,
    deadline_seconds: float = 20,
) -> dict[str, Any]:
    deadline = time.monotonic() + deadline_seconds
    last_error = "no leader"
    while time.monotonic() < deadline:
        try:
            leader = find_leader(nodes, timeout_seconds=1)
            return run_cli(
                leader.client_address,
                ["sql", sql],
                client_id=client_id,
                sequence=sequence,
                timeout_seconds=2,
            )
        except (HistoryFailure, subprocess.TimeoutExpired, OSError) as error:
            last_error = str(error)
            time.sleep(0.015)
    raise HistoryFailure(f"operation did not complete after retries: {last_error}")


def execute_mutation_with_retry(
    nodes: dict[int, ClusterNode],
    sql: str,
    client_id: str,
    initial_sequence: int,
    deadline_seconds: float = 20,
) -> tuple[dict[str, Any], int, int]:
    """Retry one logical write while preserving unambiguous request identity.

    Leadership errors, disconnects, and timeouts reuse the same sequence because
    the prior attempt may have committed. A returned write-conflict is a known,
    durable abort, so the client advances its sequence before trying the same
    logical register write again.
    """

    deadline = time.monotonic() + deadline_seconds
    sequence = initial_sequence
    conflict_retries = 0
    last_error = "no leader"
    while time.monotonic() < deadline:
        try:
            leader = find_leader(nodes, timeout_seconds=1)
            payload = run_cli(
                leader.client_address,
                ["sql", sql],
                client_id=client_id,
                sequence=sequence,
                timeout_seconds=2,
            )
            return payload, sequence, conflict_retries
        except HistoryFailure as error:
            last_error = str(error)
            if "transaction aborted: WriteConflict" in last_error:
                sequence += 1
                conflict_retries += 1
            time.sleep(0.015)
        except (subprocess.TimeoutExpired, OSError) as error:
            last_error = str(error)
            time.sleep(0.015)
    raise HistoryFailure(f"mutation did not complete after retries: {last_error}")


def stable_client_id(seed: int, client: int) -> str:
    value = ((seed & ((1 << 64) - 1)) << 64) | (client + 1)
    if value == 0:
        value = 1
    return f"{value:032x}"


def run_seed(
    seed: int,
    clients: int,
    operations_per_client: int,
    seed_directory: Path,
) -> dict[str, Any]:
    addresses = reserve_addresses(6)
    client_addresses = addresses[:3]
    peer_addresses = addresses[3:]
    nodes = {
        node_id: ClusterNode(
            node_id,
            client_addresses[node_id - 1],
            peer_addresses,
            seed_directory / f"node-{node_id}",
        )
        for node_id in range(1, 4)
    }
    clock = LogicalClock()
    events: list[dict[str, Any]] = []
    events_lock = threading.Lock()
    start_barrier = threading.Barrier(clients + 1)
    rng = random.Random(seed)
    schedules = []
    for client in range(clients):
        schedule = []
        for operation in range(operations_per_client):
            if operation % 2 == 0:
                value = seed * 100_000 + client * 1_000 + operation + 1
                schedule.append(("write", value))
            else:
                schedule.append(("read", None))
        rng.shuffle(schedule)
        schedules.append(schedule)

    def worker(client: int) -> None:
        client_id = stable_client_id(seed, client)
        next_sequence = 1
        start_barrier.wait()
        for kind, value in schedules[client]:
            call = clock.tick()
            if kind == "write":
                payload, used_sequence, conflict_retries = execute_mutation_with_retry(
                    nodes,
                    f"UPDATE linear_register SET value = {value} WHERE id = 1",
                    client_id,
                    next_sequence,
                )
                next_sequence = used_sequence + 1
                output: int | bool = mutation_index(payload) > 0
            else:
                payload = execute_with_retry(
                    nodes,
                    "SELECT value FROM linear_register WHERE id = 1",
                    client_id,
                    next_sequence,
                )
                used_sequence = None
                conflict_retries = 0
                output = query_value(payload)
            returned = clock.tick()
            with events_lock:
                events.append(
                    {
                        "client": client,
                        "kind": kind,
                        "value": value,
                        "output": output,
                        "request_sequence": used_sequence,
                        "conflict_retries": conflict_retries,
                        "call": call,
                        "return": returned,
                    }
                )
            time.sleep(0.01)

    try:
        for node in nodes.values():
            node.start()
        leader = find_leader(nodes)
        admin = stable_client_id(seed, clients + 1)
        execute_with_retry(
            nodes,
            "CREATE TABLE linear_register (id INT64 PRIMARY KEY, value INT64 NOT NULL)",
            admin,
            1,
        )
        execute_with_retry(
            nodes,
            "INSERT INTO linear_register VALUES (1, 0)",
            admin,
            2,
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as executor:
            futures = [executor.submit(worker, client) for client in range(clients)]
            start_barrier.wait()
            time.sleep(0.05)
            failed_leader = find_leader(nodes)
            failed_leader.crash()
            failure_tick = clock.tick()
            replacement = find_leader(nodes)
            failed_leader.start()
            restart_tick = clock.tick()
            for future in futures:
                future.result(timeout=30)

        final_leader = find_leader(nodes)
        final_payload = execute_with_retry(
            nodes,
            "SELECT value FROM linear_register WHERE id = 1",
            admin,
            99,
        )
        final_value = query_value(final_payload)
        history = {
            "schema": "asterdb.porcupine-register-history.v1",
            "seed": seed,
            "initial": 0,
            "clients": clients,
            "operations_per_client": operations_per_client,
            "failed_leader": failed_leader.node_id,
            "replacement_leader": replacement.node_id,
            "failure_tick": failure_tick,
            "restart_tick": restart_tick,
            "final_leader": final_leader.node_id,
            "final_value": final_value,
            "operations": sorted(events, key=lambda event: event["call"]),
        }
        history_path = seed_directory / "history.json"
        history_path.write_text(json.dumps(history, indent=2, sort_keys=True) + "\n")
        completed = subprocess.run(
            ["go", "run", ".", str(history_path)],
            cwd=CHECKER,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
            check=False,
        )
        (seed_directory / "porcupine.stdout.json").write_text(completed.stdout)
        (seed_directory / "porcupine.stderr.log").write_text(completed.stderr)
        if completed.returncode != 0:
            raise HistoryFailure(
                f"Porcupine rejected seed {seed}: {completed.stdout}{completed.stderr}"
            )
        checker = json.loads(completed.stdout)
        if checker.get("linearizable") is not True:
            raise HistoryFailure(f"Porcupine did not accept seed {seed}: {checker}")
        return {
            "seed": seed,
            "status": "pass",
            "operations": len(events),
            "failed_leader": failed_leader.node_id,
            "replacement_leader": replacement.node_id,
            "checker": checker,
        }
    finally:
        for node in nodes.values():
            node.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", default="1,2,3")
    parser.add_argument("--clients", type=int, default=3)
    parser.add_argument("--operations-per-client", type=int, default=6)
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=ROOT / "artifacts" / "linearizability",
    )
    parser.add_argument("--skip-build", action="store_true")
    arguments = parser.parse_args()
    try:
        arguments.seeds = [
            int(value, 0) for value in arguments.seeds.split(",") if value
        ]
    except ValueError as error:
        parser.error(f"invalid seed: {error}")
    if not arguments.seeds:
        parser.error("at least one seed is required")
    if arguments.clients < 2 or arguments.clients > 8:
        parser.error("--clients must be between 2 and 8")
    if (
        arguments.operations_per_client < 2
        or arguments.operations_per_client > 20
    ):
        parser.error("--operations-per-client must be between 2 and 20")
    return arguments


def main() -> int:
    arguments = parse_args()
    if not arguments.skip_build:
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
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_directory = arguments.artifacts_dir / f"run-{timestamp}-{os.getpid()}"
    run_directory.mkdir(parents=True, exist_ok=False)
    summaries = []
    try:
        for seed in arguments.seeds:
            seed_directory = run_directory / f"seed-{seed}"
            seed_directory.mkdir()
            print(f"RUN  seed={seed}", flush=True)
            summaries.append(
                run_seed(
                    seed,
                    arguments.clients,
                    arguments.operations_per_client,
                    seed_directory,
                )
            )
            print(f"PASS seed={seed}", flush=True)
    except Exception as error:
        summary = {
            "schema": "asterdb.porcupine-run.v1",
            "passed": False,
            "error": str(error),
            "seeds": summaries,
        }
        (run_directory / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n"
        )
        print(f"FAIL {error}", file=sys.stderr)
        print(f"evidence: {run_directory}")
        return 1
    summary = {
        "schema": "asterdb.porcupine-run.v1",
        "passed": True,
        "error": None,
        "seeds": summaries,
    }
    (run_directory / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    print(f"evidence: {run_directory}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

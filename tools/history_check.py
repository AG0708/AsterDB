#!/usr/bin/env python3
"""Bounded correctness checkers for AsterDB test histories.

The linearizability checker uses a Wing-Gong-style backtracking search over
operation intervals. It is intentionally bounded and dependency-free so a
failing cluster trace can be checked and minimized in CI. Multi-statement
transactions use a separate snapshot-isolation oracle; they are never passed
off as linearizable operations.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


class HistoryError(ValueError):
    pass


def _freeze_map(state: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((key, json.dumps(value, sort_keys=True)) for key, value in state.items()))


def _validate_operation(raw: dict[str, Any], position: int) -> None:
    required = {"op", "key", "start", "end"}
    missing = required - raw.keys()
    if missing:
        raise HistoryError(f"operation {position} is missing {sorted(missing)}")
    if raw["op"] not in {"get", "put", "cas"}:
        raise HistoryError(f"operation {position} has unknown op {raw['op']!r}")
    if not isinstance(raw["start"], int) or not isinstance(raw["end"], int):
        raise HistoryError(f"operation {position} interval must use integer ticks")
    if raw["start"] > raw["end"]:
        raise HistoryError(f"operation {position} starts after it ends")
    if raw["op"] == "cas":
        arguments = raw.get("input")
        if not isinstance(arguments, list) or len(arguments) != 2:
            raise HistoryError(f"operation {position} CAS input must be [expected, replacement]")


def _apply(operation: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    next_state = dict(state)
    key = str(operation["key"])
    kind = operation["op"]
    if kind == "get":
        return next_state if operation.get("output") == state.get(key) else None
    if kind == "put":
        if operation.get("output", True) is not True:
            return None
        next_state[key] = operation.get("input")
        return next_state
    expected, replacement = operation["input"]
    succeeded = state.get(key) == expected
    if operation.get("output") is not succeeded:
        return None
    if succeeded:
        next_state[key] = replacement
    return next_state


@dataclass(frozen=True)
class LinearizationResult:
    valid: bool
    order: tuple[int, ...]
    explored_states: int


def check_linearizable(
    operations: Iterable[dict[str, Any]], initial: dict[str, Any] | None = None
) -> LinearizationResult:
    """Return whether completed GET/PUT/CAS operations admit a legal order.

    Real-time precedence is preserved: if operation A completes no later than B
    starts, A must precede B. Failed/indeterminate invocations are excluded by
    the history producer rather than guessed by this checker.
    """

    ops = list(operations)
    if len(ops) > 63:
        raise HistoryError("bounded checker accepts at most 63 operations")
    for position, operation in enumerate(ops):
        _validate_operation(operation, position)
    predecessors = [0] * len(ops)
    for later, right in enumerate(ops):
        for earlier, left in enumerate(ops):
            if earlier != later and left["end"] <= right["start"]:
                predecessors[later] |= 1 << earlier
    complete = (1 << len(ops)) - 1
    memo: set[tuple[int, tuple[tuple[str, str], ...]]] = set()
    explored = 0

    def search(done: int, state: dict[str, Any], order: tuple[int, ...]) -> tuple[int, ...] | None:
        nonlocal explored
        explored += 1
        if done == complete:
            return order
        signature = (done, _freeze_map(state))
        if signature in memo:
            return None
        memo.add(signature)
        for index, operation in enumerate(ops):
            bit = 1 << index
            if done & bit or predecessors[index] & ~done:
                continue
            next_state = _apply(operation, state)
            if next_state is None:
                continue
            result = search(done | bit, next_state, order + (index,))
            if result is not None:
                return result
        return None

    order = search(0, dict(initial or {}), ())
    return LinearizationResult(order is not None, order or (), explored)


@dataclass(frozen=True)
class SnapshotResult:
    valid: bool
    errors: tuple[str, ...]


def check_snapshot_isolation(history: dict[str, Any]) -> SnapshotResult:
    """Check fixed snapshots, atomic visibility, and first-committer-wins.

    Transaction fixtures record each observed read's source commit index. This
    keeps the oracle independent from the database's returned value alone.
    """

    initial = {str(key): value for key, value in history.get("initial", {}).items()}
    transactions = history.get("transactions")
    if not isinstance(transactions, list):
        raise HistoryError("snapshot history requires a transactions list")
    errors: list[str] = []
    committed = [transaction for transaction in transactions if transaction.get("status") == "committed"]
    commit_indexes: set[int] = set()
    by_commit: dict[int, dict[str, Any]] = {}
    for transaction in committed:
        commit_ts = transaction.get("commit_ts")
        if not isinstance(commit_ts, int) or commit_ts <= 0:
            errors.append(f"{transaction.get('id')}: committed transaction lacks positive commit_ts")
            continue
        if commit_ts in commit_indexes:
            errors.append(f"duplicate commit_ts {commit_ts}")
        commit_indexes.add(commit_ts)
        by_commit[commit_ts] = {str(key): value for key, value in transaction.get("writes", {}).items()}

    for transaction in transactions:
        identifier = transaction.get("id", "<unknown>")
        read_ts = transaction.get("read_ts")
        if not isinstance(read_ts, int) or read_ts < 0:
            errors.append(f"{identifier}: invalid read_ts")
            continue
        own_writes = {str(key): value for key, value in transaction.get("writes", {}).items()}
        for read in transaction.get("reads", []):
            key = str(read.get("key"))
            observed = read.get("value")
            source = read.get("version", 0)
            if key in own_writes and read.get("own_write", False):
                if observed != own_writes[key]:
                    errors.append(f"{identifier}: read-your-writes failed for {key}")
                continue
            visible_versions = [index for index in commit_indexes if index <= read_ts and key in by_commit[index]]
            expected_version = max(visible_versions, default=0)
            expected = by_commit[expected_version][key] if expected_version else initial.get(key)
            if source != expected_version or observed != expected:
                errors.append(
                    f"{identifier}: {key} observed value/version {observed!r}/{source}, "
                    f"expected {expected!r}/{expected_version} at read_ts {read_ts}"
                )

    ordered = sorted(
        (transaction for transaction in committed if isinstance(transaction.get("commit_ts"), int)),
        key=lambda transaction: transaction["commit_ts"],
    )
    for later_position, later in enumerate(ordered):
        later_keys = set(map(str, later.get("writes", {})))
        for earlier in ordered[:later_position]:
            earlier_keys = set(map(str, earlier.get("writes", {})))
            if later_keys & earlier_keys and earlier["commit_ts"] > later.get("read_ts", -1):
                errors.append(
                    f"{later.get('id')}: committed despite first-committer-wins conflict "
                    f"with {earlier.get('id')}"
                )
    return SnapshotResult(not errors, tuple(errors))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("linearizable", "snapshot"))
    parser.add_argument("history", type=Path)
    arguments = parser.parse_args()
    history = json.loads(arguments.history.read_text(encoding="utf-8"))
    if arguments.mode == "linearizable":
        result = check_linearizable(history["operations"], history.get("initial"))
        print(json.dumps(result.__dict__, indent=2))
    else:
        result = check_snapshot_isolation(history)
        print(json.dumps(result.__dict__, indent=2))
    return 0 if result.valid else 1


if __name__ == "__main__":
    sys.exit(main())

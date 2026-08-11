#!/usr/bin/env python3
"""Run AsterDB's public release gate and emit content-addressed evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_EPOCH = 1_700_000_000
EXCLUDED_TOP_LEVEL = {".git", "artifacts", "dist", "target"}
EXCLUDED_PARTS = {"__pycache__", ".pytest_cache", "states"}


class ReleaseGateError(RuntimeError):
    pass


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_files(root: pathlib.Path) -> Iterable[pathlib.Path]:
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if not relative.parts:
            continue
        if relative.parts[0] in EXCLUDED_TOP_LEVEL:
            continue
        if any(part in EXCLUDED_PARTS for part in relative.parts):
            continue
        if path.suffix in {".pyc", ".pyo"} or path.name == ".DS_Store":
            continue
        yield path


def source_tree(root: pathlib.Path) -> Dict[str, Any]:
    digest = hashlib.sha256()
    count = 0
    total_bytes = 0
    for path in source_files(root):
        relative = path.relative_to(root).as_posix()
        file_digest = sha256_file(path)
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_digest.encode("ascii"))
        digest.update(b"\n")
        count += 1
        total_bytes += path.stat().st_size
    return {"sha256": digest.hexdigest(), "files": count, "bytes": total_bytes}


def git_output(root: pathlib.Path, *arguments: str) -> Optional[str]:
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.rstrip("\n")


def git_state(root: pathlib.Path) -> Dict[str, Any]:
    commit = git_output(root, "rev-parse", "HEAD")
    top_level = git_output(root, "rev-parse", "--show-toplevel")
    status = git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    return {
        "available": commit is not None,
        "commit": commit,
        "top_level_name": pathlib.Path(top_level).name if top_level else None,
        "clean": status == "" if status is not None else None,
        "status_entries": 0 if not status else len(status.splitlines()),
        "status_sha256": hashlib.sha256((status or "").encode("utf-8")).hexdigest(),
    }


def first_version_line(command: Sequence[str], cwd: pathlib.Path) -> Dict[str, Any]:
    executable = shutil.which(command[0])
    if executable is None:
        return {"available": False, "command": command[0], "version": None}
    try:
        completed = subprocess.run(
            list(command),
            cwd=str(cwd),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {
            "available": False,
            "command": command[0],
            "version": None,
            "error": str(error),
        }
    lines = completed.stdout.strip().splitlines()
    return {
        "available": completed.returncode == 0,
        "command": command[0],
        "version": lines[0] if lines else "",
        "returncode": completed.returncode,
    }


Step = Tuple[str, Sequence[str], pathlib.Path, Mapping[str, str]]


def release_rustflags() -> str:
    """Return Cargo-encoded flags that remove builder-specific source paths."""
    mappings = (
        (ROOT, pathlib.PurePosixPath("/usr/src/asterdb")),
        (pathlib.Path.home(), pathlib.PurePosixPath("/usr/src/build-home")),
        (pathlib.Path(tempfile.gettempdir()), pathlib.PurePosixPath("/tmp")),
    )
    flags = [
        "--remap-path-prefix={}={}".format(source.resolve(), destination)
        for source, destination in mappings
    ]
    return "\x1f".join(flags)


def step_definitions(output: pathlib.Path) -> List[Step]:
    differential = output / "differential"
    linearizability = output / "linearizability"
    return [
        ("rust-format", ["cargo", "fmt", "--all", "--", "--check"], ROOT, {}),
        (
            "rust-clippy",
            [
                "cargo",
                "clippy",
                "--workspace",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ],
            ROOT,
            {},
        ),
        (
            "rust-tests",
            ["cargo", "test", "--workspace", "--locked", "--all-targets"],
            ROOT,
            {},
        ),
        (
            "rust-docs",
            ["cargo", "doc", "--workspace", "--no-deps", "--locked"],
            ROOT,
            {"RUSTDOCFLAGS": "-D warnings"},
        ),
        ("dependency-policy", ["cargo", "deny", "check"], ROOT, {}),
        ("security-advisories", ["cargo", "audit", "--deny", "warnings"], ROOT, {}),
        (
            "sql-differential",
            [
                sys.executable,
                "tools/sql_differential.py",
                "--artifacts-dir",
                str(differential),
            ],
            ROOT,
            {},
        ),
        (
            "python-oracles",
            [
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "-s",
                "tools",
                "-p",
                "test_*.py",
                "-v",
            ],
            ROOT,
            {},
        ),
        ("porcupine-wrapper", ["go", "test", "./..."], ROOT / "tools/porcupine-check", {}),
        (
            "external-linearizability",
            [
                sys.executable,
                "tools/cluster_history.py",
                "--artifacts-dir",
                str(linearizability),
            ],
            ROOT,
            {},
        ),
        ("bounded-tla", ["tools/check_tla.sh"], ROOT, {}),
        (
            "release-binaries",
            [
                "cargo",
                "build",
                "--release",
                "--locked",
                "-p",
                "aster-server",
                "-p",
                "aster-cli",
                "-p",
                "aster-runtime",
                "--bins",
            ],
            ROOT,
            {"CARGO_ENCODED_RUSTFLAGS": release_rustflags()},
        ),
        (
            "standalone-benchmark",
            [
                sys.executable,
                "tools/benchmark.py",
                "--skip-build",
                "--output",
                str(output / "standalone-benchmark.json"),
            ],
            ROOT,
            {},
        ),
    ]


def display_command(command: Sequence[str], root: pathlib.Path) -> List[str]:
    displayed = []
    for argument in command:
        value = str(argument)
        value = value.replace(str(root), ".")
        if value == sys.executable:
            value = "python3"
        displayed.append(value)
    return displayed


def sanitize_log(path: pathlib.Path) -> None:
    encoded = path.read_bytes()
    text = encoded.decode("utf-8", errors="replace")
    temporary = pathlib.Path(tempfile.gettempdir())
    replacements = [
        (str(ROOT), "."),
        (str(pathlib.Path.home()), "$HOME"),
        (str(temporary.resolve()), "$TMPDIR"),
        (str(temporary), "$TMPDIR"),
    ]
    for original, replacement in replacements:
        if original:
            text = text.replace(original, replacement)
    path.write_text(text, encoding="utf-8")


def run_step(
    name: str,
    command: Sequence[str],
    cwd: pathlib.Path,
    additions: Mapping[str, str],
    output: pathlib.Path,
) -> Dict[str, Any]:
    log_path = output / "logs" / (name + ".log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_TERM_COLOR": "never",
            "PYTHONHASHSEED": "0",
            "SOURCE_DATE_EPOCH": environment.get(
                "SOURCE_DATE_EPOCH", str(DEFAULT_EPOCH)
            ),
            "TZ": "UTC",
        }
    )
    environment.update(additions)
    if "CARGO_ENCODED_RUSTFLAGS" in additions:
        environment.pop("RUSTFLAGS", None)
    started = time.monotonic()
    with log_path.open("wb") as log:
        completed = subprocess.run(
            list(command),
            cwd=str(cwd),
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    sanitize_log(log_path)
    duration = time.monotonic() - started
    record = {
        "name": name,
        "command": display_command(command, ROOT),
        "cwd": "." if cwd == ROOT else cwd.relative_to(ROOT).as_posix(),
        "returncode": completed.returncode,
        "passed": completed.returncode == 0,
        "duration_seconds": round(duration, 3),
        "log": log_path.relative_to(output).as_posix(),
        "log_bytes": log_path.stat().st_size,
        "log_sha256": sha256_file(log_path),
    }
    print(
        "{} {:>7.2f}s {}".format(
            "PASS" if record["passed"] else "FAIL", duration, name
        ),
        flush=True,
    )
    return record


def generate_sboms(output: pathlib.Path) -> Dict[str, Any]:
    first = output / "asterdb.spdx.json"
    second = output / "asterdb.rebuild.spdx.json"
    environment = os.environ.copy()
    environment["SOURCE_DATE_EPOCH"] = environment.get(
        "SOURCE_DATE_EPOCH", str(DEFAULT_EPOCH)
    )
    for target in (first, second):
        subprocess.run(
            [sys.executable, "tools/generate_sbom.py", "--output", str(target)],
            cwd=str(ROOT),
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )
    first_hash = sha256_file(first)
    second_hash = sha256_file(second)
    if first_hash != second_hash or first.read_bytes() != second.read_bytes():
        raise ReleaseGateError("two independent SPDX generations were not byte-identical")
    second.unlink()
    return {
        "path": first.relative_to(output).as_posix(),
        "sha256": first_hash,
        "bytes": first.stat().st_size,
        "reproducible": True,
    }


def copy_binaries(output: pathlib.Path) -> List[Dict[str, Any]]:
    destination = output / "bin"
    destination.mkdir(parents=True, exist_ok=True)
    records = []
    for name in ("aster-server", "aster-cli", "aster-runtime-node"):
        source = ROOT / "target/release" / name
        if not source.is_file():
            raise ReleaseGateError("release binary is missing: {}".format(name))
        target = destination / name
        shutil.copy2(str(source), str(target))
        binary = target.read_bytes()
        private_paths = {
            str(ROOT.resolve()).encode("utf-8"),
            str(pathlib.Path.home().resolve()).encode("utf-8"),
            str(pathlib.Path(tempfile.gettempdir()).resolve()).encode("utf-8"),
        }
        leaked = [path for path in private_paths if path and path in binary]
        if leaked:
            raise ReleaseGateError(
                "release binary contains builder-specific absolute paths: {}".format(name)
            )
        records.append(
            {
                "path": target.relative_to(output).as_posix(),
                "bytes": target.stat().st_size,
                "sha256": sha256_file(target),
            }
        )
    return records


def artifact_records(output: pathlib.Path) -> List[Dict[str, Any]]:
    records = []
    for path in sorted(output.rglob("*")):
        if not path.is_file() or path.name in {"summary.json", "summary.json.sha256"}:
            continue
        records.append(
            {
                "path": path.relative_to(output).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return records


def write_summary(output: pathlib.Path, summary: Dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    path = output / "summary.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    digest = sha256_file(path)
    (output / "summary.json.sha256").write_text(
        "{}  summary.json\n".format(digest), encoding="ascii"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--require-clean", action="store_true")
    parser.add_argument(
        "--list", action="store_true", help="print the gate without executing it"
    )
    return parser.parse_args()


def main() -> int:
    arguments = parse_args()
    timestamp = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (
        arguments.output
        if arguments.output is not None
        else ROOT / "artifacts/release" / ("run-{}-{}".format(timestamp, os.getpid()))
    ).resolve()
    steps = step_definitions(output)
    if arguments.list:
        for name, command, cwd, _ in steps:
            location = "." if cwd == ROOT else cwd.relative_to(ROOT).as_posix()
            print("{} [{}]: {}".format(name, location, " ".join(display_command(command, ROOT))))
        return 0

    output.mkdir(parents=True, exist_ok=False)
    state = git_state(ROOT)
    if arguments.require_clean and state["clean"] is not True:
        raise ReleaseGateError("--require-clean requested but Git worktree is not clean")
    versions = {
        "cargo": first_version_line(["cargo", "--version"], ROOT),
        "rustc": first_version_line(["rustc", "--version"], ROOT),
        "python": first_version_line([sys.executable, "--version"], ROOT),
        "go": first_version_line(["go", "version"], ROOT),
        "java": first_version_line(["java", "-version"], ROOT),
    }
    summary: Dict[str, Any] = {
        "schema": "asterdb.release-gate.v1",
        "passed": False,
        "source": {"tree": source_tree(ROOT), "git": state},
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "tools": versions,
        "steps": [],
        "sbom": None,
        "binaries": [],
        "artifacts": [],
        "error": None,
    }
    try:
        missing = [name for name, value in versions.items() if not value["available"]]
        if missing:
            raise ReleaseGateError("required tools unavailable: {}".format(missing))
        for name, command, cwd, additions in steps:
            record = run_step(name, command, cwd, additions, output)
            summary["steps"].append(record)
            if not record["passed"]:
                raise ReleaseGateError("release step failed: {}".format(name))
        summary["sbom"] = generate_sboms(output)
        summary["binaries"] = copy_binaries(output)
        summary["artifacts"] = artifact_records(output)
        summary["passed"] = True
    except (OSError, subprocess.SubprocessError, ReleaseGateError) as error:
        summary["error"] = str(error)
        summary["artifacts"] = artifact_records(output)
        write_summary(output, summary)
        print("release gate failed: {}".format(error), file=sys.stderr)
        print("evidence: {}".format(output))
        return 1
    write_summary(output, summary)
    print("evidence: {}".format(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

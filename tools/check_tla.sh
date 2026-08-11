#!/bin/sh
set -eu

TLA_VERSION=v1.8.0
TLA_SHA256=ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"
TLA_CACHE_DIR="${TMPDIR:-/tmp}/asterdb-tla"
TLA_JAR="${ASTERDB_TLA_JAR:-${TLA_CACHE_DIR}/tla2tools-${TLA_VERSION}.jar}"

mkdir -p "${TLA_CACHE_DIR}"
if [ ! -f "${TLA_JAR}" ]; then
    candidate="${TLA_JAR}.download.$$"
    curl --fail --location --silent --show-error --output "${candidate}" "${TLA_URL}"
    python3 - "${candidate}" "${TLA_SHA256}" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
actual = hashlib.sha256(path.read_bytes()).hexdigest()
if actual != expected:
    raise SystemExit(f"TLC checksum mismatch: expected {expected}, got {actual}")
PY
    mv "${candidate}" "${TLA_JAR}"
fi

python3 - "${TLA_JAR}" "${TLA_SHA256}" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
actual = hashlib.sha256(path.read_bytes()).hexdigest()
if actual != expected:
    raise SystemExit(f"cached TLC checksum mismatch: expected {expected}, got {actual}")
PY

repository=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "${repository}/model"
exec java -XX:+UseParallelGC -cp "${TLA_JAR}" tlc2.TLC \
    -cleanup -workers 1 -seed 20260811 -fp 0 \
    -config AsterCommit.cfg AsterCommit.tla

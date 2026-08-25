#!/usr/bin/env python3
"""Generate a deterministic-package SPDX 2.3 inventory for AsterDB releases."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import urllib.parse
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "tools" / "porcupine-check"


def run_json(command: list[str], cwd: Path) -> Any:
    return json.loads(subprocess.check_output(command, cwd=cwd, text=True))


def read_json_stream(command: list[str], cwd: Path) -> list[dict[str, Any]]:
    encoded = subprocess.check_output(command, cwd=cwd, text=True)
    decoder = json.JSONDecoder()
    values = []
    offset = 0
    while offset < len(encoded):
        while offset < len(encoded) and encoded[offset].isspace():
            offset += 1
        if offset == len(encoded):
            break
        value, offset = decoder.raw_decode(encoded, offset)
        values.append(value)
    return values


def spdx_id(kind: str, identity: str) -> str:
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return f"SPDXRef-{kind}-{digest}"


def purl(package_type: str, name: str, version: str) -> str:
    return "pkg:{}/{}@{}".format(
        package_type,
        urllib.parse.quote(name, safe="/"),
        urllib.parse.quote(version, safe=""),
    )


def created_timestamp() -> str:
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    when = (
        dt.datetime.fromtimestamp(int(epoch), tz=dt.timezone.utc)
        if epoch is not None
        else dt.datetime.now(tz=dt.timezone.utc)
    )
    return when.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rust_packages() -> list[dict[str, Any]]:
    metadata = run_json(
        ["cargo", "metadata", "--locked", "--format-version", "1"], ROOT
    )
    packages = []
    for package in metadata["packages"]:
        if package.get("source") is None:
            continue
        identity = package["id"]
        license_expression = package.get("license") or "NOASSERTION"
        if license_expression == "MIT/Apache-2.0":
            license_expression = "MIT OR Apache-2.0"
        packages.append(
            {
                "SPDXID": spdx_id("Cargo", identity),
                "name": package["name"],
                "versionInfo": package["version"],
                "downloadLocation": package.get("repository") or "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": license_expression,
                "copyrightText": "NOASSERTION",
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": purl(
                            "cargo", package["name"], package["version"]
                        ),
                    }
                ],
            }
        )
    return packages


def go_packages() -> list[dict[str, Any]]:
    packages = []
    for module in read_json_stream(["go", "list", "-m", "-json", "all"], CHECKER):
        if module.get("Main"):
            continue
        path = module["Path"]
        version = module.get("Version", "NOASSERTION")
        license_expression = (
            "MIT"
            if path == "github.com/anishathalye/porcupine"
            else "NOASSERTION"
        )
        packages.append(
            {
                "SPDXID": spdx_id("Go", f"{path}@{version}"),
                "name": path,
                "versionInfo": version,
                "downloadLocation": f"https://{path}",
                "filesAnalyzed": False,
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": license_expression,
                "copyrightText": "NOASSERTION",
                "externalRefs": [
                    {
                        "referenceCategory": "PACKAGE-MANAGER",
                        "referenceType": "purl",
                        "referenceLocator": purl("golang", path, version),
                    }
                ],
            }
        )
    return packages


def generate() -> dict[str, Any]:
    dependency_files = [ROOT / "Cargo.lock", CHECKER / "go.sum"]
    namespace_digest = hashlib.sha256()
    for path in dependency_files:
        namespace_digest.update(path.name.encode("utf-8"))
        namespace_digest.update(path.read_bytes())

    root_id = "SPDXRef-Package-AsterDB"
    dependencies = sorted(
        rust_packages() + go_packages(), key=lambda package: package["SPDXID"]
    )
    root_package = {
        "SPDXID": root_id,
        "name": "AsterDB",
        "versionInfo": "0.1.0",
        "downloadLocation": "https://github.com/AG0708/replicated-sql-database-in-rust",
        "filesAnalyzed": False,
        "licenseConcluded": "Apache-2.0",
        "licenseDeclared": "Apache-2.0",
        "copyrightText": "NOASSERTION",
    }
    relationships = [
        {
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relationshipType": "DESCRIBES",
            "relatedSpdxElement": root_id,
        }
    ]
    relationships.extend(
        {
            "spdxElementId": root_id,
            "relationshipType": "DEPENDS_ON",
            "relatedSpdxElement": package["SPDXID"],
        }
        for package in dependencies
    )
    return {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "AsterDB-0.1.0",
        "documentNamespace": (
            "https://github.com/AG0708/replicated-sql-database-in-rust/sbom/"
            + namespace_digest.hexdigest()
        ),
        "creationInfo": {
            "created": created_timestamp(),
            "creators": ["Tool: AsterDB tools/generate_sbom.py"],
        },
        "packages": [root_package, *dependencies],
        "relationships": relationships,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    encoded = json.dumps(generate(), indent=2, sort_keys=True) + "\n"
    if arguments.output is None:
        print(encoded, end="")
    else:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(encoded, encoding="utf-8")
        print(arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

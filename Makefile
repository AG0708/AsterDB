PYTHON ?= python3
CARGO ?= cargo
GO ?= go

.PHONY: benchmark verify verify-rust verify-oracles verify-external release-gate sbom

benchmark:
	$(PYTHON) tools/benchmark.py

verify: verify-rust verify-oracles verify-external

verify-rust:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --workspace --all-targets --locked -- -D warnings
	$(CARGO) test --workspace --locked --all-targets
	RUSTDOCFLAGS='-D warnings' $(CARGO) doc --workspace --no-deps --locked
	$(CARGO) deny check
	$(CARGO) audit --deny warnings

verify-oracles:
	$(PYTHON) tools/sql_differential.py
	$(PYTHON) -m unittest discover -s tools -p 'test_*.py' -v
	cd tools/porcupine-check && $(GO) test ./...
	tools/check_tla.sh

verify-external:
	$(PYTHON) tools/cluster_history.py

release-gate:
	$(PYTHON) tools/run_release_gate.py --require-clean

sbom:
	SOURCE_DATE_EPOCH=$${SOURCE_DATE_EPOCH:-1700000000} \
		$(PYTHON) tools/generate_sbom.py --output dist/asterdb.spdx.json

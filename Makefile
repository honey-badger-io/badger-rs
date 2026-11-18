GO_FIXTURES := compat/go-fixtures

.PHONY: check fmt clippy test fixtures golden miri loom compat verify go-fixtures

## check: everything CI runs for Rust (format, lint, tests)
check: fmt clippy test

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

## fixtures: regenerate compat/fixtures with Go Badger v4.9.6.
## Some files contain random IVs, so only regenerate on purpose.
fixtures:
	cd $(GO_FIXTURES) && go run . gen ../fixtures

## golden: regenerate compat/golden (deterministic; must not change unless Go code changed)
golden:
	cd $(GO_FIXTURES) && go run . golden ../golden

## miri: run unit tests under Miri (needs a nightly toolchain with miri)
miri:
	MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test -p badger --lib

## loom: explore skiplist thread interleavings with loom (separate target dir,
## since --cfg loom changes how the crate is built)
loom:
	RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=target/loom cargo test -p badger --lib --release skl::loom_tests

## compat: build tables and DBs with Rust and check them with Go Badger
COMPAT_OUT := target/compat
compat: go-fixtures
	rm -rf $(COMPAT_OUT)
	BADGER_COMPAT_OUT=$(abspath $(COMPAT_OUT)) cargo test -p badger --test table_compat builds_every
	BADGER_COMPAT_OUT=$(abspath $(COMPAT_OUT)) cargo test -p badger --test write_compat
	BADGER_COMPAT_OUT=$(abspath $(COMPAT_OUT)) cargo test -p badger --test workload
	for json in $(COMPAT_OUT)/sst/*.json; do \
		name=$$(basename $$json .json); \
		$(GO_FIXTURES)/go-fixtures verify-sst $(COMPAT_OUT)/sst/$$name/000001.sst $$json || exit 1; \
	done
	for json in $(COMPAT_OUT)/db/*.json; do \
		name=$$(basename $$json .json); \
		$(GO_FIXTURES)/go-fixtures verify $(COMPAT_OUT)/db/$$name $$json || exit 1; \
	done

## verify: check a DB with Go Badger, e.g. make verify DB=/tmp/db EXPECTED=/tmp/db.json
verify: go-fixtures
	$(GO_FIXTURES)/go-fixtures verify $(DB) $(EXPECTED)

go-fixtures:
	cd $(GO_FIXTURES) && go build -o go-fixtures .

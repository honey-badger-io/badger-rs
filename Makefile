GO_FIXTURES := compat/go-fixtures

.PHONY: check fmt clippy test fixtures golden miri verify go-fixtures

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

## verify: check a DB with Go Badger, e.g. make verify DB=/tmp/db EXPECTED=/tmp/db.json
verify: go-fixtures
	$(GO_FIXTURES)/go-fixtures verify $(DB) $(EXPECTED)

go-fixtures:
	cd $(GO_FIXTURES) && go build -o go-fixtures .

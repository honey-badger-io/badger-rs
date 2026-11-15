# Compatibility fixtures

`go-fixtures/` is a small Go program that uses the real Go Badger
(`github.com/dgraph-io/badger/v4 v4.9.6`, pinned in `go.mod`) to produce
on-disk fixtures and to check databases written by the Rust port.

```sh
make fixtures                                # regenerate compat/fixtures
make golden                                  # regenerate compat/golden/phase1.json
make verify DB=/path/to/db EXPECTED=x.json   # Go opens DB read-only and compares
make compat                                  # Rust builds tables; Go checks them (verify-sst)
cd go-fixtures && go run . dump <db-dir> [expected.json]   # print contents as JSON
```

The Rust side reads them through the `badger-fixtures` crate
(`crates/badger-fixtures`). Set `BADGER_FIXTURES=<dir>` to point the tests at
another fixtures directory.

## Golden values

`golden/phase1.json` (written by `go-fixtures golden`) records exact outputs of
Go Badger's building blocks: bloom hash and filters, bits-per-key, CRC32C and
XXHash64, AES-CTR, ValueStruct and varint encodings, key ordering, protobuf
encodings of fixed messages, and a TableIndex flatbuffer built the way
`table.Builder` builds it. `crates/badger/tests/golden_phase1.rs` checks the Rust
port against it byte for byte. Unlike the fixtures, it is fully deterministic;
CI regenerates it and fails on any difference.

## Regenerating

The fixtures are committed so Rust tests run without Go. Regenerating is not
byte-for-byte reproducible: Badger puts random IVs in KEYREGISTRY, vlog headers
and encrypted blocks, which also changes vlog offsets inside SSTs. The JSON files
and unencrypted standalone SSTs *are* reproducible. Regenerate only when
fixtures change, and commit the result in one go.

## Layout

```
fixtures/
  index.json            list of fixtures: name, kind (db|sst), description, path, expected
  db/<name>/            a complete DB directory (MANIFEST, *.sst, *.vlog, KEYREGISTRY, DISCARD, *.mem)
  db/<name>.json        expected contents of that DB
  sst/<name>/000001.sst a standalone table built with Go's table.Builder
  sst/<name>.json       the entries it was built from, plus table metadata
```

### DB fixtures

| Name | What it covers |
|---|---|
| `basic` | small inline values, binary keys containing 0x00/0xff |
| `vlog` | values above a 64-byte threshold, two vlog files |
| `versions` | several versions per key, deletes, expired / not-yet-expired entries, user meta, discard-earlier-versions |
| `snappy`, `zstd` | block compression |
| `encrypted`, `encrypted-zstd` | AES-256 encryption at rest, alone and with zstd |
| `managed` | managed mode, commit timestamps 10..50 |
| `levels` | tables in L6 overlapped by newer L0 tables (overwrites and deletes) |
| `crash-wal` | copied from a live DB: unflushed `.mem` WAL, preallocated vlog |

Expected file (`db/<name>.json`): every version of every key, as returned by a Go
iterator with `AllVersions: true` at the newest read timestamp. Order is key
ascending, then version descending. Byte strings are lowercase hex.

```json
{"format_version":1,"name":"versions",
 "options":{"compression":"none","encryption_key":"","value_threshold":128,
            "mem_table_size":524288,"value_log_file_size":1048576,"base_table_size":262144,
            "num_versions_to_keep":1000,"managed":false},
 "crash_snapshot":false,"max_version":22,"entries":[
{"key":"7665722d30303030303030","version":18,"value":"","user_meta":0,"expires_at":0,
 "deleted_or_expired":true,"discard_earlier_versions":false},
...]}
```

Expiry uses fixed timestamps (`1` = expired, `4102444800` = 2100-01-01), so
`deleted_or_expired` does not depend on when the tests run.

`crash-wal` cannot be opened read-only by Go (it would need to truncate the
WAL). Its expected contents come from a recovered copy, so a correct port must
reproduce Badger's replay and truncation logic.

### SST fixtures

| Name | Compression | Block size | Encrypted |
|---|---|---|---|
| `plain` | none | 4 KiB | no |
| `small-blocks` | none | 256 B | no |
| `snappy` | snappy | 4 KiB | no |
| `zstd` | zstd (level 1) | 4 KiB | no |
| `encrypted` | none | 4 KiB | yes |
| `encrypted-zstd` | zstd | 4 KiB | yes |

Each entry has `key` (user key), `version`, `meta`, `user_meta`, `expires_at`,
`value` and `value_len`. Meta bits used: `0x01` delete, `0x02` value pointer
(the value is 12 random bytes standing in for a pointer), `0x04` discard earlier
versions. Encrypted tables include the raw `data_key` (`key_id`, `data`, `iv`).
`smallest` / `biggest` are internal keys: user key followed by big-endian
`u64::MAX - version`.

Unencrypted, uncompressed tables are reproducible. In Phase 2, the Rust builder
must produce byte-identical files from the same entries and options.

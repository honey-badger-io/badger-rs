# Badger → Rust port plan

Port of [dgraph-io/badger v4.9.6](https://github.com/dgraph-io/badger/tree/v4.9.6) to Rust.

**Status:** Phase 1 done (2026-11-15). Next: Phase 2.

## Decisions

| Topic | Decision |
|---|---|
| Purpose | Learning port: correctness and understanding come first, performance tuning later |
| Scope | The current feature set, in full |
| On-disk format | **Byte-compatible with Go Badger v4.9.6**: SST, vlog, `.mem` WAL, MANIFEST, KEYREGISTRY, DISCARD |
| Concurrency | `std::thread` + `crossbeam-channel`, no async runtime |
| API style | Closure-based, mirroring Go: `db.update(\|txn\| ...)`, `item.value(\|v\| ...)` |
| Errors | `Result<T, Error>` everywhere; a `thiserror` enum mirroring `errors.go`; no `y.Check`-style panics |
| Ref counting | Go `IncrRef`/`DecrRef` → `Arc` + `Drop` (the file is deleted when the last reference drops) |
| Go reference source | `~/Projects/badger` at tag `v4.9.6` (`git worktree add ../badger-go-v4.9.6 v4.9.6`) |

## Size of the Go source (v4.9.6)

| Area (Go) | Non-test LOC | Responsibility |
|---|---|---|
| `db.go`, `levels.go`, `compaction.go`, `level_handler.go` | ~4.7k | Opening the DB, write pipeline, LSM levels, compaction |
| `value.go`, `discard.go` | ~1.4k | Value log and its garbage collection |
| `txn.go`, `iterator.go`, `batch.go`, `merge.go` | ~2.0k | MVCC, oracle, iterators |
| `table/` | ~2.2k | SST builder/reader, block iterator, merge iterator |
| `skl/`, `memtable.go` | ~1.2k | Arena skiplist, memtable + WAL |
| `manifest.go`, `key_registry.go` | ~0.9k | Manifest, encryption key registry |
| `stream*.go`, `backup.go`, `publisher.go` | ~1.5k | Stream framework, backup/restore, subscriptions |
| `y/`, `trie/`, `options.go`, misc | ~2.5k | Utilities, bloom, checksums, encryption, watermark |
| `pb/`, `fb/` | generated | Protobuf (manifest, KV) and FlatBuffers (table index) |
| **Total** | **~17k**, plus ~16.5k of tests and the CLI | |

## Dependency mapping

| Go | Rust |
|---|---|
| ristretto (block and index cache) | `moka` or `quick_cache`, with a weigher for cost |
| `ristretto/z` (Closer, Buffer, MemHash, Allocator) | Own `util` module: shutdown signal + `JoinHandle`s, `Vec<u8>` arena, `xxhash-rust` |
| klauspost zstd / snappy | `zstd`, `snap` |
| CRC32C, xxhash | `crc32c`, `xxhash-rust` |
| AES-CTR | `aes` + `ctr` |
| mmap / `x/sys` | `memmap2`, `rustix`/`libc`, `fs4` (dir locking) |
| protobuf / flatbuffers | `prost` + `prost-build`, `flatbuffers` (from `fb/flatbuffer.fbs`) |
| OpenTelemetry | `metrics` crate behind a `metrics` feature |
| cobra | `clap` |
| testify | built-in tests, `proptest`, `tempfile` |

## Workspace layout

```
badger-rust/
  Cargo.toml                 # workspace
  crates/
    badger/                  # library
      src/
        util/                # y/: keys+ts, checksum, bloom, encrypt, watermark, closer
        trie/
        proto/               # prost-generated (pb/)
        fb/                  # flatbuffers-generated (fb/)
        skl/                 # arena skiplist (unsafe lives here)
        table/               # builder, table, block iter, merge iter
        memtable.rs          # memtable + WAL (logFile)
        vlog/                # value log, discard stats, GC
        manifest.rs
        key_registry.rs
        levels/              # levels controller, level handler, compaction
        oracle.rs, txn.rs, iterator.rs
        batch.rs, merge.rs, stream.rs, stream_writer.rs, backup.rs, publisher.rs
        options.rs, error.rs, db.rs, lib.rs
    badger-cli/              # info, backup, restore, flatten, bank, benchmarks
    badger-fixtures/         # test support: loads compat/fixtures, writes JSON Go can verify
  compat/
    go-fixtures/             # small Go program (go.mod pins badger v4.9.6)
    fixtures/                # generated DBs/SSTs, committed (~40 MB, ~14 MB compressed)
  benches/                   # criterion benchmarks
```

## Phases

Each phase lists the Go files to study, what to build, and how to know it is done.

### Phase 0: scaffolding (done)
Details of the fixtures: `compat/README.md`. What Phase 0 taught us:
- Go cannot open a crash snapshot (unflushed `.mem`) read-only (`ErrTruncateNeeded`),
  so its expected contents come from a recovered copy.
- `Flatten` does not push L0 tables down; `CompactL0OnClose` does.
- `ValueThreshold` must be ≤ 15% of `MemTableSize` (maxBatchSize).
- Regenerating is not byte-reproducible: KEYREGISTRY, vlog headers and encrypted
  blocks carry random IVs.

- Cargo workspace, `rustfmt`/`clippy` config, CI (GitHub Actions: test, clippy, miri on `skl`).
- `compat/go-fixtures`: a Go program that generates fixtures:
  - single SSTs (no compression / snappy / zstd; with and without encryption; checksum modes)
  - `.mem` WAL files, vlog files, MANIFEST, KEYREGISTRY
  - full DBs: small and large values (so some values land in the vlog), deletes, expiries, many versions
  - a JSON "expected contents" dump next to each DB
- `compat/go-fixtures verify <dir>`: Go opens a DB written by Rust and checks it against the JSON dump.
- **Done when:** `cargo test` runs green on an empty crate and the fixtures generate.

### Phase 1: foundations (`y/`, `trie/`, `pb/`, `fb/`, `options.go`, `errors.go`) (done)
Result: `crates/badger/src/{util,trie,pb,fb,options,error,structs}`. Go's tests for
`y/` and `trie/` are ported (except PageBuffer and the allocator tests: neither type
is used outside `y`). `tests/golden_phase1.rs` matches Go byte for byte via
`compat/golden/phase1.json`, including a TableIndex built by Rust's flatbuffers
builder. `tests/sst_index.rs` decodes the index of every SST fixture.

Deviations and findings:
- `protoc`/`flatc` are not needed: the prost structs and the flatbuffers
  accessors/builders are written by hand, and the golden tests check them.
- `WaterMark` uses a mutex and condvar instead of a goroutine and channel. Beginning
  an index below `done_until` panics, as in Go, so begins must be ordered (the
  oracle does this under its lock).
- `ValueStruct<'a>` borrows its value; `Entry` owns its key and value.
- `ValuePointer` is encoded little-endian (Go copies the struct's memory).
- Go's `onDiskSize` in the table index includes the vlog bytes referenced by value
  pointers, plus the *unencrypted* index length.
- `y.Throttle`, `z.Closer`, compression helpers and options validation were moved
  to the phases that first use them (2–4). The `Logger` option was dropped in
  favour of the `log` facade.
- Under Miri, tests that use AES or CRC32C hardware instructions are skipped
  (Miri cannot execute them), and Miri runs with isolation disabled.

- Key/timestamp encoding (`KeyWithTs`, `ParseTs`, `ParseKey`, `CompareKeys`, `SameKey`).
- `ValueStruct` encoding, varints, `header` for vlog/WAL entries (`structs.go`).
- Checksums (CRC32C, xxhash64) and `pb::Checksum` verification.
- Bloom filter with Go's exact hash (`y.Hash`) and filter layout.
- AES-CTR encryption with an IV per block or entry.
- `WaterMark` (done-until tracking) using threads + channels.
- Trie used by Subscribe prefix matching.
- `prost` build for `badgerpb4.proto`; generated flatbuffers code for the table index.
- `Options` builder with the same defaults as Go `DefaultOptions`.
- **Done when:** Go unit tests for `y/` and `trie/` are ported, and bloom/checksum outputs match Go bit for bit (golden values exported from Go).

### Phase 2: tables (`table/`)
- Reading over `memmap2`: footer → index (flatbuffers) → block offsets → blocks; decompression and decryption; checksum verification modes.
- Block iterator (entry diff-encoded keys against the base key), table iterator (seek, seek-for-prev, reversed), `ConcatIterator`, `MergeIterator`.
- Block cache and index cache.
- Builder: block packing, bloom per table, index building, compression + encryption.
- **Done when:**
  - every Go-generated SST fixture reads back exactly,
  - SSTs from Rust's builder (no compression, no encryption) are **byte-identical** to Go's for the same input,
  - SSTs from Rust's builder (compressed/encrypted) are readable by Go.

### Phase 3: skiplist + memtable (`skl/`, `memtable.go`)
- Arena with `AtomicU32` offsets, lock-free insert with CAS, the same height distribution and node layout.
- Keep the `unsafe` code in `skl/` only, with safety comments on every block.
- Memtable = skiplist + WAL (`logFile`): mmapped, preallocated, encrypted entries, truncation on replay.
- **Done when:** concurrent insert/get tests pass under `loom` (small model) and `miri`, and Go `.mem` fixtures replay correctly.

### Phase 4: open a DB read-only (`manifest.go`, `key_registry.go`, `levels.go`, `level_handler.go`, `db.go` open path)
- Manifest replay (change sets, rewrite), KEYREGISTRY load, directory lock.
- Level handlers, table loading, point `get` across memtables → L0 → Ln with version selection.
- Read-only `Txn` + `Iterator` (no prefetch yet) and the vlog read path (value pointers).
- **Done when (the main milestone):** Rust opens every Go fixture DB read-only and matches its JSON dump.

### Phase 5: write path (`db.go` write loop, `txn.go`, `value.go` write side, `memtable.go` flush)
- Write channel → batching → vlog write (for large values) → WAL → skiplist.
- Oracle: read/commit timestamps, conflict detection via key fingerprints, `WaterMark`s for read and txn marks.
- `Txn` (update, set, delete, entries with TTL/meta/user meta), `db.view`/`db.update`.
- Rotating memtables, flushing to L0, writing manifest changes.
- **Done when:** Rust writes a DB and the Go verifier accepts it; crash-in-the-middle tests (kill the child process) reopen consistently.

### Phase 6: background work (`levels.go` compaction, `compaction.go`, `discard.go`, `value.go` GC)
- Compaction priorities, L0 → Lbase, dynamic level sizes, table splits, L0 stalls.
- Discard stats file, `run_value_log_gc`, vlog file rewrite.
- `flatten`, `drop_all`, `drop_prefix`, banned namespaces.
- **Done when:** a long randomized workload test (model = `BTreeMap`) passes with compaction and GC active; Go reads the result.

### Phase 7: remaining features
- Iterator prefetch, key-only iteration, `all_versions`, prefix iteration.
- Managed mode (`managed_db.go`), `WriteBatch` (`batch.go`), merge operator (`merge.go`).
- Stream framework (`stream.go`), `StreamWriter` (`stream_writer.go`).
- Backup/load (`backup.go`), Subscribe (`publisher.go`), encryption key rotation.
- **Done when:** the corresponding Go tests are ported and pass; Go-produced backups load in Rust and the reverse.

### Phase 8: CLI and hardening
- `badger-cli`: `info`, `backup`, `restore`, `flatten`, `stream`, `bank` (consistency test), `benchmark read/write`.
- Metrics behind a feature flag.
- `cargo-fuzz` on the decoders (block, index, vlog header, manifest).
- Criterion benchmarks compared with Go for interest; a profiling pass if time allows.

## Testing strategy

1. **Ported Go tests.** Port each `_test.go` alongside its module and keep the test names so they map back.
2. **Cross-implementation compatibility.** Go-written fixtures read in Rust, and Rust-written DBs verified by Go (`compat/`).
3. **Model-based tests.** `proptest` command sequences (set/delete/get/iterate/reopen) checked against a `BTreeMap`.
4. **Concurrency checks.** `loom` for the skiplist and watermark; `miri` for `unsafe` code; the ported `bank` test.
5. **Crash tests.** Spawn a writer child process, `SIGKILL` it at random points, reopen, and verify.

## Main risks

| Risk | Mitigation |
|---|---|
| `unsafe` code in the arena skiplist | Isolate it in `skl/`, document safety invariants, use loom + miri |
| Values borrowed from mmap outliving their file | Closure-based value access, `Arc` guards held by iterators/items |
| Shutdown ordering of background threads | One `Closer`-like primitive per subsystem; explicit `close()` plus best-effort `Drop` |
| Go behaviours that differ in Rust (zeroed memory, slice aliasing, panics) | Port the tests early in each phase; compare against Go fixtures |
| Small format differences (varint, endianness, padding) | Byte-identical builder test in Phase 2; golden files |
| Scope creep | Phase gates: don't start a phase until the previous "done when" passes |

## Learning notes

Keep a `NOTES.md` per phase: the Go design, why it works, and what changed in Rust. Good topics include the WiscKey key/value separation, the arena skiplist, oracle conflict detection, leveled compaction scoring, and vlog GC.

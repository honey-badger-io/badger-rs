package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"
)

// Fixed timestamps so that expiry status never changes between runs:
// expiredAt is in 1970, notExpiredAt is 2100-01-01.
const (
	expiredAt    = uint64(1)
	notExpiredAt = uint64(4102444800)
)

// fixtureEncryptionKey is the AES-256 master key used by encrypted DB fixtures.
var fixtureEncryptionKey = seqBytes(0x00, 32)

func seqBytes(start byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = start + byte(i)
	}
	return b
}

// generate writes every fixture into out. With names, it only (re)generates
// those fixtures and keeps the others in out as they are, so adding a fixture
// does not churn the committed ones (regenerating is not byte-reproducible).
func generate(out string, names ...string) error {
	only := map[string]bool{}
	for _, n := range names {
		only[n] = true
	}
	var old Index
	if len(only) > 0 {
		if err := readJSON(filepath.Join(out, "index.json"), &old); err != nil {
			return fmt.Errorf("partial gen needs an existing fixture dir: %w", err)
		}
	} else if err := prepareOutDir(out); err != nil {
		return err
	}
	existing := map[string]IndexEntry{}
	for _, e := range old.Fixtures {
		existing[e.Kind+"/"+e.Name] = e
	}
	// keep returns the existing index entry of a fixture that is not selected.
	seen := map[string]bool{}
	// A name selects both the db and the sst fixture of that name, if any.
	keep := func(kind, name string) (IndexEntry, bool) {
		seen[name] = true
		if len(only) == 0 || only[name] {
			return IndexEntry{}, false
		}
		e, ok := existing[kind+"/"+name]
		return e, ok
	}

	tmp, err := os.MkdirTemp("", "badger-fixtures-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	idx := Index{FormatVersion: FormatVersion, BadgerVersion: BadgerVersion}
	for i, s := range dbScenarios() {
		if e, ok := keep("db", s.name); ok {
			idx.Fixtures = append(idx.Fixtures, e)
			continue
		}
		if err := os.RemoveAll(filepath.Join(out, "db", s.name)); err != nil {
			return err
		}
		// Seeded by position: append new scenarios to keep the others' data.
		r := rand.New(rand.NewPCG(0xbad6e5, uint64(i)))
		e, err := genDB(out, tmp, s, r)
		if err != nil {
			return fmt.Errorf("db fixture %s: %w", s.name, err)
		}
		idx.Fixtures = append(idx.Fixtures, e)
		fmt.Printf("db  %-20s %s\n", s.name, s.desc)
	}
	for i, s := range sstScenarios() {
		if e, ok := keep("sst", s.name); ok {
			idx.Fixtures = append(idx.Fixtures, e)
			continue
		}
		r := rand.New(rand.NewPCG(0x55747, uint64(i)))
		e, err := genSST(out, s, r)
		if err != nil {
			return fmt.Errorf("sst fixture %s: %w", s.name, err)
		}
		idx.Fixtures = append(idx.Fixtures, e)
		fmt.Printf("sst %-20s %s\n", s.name, s.desc)
	}
	for name := range only {
		if !seen[name] {
			return fmt.Errorf("unknown fixture %q", name)
		}
	}
	return writeJSON(filepath.Join(out, "index.json"), idx)
}

// prepareOutDir creates out, or empties it if it holds a previous generation.
func prepareOutDir(out string) error {
	ents, err := os.ReadDir(out)
	if os.IsNotExist(err) {
		return os.MkdirAll(out, 0o755)
	}
	if err != nil {
		return err
	}
	if len(ents) == 0 {
		return nil
	}
	if _, err := os.Stat(filepath.Join(out, "index.json")); err != nil {
		return fmt.Errorf("%w: %s (no index.json)", errNotFixtureDir, out)
	}
	for _, name := range []string{"db", "sst", "index.json"} {
		if err := os.RemoveAll(filepath.Join(out, name)); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// DB fixtures

type dbScenario struct {
	name  string
	desc  string
	opts  DBOptions
	crash bool // copy the directory while the DB is still open
	// prepare, if set, runs first in its own open/close cycle with
	// CompactL0OnClose, so its data ends up below L0.
	prepare func(db *badger.DB, r *rand.Rand) error
	write   func(db *badger.DB, r *rand.Rand) error
}

// baseOptions keep fixtures small: 512 KiB memtables and 1 MiB vlog files
// (the minimum) mean a few MiB of data produce several tables and vlog files.
func baseOptions() DBOptions {
	return DBOptions{
		Compression:       "none",
		ValueThreshold:    16 << 10, // everything inline unless a scenario lowers it
		MemTableSize:      512 << 10,
		ValueLogFileSize:  1 << 20,
		BaseTableSize:     256 << 10,
		NumVersionsToKeep: 1,
	}
}

func dbScenarios() []dbScenario {
	with := func(f func(o *DBOptions)) DBOptions {
		o := baseOptions()
		f(&o)
		return o
	}
	return []dbScenario{
		{
			name: "basic",
			desc: "8k small inline values plus binary keys, no compression",
			opts: baseOptions(),
			write: func(db *badger.DB, r *rand.Rand) error {
				es := make([]*badger.Entry, 0, 8100)
				for i := 0; i < 8000; i++ {
					es = append(es, badger.NewEntry(fmtKey("basic", i), randBytes(r, 8+r.IntN(56))))
				}
				for i := 0; i < 100; i++ {
					es = append(es, badger.NewEntry(binaryKey(r), randBytes(r, r.IntN(32))))
				}
				r.Shuffle(len(es), func(i, j int) { es[i], es[j] = es[j], es[i] })
				return writeEntries(db, es, 500)
			},
		},
		{
			name: "vlog",
			desc: "2.5k values, most above the 64-byte value threshold; several vlog files",
			opts: with(func(o *DBOptions) { o.ValueThreshold = 64 }),
			write: func(db *badger.DB, r *rand.Rand) error {
				es := make([]*badger.Entry, 0, 2500)
				for i := 0; i < 2500; i++ {
					n := 100 + r.IntN(1400)
					if i%5 == 0 {
						n = r.IntN(64)
					}
					es = append(es, badger.NewEntry(fmtKey("vlog", i), randBytes(r, n)))
				}
				return writeEntries(db, es, 100)
			},
		},
		{
			name:  "versions",
			desc:  "multiple versions, deletes, expiry, user meta, discard-earlier-versions",
			opts:  with(func(o *DBOptions) { o.ValueThreshold = 128; o.NumVersionsToKeep = 1000 }),
			write: writeVersions,
		},
		{
			name:  "snappy",
			desc:  "compressible values, snappy block compression, some vlog values",
			opts:  with(func(o *DBOptions) { o.Compression = "snappy"; o.ValueThreshold = 256 }),
			write: writeCompressible("snappy"),
		},
		{
			name:  "zstd",
			desc:  "compressible values, zstd block compression, some vlog values",
			opts:  with(func(o *DBOptions) { o.Compression = "zstd"; o.ValueThreshold = 256 }),
			write: writeCompressible("zstd"),
		},
		{
			name: "encrypted",
			desc: "AES-256 encryption at rest (tables, vlog, WAL, key registry)",
			opts: with(func(o *DBOptions) {
				o.EncryptionKey = fixtureEncryptionKey
				o.ValueThreshold = 128
			}),
			write: writeMixed("enc", 2000),
		},
		{
			name: "encrypted-zstd",
			desc: "encryption combined with zstd compression",
			opts: with(func(o *DBOptions) {
				o.EncryptionKey = fixtureEncryptionKey
				o.Compression = "zstd"
				o.ValueThreshold = 128
			}),
			write: writeCompressible("enczstd"),
		},
		{
			name:  "managed",
			desc:  "managed mode with explicit commit timestamps 10,20,...,50",
			opts:  with(func(o *DBOptions) { o.Managed = true; o.NumVersionsToKeep = 1000 }),
			write: writeManaged,
		},
		{
			name:    "levels",
			desc:    "data compacted below L0, then overwritten and deleted in L0",
			opts:    baseOptions(),
			prepare: writeLevelsBase,
			write:   writeLevelsTop,
		},
		{
			name:  "crash-wal",
			desc:  "directory copied from an open DB: unflushed memtable WAL and untruncated vlog",
			opts:  with(func(o *DBOptions) { o.ValueThreshold = 64 }),
			crash: true,
			write: writeMixed("crash", 1500),
		},
		{
			name: "crash-wal-encrypted",
			desc: "like crash-wal, with AES-256 encryption: encrypted WAL and vlog entries",
			opts: with(func(o *DBOptions) {
				o.EncryptionKey = fixtureEncryptionKey
				o.ValueThreshold = 64
			}),
			crash: true,
			write: writeMixed("crashenc", 1500),
		},
	}
}

func genDB(out, tmp string, s dbScenario, r *rand.Rand) (IndexEntry, error) {
	rel := filepath.Join("db", s.name)
	dir := filepath.Join(out, rel)
	entry := IndexEntry{
		Name: s.name, Kind: "db", Description: s.desc,
		Path: rel, Expected: rel + ".json",
	}

	// Write the data. A crash scenario writes into a scratch dir and copies it.
	writeDir := dir
	if s.crash {
		writeDir = filepath.Join(tmp, s.name+"-live")
	}
	opts, err := badgerOptions(writeDir, s.opts)
	if err != nil {
		return entry, err
	}
	open := badger.Open
	if s.opts.Managed {
		open = badger.OpenManaged
	}
	if s.prepare != nil {
		db, err := open(opts.WithCompactL0OnClose(true))
		if err != nil {
			return entry, err
		}
		if err := s.prepare(db, r); err != nil {
			db.Close()
			return entry, err
		}
		if err := db.Close(); err != nil {
			return entry, err
		}
	}
	db, err := open(opts)
	if err != nil {
		return entry, err
	}
	if err := s.write(db, r); err != nil {
		db.Close()
		return entry, err
	}
	if s.crash {
		if err := db.Sync(); err != nil {
			db.Close()
			return entry, err
		}
		if err := copyDir(writeDir, dir); err != nil {
			db.Close()
			return entry, err
		}
	}
	if err := db.Close(); err != nil {
		return entry, err
	}

	exp := DBExpected{FormatVersion: FormatVersion, Name: s.name, Options: s.opts, CrashSnapshot: s.crash}
	if s.crash {
		// Go refuses to open an untruncated WAL read-only, so recover a copy.
		recovered := filepath.Join(tmp, s.name+"-recovered")
		if err := copyDir(dir, recovered); err != nil {
			return entry, err
		}
		exp.MaxVersion, exp.Entries, err = dumpDB(recovered, s.opts, false)
		if err != nil {
			return entry, err
		}
	} else {
		before, err := hashDir(dir)
		if err != nil {
			return entry, err
		}
		exp.MaxVersion, exp.Entries, err = dumpDB(dir, s.opts, true)
		if err != nil {
			return entry, err
		}
		after, err := hashDir(dir)
		if err != nil {
			return entry, err
		}
		if before != after {
			return entry, fmt.Errorf("read-only dump modified %s", dir)
		}
	}
	if len(exp.Entries) == 0 {
		return entry, fmt.Errorf("fixture has no entries")
	}
	return entry, writeEntriesJSON(filepath.Join(out, entry.Expected), exp)
}

func fmtKey(prefix string, i int) []byte { return []byte(fmt.Sprintf("%s-%07d", prefix, i)) }

// binaryKey returns a random key that may contain 0x00 and 0xff bytes.
func binaryKey(r *rand.Rand) []byte {
	k := randBytes(r, 1+r.IntN(40))
	k[0] = []byte{0x00, 0x01, 0xfe, 0xff}[r.IntN(4)]
	return k
}

func randBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

var words = []string{"badger", "level", "table", "block", "value", "log", "key", "merge",
	"compaction", "memtable", "skiplist", "oracle", "txn", "stream", "rust", "go"}

// textBytes returns n bytes of space-separated words, which compress well.
func textBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, 0, n+16)
	for len(b) < n {
		b = append(b, words[r.IntN(len(words))]...)
		b = append(b, ' ')
	}
	return b[:n]
}

func writeEntries(db *badger.DB, es []*badger.Entry, perTxn int) error {
	for start := 0; start < len(es); start += perTxn {
		chunk := es[start:min(start+perTxn, len(es))]
		err := db.Update(func(txn *badger.Txn) error {
			for _, e := range chunk {
				if err := txn.SetEntry(e); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteKeys(db *badger.DB, keys [][]byte) error {
	return db.Update(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeMixed(prefix string, n int) func(*badger.DB, *rand.Rand) error {
	return func(db *badger.DB, r *rand.Rand) error {
		es := make([]*badger.Entry, 0, n)
		for i := 0; i < n; i++ {
			es = append(es, badger.NewEntry(fmtKey(prefix, i), randBytes(r, 16+r.IntN(384))).
				WithMeta(byte(r.IntN(4))))
		}
		r.Shuffle(len(es), func(i, j int) { es[i], es[j] = es[j], es[i] })
		if err := writeEntries(db, es, 150); err != nil {
			return err
		}
		var del [][]byte
		for i := 0; i < 20; i++ {
			del = append(del, fmtKey(prefix, r.IntN(n)))
		}
		return deleteKeys(db, del)
	}
}

func writeCompressible(prefix string) func(*badger.DB, *rand.Rand) error {
	return func(db *badger.DB, r *rand.Rand) error {
		es := make([]*badger.Entry, 0, 3000)
		for i := 0; i < 3000; i++ {
			es = append(es, badger.NewEntry(fmtKey(prefix, i), textBytes(r, 20+r.IntN(480))))
		}
		return writeEntries(db, es, 200)
	}
}

func writeVersions(db *badger.DB, r *rand.Rand) error {
	const n = 1000
	value := func() []byte { return randBytes(r, 16+r.IntN(384)) }
	pick := func(count int) []int { return r.Perm(n)[:count] }

	es := make([]*badger.Entry, 0, n)
	for i := 0; i < n; i++ {
		es = append(es, badger.NewEntry(fmtKey("ver", i), value()))
	}
	if err := writeEntries(db, es, 200); err != nil {
		return err
	}
	// Four rounds of overwrites, each tagged with its round in UserMeta.
	for round := 2; round <= 5; round++ {
		es = es[:0]
		for _, i := range pick(300) {
			es = append(es, badger.NewEntry(fmtKey("ver", i), value()).WithMeta(byte(round)))
		}
		if err := writeEntries(db, es, 100); err != nil {
			return err
		}
	}
	var del [][]byte
	for _, i := range pick(100) {
		del = append(del, fmtKey("ver", i))
	}
	if err := deleteKeys(db, del); err != nil {
		return err
	}
	es = es[:0]
	for j, i := range pick(150) {
		e := badger.NewEntry(fmtKey("ver", i), value())
		switch j % 3 {
		case 0:
			e.ExpiresAt = expiredAt
		case 1:
			e.ExpiresAt = notExpiredAt
		case 2:
			e = e.WithDiscard()
		}
		es = append(es, e)
	}
	if err := writeEntries(db, es, 50); err != nil {
		return err
	}
	// One more version on top of some of the special entries.
	es = es[:0]
	for _, i := range pick(100) {
		es = append(es, badger.NewEntry(fmtKey("ver", i), value()).WithMeta(9))
	}
	return writeEntries(db, es, 100)
}

func writeManaged(db *badger.DB, r *rand.Rand) error {
	for round := 1; round <= 5; round++ {
		commitTs := uint64(round * 10)
		txn := db.NewTransactionAt(commitTs-1, true)
		for _, i := range r.Perm(500)[:200] {
			e := badger.NewEntry(fmtKey("managed", i), randBytes(r, 8+r.IntN(120))).WithMeta(byte(round))
			if err := txn.SetEntry(e); err != nil {
				txn.Discard()
				return err
			}
		}
		if round == 5 {
			for _, i := range r.Perm(500)[:30] {
				if err := txn.Delete(fmtKey("managed", i)); err != nil {
					txn.Discard()
					return err
				}
			}
		}
		if err := txn.CommitAt(commitTs, nil); err != nil {
			return err
		}
	}
	return nil
}

func writeLevelsBase(db *badger.DB, r *rand.Rand) error {
	es := make([]*badger.Entry, 0, 8000)
	for i := 0; i < 8000; i++ {
		es = append(es, badger.NewEntry(fmtKey("lvl", i), randBytes(r, 16+r.IntN(184))))
	}
	return writeEntries(db, es, 500)
}

func writeLevelsTop(db *badger.DB, r *rand.Rand) error {
	es := make([]*badger.Entry, 0, 2000)
	for _, i := range r.Perm(8000)[:2000] {
		es = append(es, badger.NewEntry(fmtKey("lvl", i), randBytes(r, 16+r.IntN(184))).WithMeta(2))
	}
	if err := writeEntries(db, es, 500); err != nil {
		return err
	}
	var del [][]byte
	for _, i := range r.Perm(8000)[:300] {
		del = append(del, fmtKey("lvl", i))
	}
	return deleteKeys(db, del)
}

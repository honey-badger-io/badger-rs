package main

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2"
)

// Value meta bits, copied from badger's structs.go (unexported there).
const (
	bitDelete                 byte = 1 << 0
	bitValuePointer           byte = 1 << 1
	bitDiscardEarlierVersions byte = 1 << 2
)

type sstScenario struct {
	name        string
	desc        string
	compression options.CompressionType
	blockSize   int
	encrypt     bool
	numKeys     int
	text        bool // compressible values
}

func sstScenarios() []sstScenario {
	return []sstScenario{
		{name: "plain", desc: "no compression, 4 KiB blocks", compression: options.None, blockSize: 4 << 10, numKeys: 1500},
		{name: "small-blocks", desc: "no compression, 256-byte blocks (many blocks)", compression: options.None, blockSize: 256, numKeys: 600},
		{name: "snappy", desc: "snappy-compressed blocks", compression: options.Snappy, blockSize: 4 << 10, numKeys: 1500, text: true},
		{name: "zstd", desc: "zstd-compressed blocks", compression: options.ZSTD, blockSize: 4 << 10, numKeys: 1500, text: true},
		{name: "encrypted", desc: "AES-encrypted blocks and index", compression: options.None, blockSize: 4 << 10, encrypt: true, numKeys: 1000},
		{name: "encrypted-zstd", desc: "zstd compression then AES encryption", compression: options.ZSTD, blockSize: 4 << 10, encrypt: true, numKeys: 1000, text: true},
	}
}

func genSST(out string, s sstScenario, r *rand.Rand) (IndexEntry, error) {
	rel := filepath.Join("sst", s.name)
	entry := IndexEntry{
		Name: s.name, Kind: "sst", Description: s.desc,
		Path: filepath.Join(rel, table.IDToFilename(1)), Expected: rel + ".json",
	}
	if err := os.MkdirAll(filepath.Join(out, rel), 0o755); err != nil {
		return entry, err
	}

	exp := SSTExpected{
		FormatVersion:      FormatVersion,
		Name:               s.name,
		Compression:        compressionName(s.compression),
		BlockSize:          s.blockSize,
		BloomFalsePositive: 0.01,
		TableSize:          2 << 20,
		Entries:            sstEntries(r, s),
	}
	opts := table.Options{
		TableSize:            exp.TableSize,
		BlockSize:            s.blockSize,
		BloomFalsePositive:   exp.BloomFalsePositive,
		Compression:          s.compression,
		ZSTDCompressionLevel: 1,
		ChkMode:              options.OnTableAndBlockRead,
	}
	if s.encrypt {
		exp.DataKey = &DataKey{KeyID: 7, Data: seqBytes(0x20, 32), IV: seqBytes(0x80, 16), CreatedAt: 1700000000}
		opts.DataKey = &pb.DataKey{
			KeyId: exp.DataKey.KeyID, Data: exp.DataKey.Data, Iv: exp.DataKey.IV, CreatedAt: exp.DataKey.CreatedAt,
		}
		cache, err := ristretto.NewCache(&ristretto.Config[uint64, *fb.TableIndex]{
			NumCounters: 1000, MaxCost: 1 << 20, BufferItems: 64,
		})
		if err != nil {
			return entry, err
		}
		defer cache.Close()
		opts.IndexCache = cache
	}

	b := table.NewTableBuilder(opts)
	defer b.Close()
	for _, e := range exp.Entries {
		b.Add(y.KeyWithTs(e.Key, e.Version), y.ValueStruct{
			Meta: e.Meta, UserMeta: e.UserMeta, ExpiresAt: e.ExpiresAt, Value: e.Value,
		}, e.ValueLen)
	}
	tbl, err := table.CreateTable(filepath.Join(out, entry.Path), b)
	if err != nil {
		return entry, err
	}
	defer tbl.MmapFile.Close(-1) // not DecrRef: that would delete the file

	if err := checkTable(tbl, exp.Entries); err != nil {
		return entry, err
	}
	exp.Smallest = tbl.Smallest()
	exp.Biggest = tbl.Biggest()
	exp.KeyCount = tbl.KeyCount()
	exp.MaxVersion = tbl.MaxVersion()
	return entry, writeEntriesJSON(filepath.Join(out, entry.Expected), exp)
}

// sstEntries returns entries in table order: user key ascending, version descending.
func sstEntries(r *rand.Rand, s sstScenario) []SSTEntry {
	var es []SSTEntry
	for i := 0; i < s.numKeys; i++ {
		key := fmt.Appendf(nil, "sst-%06d", i*3)
		if i%10 == 0 {
			key = append(key, randBytes(r, 1+r.IntN(16))...) // binary suffix
		}
		version := uint64(100 + r.IntN(900))
		for v := 0; v < 1+r.IntN(3); v++ {
			e := SSTEntry{Key: key, Version: version}
			version -= uint64(1 + r.IntN(30))
			switch k := r.IntN(20); {
			case k < 2:
				e.Meta = bitDelete
			case k < 5:
				e.Meta = bitValuePointer
				e.Value = randBytes(r, 12) // stands in for an encoded valuePointer
				e.ValueLen = uint32(100 + r.IntN(5000))
			case k < 6:
				e.Meta = bitDiscardEarlierVersions
				e.Value = randBytes(r, r.IntN(64))
			default:
				if s.text {
					e.Value = textBytes(r, r.IntN(300))
				} else {
					e.Value = randBytes(r, r.IntN(300))
				}
			}
			if r.IntN(5) == 0 {
				e.UserMeta = byte(1 + r.IntN(255))
			}
			if r.IntN(10) == 0 {
				e.ExpiresAt = uint64(1_600_000_000 + r.IntN(1_000_000_000))
			}
			es = append(es, e)
		}
	}
	return es
}

// checkTable reads tbl back with Go Badger and compares it to want.
func checkTable(tbl *table.Table, want []SSTEntry) error {
	it := tbl.NewIterator(0)
	defer it.Close()
	i := 0
	for it.Rewind(); it.Valid(); it.Next() {
		if i >= len(want) {
			return fmt.Errorf("table has more than the %d entries written", len(want))
		}
		w := want[i]
		k, v := it.Key(), it.ValueCopy()
		if !bytes.Equal(y.ParseKey(k), w.Key) || y.ParseTs(k) != w.Version {
			return fmt.Errorf("entry %d: key %x@%d, want %x@%d", i, y.ParseKey(k), y.ParseTs(k), w.Key, w.Version)
		}
		if v.Meta != w.Meta || v.UserMeta != w.UserMeta || v.ExpiresAt != w.ExpiresAt || !bytes.Equal(v.Value, w.Value) {
			return fmt.Errorf("entry %d (%x@%d): value differs", i, w.Key, w.Version)
		}
		i++
	}
	if i != len(want) {
		return fmt.Errorf("table has %d entries, want %d", i, len(want))
	}
	return nil
}

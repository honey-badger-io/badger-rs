package main

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
	fbs "github.com/google/flatbuffers/go"
	"google.golang.org/protobuf/proto"
)

// Golden files hold exact outputs of Go Badger's building blocks, for
// bit-for-bit comparison in Rust unit tests. Unlike `gen`, `golden` is fully
// deterministic: rerunning it must not change any file.

type goldenY struct {
	Hash            []goldenHash        `json:"hash"`
	Bloom           []goldenBloom       `json:"bloom"`
	BloomBitsPerKey []goldenBitsPerKey  `json:"bloom_bits_per_key"`
	Checksum        []goldenChecksum    `json:"checksum"`
	AESCTR          []goldenAES         `json:"aes_ctr"`
	ValueStruct     []goldenValueStruct `json:"value_struct"`
	KeyWithTs       []goldenKeyWithTs   `json:"key_with_ts"`
	Uvarint         []goldenUvarint     `json:"uvarint"`
	CompareKeys     []goldenCompareKeys `json:"compare_keys"`
	Proto           []goldenProto       `json:"proto"`
	TableIndex      goldenTableIndex    `json:"table_index"`
}

type goldenHash struct {
	Input Hex    `json:"input"`
	Hash  uint32 `json:"hash"`
}

type goldenBloom struct {
	Keys       []Hex         `json:"keys"`
	BitsPerKey int           `json:"bits_per_key"`
	Filter     Hex           `json:"filter"`
	Probes     []goldenProbe `json:"probes"`
}

type goldenProbe struct {
	Key        Hex  `json:"key"`
	MayContain bool `json:"may_contain"`
}

type goldenBitsPerKey struct {
	NumEntries int     `json:"num_entries"`
	FP         float64 `json:"fp"`
	Bits       int     `json:"bits"`
}

type goldenChecksum struct {
	Data     Hex    `json:"data"`
	CRC32C   uint64 `json:"crc32c"`
	XXHash64 uint64 `json:"xxhash64"`
}

type goldenAES struct {
	Key        Hex `json:"key"`
	IV         Hex `json:"iv"`
	Plaintext  Hex `json:"plaintext"`
	Ciphertext Hex `json:"ciphertext"`
}

type goldenValueStruct struct {
	Meta      byte   `json:"meta"`
	UserMeta  byte   `json:"user_meta"`
	ExpiresAt uint64 `json:"expires_at"`
	Value     Hex    `json:"value"`
	Encoded   Hex    `json:"encoded"`
}

type goldenKeyWithTs struct {
	Key     Hex    `json:"key"`
	Version uint64 `json:"version"`
	Encoded Hex    `json:"encoded"`
}

type goldenUvarint struct {
	Value   uint64 `json:"value"`
	Encoded Hex    `json:"encoded"`
}

type goldenCompareKeys struct {
	A   Hex `json:"a"`
	B   Hex `json:"b"`
	Cmp int `json:"cmp"`
}

// goldenProto is one message, constructed identically in Go and in the Rust test.
type goldenProto struct {
	Name    string `json:"name"`
	Encoded Hex    `json:"encoded"`
}

type goldenTableIndex struct {
	Offsets          []goldenBlockOffset `json:"offsets"`
	BloomFilter      Hex                 `json:"bloom_filter"`
	MaxVersion       uint64              `json:"max_version"`
	KeyCount         uint32              `json:"key_count"`
	UncompressedSize uint32              `json:"uncompressed_size"`
	OnDiskSize       uint32              `json:"on_disk_size"` // before adding the index length
	StaleDataSize    uint32              `json:"stale_data_size"`
	Encoded          Hex                 `json:"encoded"` // as table.Builder.buildIndex produces it
}

type goldenBlockOffset struct {
	Key    Hex    `json:"key"`
	Offset uint32 `json:"offset"`
	Len    uint32 `json:"len"`
}

func goldenCmd(out string) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	r := rand.New(rand.NewPCG(0x601de, 1))
	g := goldenY{}

	// Hash: every length 0..40 plus the leveldb test vectors.
	for _, s := range []string{"", "g", "go", "gop", "goph", "gophe", "gopher",
		"I had a dream it would end this way."} {
		g.Hash = append(g.Hash, goldenHash{Input: []byte(s), Hash: y.Hash([]byte(s))})
	}
	for n := 0; n <= 40; n++ {
		b := randBytes(r, n)
		g.Hash = append(g.Hash, goldenHash{Input: b, Hash: y.Hash(b)})
	}

	// Bloom filters of several sizes and bits-per-key.
	for _, tc := range []struct{ n, bpk int }{{0, 10}, {1, 10}, {2, 10}, {10, 10}, {100, 7}, {1000, 10}, {50, 0}, {50, 50}} {
		keys := []Hex{} // not nil: JSON [] rather than null
		var hashes []uint32
		for i := 0; i < tc.n; i++ {
			k := randBytes(r, 1+r.IntN(24))
			keys = append(keys, k)
			hashes = append(hashes, y.Hash(k))
		}
		f := y.NewFilter(hashes, tc.bpk)
		gb := goldenBloom{Keys: keys, BitsPerKey: tc.bpk, Filter: Hex(f)}
		for i := 0; i < 50; i++ {
			k := randBytes(r, 1+r.IntN(24))
			if i < len(keys) {
				k = keys[i]
			}
			gb.Probes = append(gb.Probes, goldenProbe{Key: k, MayContain: f.MayContainKey(k)})
		}
		g.Bloom = append(g.Bloom, gb)
	}
	for _, n := range []int{1, 10, 100, 1000, 12345} {
		for _, fp := range []float64{0.5, 0.1, 0.01, 0.001, 0.0001} {
			g.BloomBitsPerKey = append(g.BloomBitsPerKey, goldenBitsPerKey{n, fp, y.BloomBitsPerKey(n, fp)})
		}
	}

	// Checksums.
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	for _, n := range []int{0, 1, 3, 8, 15, 16, 31, 64, 100, 1000, 4096} {
		b := randBytes(r, n)
		g.Checksum = append(g.Checksum, goldenChecksum{
			Data: b, CRC32C: uint64(crc32.Checksum(b, castagnoli)), XXHash64: xxhash.Sum64(b),
		})
		if c := y.CalculateChecksum(b, pb.Checksum_CRC32C); c != g.Checksum[len(g.Checksum)-1].CRC32C {
			return fmt.Errorf("crc32c mismatch with y.CalculateChecksum")
		}
	}

	// AES-CTR, as y.XORBlock: all key sizes, lengths that are not block multiples,
	// and an IV that overflows its low 64 bits.
	for _, tc := range []struct {
		keyLen, n int
		iv        []byte
	}{
		{16, 0, nil}, {16, 1, nil}, {16, 33, nil}, {24, 100, nil}, {32, 16, nil}, {32, 1000, nil},
		{32, 64, append(make([]byte, 8), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe)},
		{16, 64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	} {
		key, pt := randBytes(r, tc.keyLen), randBytes(r, tc.n)
		iv := tc.iv
		if iv == nil {
			iv = randBytes(r, aes.BlockSize)
		}
		ct := make([]byte, len(pt))
		if err := y.XORBlock(ct, pt, key, iv); err != nil {
			return err
		}
		block, _ := aes.NewCipher(key)
		check := make([]byte, len(pt))
		cipher.NewCTR(block, iv).XORKeyStream(check, ct)
		if string(check) != string(pt) {
			return fmt.Errorf("aes-ctr round trip failed")
		}
		g.AESCTR = append(g.AESCTR, goldenAES{Key: key, IV: iv, Plaintext: pt, Ciphertext: ct})
	}

	// ValueStruct encoding.
	for _, tc := range []y.ValueStruct{
		{},
		{Meta: 1},
		{Meta: 2, UserMeta: 0xff, ExpiresAt: 127, Value: []byte("v")},
		{Meta: 0x40, UserMeta: 3, ExpiresAt: 128, Value: randBytes(r, 20)},
		{ExpiresAt: math.MaxUint64, Value: randBytes(r, 300)},
		{UserMeta: 7, ExpiresAt: 4102444800, Value: []byte("hello world")},
	} {
		buf := make([]byte, tc.EncodedSize())
		n := tc.Encode(buf)
		g.ValueStruct = append(g.ValueStruct, goldenValueStruct{
			Meta: tc.Meta, UserMeta: tc.UserMeta, ExpiresAt: tc.ExpiresAt, Value: tc.Value, Encoded: buf[:n],
		})
	}

	// Keys.
	for _, tc := range []struct {
		k string
		v uint64
	}{{"", 0}, {"a", 0}, {"a", 1}, {"key", 42}, {"key", math.MaxUint64}, {"\x00\xff", 1 << 40}} {
		g.KeyWithTs = append(g.KeyWithTs, goldenKeyWithTs{Key: []byte(tc.k), Version: tc.v, Encoded: y.KeyWithTs([]byte(tc.k), tc.v)})
	}
	for _, v := range []uint64{0, 1, 127, 128, 255, 300, 16383, 16384, 1 << 32, math.MaxUint64} {
		g.Uvarint = append(g.Uvarint, goldenUvarint{Value: v, Encoded: binary.AppendUvarint(nil, v)})
	}
	cmpKeys := [][2][]byte{
		{y.KeyWithTs([]byte("a"), 1), y.KeyWithTs([]byte("a"), 2)},
		{y.KeyWithTs([]byte("a"), 5), y.KeyWithTs([]byte("aa"), 5)},
		{y.KeyWithTs([]byte("a"), 1), y.KeyWithTs([]byte("aa"), 9)},
		{y.KeyWithTs([]byte("b"), 1), y.KeyWithTs([]byte("a"), 1)},
		{y.KeyWithTs([]byte("k"), 7), y.KeyWithTs([]byte("k"), 7)},
		{y.KeyWithTs([]byte(""), 3), y.KeyWithTs([]byte("\x00"), 3)},
	}
	for _, p := range cmpKeys {
		g.CompareKeys = append(g.CompareKeys, goldenCompareKeys{A: p[0], B: p[1], Cmp: y.CompareKeys(p[0], p[1])})
	}

	// Protobuf messages. Keep in sync with crates/badger/src/pb.rs tests.
	for _, m := range goldenProtoMessages() {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m.msg)
		if err != nil {
			return err
		}
		g.Proto = append(g.Proto, goldenProto{Name: m.name, Encoded: b})
	}

	g.TableIndex = goldenIndex()
	return writeJSON(filepath.Join(out, "phase1.json"), g)
}

type namedProto struct {
	name string
	msg  proto.Message
}

func goldenProtoMessages() []namedProto {
	return []namedProto{
		{"checksum_crc32c", &pb.Checksum{Algo: pb.Checksum_CRC32C, Sum: 0xdeadbeef}},
		{"checksum_xxhash64", &pb.Checksum{Algo: pb.Checksum_XXHash64, Sum: 1<<63 + 5}},
		{"data_key", &pb.DataKey{KeyId: 7, Data: seqBytes(0x20, 32), Iv: seqBytes(0x80, 16), CreatedAt: 1700000000}},
		{"data_key_negative_created_at", &pb.DataKey{KeyId: 1, CreatedAt: -1}},
		{"manifest_change_set", &pb.ManifestChangeSet{Changes: []*pb.ManifestChange{
			{Id: 1, Op: pb.ManifestChange_CREATE, Level: 0, Compression: 1},
			{Id: 2, Op: pb.ManifestChange_DELETE},
			{Id: 3, Op: pb.ManifestChange_CREATE, Level: 6, KeyId: 9, EncryptionAlgo: pb.EncryptionAlgo_aes, Compression: 2},
		}}},
		{"kv_list", &pb.KVList{Kv: []*pb.KV{
			{Key: []byte("k1"), Value: []byte("v1"), UserMeta: []byte{1}, Version: 5, ExpiresAt: 100, Meta: []byte{2}, StreamId: 3},
			{Key: []byte("k2"), StreamDone: true},
		}, AllocRef: 77}},
		{"match", &pb.Match{Prefix: []byte("abc"), IgnoreBytes: "1, 3-4"}},
		{"empty_kv", &pb.KV{}},
	}
}

// goldenIndex builds a TableIndex exactly like table.Builder.buildIndex.
func goldenIndex() goldenTableIndex {
	gi := goldenTableIndex{
		Offsets: []goldenBlockOffset{
			{Key: y.KeyWithTs([]byte("alpha"), 9), Offset: 0, Len: 4000},
			{Key: y.KeyWithTs([]byte("bravo"), 3), Offset: 4000, Len: 4090},
			{Key: y.KeyWithTs([]byte("charlie-with-a-longer-key"), 1), Offset: 8090, Len: 123},
		},
		BloomFilter:      seqBytes(0x10, 21),
		MaxVersion:       9,
		KeyCount:         321,
		UncompressedSize: 8213,
		OnDiskSize:       8213,
		StaleDataSize:    17,
	}
	builder := fbs.NewBuilder(3 << 20)
	var uoffs []fbs.UOffsetT
	for _, o := range gi.Offsets {
		k := builder.CreateByteVector(o.Key)
		fb.BlockOffsetStart(builder)
		fb.BlockOffsetAddKey(builder, k)
		fb.BlockOffsetAddOffset(builder, o.Offset)
		fb.BlockOffsetAddLen(builder, o.Len)
		uoffs = append(uoffs, fb.BlockOffsetEnd(builder))
	}
	fb.TableIndexStartOffsetsVector(builder, len(uoffs))
	for i := len(uoffs) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(uoffs[i])
	}
	boEnd := builder.EndVector(len(uoffs))
	bfoff := builder.CreateByteVector(gi.BloomFilter)
	fb.TableIndexStart(builder)
	fb.TableIndexAddOffsets(builder, boEnd)
	fb.TableIndexAddBloomFilter(builder, bfoff)
	fb.TableIndexAddMaxVersion(builder, gi.MaxVersion)
	fb.TableIndexAddUncompressedSize(builder, gi.UncompressedSize)
	fb.TableIndexAddKeyCount(builder, gi.KeyCount)
	fb.TableIndexAddOnDiskSize(builder, gi.OnDiskSize)
	fb.TableIndexAddStaleDataSize(builder, gi.StaleDataSize)
	builder.Finish(fb.TableIndexEnd(builder))
	buf := builder.FinishedBytes()
	index := fb.GetRootAsTableIndex(buf, 0)
	y.AssertTrue(index.MutateOnDiskSize(index.OnDiskSize() + uint32(len(buf))))
	gi.Encoded = buf
	return gi
}

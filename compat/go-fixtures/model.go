package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// FormatVersion is bumped whenever the JSON layout below changes.
const FormatVersion = 1

// BadgerVersion is the Go Badger release the fixtures are produced with.
const BadgerVersion = "v4.9.6"

// Hex is a byte slice that is encoded as a lowercase hex string in JSON.
type Hex []byte

func (h Hex) MarshalJSON() ([]byte, error) { return json.Marshal(hex.EncodeToString(h)) }

func (h *Hex) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	d, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	*h = d
	return nil
}

// Index is fixtures/index.json.
type Index struct {
	FormatVersion int          `json:"format_version"`
	BadgerVersion string       `json:"badger_version"`
	Fixtures      []IndexEntry `json:"fixtures"`
}

type IndexEntry struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"` // "db" or "sst"
	Description string `json:"description"`
	Path        string `json:"path"`     // relative to the index: DB directory or .sst file
	Expected    string `json:"expected"` // relative to the index: expected-contents JSON
}

// DBOptions are the options a reader needs to open a fixture DB.
type DBOptions struct {
	Compression       string `json:"compression"` // none | snappy | zstd (used for writing)
	EncryptionKey     Hex    `json:"encryption_key"`
	ValueThreshold    int64  `json:"value_threshold"`
	MemTableSize      int64  `json:"mem_table_size"`
	ValueLogFileSize  int64  `json:"value_log_file_size"`
	BaseTableSize     int64  `json:"base_table_size"`
	NumVersionsToKeep int    `json:"num_versions_to_keep"`
	Managed           bool   `json:"managed"`
}

// DBExpected is the expected contents of a fixture DB, as seen through an
// iterator with AllVersions=true at the newest read timestamp.
type DBExpected struct {
	FormatVersion int       `json:"format_version"`
	Name          string    `json:"name"`
	Options       DBOptions `json:"options"`
	// CrashSnapshot is true when the directory was copied from a live DB
	// without closing it (unflushed memtable WAL, untruncated vlog). Go cannot
	// open such a directory read-only; the expected contents come from a
	// recovered copy.
	CrashSnapshot bool      `json:"crash_snapshot"`
	MaxVersion    uint64    `json:"max_version"`
	Entries       []DBEntry `json:"entries"`
}

type DBEntry struct {
	Key                    Hex    `json:"key"`
	Version                uint64 `json:"version"`
	Value                  Hex    `json:"value"`
	UserMeta               byte   `json:"user_meta"`
	ExpiresAt              uint64 `json:"expires_at"`
	DeletedOrExpired       bool   `json:"deleted_or_expired"`
	DiscardEarlierVersions bool   `json:"discard_earlier_versions"`
}

// SSTExpected describes a standalone table built with table.Builder.
type SSTExpected struct {
	FormatVersion      int        `json:"format_version"`
	Name               string     `json:"name"`
	Compression        string     `json:"compression"`
	BlockSize          int        `json:"block_size"`
	BloomFalsePositive float64    `json:"bloom_false_positive"`
	TableSize          uint64     `json:"table_size"`
	DataKey            *DataKey   `json:"data_key"` // nil when unencrypted
	Smallest           Hex        `json:"smallest"` // internal key (user key + 8-byte ts)
	Biggest            Hex        `json:"biggest"`
	KeyCount           uint32     `json:"key_count"`
	MaxVersion         uint64     `json:"max_version"`
	Entries            []SSTEntry `json:"entries"`
}

type DataKey struct {
	KeyID     uint64 `json:"key_id"`
	Data      Hex    `json:"data"`
	IV        Hex    `json:"iv"`
	CreatedAt int64  `json:"created_at"`
}

// SSTEntry is one entry in table order (user key ascending, version descending).
type SSTEntry struct {
	Key       Hex    `json:"key"` // user key, without timestamp
	Version   uint64 `json:"version"`
	Meta      byte   `json:"meta"`
	UserMeta  byte   `json:"user_meta"`
	ExpiresAt uint64 `json:"expires_at"`
	Value     Hex    `json:"value"`
	// ValueLen is the valueLen argument passed to Builder.Add: the length of
	// the value in the vlog for value-pointer entries, 0 otherwise.
	ValueLen uint32 `json:"value_len"`
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// writeEntriesJSON writes a fixture file compactly with one entry per line,
// which keeps files small while still diffing sensibly.
func writeEntriesJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = bytes.ReplaceAll(b, []byte(`[{"key":`), []byte("[\n{\"key\":"))
	b = bytes.ReplaceAll(b, []byte(`},{"key":`), []byte("},\n{\"key\":"))
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("parsing %s: %w", path, err)
	}
	return nil
}

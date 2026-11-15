package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

func compressionFromName(name string) (options.CompressionType, error) {
	switch name {
	case "", "none":
		return options.None, nil
	case "snappy":
		return options.Snappy, nil
	case "zstd":
		return options.ZSTD, nil
	}
	return 0, fmt.Errorf("unknown compression %q", name)
}

func compressionName(c options.CompressionType) string {
	switch c {
	case options.None:
		return "none"
	case options.Snappy:
		return "snappy"
	case options.ZSTD:
		return "zstd"
	}
	return fmt.Sprintf("unknown(%d)", c)
}

// badgerOptions turns fixture options into Go Badger options for dir.
func badgerOptions(dir string, o DBOptions) (badger.Options, error) {
	c, err := compressionFromName(o.Compression)
	if err != nil {
		return badger.Options{}, err
	}
	opts := badger.DefaultOptions(dir).
		WithLoggingLevel(badger.WARNING).
		WithMetricsEnabled(false).
		WithCompression(c).
		WithNumCompactors(2)
	if o.MemTableSize > 0 {
		opts = opts.WithMemTableSize(o.MemTableSize)
	}
	if o.ValueLogFileSize > 0 {
		opts = opts.WithValueLogFileSize(o.ValueLogFileSize)
	}
	if o.BaseTableSize > 0 {
		opts = opts.WithBaseTableSize(o.BaseTableSize)
	}
	if o.ValueThreshold > 0 {
		opts = opts.WithValueThreshold(o.ValueThreshold)
	}
	if o.NumVersionsToKeep > 0 {
		opts = opts.WithNumVersionsToKeep(o.NumVersionsToKeep)
	}
	if len(o.EncryptionKey) > 0 {
		opts = opts.WithEncryptionKey(o.EncryptionKey).WithIndexCacheSize(1 << 20)
	}
	return opts, nil
}

// dumpDB opens dir and returns every version of every key, newest first per key.
func dumpDB(dir string, o DBOptions, readOnly bool) (maxVersion uint64, entries []DBEntry, err error) {
	opts, err := badgerOptions(dir, o)
	if err != nil {
		return 0, nil, err
	}
	db, err := badger.Open(opts.WithReadOnly(readOnly))
	if err != nil {
		return 0, nil, fmt.Errorf("opening %s: %w", dir, err)
	}
	defer func() {
		if cerr := db.Close(); err == nil {
			err = cerr
		}
	}()

	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{AllVersions: true})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("reading value of %x@%d: %w", item.Key(), item.Version(), err)
			}
			entries = append(entries, DBEntry{
				Key:                    item.KeyCopy(nil),
				Version:                item.Version(),
				Value:                  val,
				UserMeta:               item.UserMeta(),
				ExpiresAt:              item.ExpiresAt(),
				DeletedOrExpired:       item.IsDeletedOrExpired(),
				DiscardEarlierVersions: item.DiscardEarlierVersions(),
			})
		}
		return nil
	})
	return db.MaxVersion(), entries, err
}

func dumpCmd(dir, optsPath string) error {
	var exp DBExpected
	if optsPath != "" {
		if err := readJSON(optsPath, &exp); err != nil {
			return err
		}
	}
	maxVersion, entries, err := dumpDB(dir, exp.Options, true)
	if err != nil {
		return err
	}
	exp.FormatVersion = FormatVersion
	exp.MaxVersion = maxVersion
	exp.Entries = entries
	return writeJSON("/dev/stdout", exp)
}

func verifyCmd(dir, expectedPath string) error {
	var exp DBExpected
	if err := readJSON(expectedPath, &exp); err != nil {
		return err
	}
	_, got, err := dumpDB(dir, exp.Options, true)
	if err != nil {
		return err
	}
	if diffs := diffEntries(exp.Entries, got); len(diffs) > 0 {
		for _, d := range diffs {
			fmt.Fprintln(os.Stderr, d)
		}
		return fmt.Errorf("%s does not match %s", dir, expectedPath)
	}
	fmt.Printf("ok: %s matches %s (%d entries)\n", dir, expectedPath, len(got))
	return nil
}

func diffEntries(want, got []DBEntry) []string {
	const maxDiffs = 20
	var diffs []string
	add := func(format string, args ...any) {
		if len(diffs) < maxDiffs {
			diffs = append(diffs, fmt.Sprintf(format, args...))
		}
	}
	if len(want) != len(got) {
		add("entry count: want %d, got %d", len(want), len(got))
	}
	for i := 0; i < len(want) && i < len(got); i++ {
		w, g := want[i], got[i]
		if !bytes.Equal(w.Key, g.Key) || w.Version != g.Version {
			add("#%d: want key %x@%d, got %x@%d", i, w.Key, w.Version, g.Key, g.Version)
			continue
		}
		if !bytes.Equal(w.Value, g.Value) {
			add("#%d %x@%d: value differs (want %d bytes, got %d bytes)", i, w.Key, w.Version, len(w.Value), len(g.Value))
		}
		if w.UserMeta != g.UserMeta || w.ExpiresAt != g.ExpiresAt ||
			w.DeletedOrExpired != g.DeletedOrExpired || w.DiscardEarlierVersions != g.DiscardEarlierVersions {
			add("#%d %x@%d: metadata differs: want %+v, got %+v", i, w.Key, w.Version,
				[]any{w.UserMeta, w.ExpiresAt, w.DeletedOrExpired, w.DiscardEarlierVersions},
				[]any{g.UserMeta, g.ExpiresAt, g.DeletedOrExpired, g.DiscardEarlierVersions})
		}
	}
	return diffs
}

// hashDir returns a digest of every file name and content under dir, used to
// assert that a read-only open does not modify a fixture.
func hashDir(dir string) (string, error) {
	var names []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			names = append(names, p)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		rel, _ := filepath.Rel(dir, n)
		fmt.Fprintf(h, "%s\x00", rel)
		f, err := os.Open(n)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// copyDir copies the regular files of src into dst (created), skipping the
// LOCK file so the copy can be opened while src is still open.
func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	ents, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir() || e.Name() == "LOCK" {
			continue
		}
		if err := copyFile(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

var errNotFixtureDir = errors.New("refusing to clean a directory that does not look like a fixtures directory")

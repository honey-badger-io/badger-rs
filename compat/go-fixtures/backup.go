package main

import (
	"bytes"
	"fmt"
	"math"
	"os"

	badger "github.com/dgraph-io/badger/v4"
)

// backupCmd writes a full Go backup of the DB in dir (opened read-only with
// the options in expectedPath) to out.
func backupCmd(dir, expectedPath, out string) (err error) {
	var exp DBExpected
	if err := readJSON(expectedPath, &exp); err != nil {
		return err
	}
	opts, err := badgerOptions(dir, exp.Options)
	if err != nil {
		return err
	}
	opts = opts.WithReadOnly(true)
	var db *badger.DB
	if exp.Options.Managed {
		db, err = badger.OpenManaged(opts)
	} else {
		db, err = badger.Open(opts)
	}
	if err != nil {
		return fmt.Errorf("opening %s: %w", dir, err)
	}
	defer func() {
		if cerr := db.Close(); err == nil {
			err = cerr
		}
	}()
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	if exp.Options.Managed {
		_, err = db.NewStreamAt(math.MaxUint64).Backup(f, 0)
	} else {
		_, err = db.Backup(f, 0)
	}
	return err
}

// verifyBackupCmd loads the backup in bakPath into a fresh DB and checks that
// its latest visible entries match those of the DB described by expectedPath.
func verifyBackupCmd(bakPath, expectedPath string) (err error) {
	var exp DBExpected
	if err := readJSON(expectedPath, &exp); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp("", "verify-backup")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	opts := badger.DefaultOptions(tmp).WithLoggingLevel(badger.WARNING).WithMetricsEnabled(false)
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); err == nil {
			err = cerr
		}
	}()
	f, err := os.Open(bakPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := db.Load(f, 16); err != nil {
		return fmt.Errorf("loading %s: %w", bakPath, err)
	}

	var got []DBEntry
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			got = append(got, DBEntry{
				Key:                    item.KeyCopy(nil),
				Version:                item.Version(),
				Value:                  val,
				UserMeta:               item.UserMeta(),
				ExpiresAt:              item.ExpiresAt(),
				DiscardEarlierVersions: item.DiscardEarlierVersions(),
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	want := liveEntries(exp.Entries)
	if diffs := diffEntries(want, got); len(diffs) > 0 {
		for _, d := range diffs {
			fmt.Fprintln(os.Stderr, d)
		}
		return fmt.Errorf("backup %s does not match %s", bakPath, expectedPath)
	}
	fmt.Printf("ok: backup %s matches %s (%d live keys)\n", bakPath, expectedPath, len(got))
	return nil
}

// liveEntries keeps the newest version of each key, unless it is deleted or
// expired: what a latest-version iteration returns. entries are sorted by
// key, newest version first.
func liveEntries(entries []DBEntry) []DBEntry {
	var out []DBEntry
	var prev []byte
	for i, e := range entries {
		if i > 0 && bytes.Equal(e.Key, prev) {
			continue
		}
		prev = e.Key
		if !e.DeletedOrExpired {
			out = append(out, e)
		}
	}
	return out
}

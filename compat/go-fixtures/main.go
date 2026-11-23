// Command go-fixtures produces and checks on-disk fixtures for the Rust port of
// Badger, using the real Go implementation (github.com/dgraph-io/badger/v4 v4.9.6).
//
//	go-fixtures gen <out-dir> [name...]       regenerate every fixture (or only the
//	                                          named ones) into <out-dir>
//	go-fixtures golden <out-dir>              write deterministic golden values (phase1.json)
//	go-fixtures dump <db-dir> [options.json]  print a DB's contents as fixture JSON
//	go-fixtures verify <db-dir> <expected.json>
//	                                          open <db-dir> with Go Badger (read-only)
//	                                          and compare it to <expected.json>
//	go-fixtures verify-sst <file.sst> <expected.json>
//	                                          open a table with Go Badger and compare it
//	go-fixtures backup <db-dir> <expected.json> <out.bak>
//	                                          write a full Go backup of a DB
//	go-fixtures verify-backup <file.bak> <expected.json>
//	                                          load a backup into a fresh DB and compare
//	                                          its latest entries with the source DB's
//
// The JSON formats are documented in ../README.md and mirrored by the
// `badger-fixtures` Rust crate.
package main

import (
	"fmt"
	"os"
)

const usage = `usage:
  go-fixtures gen <out-dir> [name...]
  go-fixtures golden <out-dir>
  go-fixtures dump <db-dir> [expected-or-options.json]
  go-fixtures verify <db-dir> <expected.json>
  go-fixtures verify-sst <file.sst> <expected.json>
  go-fixtures backup <db-dir> <expected.json> <out.bak>
  go-fixtures verify-backup <file.bak> <expected.json>
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "gen":
		if len(rest) < 1 {
			return fmt.Errorf("gen takes an output directory\n%s", usage)
		}
		return generate(rest[0], rest[1:]...)
	case "golden":
		if len(rest) != 1 {
			return fmt.Errorf("golden takes exactly one argument\n%s", usage)
		}
		return goldenCmd(rest[0])
	case "dump":
		if len(rest) < 1 || len(rest) > 2 {
			return fmt.Errorf("dump takes one or two arguments\n%s", usage)
		}
		return dumpCmd(rest[0], optionalArg(rest, 1))
	case "verify":
		if len(rest) != 2 {
			return fmt.Errorf("verify takes exactly two arguments\n%s", usage)
		}
		return verifyCmd(rest[0], rest[1])
	case "verify-sst":
		if len(rest) != 2 {
			return fmt.Errorf("verify-sst takes exactly two arguments\n%s", usage)
		}
		return verifySSTCmd(rest[0], rest[1])
	case "backup":
		if len(rest) != 3 {
			return fmt.Errorf("backup takes exactly three arguments\n%s", usage)
		}
		return backupCmd(rest[0], rest[1], rest[2])
	case "verify-backup":
		if len(rest) != 2 {
			return fmt.Errorf("verify-backup takes exactly two arguments\n%s", usage)
		}
		return verifyBackupCmd(rest[0], rest[1])
	default:
		return fmt.Errorf("unknown command %q\n%s", cmd, usage)
	}
}

func optionalArg(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}
	return ""
}

// Command go-fixtures produces and checks on-disk fixtures for the Rust port of
// Badger, using the real Go implementation (github.com/dgraph-io/badger/v4 v4.9.6).
//
//	go-fixtures gen <out-dir>                 regenerate every fixture into <out-dir>
//	go-fixtures dump <db-dir> [options.json]  print a DB's contents as fixture JSON
//	go-fixtures verify <db-dir> <expected.json>
//	                                          open <db-dir> with Go Badger (read-only)
//	                                          and compare it to <expected.json>
//
// The JSON formats are documented in ../README.md and mirrored by the
// `badger-fixtures` Rust crate.
package main

import (
	"fmt"
	"os"
)

const usage = `usage:
  go-fixtures gen <out-dir>
  go-fixtures dump <db-dir> [expected-or-options.json]
  go-fixtures verify <db-dir> <expected.json>
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
		if len(rest) != 1 {
			return fmt.Errorf("gen takes exactly one argument\n%s", usage)
		}
		return generate(rest[0])
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

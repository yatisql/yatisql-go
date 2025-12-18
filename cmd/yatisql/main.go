// yatisql - yet another tabular inefficient SQL
//
// A simple Go CLI tool that streams CSV/TSV files into SQLite,
// executes SQL queries, and exports results back to CSV/TSV format.
package main

import (
	"os"

	"github.com/fatih/color"

	"github.com/yatisql/yatisql-go/internal/cli"
)

// Version information (set via ldflags at build time)
// These variables are intentionally unused in code but set via ldflags
var (
	version   = "dev"     //nolint:unused // Set via ldflags
	buildTime = "unknown" //nolint:unused // Set via ldflags
)

func main() {
	if err := cli.Execute(); err != nil {
		errorColor := color.New(color.FgRed, color.Bold)
		_, _ = errorColor.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

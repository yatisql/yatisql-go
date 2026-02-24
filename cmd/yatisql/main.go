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
var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	cli.SetBuildInfo(version, buildTime)
	if err := cli.Execute(); err != nil {
		errorColor := color.New(color.FgRed, color.Bold)
		_, _ = errorColor.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

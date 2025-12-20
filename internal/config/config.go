// Package config provides configuration types and parsing for yatisql.
package config

import (
	"fmt"
	"strings"
)

// Config holds all configuration options for yatisql.
type Config struct {
	InputFiles   []string
	OutputFiles  []string // Multiple output files, one per query
	SQLQueries   []string // Multiple SQL queries
	Delimiter    rune
	DBPath       string
	TableNames   []string
	IndexColumns []string // Columns to create indexes on
	HasHeader    bool
	KeepDB       bool // Track if db should be kept (explicitly set)
}

// ParseDelimiter converts a delimiter string to a rune.
// Valid values: "comma", "csv", "tab", "tsv", "auto".
// Returns 0 for auto-detection.
func ParseDelimiter(delimiterStr string) (rune, error) {
	switch strings.ToLower(delimiterStr) {
	case "comma", "csv":
		return ',', nil
	case "tab", "tsv":
		return '\t', nil
	case "auto":
		return 0, nil
	default:
		return 0, fmt.Errorf("invalid delimiter: %s (use 'comma', 'tab', or 'auto')", delimiterStr)
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	// Check if we have at least one input file or query
	if len(c.InputFiles) == 0 && len(c.SQLQueries) == 0 {
		return fmt.Errorf("must specify at least one input file or a query")
	}

	// Check if stdin is used with multiple queries
	hasStdin := false
	for _, inputFile := range c.InputFiles {
		if inputFile == "-" || inputFile == "" {
			hasStdin = true
			break
		}
	}
	if hasStdin && len(c.SQLQueries) > 1 {
		return fmt.Errorf("multiple queries not supported with stdin input (stdin can only be read once)")
	}

	// If outputs are provided, they must match query count
	if len(c.OutputFiles) > 0 && len(c.SQLQueries) > 0 {
		if len(c.OutputFiles) != len(c.SQLQueries) {
			return fmt.Errorf("number of output files (%d) must match number of queries (%d)", len(c.OutputFiles), len(c.SQLQueries))
		}
	}

	return nil
}

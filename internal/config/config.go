// Package config provides configuration types and parsing for yatisql.
package config

import (
	"fmt"
	"strings"
)

// Config holds all configuration options for yatisql.
type Config struct {
	InputFiles   []string
	OutputFile   string
	SQLQuery     string
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
	if len(c.InputFiles) == 0 && c.SQLQuery == "" {
		return fmt.Errorf("must specify at least one input file or a query")
	}
	return nil
}

// Package cli provides the command-line interface for yatisql.
package cli

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/yatisql/yatisql-go/internal/config"
	"github.com/yatisql/yatisql-go/internal/database"
	"github.com/yatisql/yatisql-go/internal/exporter"
	"github.com/yatisql/yatisql-go/internal/importer"
)

var (
	// Colors for output
	successColor = color.New(color.FgGreen, color.Bold)
	infoColor    = color.New(color.FgCyan)
	warnColor    = color.New(color.FgYellow)
)

var rootCmd = &cobra.Command{
	Use:   "yatisql",
	Short: "Query CSV/TSV files using SQL",
	Long: `yatisql - yet another tabular inefficient SQL

A simple tool that streams CSV/TSV files into SQLite, executes SQL queries,
and exports results back to CSV/TSV format.

Features:
  • Stream large CSV/TSV files efficiently
  • Execute SQL queries on imported data
  • Support for compressed files (.gz, .bz2)
  • JOIN multiple tables
  • Automatic delimiter detection`,
	Example: `  # Import and query in one command
  yatisql -i data.csv -q "SELECT * FROM data LIMIT 10" -o results.csv

  # Import multiple files and JOIN
  yatisql -i users.csv,orders.csv -t users,orders -q "SELECT * FROM users u JOIN orders o ON u.id = o.user_id" -o joined.csv

  # Query compressed file
  yatisql -i data.csv.gz -q "SELECT COUNT(*) FROM data"`,
	RunE: runCommand,
}

func init() {
	rootCmd.Flags().StringSliceP("input", "i", []string{}, "Input CSV/TSV file(s), comma-separated for multiple files")
	rootCmd.Flags().StringSliceP("table", "t", []string{}, "Table name(s) for imported data, comma-separated (default: 'data', 'data2', etc.)")
	rootCmd.Flags().StringP("output", "o", "", "Output CSV/TSV file path (default: stdout)")
	rootCmd.Flags().StringP("query", "q", "", "SQL query to execute")
	rootCmd.Flags().StringP("db", "d", "", "SQLite database path (default: temporary file, deleted after execution)")
	rootCmd.Flags().BoolP("header", "H", true, "Input file has header row")
	rootCmd.Flags().String("delimiter", "auto", "Field delimiter: 'comma', 'tab', or 'auto' (default: auto)")
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func runCommand(cmd *cobra.Command, args []string) error {
	cfg := &config.Config{}

	// Get flags
	inputFiles, _ := cmd.Flags().GetStringSlice("input")
	tableNames, _ := cmd.Flags().GetStringSlice("table")
	outputFile, _ := cmd.Flags().GetString("output")
	query, _ := cmd.Flags().GetString("query")
	dbPath, _ := cmd.Flags().GetString("db")
	hasHeader, _ := cmd.Flags().GetBool("header")
	delimiterStr, _ := cmd.Flags().GetString("delimiter")

	cfg.InputFiles = inputFiles
	cfg.TableNames = tableNames
	cfg.OutputFile = outputFile
	cfg.SQLQuery = query
	cfg.DBPath = dbPath
	cfg.HasHeader = hasHeader
	cfg.KeepDB = cmd.Flags().Changed("db")

	// Parse delimiter
	delimiter, err := config.ParseDelimiter(delimiterStr)
	if err != nil {
		return err
	}
	cfg.Delimiter = delimiter

	// Validate inputs
	if err := cfg.Validate(); err != nil {
		return err
	}

	return run(cfg)
}

func run(cfg *config.Config) error {
	// Open database
	db, err := database.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() {
		db.DB.Close()
		if db.ShouldCleanup {
			if err := db.Cleanup(); err != nil {
				warnColor.Fprintf(os.Stderr, "Warning: %v\n", err)
			} else {
				infoColor.Printf("Cleaned up temporary database\n")
			}
		}
	}()

	if db.IsTemp {
		infoColor.Printf("Using temporary database: %s\n", db.Path)
	} else {
		infoColor.Printf("Opening database: %s\n", db.Path)
	}

	// Import CSV/TSV files into SQLite
	if len(cfg.InputFiles) > 0 {
		for i, inputFile := range cfg.InputFiles {
			// Determine delimiter for this file if auto
			delimiter := cfg.Delimiter
			if delimiter == 0 {
				delimiter = importer.DetectDelimiter(inputFile)
			}

			// Determine table name
			tableName := "data"
			if i < len(cfg.TableNames) {
				tableName = cfg.TableNames[i]
			} else if i > 0 {
				tableName = fmt.Sprintf("data%d", i+1)
			}

			infoColor.Printf("Importing %s → table '%s'\n", inputFile, tableName)
			result, err := importer.Import(db.DB, inputFile, tableName, delimiter, cfg.HasHeader)
			if err != nil {
				return fmt.Errorf("failed to import CSV %s: %w", inputFile, err)
			}
			infoColor.Printf("  Imported %d rows\n", result.RowCount)
			successColor.Printf("✓ Successfully imported %s into table '%s'\n", inputFile, tableName)
		}
	}

	// Execute SQL query and export results
	if cfg.SQLQuery != "" {
		outputDelimiter := cfg.Delimiter
		if outputDelimiter == 0 {
			outputDelimiter = exporter.DetectOutputDelimiter(cfg.OutputFile)
		}

		infoColor.Printf("Executing query...\n")
		result, err := exporter.Execute(db.DB, cfg.SQLQuery, cfg.OutputFile, outputDelimiter)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		infoColor.Printf("  Exported %d rows\n", result.RowCount)
		if cfg.OutputFile != "" {
			successColor.Printf("✓ Query results exported to %s\n", cfg.OutputFile)
		}
	}

	return nil
}

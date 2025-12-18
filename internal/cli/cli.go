// Package cli provides the command-line interface for yatisql.
package cli

import (
	"fmt"
	"os"
	"runtime/trace"
	"sync"
	"time"

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

// getHelpWithASCII returns help text with ASCII art.
func getHelpWithASCII() string {
	return `yatisql - yet another tabular inefficient SQL

A simple tool that streams CSV/TSV files into SQLite, executes SQL queries,
and exports results back to CSV/TSV format.

Features:
  • Stream large CSV/TSV files efficiently
  • Execute SQL queries on imported data
  • Support for compressed files (.gz, .bz2)
  • JOIN multiple tables
  • Automatic delimiter detection`
}

// getHelpText returns plain help text without ASCII art.
func getHelpText() string {
	return `yatisql - yet another tabular inefficient SQL

A simple tool that streams CSV/TSV files into SQLite, executes SQL queries,
and exports results back to CSV/TSV format.

Features:
  • Stream large CSV/TSV files efficiently
  • Execute SQL queries on imported data
  • Support for compressed files (.gz, .bz2)
  • JOIN multiple tables
  • Automatic delimiter detection`
}

var rootCmd = &cobra.Command{
	Use:   "yatisql",
	Short: "Query CSV/TSV files using SQL",
	Long: func() string {
		// Show ASCII art in help text if terminal
		if isTerminal() {
			return getHelpWithASCII()
		}
		return getHelpText()
	}(),
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
	rootCmd.Flags().String("trace", "", "Write execution trace to file (use 'go tool trace <file>' to view)")
	rootCmd.Flags().Bool("trace-debug", false, "Enable debug logging for concurrent execution")
	rootCmd.Flags().BoolP("progress", "p", false, "Show progress bars for file import operations")
	rootCmd.Flags().StringSliceP("index", "x", []string{}, "Column(s) to create indexes on, comma-separated")
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
	traceFile, _ := cmd.Flags().GetString("trace")
	traceDebug, _ := cmd.Flags().GetBool("trace-debug")
	showProgress, _ := cmd.Flags().GetBool("progress")
	indexColumns, _ := cmd.Flags().GetStringSlice("index")

	cfg.InputFiles = inputFiles
	cfg.TableNames = tableNames
	cfg.OutputFile = outputFile
	cfg.SQLQuery = query
	cfg.DBPath = dbPath
	cfg.HasHeader = hasHeader
	cfg.KeepDB = cmd.Flags().Changed("db")
	cfg.IndexColumns = indexColumns

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

	// Setup trace if requested
	if traceFile != "" {
		f, err := os.Create(traceFile)
		if err != nil {
			return fmt.Errorf("failed to create trace file: %w", err)
		}
		defer f.Close()

		if err := trace.Start(f); err != nil {
			return fmt.Errorf("failed to start trace: %w", err)
		}
		defer trace.Stop()
		infoColor.Printf("Tracing execution to %s (use 'go tool trace %s' to view)\n", traceFile, traceFile)
	}

	return run(cfg, traceDebug, showProgress)
}

func run(cfg *config.Config, traceDebug, showProgress bool) error {
	// Show ASCII art at the start if we have input files
	if len(cfg.InputFiles) > 0 && isTerminal() {
		PrintASCIIArt()
	}

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

	// Import CSV/TSV files into SQLite (concurrently)
	if len(cfg.InputFiles) > 0 {
		// Build file inputs for concurrent import
		inputs := make([]importer.FileInput, len(cfg.InputFiles))
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

			inputs[i] = importer.FileInput{
				FilePath:     inputFile,
				TableName:    tableName,
				Delimiter:    delimiter,
				HasHeader:    cfg.HasHeader,
				IndexColumns: cfg.IndexColumns,
			}
		}

		// Import all files concurrently with progress reporting
		var tracker *ProgressTracker
		if showProgress && isTerminal() {
			tracker = NewProgressTracker(true)
		} else {
			tracker = NewProgressTracker(false)
		}

		var mu sync.Mutex
		progressCallback := func(event string, filePath, tableName string, details ...interface{}) {
			mu.Lock()
			defer mu.Unlock()

			switch event {
			case "parse_start":
				if !showProgress || !isTerminal() {
					infoColor.Printf("  [→] Parsing & writing %s → table '%s' (streaming)...\n", filePath, tableName)
				} else {
					tracker.StartParse(filePath, tableName)
				}
			case "parse_complete":
				rowCount := details[0].(int)
				duration := details[1].(time.Duration)
				if !showProgress || !isTerminal() {
					infoColor.Printf("  [✓] Completed streaming %s (%d rows parsed & written) in %v\n", filePath, rowCount, duration.Round(time.Millisecond))
				} else {
					tracker.FinishParse(filePath, int64(rowCount), duration)
				}
			case "parse_error":
				err := details[0].(error)
				if !showProgress || !isTerminal() {
					warnColor.Printf("  [✗] Parse failed: %s - %v\n", filePath, err)
				} else {
					tracker.Error(filePath, err, "Parse")
				}
			case "write_start":
				rowCount := int64(0)
				if len(details) > 0 {
					switch rc := details[0].(type) {
					case int:
						rowCount = int64(rc)
					case int64:
						rowCount = rc
					}
				}
				if !showProgress || !isTerminal() {
					infoColor.Printf("  [→] Writing %s to database...\n", filePath)
				} else {
					tracker.StartWrite(filePath, tableName, rowCount)
				}
			case "write_complete":
				rowCount := details[0].(int)
				if !showProgress || !isTerminal() {
					infoColor.Printf("  [✓] Imported %d rows into '%s'\n", rowCount, tableName)
					successColor.Printf("✓ Successfully imported table '%s'\n", tableName)
				} else {
					tracker.FinishWrite(filePath, tableName, int64(rowCount))
				}
			case "write_error":
				err := details[0].(error)
				if !showProgress || !isTerminal() {
					warnColor.Printf("  [✗] Write failed: %s - %v\n", filePath, err)
				} else {
					tracker.Error(filePath, err, "Write")
				}
			case "index_start":
				indexCols := details[0].([]string)
				if !showProgress || !isTerminal() {
					infoColor.Printf("  [→] Creating %d index(es) on '%s'...\n", len(indexCols), tableName)
				} else {
					tracker.StartIndex(filePath, tableName, len(indexCols))
				}
			case "index_complete":
				indexCount := details[0].(int)
				duration := details[1].(time.Duration)
				if !showProgress || !isTerminal() {
					successColor.Printf("  [✓] Created %d index(es) on '%s' in %v\n", indexCount, tableName, duration.Round(time.Millisecond))
				} else {
					tracker.FinishIndex(filePath, tableName, indexCount, duration)
				}
			case "index_error":
				err := details[0].(error)
				if !showProgress || !isTerminal() {
					warnColor.Printf("  [✗] Index creation failed on '%s': %v\n", tableName, err)
				} else {
					tracker.Error(filePath, err, "index")
				}
			}
		}

		parseProgressCallback := func(filePath string, rowsRead int64) {
			if showProgress && isTerminal() {
				tracker.UpdateParse(filePath, rowsRead)
			}
		}

		writeProgressCallback := func(filePath string, rowsWritten int64) {
			if showProgress && isTerminal() {
				tracker.UpdateWrite(filePath, rowsWritten)
			}
		}

		results, err := importer.ImportConcurrent(db.DB, inputs, traceDebug, progressCallback, parseProgressCallback, writeProgressCallback)

		// Stop progress tracker render loop
		tracker.Stop()

		if err != nil {
			warnColor.Fprintf(os.Stderr, "Warning: some imports failed:\n%v\n", err)
		}

		// If all imports failed, return the error
		if len(results) == 0 && err != nil {
			return fmt.Errorf("all imports failed: %w", err)
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

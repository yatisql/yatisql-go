// Package cli provides the command-line interface for yatisql.
package cli

import (
	"fmt"
	"os"
	"runtime/trace"
	"strings"
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
  yatisql -i data.csv.gz -q "SELECT COUNT(*) FROM data"

  # Read from stdin and write to stdout (pipeline-friendly)
  cat data.csv | yatisql -q "SELECT * FROM data LIMIT 10"

  # Explicit stdin with explicit stdout
  yatisql -i - -q "SELECT * FROM data" -o -

  # Multiple queries with multiple outputs
  yatisql -i data.csv -q "SELECT * FROM data LIMIT 10" -q "SELECT COUNT(*) FROM data" -o "first10.csv,count.csv"

  # Multiple queries (all to stdout sequentially)
  yatisql -i data.csv -q "SELECT * FROM data LIMIT 5" -q "SELECT COUNT(*) FROM data"`,
	RunE: runCommand,
}

func init() {
	rootCmd.Flags().StringSliceP("input", "i", []string{}, "Input CSV/TSV file(s), comma-separated for multiple files (use '-' or omit for stdin)")
	rootCmd.Flags().StringSliceP("table", "t", []string{}, "Table name(s) for imported data, comma-separated (default: 'data', 'data2', etc.)")
	rootCmd.Flags().StringSliceP("output", "o", []string{}, "Output CSV/TSV file path(s), comma-separated (default: stdout). Must match number of queries.")
	rootCmd.Flags().StringSliceP("query", "q", []string{}, "SQL query(ies) to execute (can specify multiple -q flags)")
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
	outputFilesRaw, _ := cmd.Flags().GetStringSlice("output")
	queries, _ := cmd.Flags().GetStringSlice("query")
	dbPath, _ := cmd.Flags().GetString("db")
	hasHeader, _ := cmd.Flags().GetBool("header")
	delimiterStr, _ := cmd.Flags().GetString("delimiter")
	traceFile, _ := cmd.Flags().GetString("trace")
	traceDebug, _ := cmd.Flags().GetBool("trace-debug")
	showProgress, _ := cmd.Flags().GetBool("progress")
	indexColumns, _ := cmd.Flags().GetStringSlice("index")

	// Parse comma-separated output files
	var outputFiles []string
	for _, output := range outputFilesRaw {
		// Split by comma and trim spaces
		parts := strings.Split(output, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				outputFiles = append(outputFiles, trimmed)
			}
		}
	}

	// Handle stdin: if -i is omitted but queries are provided, treat as stdin input
	if len(inputFiles) == 0 && len(queries) > 0 {
		inputFiles = []string{"-"}
	}

	cfg.InputFiles = inputFiles
	cfg.TableNames = tableNames
	cfg.OutputFiles = outputFiles
	cfg.SQLQueries = queries
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

	// If stdin is used and delimiter is auto, default to comma
	if len(inputFiles) > 0 && (inputFiles[0] == "-" || inputFiles[0] == "") && delimiter == 0 {
		cfg.Delimiter = ','
	}

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
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return err
	}

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
		// Check if any input is stdin
		hasStdin := false
		for _, inputFile := range cfg.InputFiles {
			if inputFile == "-" || inputFile == "" {
				hasStdin = true
				break
			}
		}

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
		// Disable progress bars for stdin (no file path to track)
		var tracker *ProgressTracker
		if showProgress && isTerminal() && !hasStdin {
			tracker = NewProgressTracker(true)
		} else {
			tracker = NewProgressTracker(false)
		}

		var mu sync.Mutex
		isStdin := func(path string) bool {
			return path == "-" || path == ""
		}
		progressCallback := func(event string, filePath, tableName string, details ...interface{}) {
			mu.Lock()
			defer mu.Unlock()

			switch event {
			case "parse_start":
				// Skip progress output for stdin
				if isStdin(filePath) {
					// Silent for stdin
					break
				}
				switch {
				case !showProgress || !isTerminal():
					infoColor.Printf("  [→] Parsing & writing %s → table '%s' (streaming)...\n", filePath, tableName)
				default:
					tracker.StartParse(filePath, tableName)
				}
			case "parse_complete":
				rowCount := details[0].(int)
				duration := details[1].(time.Duration)
				// Skip progress output for stdin
				if isStdin(filePath) {
					// Silent for stdin
					break
				}
				switch {
				case !showProgress || !isTerminal():
					infoColor.Printf("  [✓] Completed streaming %s (%d rows parsed & written) in %v\n", filePath, rowCount, duration.Round(time.Millisecond))
				default:
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
				// Skip progress output for stdin
				if isStdin(filePath) {
					// Silent for stdin
					break
				}
				switch {
				case !showProgress || !isTerminal():
					infoColor.Printf("  [→] Writing %s to database...\n", filePath)
				default:
					tracker.StartWrite(filePath, tableName, rowCount)
				}
			case "write_complete":
				rowCount := details[0].(int)
				// Skip progress output for stdin
				if isStdin(filePath) {
					// Silent for stdin
					break
				}
				switch {
				case !showProgress || !isTerminal():
					infoColor.Printf("  [✓] Imported %d rows into '%s'\n", rowCount, tableName)
					successColor.Printf("✓ Successfully imported table '%s'\n", tableName)
				default:
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
			// Skip progress updates for stdin
			if (filePath != "-" && filePath != "") && showProgress && isTerminal() {
				tracker.UpdateParse(filePath, rowsRead)
			}
		}

		writeProgressCallback := func(filePath string, rowsWritten int64) {
			// Skip progress updates for stdin
			if (filePath != "-" && filePath != "") && showProgress && isTerminal() {
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

	// Execute SQL queries and export results
	if len(cfg.SQLQueries) > 0 {
		// Determine output files - use provided outputs or default to stdout for each
		outputFiles := cfg.OutputFiles
		if len(outputFiles) == 0 {
			// No outputs provided, all queries write to stdout
			outputFiles = make([]string, len(cfg.SQLQueries))
			for i := range outputFiles {
				outputFiles[i] = "" // Empty string means stdout
			}
		} else if len(outputFiles) != len(cfg.SQLQueries) {
			// This should be caught by Validate(), but check here for safety
			return fmt.Errorf("number of output files (%d) must match number of queries (%d)", len(outputFiles), len(cfg.SQLQueries))
		}

		// Check if any queries write to stdout (can't be concurrent)
		hasStdout := false
		for _, outputFile := range outputFiles {
			if outputFile == "" {
				hasStdout = true
				break
			}
		}

		if hasStdout || len(cfg.SQLQueries) == 1 {
			// Sequential execution for stdout or single query
			for i, query := range cfg.SQLQueries {
				outputFile := outputFiles[i]

				// Determine delimiter for this output
				outputDelimiter := cfg.Delimiter
				if outputDelimiter == 0 {
					outputDelimiter = exporter.DetectOutputDelimiter(outputFile)
				}

				// Show which query is being executed
				if len(cfg.SQLQueries) > 1 {
					infoColor.Printf("Executing query %d/%d...\n", i+1, len(cfg.SQLQueries))
				} else {
					infoColor.Printf("Executing query...\n")
				}

				result, err := exporter.Execute(db.DB, query, outputFile, outputDelimiter)
				if err != nil {
					return fmt.Errorf("failed to execute query %d: %w", i+1, err)
				}
				infoColor.Printf("  Exported %d rows\n", result.RowCount)
				if outputFile != "" {
					successColor.Printf("✓ Query %d results exported to %s\n", i+1, outputFile)
				} else if len(cfg.SQLQueries) > 1 {
					successColor.Printf("✓ Query %d results written to stdout\n", i+1)
				}
			}
		} else {
			// Concurrent execution for multiple file outputs
			var queryWg sync.WaitGroup
			var queryMu sync.Mutex
			var queryErrs []error

			for i, query := range cfg.SQLQueries {
				queryWg.Add(1)
				go func(queryIdx int, q string, outFile string) {
					defer queryWg.Done()

					// Determine delimiter for this output
					outputDelimiter := cfg.Delimiter
					if outputDelimiter == 0 {
						outputDelimiter = exporter.DetectOutputDelimiter(outFile)
					}

					queryMu.Lock()
					infoColor.Printf("Executing query %d/%d...\n", queryIdx+1, len(cfg.SQLQueries))
					queryMu.Unlock()

					result, err := exporter.Execute(db.DB, q, outFile, outputDelimiter)
					if err != nil {
						queryMu.Lock()
						queryErrs = append(queryErrs, fmt.Errorf("query %d: %w", queryIdx+1, err))
						queryMu.Unlock()
						return
					}

					queryMu.Lock()
					infoColor.Printf("  Exported %d rows\n", result.RowCount)
					successColor.Printf("✓ Query %d results exported to %s\n", queryIdx+1, outFile)
					queryMu.Unlock()
				}(i, query, outputFiles[i])
			}

			queryWg.Wait()

			if len(queryErrs) > 0 {
				return fmt.Errorf("query execution errors: %v", queryErrs)
			}
		}
	}

	return nil
}

package main

import (
	"compress/bzip2"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
)

const (
	batchSize = 10000 // Number of rows to insert in a single transaction
)

var (
	// Colors for output
	successColor = color.New(color.FgGreen, color.Bold)
	errorColor   = color.New(color.FgRed, color.Bold)
	infoColor    = color.New(color.FgCyan)
	warnColor    = color.New(color.FgYellow)
)

type Config struct {
	InputFiles []string
	OutputFile string
	SQLQuery   string
	Delimiter  rune
	DBPath     string
	TableNames []string
	HasHeader  bool
	KeepDB     bool // Track if db should be kept (explicitly set)
}

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

func main() {
	if err := rootCmd.Execute(); err != nil {
		errorColor.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runCommand(cmd *cobra.Command, args []string) error {
	config := &Config{}

	// Get flags
	inputFiles, _ := cmd.Flags().GetStringSlice("input")
	tableNames, _ := cmd.Flags().GetStringSlice("table")
	outputFile, _ := cmd.Flags().GetString("output")
	query, _ := cmd.Flags().GetString("query")
	dbPath, _ := cmd.Flags().GetString("db")
	hasHeader, _ := cmd.Flags().GetBool("header")
	delimiterStr, _ := cmd.Flags().GetString("delimiter")

	config.InputFiles = inputFiles
	config.TableNames = tableNames
	config.OutputFile = outputFile
	config.SQLQuery = query
	config.DBPath = dbPath
	config.HasHeader = hasHeader
	config.KeepDB = cmd.Flags().Changed("db") // If db was explicitly set, keep it

	// Parse delimiter
	switch strings.ToLower(delimiterStr) {
	case "comma", "csv":
		config.Delimiter = ','
	case "tab", "tsv":
		config.Delimiter = '\t'
	case "auto":
		config.Delimiter = 0
	default:
		return fmt.Errorf("invalid delimiter: %s (use 'comma', 'tab', or 'auto')", delimiterStr)
	}

	// Validate inputs
	if len(config.InputFiles) == 0 && config.SQLQuery == "" {
		return fmt.Errorf("must specify at least one input file or a query")
	}

	return run(config)
}

func run(config *Config) error {
	var tempDBPath string
	var shouldCleanup bool

	// Create temporary database file if not explicitly set
	if config.DBPath == "" {
		tmpFile, err := os.CreateTemp("", "yatisql-*.db")
		if err != nil {
			return fmt.Errorf("failed to create temporary database: %w", err)
		}
		tmpFile.Close()
		tempDBPath = tmpFile.Name()
		config.DBPath = tempDBPath
		shouldCleanup = true
		infoColor.Printf("Using temporary database: %s\n", tempDBPath)
	} else {
		infoColor.Printf("Opening database: %s\n", config.DBPath)
	}

	// Ensure cleanup of temp file
	if shouldCleanup {
		defer func() {
			if err := os.Remove(tempDBPath); err != nil {
				warnColor.Printf("Warning: failed to remove temporary database %s: %v\n", tempDBPath, err)
			} else {
				infoColor.Printf("Cleaned up temporary database\n")
			}
		}()
	}

	// Create directory for database file if it doesn't exist (for paths like db/test.db)
	if !shouldCleanup && config.DBPath != "" {
		dbDir := filepath.Dir(config.DBPath)
		if dbDir != "." && dbDir != "" {
			if err := os.MkdirAll(dbDir, 0755); err != nil {
				return fmt.Errorf("failed to create database directory %s: %w", dbDir, err)
			}
		}
	}

	// Open database
	db, err := sql.Open("sqlite3", config.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Import CSV/TSV files into SQLite
	if len(config.InputFiles) > 0 {
		defaultDelimiter := config.Delimiter

		for i, inputFile := range config.InputFiles {
			// Determine delimiter for this file if auto
			delimiter := defaultDelimiter
			if delimiter == 0 {
				ext := strings.ToLower(filepath.Ext(inputFile))
				if ext == ".tsv" {
					delimiter = '\t'
				} else {
					delimiter = ','
				}
			}

			// Determine table name
			tableName := "data"
			if i < len(config.TableNames) {
				tableName = config.TableNames[i]
			} else if i > 0 {
				tableName = fmt.Sprintf("data%d", i+1)
			}

			infoColor.Printf("Importing %s → table '%s'\n", inputFile, tableName)
			if err := importCSV(db, inputFile, tableName, delimiter, config.HasHeader); err != nil {
				return fmt.Errorf("failed to import CSV %s: %w", inputFile, err)
			}
			successColor.Printf("✓ Successfully imported %s into table '%s'\n", inputFile, tableName)
		}
	}

	// Execute SQL query and export results
	if config.SQLQuery != "" {
		outputDelimiter := config.Delimiter
		if outputDelimiter == 0 {
			if config.OutputFile != "" {
				ext := strings.ToLower(filepath.Ext(config.OutputFile))
				if ext == ".tsv" {
					outputDelimiter = '\t'
				} else {
					outputDelimiter = ','
				}
			} else {
				outputDelimiter = ','
			}
		}

		infoColor.Printf("Executing query...\n")
		if err := executeQuery(db, config.SQLQuery, config.OutputFile, outputDelimiter); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		if config.OutputFile != "" {
			successColor.Printf("✓ Query results exported to %s\n", config.OutputFile)
		}
	}

	return nil
}

// openFile opens a file, handling compression automatically based on extension
func openFile(filePath string) (io.ReadCloser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".gz":
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return &gzipFile{file: file, reader: gzReader}, nil
	case ".bz2":
		return &bzip2File{file: file, reader: bzip2.NewReader(file)}, nil
	default:
		return file, nil
	}
}

// gzipFile wraps gzip reader and file to close both
type gzipFile struct {
	file   *os.File
	reader *gzip.Reader
}

func (g *gzipFile) Read(p []byte) (int, error) {
	return g.reader.Read(p)
}

func (g *gzipFile) Close() error {
	g.reader.Close()
	return g.file.Close()
}

// bzip2File wraps bzip2 reader and file to close both
type bzip2File struct {
	file   *os.File
	reader io.Reader
}

func (b *bzip2File) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *bzip2File) Close() error {
	return b.file.Close()
}

func importCSV(db *sql.DB, filePath, tableName string, delimiter rune, hasHeader bool) error {
	file, err := openFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = delimiter
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read header row if present
	var headers []string
	var firstDataRow []string

	if hasHeader {
		headerRow, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read header: %w", err)
		}
		headers = headerRow
	} else {
		firstRow, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read first row: %w", err)
		}
		headers = make([]string, len(firstRow))
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i+1)
		}
		firstDataRow = firstRow
	}

	// Create table
	if err := createTable(db, tableName, headers); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Stream rows in batches
	var batch [][]string
	rowCount := 0

	if !hasHeader && firstDataRow != nil {
		batch = append(batch, firstDataRow)
		rowCount++
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}

		batch = append(batch, record)
		rowCount++

		if len(batch) >= batchSize {
			if err := insertBatch(db, tableName, headers, batch); err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	// Insert remaining rows
	if len(batch) > 0 {
		if err := insertBatch(db, tableName, headers, batch); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
	}

	infoColor.Printf("  Imported %d rows\n", rowCount)
	return nil
}

func createTable(db *sql.DB, tableName string, headers []string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	columns := make([]string, len(headers))
	for i, header := range headers {
		sanitized := sanitizeColumnName(header)
		columns[i] = fmt.Sprintf("%s TEXT", sanitized)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(columns, ", "))
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func sanitizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "unnamed"
	}

	result := make([]rune, 0, len(name))
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result = append(result, r)
		} else {
			result = append(result, '_')
		}
	}

	sanitized := string(result)
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "col_" + sanitized
	}

	return sanitized
}

func insertBatch(db *sql.DB, tableName string, headers []string, batch [][]string) error {
	if len(batch) == 0 {
		return nil
	}

	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := "(" + strings.Join(placeholders, ", ") + ")"

	sanitizedHeaders := make([]string, len(headers))
	for i, h := range headers {
		sanitizedHeaders[i] = sanitizeColumnName(h)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(sanitizedHeaders, ", "),
		placeholderStr)

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range batch {
		values := make([]interface{}, len(headers))
		for i := range headers {
			if i < len(row) {
				values[i] = row[i]
			} else {
				values[i] = ""
			}
		}

		if _, err := stmt.Exec(values...); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// openOutputFile opens an output file, handling compression automatically based on extension
func openOutputFile(filePath string) (io.WriteCloser, error) {
	if filePath == "" {
		return os.Stdout, nil
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".gz":
		return gzip.NewWriter(file), nil
	case ".bz2":
		file.Close()
		return nil, fmt.Errorf("bzip2 output compression not yet supported, use .gz instead")
	default:
		return file, nil
	}
}

func executeQuery(db *sql.DB, query, outputFile string, delimiter rune) error {
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	output, err := openOutputFile(outputFile)
	if err != nil {
		return err
	}
	defer output.Close()

	writer := csv.NewWriter(output)
	writer.Comma = delimiter
	defer writer.Flush()

	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		record := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	infoColor.Printf("  Exported %d rows\n", rowCount)
	return nil
}

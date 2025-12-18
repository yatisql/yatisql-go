package main

import (
	"compress/bzip2"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const (
	batchSize = 10000 // Number of rows to insert in a single transaction
)

type Config struct {
	InputFiles []string // Multiple input files
	OutputFile string
	SQLQuery   string
	Delimiter  rune
	DBPath     string
	TableNames []string // Multiple table names (one per input file)
	HasHeader  bool
}

func main() {
	config := parseFlags()

	if err := run(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() *Config {
	config := &Config{}

	inputFlag := flag.String("input", "", "Input CSV/TSV file path(s), comma-separated for multiple files")
	tableFlag := flag.String("table", "", "Table name(s) for imported data, comma-separated (default: 'data', 'data2', etc.)")
	flag.StringVar(&config.OutputFile, "output", "", "Output CSV/TSV file path (default: stdout)")
	flag.StringVar(&config.SQLQuery, "query", "", "SQL query to execute")
	flag.StringVar(&config.DBPath, "db", ":memory:", "SQLite database path (default: in-memory)")
	flag.BoolVar(&config.HasHeader, "header", true, "Input file has header row")

	delimiter := flag.String("delimiter", "auto", "Field delimiter: 'comma', 'tab', or 'auto' (default: auto)")

	flag.Parse()

	// Parse input files
	if *inputFlag != "" {
		config.InputFiles = strings.Split(*inputFlag, ",")
		for i := range config.InputFiles {
			config.InputFiles[i] = strings.TrimSpace(config.InputFiles[i])
		}
	}

	// Parse table names
	if *tableFlag != "" {
		config.TableNames = strings.Split(*tableFlag, ",")
		for i := range config.TableNames {
			config.TableNames[i] = strings.TrimSpace(config.TableNames[i])
		}
	}

	// Determine delimiter
	switch strings.ToLower(*delimiter) {
	case "comma", "csv":
		config.Delimiter = ','
	case "tab", "tsv":
		config.Delimiter = '\t'
	case "auto":
		// Will be determined from file extension
		config.Delimiter = 0
	default:
		fmt.Fprintf(os.Stderr, "Invalid delimiter: %s\n", *delimiter)
		os.Exit(1)
	}

	return config
}

func run(config *Config) error {
	// Open database
	db, err := sql.Open("sqlite3", config.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Import CSV/TSV files into SQLite
	if len(config.InputFiles) > 0 {
		// Determine default delimiter if auto
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
				// Default naming: data, data2, data3, etc.
				tableName = fmt.Sprintf("data%d", i+1)
			}

			if err := importCSV(db, inputFile, tableName, delimiter, config.HasHeader); err != nil {
				return fmt.Errorf("failed to import CSV %s: %w", inputFile, err)
			}
			fmt.Fprintf(os.Stderr, "Successfully imported %s into table '%s'\n", inputFile, tableName)
		}
	}

	// Execute SQL query and export results
	if config.SQLQuery != "" {
		// Use default delimiter for output (or determine from output file extension)
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

		if err := executeQuery(db, config.SQLQuery, config.OutputFile, outputDelimiter); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
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
	var firstDataRow []string // Store first row if no header

	if hasHeader {
		headerRow, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read header: %w", err)
		}
		headers = headerRow
	} else {
		// Read first row to determine column count and use as data
		firstRow, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read first row: %w", err)
		}
		headers = make([]string, len(firstRow))
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i+1)
		}
		firstDataRow = firstRow // Store to process as data
	}

	// Create table
	if err := createTable(db, tableName, headers); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Stream rows in batches
	var batch [][]string
	rowCount := 0

	// If no header, add the first row we read as data
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

	fmt.Fprintf(os.Stderr, "Imported %d rows\n", rowCount)
	return nil
}

func createTable(db *sql.DB, tableName string, headers []string) error {
	// Drop table if exists
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Create table with TEXT columns (SQLite is flexible with types)
	columns := make([]string, len(headers))
	for i, header := range headers {
		// Sanitize column names
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
	// Remove or replace invalid characters for SQL identifiers
	name = strings.TrimSpace(name)
	if name == "" {
		return "unnamed"
	}

	// Replace spaces and special chars with underscore
	result := make([]rune, 0, len(name))
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			result = append(result, r)
		} else {
			result = append(result, '_')
		}
	}

	sanitized := string(result)
	// Ensure it doesn't start with a number
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "col_" + sanitized
	}

	return sanitized
}

func insertBatch(db *sql.DB, tableName string, headers []string, batch [][]string) error {
	if len(batch) == 0 {
		return nil
	}

	// Build placeholders
	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := "(" + strings.Join(placeholders, ", ") + ")"

	// Build INSERT statement
	sanitizedHeaders := make([]string, len(headers))
	for i, h := range headers {
		sanitizedHeaders[i] = sanitizeColumnName(h)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(sanitizedHeaders, ", "),
		placeholderStr)

	// Begin transaction
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

	// Insert each row
	for _, row := range batch {
		// Pad row if necessary
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
		// Note: bzip2 writing requires a library like github.com/dsnet/compress/bzip2
		// For now, we'll return an error suggesting to use gzip
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

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Open output file or use stdout (with compression support)
	output, err := openOutputFile(outputFile)
	if err != nil {
		return err
	}
	defer output.Close()

	writer := csv.NewWriter(output)
	writer.Comma = delimiter
	defer writer.Flush()

	// Write header
	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write rows
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

	fmt.Fprintf(os.Stderr, "Exported %d rows\n", rowCount)
	return nil
}

package importer

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime/trace"
	"sync"
	"time"

	"github.com/yatisql/yatisql-go/internal/database"
)

// Result contains the result of an import operation.
type Result struct {
	TableName string
	RowCount  int
}

// ParsedFile holds the pre-parsed content of a CSV/TSV file.
// This allows file parsing to happen concurrently before database writes.
type ParsedFile struct {
	FilePath  string
	TableName string
	Headers   []string
	Rows      [][]string
	Error     error
}

// FileInput describes a file to be imported.
type FileInput struct {
	FilePath  string
	TableName string
	Delimiter rune
	HasHeader bool
}

// ParseFile reads and parses a CSV/TSV file into memory.
// This function is safe to call concurrently.
// If progressCallback is provided, it will be called periodically with the number of rows read.
func ParseFile(input FileInput, progressCallback ParseProgressCallback) *ParsedFile {
	result := &ParsedFile{
		FilePath:  input.FilePath,
		TableName: input.TableName,
	}

	file, err := OpenFile(input.FilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to open file: %w", err)
		return result
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = input.Delimiter
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read header row if present
	if input.HasHeader {
		headerRow, err := reader.Read()
		if err != nil {
			result.Error = fmt.Errorf("failed to read header: %w", err)
			return result
		}
		result.Headers = headerRow
	} else {
		firstRow, err := reader.Read()
		if err != nil {
			result.Error = fmt.Errorf("failed to read first row: %w", err)
			return result
		}
		result.Headers = make([]string, len(firstRow))
		for i := range result.Headers {
			result.Headers[i] = fmt.Sprintf("col%d", i+1)
		}
		result.Rows = append(result.Rows, firstRow)
	}

	// Read all remaining rows
	rowCount := int64(0)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			result.Error = fmt.Errorf("failed to read row: %w", err)
			return result
		}
		result.Rows = append(result.Rows, record)
		rowCount++

		// Report progress every 1000 rows
		if progressCallback != nil && rowCount%1000 == 0 {
			progressCallback(input.FilePath, rowCount)
		}
	}

	// Final progress update
	if progressCallback != nil {
		progressCallback(input.FilePath, rowCount)
	}

	return result
}

// WriteToDatabase writes a parsed file to the database.
// This function is safe to call concurrently for different tables when WAL mode is enabled.
// If progressCallback is provided, it will be called after each batch is written.
func WriteToDatabase(db *sql.DB, parsed *ParsedFile, progressCallback WriteProgressCallback) (*Result, error) {
	if parsed.Error != nil {
		return nil, parsed.Error
	}

	// Create table
	if err := database.CreateTable(db, parsed.TableName, parsed.Headers); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Insert rows in batches
	rowCount := len(parsed.Rows)
	rowsWritten := int64(0)
	for i := 0; i < rowCount; i += database.BatchSize {
		end := i + database.BatchSize
		if end > rowCount {
			end = rowCount
		}
		batch := parsed.Rows[i:end]
		if err := database.InsertBatch(db, parsed.TableName, parsed.Headers, batch); err != nil {
			return nil, fmt.Errorf("failed to insert batch: %w", err)
		}
		rowsWritten += int64(len(batch))

		// Report progress after each batch
		if progressCallback != nil {
			progressCallback(parsed.FilePath, rowsWritten)
		}
	}

	return &Result{
		TableName: parsed.TableName,
		RowCount:  rowCount,
	}, nil
}

// ProgressCallback is called to report progress during concurrent import.
type ProgressCallback func(event string, filePath, tableName string, details ...interface{})

// ParseProgressCallback is called during file parsing to report row-by-row progress.
type ParseProgressCallback func(filePath string, rowsRead int64)

// WriteProgressCallback is called during database writing to report batch-by-batch progress.
type WriteProgressCallback func(filePath string, rowsWritten int64)

// ImportConcurrent imports multiple files concurrently using streaming.
// Files are parsed and written in parallel - batches are written as soon as they're parsed.
// This prevents loading entire files into memory, making it suitable for very large files.
// Returns results for successful imports and a combined error for any failures.
// If progressCallback is provided, it will be called with progress events:
//   - "parse_start": when parsing starts for a file
//   - "parse_complete": when parsing completes (details[0] = rowCount, details[1] = duration)
//   - "parse_error": when parsing fails (details[0] = error)
//   - "write_start": when writing to database starts
//   - "write_complete": when writing completes (details[0] = rowCount)
//
// If parseProgressCallback is provided, it will be called periodically during parsing.
// If writeProgressCallback is provided, it will be called after each batch is written.
func ImportConcurrent(db *sql.DB, inputs []FileInput, debug bool, progressCallback ProgressCallback, parseProgressCallback ParseProgressCallback, writeProgressCallback WriteProgressCallback) ([]*Result, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	startTime := time.Now()
	if debug {
		log.Printf("[STREAMING] Starting streaming import of %d files", len(inputs))
	}

	// Create a trace region for concurrent import
	ctx, task := trace.NewTask(context.Background(), "ImportConcurrent")
	defer task.End()

	var results []*Result
	var errs []error
	var resultsMu sync.Mutex
	var importWg sync.WaitGroup

	// Process each file concurrently - parse and write in streaming fashion
	for _, input := range inputs {
		importWg.Add(1)
		go func(inp FileInput) {
			defer importWg.Done()

			trace.WithRegion(ctx, fmt.Sprintf("import_file_%s", inp.FilePath), func() {
				if progressCallback != nil {
					progressCallback("parse_start", inp.FilePath, inp.TableName)
				}
				if debug {
					log.Printf("[STREAMING] Starting concurrent streaming import of %s", inp.FilePath)
				}

				parseStart := time.Now()
				result, err := importFileStreaming(db, inp, progressCallback, parseProgressCallback, writeProgressCallback, debug, ctx)
				parseDuration := time.Since(parseStart)

				resultsMu.Lock()
				if err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", inp.FilePath, err))
					if progressCallback != nil {
						progressCallback("parse_error", inp.FilePath, inp.TableName, err)
					}
					if debug {
						log.Printf("[STREAMING] Failed to import %s: %v", inp.FilePath, err)
					}
				} else {
					results = append(results, result)
					if progressCallback != nil {
						progressCallback("parse_complete", inp.FilePath, inp.TableName, result.RowCount, parseDuration)
						progressCallback("write_complete", inp.FilePath, inp.TableName, result.RowCount)
					}
					if debug {
						log.Printf("[STREAMING] Successfully imported %s (%d rows) in %v", inp.FilePath, result.RowCount, parseDuration)
					}
				}
				resultsMu.Unlock()
			})
		}(input)
	}

	importWg.Wait()

	if debug {
		log.Printf("[STREAMING] All imports completed in %v", time.Since(startTime))
	}

	return results, errors.Join(errs...)
}

// importFileStreaming streams a file: parses in batches and writes immediately.
// This keeps memory usage low - only one batch is in memory at a time.
func importFileStreaming(db *sql.DB, input FileInput, progressCallback ProgressCallback, parseProgressCallback ParseProgressCallback, writeProgressCallback WriteProgressCallback, debug bool, ctx context.Context) (*Result, error) {
	file, err := OpenFile(input.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = input.Delimiter
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read header row
	var headers []string
	if input.HasHeader {
		headerRow, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
		headers = headerRow
	} else {
		firstRow, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read first row: %w", err)
		}
		headers = make([]string, len(firstRow))
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i+1)
		}
	}

	// Create table first
	if err := database.CreateTable(db, input.TableName, headers); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	if progressCallback != nil {
		progressCallback("write_start", input.FilePath, input.TableName, int64(0))
	}

	// Stream: read batches and write immediately
	batch := make([][]string, 0, database.BatchSize)
	rowCount := 0
	rowsWritten := int64(0)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		batch = append(batch, record)
		rowCount++

		// Report parse progress
		if parseProgressCallback != nil && rowCount%1000 == 0 {
			parseProgressCallback(input.FilePath, int64(rowCount))
		}

		// When batch is full, write it immediately
		if len(batch) >= database.BatchSize {
			if err := database.InsertBatch(db, input.TableName, headers, batch); err != nil {
				return nil, fmt.Errorf("failed to insert batch: %w", err)
			}
			rowsWritten += int64(len(batch))

			if writeProgressCallback != nil {
				writeProgressCallback(input.FilePath, rowsWritten)
			}

			// Clear batch for next iteration
			batch = batch[:0]
		}
	}

	// Write remaining rows in final batch
	if len(batch) > 0 {
		if err := database.InsertBatch(db, input.TableName, headers, batch); err != nil {
			return nil, fmt.Errorf("failed to insert final batch: %w", err)
		}
		rowsWritten += int64(len(batch))

		if writeProgressCallback != nil {
			writeProgressCallback(input.FilePath, rowsWritten)
		}
	}

	return &Result{
		TableName: input.TableName,
		RowCount:  rowCount,
	}, nil
}

// Import imports a CSV/TSV file into a SQLite table.
// Returns the number of rows imported.
func Import(db *sql.DB, filePath, tableName string, delimiter rune, hasHeader bool) (*Result, error) {
	parsed := ParseFile(FileInput{
		FilePath:  filePath,
		TableName: tableName,
		Delimiter: delimiter,
		HasHeader: hasHeader,
	}, nil)
	return WriteToDatabase(db, parsed, nil)
}

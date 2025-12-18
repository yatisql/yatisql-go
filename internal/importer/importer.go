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
func ParseFile(input FileInput) *ParsedFile {
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
	}

	return result
}

// WriteToDatabase writes a parsed file to the database.
// This function should be called serially to avoid SQLite locking issues.
func WriteToDatabase(db *sql.DB, parsed *ParsedFile) (*Result, error) {
	if parsed.Error != nil {
		return nil, parsed.Error
	}

	// Create table
	if err := database.CreateTable(db, parsed.TableName, parsed.Headers); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Insert rows in batches
	rowCount := len(parsed.Rows)
	for i := 0; i < rowCount; i += database.BatchSize {
		end := i + database.BatchSize
		if end > rowCount {
			end = rowCount
		}
		batch := parsed.Rows[i:end]
		if err := database.InsertBatch(db, parsed.TableName, parsed.Headers, batch); err != nil {
			return nil, fmt.Errorf("failed to insert batch: %w", err)
		}
	}

	return &Result{
		TableName: parsed.TableName,
		RowCount:  rowCount,
	}, nil
}

// ImportConcurrent imports multiple files concurrently.
// Files are parsed in parallel, then written to the database sequentially.
// Returns results for successful imports and a combined error for any failures.
func ImportConcurrent(db *sql.DB, inputs []FileInput, debug bool) ([]*Result, error) {
	if len(inputs) == 0 {
		return nil, nil
	}

	// Parse all files concurrently
	parsedFiles := make([]*ParsedFile, len(inputs))
	var wg sync.WaitGroup

	startTime := time.Now()
	if debug {
		log.Printf("[CONCURRENT] Starting concurrent parse of %d files", len(inputs))
	}

	// Create a trace region for concurrent parsing
	ctx, task := trace.NewTask(context.Background(), "ImportConcurrent")
	defer task.End()

	trace.WithRegion(ctx, "concurrent_parse", func() {
		for i, input := range inputs {
			wg.Add(1)
			go func(idx int, inp FileInput) {
				defer wg.Done()

				// Create trace region for each file parse
				trace.WithRegion(ctx, fmt.Sprintf("parse_file_%s", inp.FilePath), func() {
					if debug {
						log.Printf("[GOROUTINE-%d] Starting parse of %s", idx, inp.FilePath)
					}

					start := time.Now()
					parsedFiles[idx] = ParseFile(inp)
					duration := time.Since(start)

					if debug {
						if parsedFiles[idx].Error != nil {
							log.Printf("[GOROUTINE-%d] Finished parse of %s (ERROR: %v) in %v", idx, inp.FilePath, parsedFiles[idx].Error, duration)
						} else {
							log.Printf("[GOROUTINE-%d] Finished parse of %s (%d rows) in %v", idx, inp.FilePath, len(parsedFiles[idx].Rows), duration)
						}
					}
				})
			}(i, input)
		}
		wg.Wait()
	})

	parseDuration := time.Since(startTime)

	if debug {
		log.Printf("[CONCURRENT] All files parsed in %v, starting sequential database writes", parseDuration)
	}

	// Write to database sequentially and collect errors
	var results []*Result
	var errs []error

	writeStart := time.Now()
	trace.WithRegion(ctx, "sequential_write", func() {
		for _, parsed := range parsedFiles {
			trace.WithRegion(ctx, fmt.Sprintf("write_db_%s", parsed.FilePath), func() {
				if debug {
					log.Printf("[SEQUENTIAL] Writing %s to database", parsed.FilePath)
				}

				result, err := WriteToDatabase(db, parsed)
				if err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", parsed.FilePath, err))
					if debug {
						log.Printf("[SEQUENTIAL] Failed to write %s: %v", parsed.FilePath, err)
					}
				} else {
					results = append(results, result)
					if debug {
						log.Printf("[SEQUENTIAL] Successfully wrote %s (%d rows)", parsed.FilePath, result.RowCount)
					}
				}
			})
		}
	})
	writeDuration := time.Since(writeStart)

	if debug {
		log.Printf("[CONCURRENT] All database writes completed in %v", writeDuration)
		log.Printf("[CONCURRENT] Total time: parse=%v, write=%v, total=%v", parseDuration, writeDuration, time.Since(startTime))
	}

	return results, errors.Join(errs...)
}

// Import imports a CSV/TSV file into a SQLite table.
// Returns the number of rows imported.
func Import(db *sql.DB, filePath, tableName string, delimiter rune, hasHeader bool) (*Result, error) {
	parsed := ParseFile(FileInput{
		FilePath:  filePath,
		TableName: tableName,
		Delimiter: delimiter,
		HasHeader: hasHeader,
	})
	return WriteToDatabase(db, parsed)
}

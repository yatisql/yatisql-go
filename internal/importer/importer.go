package importer

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/yatisql/yatisql-go/internal/database"
)

// Result contains the result of an import operation.
type Result struct {
	TableName string
	RowCount  int
}

// Import imports a CSV/TSV file into a SQLite table.
// Returns the number of rows imported.
func Import(db *sql.DB, filePath, tableName string, delimiter rune, hasHeader bool) (*Result, error) {
	file, err := OpenFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
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
		firstDataRow = firstRow
	}

	// Create table
	if err := database.CreateTable(db, tableName, headers); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
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
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		batch = append(batch, record)
		rowCount++

		if len(batch) >= database.BatchSize {
			if err := database.InsertBatch(db, tableName, headers, batch); err != nil {
				return nil, fmt.Errorf("failed to insert batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	// Insert remaining rows
	if len(batch) > 0 {
		if err := database.InsertBatch(db, tableName, headers, batch); err != nil {
			return nil, fmt.Errorf("failed to insert final batch: %w", err)
		}
	}

	return &Result{
		TableName: tableName,
		RowCount:  rowCount,
	}, nil
}

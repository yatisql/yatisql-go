package exporter

import (
	"database/sql"
	"encoding/csv"
	"fmt"
)

// Result contains the result of a query export operation.
type Result struct {
	RowCount int
}

// Execute executes a SQL query and exports results to the specified output file.
// If outputFile is empty, outputs to stdout.
func Execute(db *sql.DB, query, outputFile string, delimiter rune) (*Result, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	output, err := OpenOutputFile(outputFile)
	if err != nil {
		return nil, err
	}
	defer output.Close()

	writer := csv.NewWriter(output)
	writer.Comma = delimiter
	defer writer.Flush()

	if err := writer.Write(columns); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
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
			return nil, fmt.Errorf("failed to write row: %w", err)
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return &Result{RowCount: rowCount}, nil
}

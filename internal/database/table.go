package database

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	// BatchSize is the number of rows to insert in a single transaction.
	BatchSize = 10000
)

// CreateTable creates a new table with the given name and column headers.
// All columns are created as TEXT type.
// Drops the table first if it already exists.
func CreateTable(db *sql.DB, tableName string, headers []string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	columns := make([]string, len(headers))
	for i, header := range headers {
		sanitized := SanitizeColumnName(header)
		columns[i] = fmt.Sprintf("%s TEXT", sanitized)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(columns, ", "))
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// InsertBatch inserts a batch of rows into the specified table within a transaction.
func InsertBatch(db *sql.DB, tableName string, headers []string, batch [][]string) error {
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
		sanitizedHeaders[i] = SanitizeColumnName(h)
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

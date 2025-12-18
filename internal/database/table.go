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

// GetTableColumns returns the column names for a table.
func GetTableColumns(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dfltValue interface{}
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		columns = append(columns, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading columns: %w", err)
	}

	return columns, nil
}

// ValidateColumns checks if all specified columns exist in the table.
// Returns an error listing any missing columns.
func ValidateColumns(db *sql.DB, tableName string, columns []string) error {
	tableColumns, err := GetTableColumns(db, tableName)
	if err != nil {
		return err
	}

	// Build a set of existing columns (case-insensitive)
	existing := make(map[string]bool)
	for _, col := range tableColumns {
		existing[strings.ToLower(col)] = true
	}

	// Check for missing columns
	var missing []string
	for _, col := range columns {
		sanitized := SanitizeColumnName(col)
		if !existing[strings.ToLower(sanitized)] {
			missing = append(missing, col)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("columns not found in table '%s': %s", tableName, strings.Join(missing, ", "))
	}

	return nil
}

// CreateIndex creates an index on the specified column for a table.
// Returns an error if the column doesn't exist.
func CreateIndex(db *sql.DB, tableName, column string) error {
	// Validate column exists first
	if err := ValidateColumns(db, tableName, []string{column}); err != nil {
		return err
	}

	sanitizedColumn := SanitizeColumnName(column)
	indexName := fmt.Sprintf("idx_%s_%s", tableName, sanitizedColumn)

	createSQL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", indexName, tableName, sanitizedColumn)
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create index on %s.%s: %w", tableName, column, err)
	}

	return nil
}

// CreateIndexes creates indexes on multiple columns for a table.
// Validates all columns exist before creating any indexes.
func CreateIndexes(db *sql.DB, tableName string, columns []string) error {
	if len(columns) == 0 {
		return nil
	}

	// Validate all columns exist first (fail early)
	if err := ValidateColumns(db, tableName, columns); err != nil {
		return err
	}

	// Create indexes
	for _, column := range columns {
		if err := CreateIndex(db, tableName, column); err != nil {
			return err
		}
	}

	return nil
}

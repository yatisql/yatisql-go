// Package database provides SQLite database management for yatisql.
package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// DB wraps a SQLite database connection with additional metadata.
type DB struct {
	*sql.DB
	Path          string
	IsTemp        bool
	ShouldCleanup bool
}

// Open opens or creates a SQLite database.
// If dbPath is empty, a temporary database is created.
// Returns a DB wrapper that tracks whether cleanup is needed.
func Open(dbPath string) (*DB, error) {
	var path string
	var isTemp bool
	var shouldCleanup bool

	if dbPath == "" {
		// Create temporary database file
		tmpFile, err := os.CreateTemp("", "yatisql-*.db")
		if err != nil {
			return nil, fmt.Errorf("failed to create temporary database: %w", err)
		}
		tmpFile.Close()
		path = tmpFile.Name()
		isTemp = true
		shouldCleanup = true
	} else {
		path = dbPath
		isTemp = false
		shouldCleanup = false

		// Create directory for database file if it doesn't exist
		dbDir := filepath.Dir(path)
		if dbDir != "." && dbDir != "" {
			if err := os.MkdirAll(dbDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create database directory %s: %w", dbDir, err)
			}
		}
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		if shouldCleanup {
			os.Remove(path)
		}
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &DB{
		DB:            db,
		Path:          path,
		IsTemp:        isTemp,
		ShouldCleanup: shouldCleanup,
	}, nil
}

// Cleanup removes the temporary database file if applicable.
// Returns any error that occurred during removal.
func (d *DB) Cleanup() error {
	if d.ShouldCleanup {
		if err := os.Remove(d.Path); err != nil {
			return fmt.Errorf("failed to remove temporary database %s: %w", d.Path, err)
		}
	}
	return nil
}

// Close closes the database connection and cleans up if necessary.
func (d *DB) Close() error {
	if err := d.DB.Close(); err != nil {
		return err
	}
	return d.Cleanup()
}

package database

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSanitizeColumnName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "name", "name"},
		{"uppercase", "Name", "Name"},
		{"with underscore", "first_name", "first_name"},
		{"with spaces", "first name", "first_name"},
		{"with special chars", "user@email", "user_email"},
		{"starts with number", "1column", "col_1column"},
		{"empty", "", "unnamed"},
		{"only spaces", "   ", "unnamed"},
		{"mixed", "User Name (Primary)", "User_Name__Primary_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeColumnName(tt.input)
			if got != tt.want {
				t.Errorf("SanitizeColumnName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestOpenTempDatabase(t *testing.T) {
	db, err := Open("")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if !db.IsTemp {
		t.Error("Expected IsTemp to be true for empty path")
	}
	if !db.ShouldCleanup {
		t.Error("Expected ShouldCleanup to be true for temp db")
	}
	if db.Path == "" {
		t.Error("Expected Path to be set")
	}

	// Verify file exists
	if _, err := os.Stat(db.Path); os.IsNotExist(err) {
		t.Error("Expected temp database file to exist")
	}

	// Close and verify cleanup
	path := db.Path
	if err := db.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// File should be removed after close
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("Expected temp database file to be removed after close")
		os.Remove(path) // cleanup
	}
}

func TestOpenPersistentDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if db.IsTemp {
		t.Error("Expected IsTemp to be false for explicit path")
	}
	if db.ShouldCleanup {
		t.Error("Expected ShouldCleanup to be false for persistent db")
	}
	if db.Path != dbPath {
		t.Errorf("Path = %q, want %q", db.Path, dbPath)
	}
}

func TestOpenDatabaseWithSubdirectory(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "subdir", "nested", "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	// Verify directory was created
	dir := filepath.Dir(dbPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Error("Expected directory to be created")
	}
}

func TestCreateTableAndInsertBatch(t *testing.T) {
	db, err := Open("")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	headers := []string{"id", "name", "age"}
	if err := CreateTable(db.DB, "test", headers); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	batch := [][]string{
		{"1", "Alice", "30"},
		{"2", "Bob", "25"},
		{"3", "Charlie", "35"},
	}

	if err := InsertBatch(db.DB, "test", headers, batch); err != nil {
		t.Fatalf("InsertBatch() error = %v", err)
	}

	// Verify data
	var count int
	err = db.DB.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

func TestInsertBatchEmpty(t *testing.T) {
	db, err := Open("")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	headers := []string{"id", "name"}
	if err := CreateTable(db.DB, "test", headers); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Empty batch should not error
	if err := InsertBatch(db.DB, "test", headers, [][]string{}); err != nil {
		t.Fatalf("InsertBatch() with empty batch error = %v", err)
	}
}


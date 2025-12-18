package exporter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yatisql/yatisql-go/internal/database"
)

func TestDetectOutputDelimiter(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		want     rune
	}{
		{"empty", "", ','},
		{"csv file", "output.csv", ','},
		{"tsv file", "output.tsv", '\t'},
		{"csv.gz file", "output.csv.gz", ','},
		{"tsv.gz file", "output.tsv.gz", '\t'},
		{"no extension", "output", ','},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectOutputDelimiter(tt.filePath)
			if got != tt.want {
				t.Errorf("DetectOutputDelimiter(%q) = %q, want %q", tt.filePath, got, tt.want)
			}
		})
	}
}

func TestExecuteQuery(t *testing.T) {
	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	// Create test table
	headers := []string{"id", "name", "age"}
	if err := database.CreateTable(db.DB, "test", headers); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	batch := [][]string{
		{"1", "Alice", "30"},
		{"2", "Bob", "25"},
		{"3", "Charlie", "35"},
	}
	if err := database.InsertBatch(db.DB, "test", headers, batch); err != nil {
		t.Fatalf("InsertBatch() error = %v", err)
	}

	// Execute query to file
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.csv")

	result, err := Execute(db.DB, "SELECT * FROM test ORDER BY id", outputPath, ',')
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}

	// Verify output file
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 4 { // header + 3 rows
		t.Errorf("Expected 4 lines, got %d", len(lines))
	}
	if lines[0] != "id,name,age" {
		t.Errorf("Expected header 'id,name,age', got %q", lines[0])
	}
}

func TestExecuteQueryWithFilter(t *testing.T) {
	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	headers := []string{"id", "name", "age"}
	if err := database.CreateTable(db.DB, "test", headers); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	batch := [][]string{
		{"1", "Alice", "30"},
		{"2", "Bob", "25"},
		{"3", "Charlie", "35"},
	}
	if err := database.InsertBatch(db.DB, "test", headers, batch); err != nil {
		t.Fatalf("InsertBatch() error = %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.csv")

	result, err := Execute(db.DB, "SELECT name FROM test WHERE age > 28", outputPath, ',')
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2 (Alice and Charlie)", result.RowCount)
	}
}

func TestExecuteQueryToGzip(t *testing.T) {
	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	headers := []string{"id", "name"}
	if err := database.CreateTable(db.DB, "test", headers); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	batch := [][]string{{"1", "Alice"}}
	if err := database.InsertBatch(db.DB, "test", headers, batch); err != nil {
		t.Fatalf("InsertBatch() error = %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.csv.gz")

	result, err := Execute(db.DB, "SELECT * FROM test", outputPath, ',')
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	// Verify file exists and is gzipped
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() == 0 {
		t.Error("Expected non-empty gzip file")
	}
}


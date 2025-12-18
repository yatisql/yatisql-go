package importer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yatisql/yatisql-go/internal/database"
)

func TestDetectDelimiter(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		want     rune
	}{
		{"csv file", "data.csv", ','},
		{"tsv file", "data.tsv", '\t'},
		{"csv.gz file", "data.csv.gz", ','},
		{"tsv.gz file", "data.tsv.gz", '\t'},
		{"csv.bz2 file", "data.csv.bz2", ','},
		{"tsv.bz2 file", "data.tsv.bz2", '\t'},
		{"no extension", "data", ','},
		{"unknown extension", "data.txt", ','},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectDelimiter(tt.filePath)
			if got != tt.want {
				t.Errorf("DetectDelimiter(%q) = %q, want %q", tt.filePath, got, tt.want)
			}
		})
	}
}

func TestImportCSV(t *testing.T) {
	// Find testdata directory
	testdataPath := findTestdata(t)
	csvPath := filepath.Join(testdataPath, "sample.csv")

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, csvPath, "test", ',', true)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.TableName != "test" {
		t.Errorf("TableName = %q, want %q", result.TableName, "test")
	}
	if result.RowCount != 10 {
		t.Errorf("RowCount = %d, want 10", result.RowCount)
	}

	// Verify data
	var count int
	err = db.DB.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if count != 10 {
		t.Errorf("Expected 10 rows in database, got %d", count)
	}
}

func TestImportTSV(t *testing.T) {
	testdataPath := findTestdata(t)
	tsvPath := filepath.Join(testdataPath, "sample.tsv")

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, tsvPath, "test", '\t', true)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.RowCount != 10 {
		t.Errorf("RowCount = %d, want 10", result.RowCount)
	}
}

func TestImportWithoutHeader(t *testing.T) {
	// Create temp file without header
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "noheader.csv")
	content := "1,Alice,30\n2,Bob,25\n3,Charlie,35\n"
	if err := os.WriteFile(tmpFile, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, tmpFile, "test", ',', false)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}

	// Verify column names are auto-generated
	rows, err := db.Query("SELECT col1, col2, col3 FROM test LIMIT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("Expected at least one row")
	}
}

// findTestdata locates the testdata directory relative to the test file.
func findTestdata(t *testing.T) string {
	// Try different relative paths
	paths := []string{
		"../../testdata",
		"../../../testdata",
		"testdata",
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	t.Skip("testdata directory not found")
	return ""
}

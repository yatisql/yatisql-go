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

func TestImportConcurrent(t *testing.T) {
	testdataPath := findTestdata(t)
	usersPath := filepath.Join(testdataPath, "multi_file", "users.csv")
	ordersPath := filepath.Join(testdataPath, "multi_file", "orders.csv")

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	inputs := []FileInput{
		{FilePath: usersPath, TableName: "users", Delimiter: ',', HasHeader: true},
		{FilePath: ordersPath, TableName: "orders", Delimiter: ',', HasHeader: true},
	}

	results, err := ImportConcurrent(db.DB, inputs, false)
	if err != nil {
		t.Fatalf("ImportConcurrent() error = %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Verify both tables exist and have correct row counts
	var usersCount, ordersCount int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM users").Scan(&usersCount); err != nil {
		t.Fatalf("Query users count error = %v", err)
	}
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM orders").Scan(&ordersCount); err != nil {
		t.Fatalf("Query orders count error = %v", err)
	}

	if usersCount != 5 {
		t.Errorf("users table has %d rows, want 5", usersCount)
	}
	if ordersCount != 8 {
		t.Errorf("orders table has %d rows, want 8", ordersCount)
	}

	// Verify we can JOIN the tables
	var joinCount int
	joinQuery := "SELECT COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id"
	if err := db.DB.QueryRow(joinQuery).Scan(&joinCount); err != nil {
		t.Fatalf("JOIN query error = %v", err)
	}
	if joinCount != 8 {
		t.Errorf("JOIN returned %d rows, want 8", joinCount)
	}
}

func TestImportConcurrentPartialFailure(t *testing.T) {
	testdataPath := findTestdata(t)
	usersPath := filepath.Join(testdataPath, "multi_file", "users.csv")
	nonExistentPath := filepath.Join(testdataPath, "nonexistent.csv")

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	inputs := []FileInput{
		{FilePath: usersPath, TableName: "users", Delimiter: ',', HasHeader: true},
		{FilePath: nonExistentPath, TableName: "missing", Delimiter: ',', HasHeader: true},
	}

	results, err := ImportConcurrent(db.DB, inputs, false)

	// Should have one successful result
	if len(results) != 1 {
		t.Errorf("Expected 1 successful result, got %d", len(results))
	}

	// Should have an error for the missing file
	if err == nil {
		t.Error("Expected error for missing file, got nil")
	}

	// The successful import should still work
	var usersCount int
	if err := db.DB.QueryRow("SELECT COUNT(*) FROM users").Scan(&usersCount); err != nil {
		t.Fatalf("Query users count error = %v", err)
	}
	if usersCount != 5 {
		t.Errorf("users table has %d rows, want 5", usersCount)
	}
}

func TestImportConcurrentEmpty(t *testing.T) {
	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	results, err := ImportConcurrent(db.DB, []FileInput{}, false)
	if err != nil {
		t.Errorf("ImportConcurrent() with empty input error = %v", err)
	}
	if results != nil {
		t.Errorf("Expected nil results for empty input, got %v", results)
	}
}

func TestParseFile(t *testing.T) {
	testdataPath := findTestdata(t)
	csvPath := filepath.Join(testdataPath, "sample.csv")

	parsed := ParseFile(FileInput{
		FilePath:  csvPath,
		TableName: "test",
		Delimiter: ',',
		HasHeader: true,
	})

	if parsed.Error != nil {
		t.Fatalf("ParseFile() error = %v", parsed.Error)
	}

	if parsed.TableName != "test" {
		t.Errorf("TableName = %q, want %q", parsed.TableName, "test")
	}

	if len(parsed.Headers) == 0 {
		t.Error("Expected headers to be populated")
	}

	if len(parsed.Rows) != 10 {
		t.Errorf("Expected 10 rows, got %d", len(parsed.Rows))
	}
}

func TestParseFileError(t *testing.T) {
	parsed := ParseFile(FileInput{
		FilePath:  "/nonexistent/file.csv",
		TableName: "test",
		Delimiter: ',',
		HasHeader: true,
	})

	if parsed.Error == nil {
		t.Error("Expected error for nonexistent file, got nil")
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

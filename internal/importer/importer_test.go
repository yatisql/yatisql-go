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

func TestImportTSVWithMissingValues(t *testing.T) {
	// TSV with 3 columns; data row has first value, double tab, last value (middle column empty)
	tmpDir := t.TempDir()
	tsvPath := filepath.Join(tmpDir, "missing.tsv")
	content := "col1\tcol2\tcol3\nfirst\t\tlast\n"
	if err := os.WriteFile(tsvPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, tsvPath, "test", '\t', true)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	var col1, col2, col3 string
	err = db.DB.QueryRow("SELECT col1, col2, col3 FROM test LIMIT 1").Scan(&col1, &col2, &col3)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if col1 != "first" {
		t.Errorf("col1 = %q, want %q", col1, "first")
	}
	if col2 != "" {
		t.Errorf("col2 = %q, want empty string", col2)
	}
	if col3 != "last" {
		t.Errorf("col3 = %q, want %q", col3, "last")
	}
}

func TestImportTSVWithMissingValuesMultipleRows(t *testing.T) {
	// Multiple rows: first row has empty middle, second has empty first, third has empty last
	tmpDir := t.TempDir()
	tsvPath := filepath.Join(tmpDir, "missing_multi.tsv")
	content := "col1\tcol2\tcol3\nfirst\t\tlast\n\tmid\tlast2\nfirst2\tmid2\t\n"
	if err := os.WriteFile(tsvPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, tsvPath, "test", '\t', true)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", result.RowCount)
	}

	rows, err := db.Query("SELECT col1, col2, col3 FROM test ORDER BY rowid")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	want := []struct{ col1, col2, col3 string }{
		{"first", "", "last"},
		{"", "mid", "last2"},
		{"first2", "mid2", ""},
	}
	for i := 0; rows.Next(); i++ {
		var col1, col2, col3 string
		if err := rows.Scan(&col1, &col2, &col3); err != nil {
			t.Fatalf("Scan row %d: %v", i, err)
		}
		if i >= len(want) {
			t.Fatalf("got more than %d rows", len(want))
		}
		if col1 != want[i].col1 || col2 != want[i].col2 || col3 != want[i].col3 {
			t.Errorf("row %d: got (col1=%q, col2=%q, col3=%q), want (%q, %q, %q)",
				i, col1, col2, col3, want[i].col1, want[i].col2, want[i].col3)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestImportTSVWithAllEmptyRow(t *testing.T) {
	// One row with all three columns empty (triple tab between nothing)
	tmpDir := t.TempDir()
	tsvPath := filepath.Join(tmpDir, "empty_row.tsv")
	content := "col1\tcol2\tcol3\n\t\t\n"
	if err := os.WriteFile(tsvPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, tsvPath, "test", '\t', true)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	var col1, col2, col3 string
	err = db.DB.QueryRow("SELECT col1, col2, col3 FROM test LIMIT 1").Scan(&col1, &col2, &col3)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if col1 != "" || col2 != "" || col3 != "" {
		t.Errorf("got (col1=%q, col2=%q, col3=%q), want all empty", col1, col2, col3)
	}
}

func TestImportTSVWithLeadingTabs(t *testing.T) {
	// Tab at beginning of line: line starting with tab yields empty first column.
	// (With tab as delimiter, a leading tab is parsed as an empty first field.)
	tmpDir := t.TempDir()
	tsvPath := filepath.Join(tmpDir, "leading_tabs.tsv")
	content := "col1\tcol2\tcol3\n\tmid\tlast\n\ta\tb\n"
	if err := os.WriteFile(tsvPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, tsvPath, "test", '\t', true)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", result.RowCount)
	}

	rows, err := db.Query("SELECT col1, col2, col3 FROM test ORDER BY rowid")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	want := []struct{ col1, col2, col3 string }{
		{"", "mid", "last"},
		{"", "a", "b"},
	}
	for i := 0; rows.Next(); i++ {
		var col1, col2, col3 string
		if err := rows.Scan(&col1, &col2, &col3); err != nil {
			t.Fatalf("Scan row %d: %v", i+1, err)
		}
		if i >= len(want) {
			t.Fatalf("got more than %d rows", len(want))
		}
		if col1 != want[i].col1 || col2 != want[i].col2 || col3 != want[i].col3 {
			t.Errorf("row %d (leading tab): got (col1=%q, col2=%q, col3=%q), want (%q, %q, %q)",
				i+1, col1, col2, col3, want[i].col1, want[i].col2, want[i].col3)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
}

func TestImportCSVWithMissingValues(t *testing.T) {
	// CSV with empty middle field: a,,c
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "missing.csv")
	content := "col1,col2,col3\na,,c\n"
	if err := os.WriteFile(csvPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	result, err := Import(db.DB, csvPath, "test", ',', true)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}

	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}

	var col1, col2, col3 string
	err = db.DB.QueryRow("SELECT col1, col2, col3 FROM test LIMIT 1").Scan(&col1, &col2, &col3)
	if err != nil {
		t.Fatalf("QueryRow() error = %v", err)
	}
	if col1 != "a" {
		t.Errorf("col1 = %q, want %q", col1, "a")
	}
	if col2 != "" {
		t.Errorf("col2 = %q, want empty string", col2)
	}
	if col3 != "c" {
		t.Errorf("col3 = %q, want %q", col3, "c")
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

	results, err := ImportConcurrent(db.DB, inputs, false, nil, nil, nil)
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

	results, err := ImportConcurrent(db.DB, inputs, false, nil, nil, nil)

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

	results, err := ImportConcurrent(db.DB, []FileInput{}, false, nil, nil, nil)
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
	}, nil)

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
	}, nil)

	if parsed.Error == nil {
		t.Error("Expected error for nonexistent file, got nil")
	}
}

func TestImportWithIndexColumns(t *testing.T) {
	testdataPath := findTestdata(t)
	csvPath := filepath.Join(testdataPath, "sample.csv")

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	inputs := []FileInput{
		{
			FilePath:     csvPath,
			TableName:    "test",
			Delimiter:    ',',
			HasHeader:    true,
			IndexColumns: []string{"id", "name"},
		},
	}

	results, err := ImportConcurrent(db.DB, inputs, false, nil, nil, nil)
	if err != nil {
		t.Fatalf("ImportConcurrent() error = %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	// Verify indexes were created
	var indexCount int
	err = db.DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='test'").Scan(&indexCount)
	if err != nil {
		t.Fatalf("Query index error = %v", err)
	}
	if indexCount != 2 {
		t.Errorf("Expected 2 indexes, got %d", indexCount)
	}
}

func TestImportWithInvalidIndexColumn(t *testing.T) {
	testdataPath := findTestdata(t)
	csvPath := filepath.Join(testdataPath, "sample.csv")

	db, err := database.Open("")
	if err != nil {
		t.Fatalf("database.Open() error = %v", err)
	}
	defer db.Close()

	inputs := []FileInput{
		{
			FilePath:     csvPath,
			TableName:    "test",
			Delimiter:    ',',
			HasHeader:    true,
			IndexColumns: []string{"nonexistent_column"},
		},
	}

	_, err = ImportConcurrent(db.DB, inputs, false, nil, nil, nil)
	if err == nil {
		t.Error("Expected error for nonexistent index column, got nil")
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

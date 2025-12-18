package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yatisql/yatisql-go/internal/config"
)

func TestExecuteHelp(t *testing.T) {
	// Just verify the command structure is valid by checking rootCmd is defined
	if rootCmd == nil {
		t.Error("rootCmd should be defined")
	}
	if rootCmd.Use != "yatisql" {
		t.Errorf("rootCmd.Use = %q, want %q", rootCmd.Use, "yatisql")
	}
}

func TestEndToEndImportAndQuery(t *testing.T) {
	testdataPath := findTestdata(t)
	csvPath := filepath.Join(testdataPath, "sample.csv")

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "output.csv")

	cfg := &config.Config{
		InputFiles: []string{csvPath},
		SQLQuery:   "SELECT name, age FROM data WHERE CAST(age AS INTEGER) > 30 ORDER BY CAST(age AS INTEGER)",
		OutputFile: outputPath,
		HasHeader:  true,
		Delimiter:  ',',
	}

	if err := run(cfg, false); err != nil {
		t.Fatalf("run() error = %v", err)
	}

	// Verify output
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	// Should have header + rows for age > 30 (Charlie:35, Eve:32, Frank:45, Henry:38, Jack:41)
	if len(lines) < 2 {
		t.Errorf("Expected at least 2 lines (header + data), got %d", len(lines))
	}

	// Verify header
	if lines[0] != "name,age" {
		t.Errorf("Expected header 'name,age', got %q", lines[0])
	}
}

func TestEndToEndMultipleFiles(t *testing.T) {
	testdataPath := findTestdata(t)
	usersPath := filepath.Join(testdataPath, "multi_file", "users.csv")
	ordersPath := filepath.Join(testdataPath, "multi_file", "orders.csv")

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "joined.csv")

	cfg := &config.Config{
		InputFiles: []string{usersPath, ordersPath},
		TableNames: []string{"users", "orders"},
		SQLQuery:   "SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name, o.product",
		OutputFile: outputPath,
		HasHeader:  true,
		Delimiter:  ',',
	}

	if err := run(cfg, false); err != nil {
		t.Fatalf("run() error = %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	// Should have header + 8 order rows
	if len(lines) != 9 {
		t.Errorf("Expected 9 lines (header + 8 orders), got %d", len(lines))
	}
}

func TestRunWithTempDatabase(t *testing.T) {
	testdataPath := findTestdata(t)
	csvPath := filepath.Join(testdataPath, "sample.csv")

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "count.csv")

	cfg := &config.Config{
		InputFiles: []string{csvPath},
		SQLQuery:   "SELECT COUNT(*) as total FROM data",
		OutputFile: outputPath,
		HasHeader:  true,
		Delimiter:  ',',
	}

	if err := run(cfg, false); err != nil {
		t.Fatalf("run() error = %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 {
		t.Errorf("Expected 2 lines (header + count), got %d", len(lines))
	}
	if lines[1] != "10" {
		t.Errorf("Expected count '10', got %q", lines[1])
	}
}

func TestRunWithPersistentDatabase(t *testing.T) {
	testdataPath := findTestdata(t)
	csvPath := filepath.Join(testdataPath, "sample.csv")

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	outputPath := filepath.Join(tmpDir, "output.csv")

	// First run: import data
	cfg1 := &config.Config{
		InputFiles: []string{csvPath},
		DBPath:     dbPath,
		HasHeader:  true,
		Delimiter:  ',',
		KeepDB:     true,
	}

	if err := run(cfg1, false); err != nil {
		t.Fatalf("run() import error = %v", err)
	}

	// Verify database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Expected database file to exist")
	}

	// Second run: query existing database
	cfg2 := &config.Config{
		DBPath:     dbPath,
		SQLQuery:   "SELECT name FROM data WHERE name LIKE 'A%'",
		OutputFile: outputPath,
		Delimiter:  ',',
		KeepDB:     true,
	}

	if err := run(cfg2, false); err != nil {
		t.Fatalf("run() query error = %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	// Should have header + 1 row (Alice)
	if len(lines) != 2 {
		t.Errorf("Expected 2 lines (header + Alice), got %d", len(lines))
	}
}

// findTestdata locates the testdata directory relative to the test file.
func findTestdata(t *testing.T) string {
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

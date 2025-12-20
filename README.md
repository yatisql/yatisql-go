# yatisql-go

[![CI](https://github.com/yatisql/yatisql-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/yatisql/yatisql-go/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/yatisql/yatisql-go)](https://goreportcard.com/report/github.com/yatisql/yatisql-go)

```
██╗   ██╗ █████╗ ████████╗██╗███████╗ ██████╗ ██╗     
╚██╗ ██╔╝██╔══██╗╚══██╔══╝██║██╔════╝██╔═══██╗██║     
 ╚████╔╝ ███████║   ██║   ██║███████╗██║   ██║██║     
  ╚██╔╝  ██╔══██║   ██║   ██║╚════██║██║▄▄ ██║██║     
   ██║   ██║  ██║   ██║   ██║███████║╚██████╔╝███████╗
   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝╚══════╝ ╚══▀▀═╝ ╚══════╝
          Yet Another Tabular Inefficient SQL          
```

A fast Go CLI tool that streams CSV/TSV files into SQLite, executes SQL queries, and exports results back to CSV/TSV format.

## Why yatisql Instead of Just SQLite?

You might wonder: *"SQLite already has a `.import` command and a CLI—why use yatisql?"*

Here's what yatisql brings to the table:

| Pain Point with Raw SQLite                           | yatisql Solution                                     |
| ---------------------------------------------------- | ---------------------------------------------------- |
| Manual schema creation before import                 | Automatically infers columns from CSV headers        |
| `.import` loads entire file into memory              | Streaming mode handles 100GB+ files with ~50MB RAM   |
| Sequential imports only                              | Concurrent imports with parallel goroutines          |
| No progress feedback on large files                  | Real-time progress bars with row counts and speeds   |
| Manual gzip handling (`gunzip` → import → cleanup)   | Transparent .gz compression/decompression            |
| Column names with spaces/special chars break queries | Automatic column name sanitization                   |
| Temp databases require manual cleanup                | Auto-deleted temp DBs when no `-d` flag is used      |
| Joining CSVs requires multiple steps                 | One-liner: `-i a.csv,b.csv -q "SELECT ... JOIN ..."` |

**TL;DR:** yatisql is the "batteries-included" wrapper that handles all the tedious boilerplate around CSV→SQL workflows, so you can focus on your query instead of fighting with imports.

## Why Not Just Use Python (pandas)?

Python with pandas is the go-to for data wrangling, but it has trade-offs:

| Python/pandas Pain Point                              | yatisql Advantage                                   |
| ----------------------------------------------------- | --------------------------------------------------- |
| Requires Python + pip + virtual env setup             | Single static binary, zero dependencies             |
| `pd.read_csv()` loads entire file into RAM            | Streaming keeps memory flat regardless of file size |
| Writing a script for a one-off query                  | One CLI command, no code needed                     |
| Slower for large datasets (Python interpreter)        | Go compiled binary, 5-10x faster on big files       |
| Dependency hell (`numpy`, `pandas` version conflicts) | No runtime dependencies to break                    |
| Awkward in shell pipelines and cron jobs              | Native CLI, pipes, and exit codes work as expected  |

*"But I can bundle Python into a binary with PyInstaller/Nuitka!"* — True, but you'd still need to write the script first, the resulting binary would be 50-200MB (bundled interpreter + numpy + pandas), and pandas would still load your CSV into RAM. yatisql is ~15MB and streams by design.

**When to use pandas instead:** If you need complex transformations, ML preprocessing, or visualization—pandas is the right tool. yatisql is for when you just need to **query** CSVs fast without ceremony.

## Features

- 🚀 **Concurrent file processing** - Import multiple files in parallel
- 📊 **Streaming mode** - Process 100GB+ files with minimal memory usage
- 🔍 **Execute SQL queries** on imported data
- 📤 **Export query results** to CSV/TSV files
- 🗜️ **Compression support** - Handles gzip-compressed files (.gz) automatically
- 🔗 **JOIN support** - Import multiple files and join them in SQL queries
- 🔑 **Index creation** - Create indexes on columns with `-x` flag for faster queries
- 📈 **Progress bars** - Real-time progress with `-p` flag
- 🎨 **Colored output** for better readability
- ⚡ **WAL mode** - Concurrent writes to different tables
- 📁 **Automatic directory creation** for database paths
- 🔄 **Stdin/stdout support** - Read from stdin and write to stdout for pipeline usage

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/yatisql/yatisql-go.git
cd yatisql-go

# Build using Make
make build

# Or install to GOPATH/bin
make install
```

### Using Go Install

```bash
go install github.com/yatisql/yatisql-go/cmd/yatisql@latest
```

### From Releases

Download pre-built binaries from the [Releases](https://github.com/yatisql/yatisql-go/releases) page.

## Usage

### Basic Example: Import CSV and Query

```bash
# Import CSV file and execute a query (uses temporary database, auto-cleaned)
yatisql -i data.csv -q "SELECT * FROM data LIMIT 10" -o results.csv

# Or with long flags
yatisql --input data.csv --query "SELECT * FROM data LIMIT 10" --output results.csv
```

### Import Multiple Files Concurrently

```bash
# Import multiple large files in parallel with progress bars
yatisql -i users.csv.gz,orders.csv.gz,products.csv.gz -t users,orders,products -d warehouse.db -p

# Then query with JOINs
yatisql -d warehouse.db -q "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id" -o report.csv
```

### Progress Bars

```bash
# Show progress bars for large file imports
yatisql -i huge_file.csv.gz -d data.db -t mytable -p
```

Output:
```
huge_file.csv.gz       ⠙ 1.2M rows (250K/s)
```

### Import Only

```bash
# Import CSV into persistent SQLite database
yatisql -i data.csv -d mydata.db -t mytable

# With directory path (creates directories automatically)
yatisql -i data.csv -d db/production/mydata.db -t mytable
```

### Query Existing Database

```bash
# Query an existing SQLite database
yatisql -d mydata.db -q "SELECT COUNT(*) FROM mytable" -o count.csv
```

### TSV Files

```bash
# Import TSV file
yatisql -i data.tsv --delimiter tab -q "SELECT * FROM data WHERE age > 30" -o filtered.tsv
```

### Stdin and Stdout (Pipeline Support)

yatisql supports reading from stdin and writing to stdout, making it perfect for shell pipelines:

```bash
# Read from stdin (omit -i flag or use '-')
cat data.csv | yatisql -q "SELECT * FROM data LIMIT 10"

# Explicit stdin indicator
cat data.csv | yatisql -i - -q "SELECT * FROM data LIMIT 10"

# Write to stdout (omit -o flag)
yatisql -i data.csv -q "SELECT * FROM data LIMIT 10"

# Full pipeline: stdin → query → stdout
cat data.csv | yatisql -q "SELECT name, age FROM data WHERE CAST(age AS INTEGER) > 30" | head -5

# Chain with other tools
cat data.csv | yatisql -q "SELECT * FROM data" | grep "pattern" | sort

# With explicit delimiter for stdin
cat data.tsv | yatisql --delimiter tab -q "SELECT * FROM data LIMIT 10"
```

**Notes:**
- When reading from stdin, delimiter defaults to comma (`,`) if `--delimiter auto` is used
- Progress bars are automatically disabled when reading from stdin
- Stdin cannot be compressed (no `.gz` support for stdin)
- Output to stdout is CSV format by default

## Command Line Options

| Flag            | Short | Description                                                                                                          |
| --------------- | ----- | -------------------------------------------------------------------------------------------------------------------- |
| `--input`       | `-i`  | Input CSV/TSV file path(s), comma-separated for multiple files (supports .gz compression). Use `-` or omit for stdin |
| `--output`      | `-o`  | Output CSV/TSV file path (default: stdout, supports .gz compression). Use `-` for explicit stdout                    |
| `--query`       | `-q`  | SQL query to execute                                                                                                 |
| `--db`          | `-d`  | SQLite database path (default: temporary file, auto-deleted after execution)                                         |
| `--table`       | `-t`  | Table name(s) for imported data, comma-separated (default: `data`, `data2`, etc.)                                    |
| `--index`       | `-x`  | Column(s) to create indexes on, comma-separated (validates columns exist early)                                      |
| `--header`      | `-H`  | Input file has header row (default: `true`)                                                                          |
| `--delimiter`   |       | Field delimiter: `comma`, `tab`, or `auto` (default: `auto`)                                                         |
| `--progress`    | `-p`  | Show progress bars for file import operations                                                                        |
| `--trace`       |       | Write execution trace to file (use `go tool trace <file>` to view)                                                   |
| `--trace-debug` |       | Enable debug logging for concurrent execution                                                                        |

### Database Behavior

- **Default (no `-d` flag)**: Creates a temporary database file that is automatically deleted after execution
- **With `-d` flag**: Creates/uses the specified database file and keeps it persistent
- **Directory paths**: Automatically creates parent directories if they don't exist (e.g., `-d db/production/data.db`)
- **WAL mode**: SQLite Write-Ahead Logging is enabled for better concurrent write performance

## Examples

### Filter and Export

```bash
yatisql -i users.csv -q "SELECT name, email FROM data WHERE age > 18" -o adults.csv
```

### Aggregate Functions

```bash
yatisql -i sales.csv -q "SELECT category, SUM(amount) as total FROM data GROUP BY category" -o summary.csv
```

### JOIN Multiple Tables

Import multiple files and join them in a single command:

```bash
# Import multiple files and execute JOIN query
yatisql -i users.csv,orders.csv -t users,orders -q "SELECT u.name, u.email, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id" -o joined.csv
```

Or import separately and query later:

```bash
# Import users
yatisql -i users.csv -d app.db -t users

# Import orders
yatisql -i orders.csv -d app.db -t orders

# Query with JOIN
yatisql -d app.db -q "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id" -o joined.csv
```

### Large Files with Progress

```bash
# Import 100GB+ files with streaming (low memory) and progress bars
yatisql -i huge1.csv.gz,huge2.csv.gz -t table1,table2 -d warehouse.db -p

# Output:
# huge1.csv.gz           ⠙ 5.2M rows (180K/s)
# huge2.csv.gz           ⠹ 3.8M rows (165K/s)
```

### Compressed Files

```bash
# Import compressed CSV
yatisql -i data.csv.gz -q "SELECT * FROM data LIMIT 10" -o results.csv

# Export to compressed CSV
yatisql -i data.csv -q "SELECT * FROM data" -o results.csv.gz

# Chain compressed files
yatisql -i data1.csv.gz,data2.csv.gz -t table1,table2 -q "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id" -o joined.csv.gz
```

### Create Indexes

Create indexes on columns for faster queries:

```bash
# Create index on a single column
yatisql -i data.csv -d mydata.db -x user_id

# Create indexes on multiple columns
yatisql -i data.csv -d mydata.db -x user_id,email,created_at

# With progress bars
yatisql -i accidents.csv.gz -d accidents.db -t accidents -x State,City,Severity -p
```

Output:
```
  [→] Parsing & writing accidents.csv.gz → table 'accidents' (streaming)...
  [→] Creating 3 index(es) on 'accidents'...
  [✓] Created 3 index(es) on 'accidents' in 1.234s
  [✓] Completed streaming accidents.csv.gz (500000 rows) in 3.5s
✓ Successfully imported table 'accidents'
```

**Early validation**: If a column doesn't exist in the CSV, the import fails immediately with a clear error:
```
Error: index columns not found in file 'data.csv': nonexistent_column
```

### Debugging & Tracing

```bash
# Enable execution trace for performance analysis
yatisql -i data.csv -d test.db --trace trace.out

# View trace
go tool trace trace.out

# Enable debug logging
yatisql -i data.csv -d test.db --trace-debug
```

## Project Structure

```
yatisql-go/
├── cmd/
│   └── yatisql/
│       └── main.go              # Entry point
├── internal/
│   ├── cli/                     # Cobra command setup, flags, progress bars
│   ├── config/                  # Configuration types
│   ├── database/                # SQLite operations (WAL mode)
│   ├── exporter/                # Query execution, CSV export
│   └── importer/                # CSV/TSV import, streaming, compression
├── scripts/                     # Utility scripts
├── testdata/                    # Test fixtures
├── .github/workflows/           # CI/CD pipelines
├── .gitignore
├── .golangci.yaml               # Linter configuration
├── .goreleaser.yaml             # Release automation
├── Makefile                     # Build automation
├── go.mod
├── go.sum
├── LICENSE
└── README.md
```

## Development

### Prerequisites

- Go 1.21 or later
- SQLite3 (for CGO)
- golangci-lint (optional, for linting)

### Building

```bash
# Build binary
make build

# Run tests
make test

# Run tests with coverage
make test-coverage

# Run linter
make lint

# Clean build artifacts
make clean

# Show all available targets
make help
```

### Running Tests

```bash
# Run all tests
make test

# Run tests with race detection and coverage
go test -v -race -cover ./...

# Run tests for a specific package
go test -v ./internal/database/...
```

### Code Quality

```bash
# Format code
make fmt

# Run linter
make lint

# Run linter with auto-fix
make lint-fix
```

## Performance

### Streaming Mode

yatisql uses streaming to process files with minimal memory:
- Reads CSV rows in batches
- Writes to SQLite immediately
- Only one batch (~10,000 rows) in memory at a time
- Suitable for files larger than available RAM

### Concurrent Processing

- Multiple files are processed in parallel goroutines
- SQLite WAL mode enables concurrent writes to different tables
- Progress bars show real-time status for each file

### Benchmarks

Typical performance on modern hardware:
- **Import speed**: 150-250K rows/second (depends on row size)
- **Memory usage**: ~50-100MB regardless of file size
- **Compression**: gzip files are decompressed on-the-fly

## Notes

- **Streaming**: Large files are streamed in batches for constant memory usage
- **Column sanitization**: Column names are automatically sanitized for SQL compatibility
- **Data types**: All data is stored as TEXT in SQLite for maximum flexibility
- **Compression**: Supports gzip (.gz) for both input and output files automatically
- **Multiple files**: Use comma-separated values for `-i`/`--input` and `-t`/`--table` flags
- **Concurrent imports**: Multiple files are imported in parallel for faster processing
- **WAL mode**: SQLite Write-Ahead Logging is enabled for concurrent write performance
- **Indexing**: Create indexes with `-x` flag; columns are validated early before import starts
- **Colored output**: Success messages are green, errors are red, info messages are cyan

## Getting Help

```bash
# Show help
yatisql --help

# Or
yatisql -h
```

## License

MIT License - see [LICENSE](LICENSE) for details.

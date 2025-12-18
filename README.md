# yatisql-go
yet another tabular inefficient SQL in Golang

A simple Go CLI tool that streams CSV/TSV files into SQLite, executes SQL queries, and exports results back to CSV/TSV format.

## Features

- 🚀 **Stream large CSV/TSV files** into SQLite database efficiently
- 🔍 **Execute SQL queries** on imported data
- 📤 **Export query results** to CSV/TSV files
- 🗜️ **Compression support**: Automatically handles gzip-compressed files (.gz) for both input and output
- 🔗 **JOIN support**: Import multiple files and join them in SQL queries
- 🎨 **Colored output** for better readability
- 📁 **Automatic directory creation** for database paths
- 🔄 **Temporary databases**: Default behavior creates and cleans up temporary database files
- ⚡ Batch processing for memory efficiency (10,000 rows at a time)

## Installation

```bash
go mod download
go build -o yatisql main.go
```

## Usage

### Basic Example: Import CSV and Query

```bash
# Import CSV file and execute a query (uses temporary database, auto-cleaned)
yatisql -i data.csv -q "SELECT * FROM data LIMIT 10" -o results.csv

# Or with long flags
yatisql --input data.csv --query "SELECT * FROM data LIMIT 10" --output results.csv
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

### Multiple Operations

```bash
# Import, then query
yatisql -i data.csv -d temp.db -t data -q "SELECT name, age FROM data WHERE age > 25 ORDER BY age" -o results.csv
```

## Command Line Options

| Flag          | Short | Description                                                                                                                |
| ------------- | ----- | -------------------------------------------------------------------------------------------------------------------------- |
| `--input`     | `-i`  | Input CSV/TSV file path(s), comma-separated for multiple files (supports .gz compression)                                  |
| `--output`    | `-o`  | Output CSV/TSV file path (default: stdout, supports .gz compression)                                                       |
| `--query`     | `-q`  | SQL query to execute                                                                                                       |
| `--db`        | `-d`  | SQLite database path (default: temporary file, auto-deleted after execution). Supports directory paths like `db/mydata.db` |
| `--table`     | `-t`  | Table name(s) for imported data, comma-separated (default: `data`, `data2`, etc.)                                          |
| `--header`    | `-H`  | Input file has header row (default: `true`)                                                                                |
| `--delimiter` |       | Field delimiter: `comma`, `tab`, or `auto` (default: `auto`)                                                               |

### Database Behavior

- **Default (no `-d` flag)**: Creates a temporary database file that is automatically deleted after execution
- **With `-d` flag**: Creates/uses the specified database file and keeps it persistent
- **Directory paths**: Automatically creates parent directories if they don't exist (e.g., `-d db/production/data.db`)

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

### Compressed Files

```bash
# Import compressed CSV
yatisql -i data.csv.gz -q "SELECT * FROM data LIMIT 10" -o results.csv

# Export to compressed CSV
yatisql -i data.csv -q "SELECT * FROM data" -o results.csv.gz

# Chain compressed files
yatisql -i data1.csv.gz,data2.csv.gz -t table1,table2 -q "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id" -o joined.csv.gz
```

### Database Management

```bash
# Use temporary database (default - auto-deleted)
yatisql -i data.csv -q "SELECT * FROM data" -o results.csv

# Use persistent database in current directory
yatisql -i data.csv -d mydata.db -q "SELECT * FROM data"

# Use database in custom directory (creates directories automatically)
yatisql -i data.csv -d db/production/data.db -q "SELECT * FROM data"

# Query existing database
yatisql -d db/production/data.db -q "SELECT COUNT(*) FROM data"
```

## Notes

- **Batch processing**: Large files are processed in batches (10,000 rows at a time) for memory efficiency
- **Column sanitization**: Column names are automatically sanitized for SQL compatibility
- **Data types**: All data is stored as TEXT in SQLite for maximum flexibility
- **Compression**: Supports gzip (.gz) for both input and output files automatically
- **Multiple files**: Use comma-separated values for `-i`/`--input` and `-t`/`--table` flags to import multiple files at once
- **Temporary databases**: By default, creates temporary database files that are automatically cleaned up after execution
- **Directory creation**: Database paths with directories (e.g., `db/mydata.db`) automatically create parent directories if needed
- **Colored output**: Success messages are green, errors are red, info messages are cyan

## Getting Help

```bash
# Show help
yatisql --help

# Or
yatisql -h
```

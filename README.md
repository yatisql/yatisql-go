# yatisql-go
yet another tabular inefficient SQL in Golang

A simple Go wrapper that streams CSV/TSV files into SQLite, executes SQL queries, and exports results back to CSV/TSV format.

## Features

- Stream large CSV/TSV files into SQLite database efficiently
- Execute SQL queries on imported data
- Export query results to CSV/TSV files
- **Compression support**: Automatically handles gzip-compressed files (.gz) for both input and output
- **JOIN support**: Import multiple files and join them in SQL queries
- Automatic delimiter detection (CSV vs TSV)
- Batch processing for memory efficiency
- Support for in-memory or persistent databases

## Installation

```bash
go mod download
go build -o yatisql main.go
```

## Usage

### Basic Example: Import CSV and Query

```bash
# Import CSV file and execute a query
./yatisql -input data.csv -query "SELECT * FROM data LIMIT 10" -output results.csv
```

### Import Only

```bash
# Import CSV into SQLite database
./yatisql -input data.csv -db mydata.db -table mytable
```

### Query Existing Database

```bash
# Query an existing SQLite database
./yatisql -db mydata.db -query "SELECT COUNT(*) FROM mytable" -output count.csv
```

### TSV Files

```bash
# Import TSV file
./yatisql -input data.tsv -delimiter tab -query "SELECT * FROM data WHERE age > 30" -output filtered.tsv
```

### Multiple Operations

```bash
# Import, then query
./yatisql -input data.csv -db temp.db -table data -query "SELECT name, age FROM data WHERE age > 25 ORDER BY age" -output results.csv
```

## Command Line Options

- `-input <files>`: Input CSV/TSV file path(s), comma-separated for multiple files (supports .gz compression)
- `-output <file>`: Output CSV/TSV file path (default: stdout, supports .gz compression)
- `-query <sql>`: SQL query to execute
- `-db <path>`: SQLite database path (default: `:memory:`)
- `-table <names>`: Table name(s) for imported data, comma-separated (default: `data`, `data2`, etc.)
- `-header`: Input file has header row (default: `true`)
- `-delimiter <type>`: Field delimiter: `comma`, `tab`, or `auto` (default: `auto`)

## Examples

### Filter and Export

```bash
./yatisql -input users.csv -query "SELECT name, email FROM data WHERE age > 18" -output adults.csv
```

### Aggregate Functions

```bash
./yatisql -input sales.csv -query "SELECT category, SUM(amount) as total FROM data GROUP BY category" -output summary.csv
```

### JOIN Multiple Tables

Import multiple files and join them in a single command:

```bash
# Import multiple files and execute JOIN query
./yatisql -input users.csv,orders.csv -table users,orders -query "SELECT u.name, u.email, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id" -output joined.csv
```

Or import separately and query later:

```bash
# Import users
./yatisql -input users.csv -db app.db -table users

# Import orders
./yatisql -input orders.csv -db app.db -table orders

# Query with JOIN
./yatisql -db app.db -query "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id" -output joined.csv
```

### Compressed Files

```bash
# Import compressed CSV
./yatisql -input data.csv.gz -query "SELECT * FROM data LIMIT 10" -output results.csv

# Export to compressed CSV
./yatisql -input data.csv -query "SELECT * FROM data" -output results.csv.gz

# Chain compressed files
./yatisql -input data1.csv.gz,data2.csv.gz -table table1,table2 -query "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id" -output joined.csv.gz
```

## Notes

- Large files are processed in batches (10,000 rows at a time) for memory efficiency
- Column names are automatically sanitized for SQL compatibility
- All data is stored as TEXT in SQLite for maximum flexibility
- Compression: Supports gzip (.gz) for both input and output files automatically
- Multiple files: Use comma-separated values for `-input` and `-table` flags to import multiple files at once

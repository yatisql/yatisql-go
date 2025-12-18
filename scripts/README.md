# Scripts

## generate_large_csv.go

Generates large CSV files for testing purposes. Supports compression, configurable rows/columns, and **streams data directly to disk** to minimize memory usage.

### Usage

```bash
# Generate 1 million rows with 10 columns, compressed
go run scripts/generate_large_csv.go -rows 1000000 -cols 10 -output large_data.csv.gz

# Generate uncompressed CSV
go run scripts/generate_large_csv.go -rows 500000 -cols 5 -output data.csv -compress=false

# Quick usage with shell script
./scripts/generate_large_csv.sh 1000000 10 large_data.csv.gz
```

### Options

- `-rows`: Number of rows to generate (default: 1000000)
- `-cols`: Number of columns to generate (default: 10)
- `-output`: Output file path (default: large_data.csv.gz)
- `-compress`: Compress output with gzip (default: true)
- `-seed`: Random seed for reproducible data (default: current time)
- `-batch`: Batch size for writing/flushing (rows per flush, default: 10000)
- `-flush-every`: Print progress every N rows (default: 100000)

### Streaming to Disk

The script **streams data directly to disk** by:
- Writing in configurable batches (default: 10,000 rows)
- Flushing CSV buffer after each batch
- Flushing gzip compressor after each batch
- Syncing file to disk after each batch (`file.Sync()`)

This ensures:
- **Low memory usage** - Only one batch is held in memory at a time
- **Data safety** - Data is persisted even if process is interrupted
- **Progress visibility** - File grows incrementally, can be monitored

### Examples

```bash
# Generate 10 million rows for stress testing
go run scripts/generate_large_csv.go -rows 10000000 -cols 20 -output huge_test.csv.gz

# Generate smaller test file
go run scripts/generate_large_csv.go -rows 10000 -cols 5 -output small_test.csv

# Generate with specific seed for reproducibility
go run scripts/generate_large_csv.go -rows 100000 -seed 12345 -output test.csv.gz

# Custom batch size for more frequent flushing (useful for very large files)
go run scripts/generate_large_csv.go -rows 50000000 -batch 5000 -flush-every 50000 -output huge.csv.gz

# Monitor file growth while generating (in another terminal)
watch -n 1 'ls -lh huge.csv.gz'
```


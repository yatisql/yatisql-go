#!/bin/bash
# Simple wrapper script to generate large CSV files

# Default values
ROWS=${1:-1000000}
COLS=${2:-10}
OUTPUT=${3:-large_data.csv.gz}

echo "Generating $ROWS rows with $COLS columns -> $OUTPUT"

go run scripts/generate_large_csv.go \
-rows "$ROWS" \
-cols "$COLS" \
-output "$OUTPUT"


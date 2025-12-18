package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	var (
		rows       = flag.Int("rows", 1000000, "Number of rows to generate")
		cols       = flag.Int("cols", 10, "Number of columns")
		output     = flag.String("output", "large_data.csv.gz", "Output file path")
		compress   = flag.Bool("compress", true, "Compress output with gzip")
		seed       = flag.Int64("seed", time.Now().UnixNano(), "Random seed")
		batchSize  = flag.Int("batch", 10000, "Batch size for writing (rows per flush)")
		flushEvery = flag.Int("flush-every", 100000, "Print progress every N rows")
	)
	flag.Parse()

	rand.Seed(*seed)

	// Open output file
	var file *os.File
	var err error
	if *compress {
		file, err = os.Create(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
	} else {
		file, err = os.Create(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
	}

	// Create writer (with or without compression)
	var writer *csv.Writer
	var gzWriter *gzip.Writer
	if *compress {
		gzWriter = gzip.NewWriter(file)
		defer gzWriter.Close()
		writer = csv.NewWriter(gzWriter)
	} else {
		writer = csv.NewWriter(file)
	}
	defer writer.Flush()

	// Generate header
	header := make([]string, *cols)
	for i := 0; i < *cols; i++ {
		header[i] = fmt.Sprintf("col%d", i+1)
	}
	if err := writer.Write(header); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing header: %v\n", err)
		os.Exit(1)
	}

	// Generate rows
	batch := make([][]string, 0, *batchSize)

	for i := 0; i < *rows; i++ {
		row := make([]string, *cols)
		for j := 0; j < *cols; j++ {
			// Generate varied data types
			switch j % 4 {
			case 0: // Integer
				row[j] = fmt.Sprintf("%d", rand.Intn(1000000))
			case 1: // Float
				row[j] = fmt.Sprintf("%.2f", rand.Float64()*1000)
			case 2: // String
				row[j] = fmt.Sprintf("value_%d_%d", i, j)
			case 3: // Mixed
				row[j] = fmt.Sprintf("id_%d", rand.Intn(10000))
			}
		}
		batch = append(batch, row)

		// Write batch periodically and flush to disk
		if len(batch) >= *batchSize {
			if err := writer.WriteAll(batch); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing batch: %v\n", err)
				os.Exit(1)
			}
			writer.Flush() // Flush CSV buffer
			if gzWriter != nil {
				if err := gzWriter.Flush(); err != nil {
					fmt.Fprintf(os.Stderr, "Error flushing gzip: %v\n", err)
					os.Exit(1)
				}
			}
			if err := file.Sync(); err != nil {
				fmt.Fprintf(os.Stderr, "Error syncing to disk: %v\n", err)
				os.Exit(1)
			}
			batch = batch[:0]
			if (i+1)%*flushEvery == 0 {
				fmt.Fprintf(os.Stderr, "Generated %d rows (flushed to disk)...\n", i+1)
			}
		}
	}

	// Write remaining rows
	if len(batch) > 0 {
		if err := writer.WriteAll(batch); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing final batch: %v\n", err)
			os.Exit(1)
		}
		writer.Flush()
		if gzWriter != nil {
			gzWriter.Flush()
		}
		file.Sync()
	}

	fmt.Fprintf(os.Stderr, "Successfully generated %d rows with %d columns in %s\n", *rows, *cols, *output)
}

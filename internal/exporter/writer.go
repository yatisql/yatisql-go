// Package exporter provides query execution and result export functionality.
package exporter

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// OpenOutputFile opens an output file, handling compression automatically based on extension.
// If filePath is empty, returns os.Stdout.
func OpenOutputFile(filePath string) (io.WriteCloser, error) {
	if filePath == "" {
		return os.Stdout, nil
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".gz":
		return &gzipWriter{file: file, writer: gzip.NewWriter(file)}, nil
	case ".bz2":
		file.Close()
		return nil, fmt.Errorf("bzip2 output compression not yet supported, use .gz instead")
	default:
		return file, nil
	}
}

// gzipWriter wraps gzip writer and file to close both properly.
type gzipWriter struct {
	file   *os.File
	writer *gzip.Writer
}

func (g *gzipWriter) Write(p []byte) (int, error) {
	return g.writer.Write(p)
}

func (g *gzipWriter) Close() error {
	if err := g.writer.Close(); err != nil {
		g.file.Close()
		return err
	}
	return g.file.Close()
}

// DetectOutputDelimiter detects the output delimiter based on file extension.
// Returns ',' for CSV files and '\t' for TSV files.
func DetectOutputDelimiter(filePath string) rune {
	if filePath == "" {
		return ','
	}

	// Strip compression extensions first
	path := filePath
	for {
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".gz" || ext == ".bz2" {
			path = strings.TrimSuffix(path, filepath.Ext(path))
			continue
		}
		break
	}

	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".tsv" {
		return '\t'
	}
	return ','
}

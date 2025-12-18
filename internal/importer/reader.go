// Package importer provides CSV/TSV file import functionality for yatisql.
package importer

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// OpenFile opens a file, handling compression automatically based on extension.
// Supports .gz (gzip) and .bz2 (bzip2) compressed files.
func OpenFile(filePath string) (io.ReadCloser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".gz":
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return &gzipFile{file: file, reader: gzReader}, nil
	case ".bz2":
		return &bzip2File{file: file, reader: bzip2.NewReader(file)}, nil
	default:
		return file, nil
	}
}

// gzipFile wraps gzip reader and file to close both.
type gzipFile struct {
	file   *os.File
	reader *gzip.Reader
}

func (g *gzipFile) Read(p []byte) (int, error) {
	return g.reader.Read(p)
}

func (g *gzipFile) Close() error {
	g.reader.Close()
	return g.file.Close()
}

// bzip2File wraps bzip2 reader and file to close both.
type bzip2File struct {
	file   *os.File
	reader io.Reader
}

func (b *bzip2File) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *bzip2File) Close() error {
	return b.file.Close()
}

// DetectDelimiter detects the delimiter based on file extension.
// Returns ',' for CSV files and '\t' for TSV files.
func DetectDelimiter(filePath string) rune {
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

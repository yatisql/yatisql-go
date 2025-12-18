package cli

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
)

// ProgressTracker manages multiple concurrent progress bars.
type ProgressTracker struct {
	mu      sync.Mutex
	enabled bool
	bars    []*barState
	stopCh  chan struct{}
	doneCh  chan struct{}
	started bool
}

type barState struct {
	key       string
	label     string
	current   int64
	total     int64
	startTime time.Time
	done      bool
	doneMsg   string
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker(enabled bool) *ProgressTracker {
	return &ProgressTracker{
		enabled: enabled,
		bars:    make([]*barState, 0),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// startRenderLoop starts the render loop if not already started.
func (pt *ProgressTracker) startRenderLoop() {
	if pt.started {
		return
	}
	pt.started = true
	go pt.renderLoop()
}

// renderLoop continuously redraws all progress bars.
func (pt *ProgressTracker) renderLoop() {
	defer close(pt.doneCh)

	// Hide cursor
	fmt.Print("\033[?25l")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	firstRender := true

	for {
		select {
		case <-pt.stopCh:
			fmt.Print("\033[?25h") // Show cursor
			return
		case <-ticker.C:
			pt.render(firstRender)
			firstRender = false
		}
	}
}

// render draws all progress bars.
func (pt *ProgressTracker) render(firstRender bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if len(pt.bars) == 0 {
		return
	}

	// Move cursor up to overwrite previous render (except first time)
	if !firstRender {
		fmt.Printf("\033[%dA", len(pt.bars))
	}

	// Render each bar on its own line
	for _, bar := range pt.bars {
		fmt.Print("\r\033[K") // Clear line
		if bar.done {
			fmt.Print(bar.doneMsg)
		} else {
			pt.drawBar(bar)
		}
		fmt.Println()
	}
}

// drawBar draws a single progress bar.
func (pt *ProgressTracker) drawBar(bar *barState) {
	const width = 30

	elapsed := time.Since(bar.startTime)
	var rate float64
	if elapsed.Seconds() > 0 {
		rate = float64(bar.current) / elapsed.Seconds()
	}

	labelColor := color.New(color.FgCyan)
	barColor := color.New(color.FgYellow)

	labelColor.Printf("%s ", bar.label)

	if bar.total > 0 {
		// Known total - show progress bar
		percent := float64(bar.current) / float64(bar.total) * 100
		filled := int(float64(width) * percent / 100)
		if filled > width {
			filled = width
		}
		empty := width - filled

		fmt.Print("[")
		barColor.Print(strings.Repeat("█", filled))
		fmt.Print(strings.Repeat("░", empty))
		fmt.Print("] ")
		fmt.Printf("%5.1f%% %s/%s %s/s",
			percent,
			fmtNum(bar.current),
			fmtNum(bar.total),
			fmtNum(int64(rate)))
	} else {
		// Unknown total - spinner
		spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
		idx := int(time.Now().UnixMilli()/100) % len(spinner)
		fmt.Printf("%s %s rows (%s/s)",
			spinner[idx],
			fmtNum(bar.current),
			fmtNum(int64(rate)))
	}
}

// Stop stops the render loop and prints final state.
func (pt *ProgressTracker) Stop() {
	if !pt.enabled || !pt.started {
		return
	}

	// Final render to show all completion messages
	pt.render(false)

	close(pt.stopCh)
	<-pt.doneCh
}

// findBar finds a bar by key.
func (pt *ProgressTracker) findBar(key string) *barState {
	for _, bar := range pt.bars {
		if bar.key == key {
			return bar
		}
	}
	return nil
}

// StartParse starts tracking parsing for a file.
func (pt *ProgressTracker) StartParse(filePath, tableName string) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.startRenderLoop()

	bar := &barState{
		key:       "parse:" + filePath,
		label:     getShortPath(filePath),
		total:     0, // Unknown initially
		startTime: time.Now(),
	}
	pt.bars = append(pt.bars, bar)
}

// UpdateParse updates parse progress.
func (pt *ProgressTracker) UpdateParse(filePath string, rows int64) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	if bar := pt.findBar("parse:" + filePath); bar != nil {
		bar.current = rows
	}
}

// FinishParse finishes parse progress.
func (pt *ProgressTracker) FinishParse(filePath string, rows int64, duration time.Duration) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	if bar := pt.findBar("parse:" + filePath); bar != nil {
		bar.current = rows
		bar.total = rows
		bar.done = true
		bar.doneMsg = color.CyanString("  ✓ Parsed %s (%s rows) in %v",
			getShortPath(filePath), fmtNum(rows), duration.Round(time.Millisecond))
	}
}

// StartWrite starts tracking writing for a file.
func (pt *ProgressTracker) StartWrite(filePath, tableName string, totalRows int64) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.startRenderLoop()

	bar := &barState{
		key:       "write:" + filePath,
		label:     getShortPath(filePath) + " → " + tableName,
		total:     totalRows,
		startTime: time.Now(),
	}
	pt.bars = append(pt.bars, bar)
}

// UpdateWrite updates write progress.
func (pt *ProgressTracker) UpdateWrite(filePath string, rows int64) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	if bar := pt.findBar("write:" + filePath); bar != nil {
		bar.current = rows
	}
}

// FinishWrite finishes write progress.
func (pt *ProgressTracker) FinishWrite(filePath, tableName string, rows int64) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	if bar := pt.findBar("write:" + filePath); bar != nil {
		bar.current = rows
		bar.done = true
		bar.doneMsg = color.GreenString("✓ Imported %s rows into '%s'", fmtNum(rows), tableName)
	}
}

// StartIndex starts tracking index creation for a table.
func (pt *ProgressTracker) StartIndex(filePath, tableName string, indexCount int) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	bar := &barState{
		key:       "index:" + filePath,
		label:     tableName + " (indexing)",
		total:     0, // Unknown
		startTime: time.Now(),
	}
	pt.bars = append(pt.bars, bar)
}

// FinishIndex finishes index creation.
func (pt *ProgressTracker) FinishIndex(filePath, tableName string, indexCount int, duration time.Duration) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	if bar := pt.findBar("index:" + filePath); bar != nil {
		bar.done = true
		bar.doneMsg = color.GreenString("  ✓ Created %d index(es) on '%s' in %v", indexCount, tableName, duration.Round(time.Millisecond))
	}
}

// Error handles errors.
func (pt *ProgressTracker) Error(filePath string, err error, phase string) {
	if !pt.enabled {
		return
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	if bar := pt.findBar(phase + ":" + filePath); bar != nil {
		bar.done = true
		bar.doneMsg = color.YellowString("  ✗ %s failed: %v", getShortPath(filePath), err)
	}
}

// Helper functions

func fmtNum(n int64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}

func getShortPath(filePath string) string {
	parts := strings.Split(filePath, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return filePath
}

func isTerminal() bool {
	fileInfo, _ := os.Stdout.Stat()
	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

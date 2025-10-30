// Package io contains component implementations for file I/O and streaming.
//
// # StreamingFileInput Plugin
//
// The StreamingFileInput plugin reads from files continuously with the following features:
//   - Automatic recovery from crashes using persistent position tracking
//   - Seamless handling of file rotations
//   - At-least-once semantics with ack-based position updates (exactly-once in normal operation)
//   - Comprehensive metrics and observability via OpenTelemetry
//   - No external process dependencies
//
// # Semantics
//
// The plugin provides at-least-once semantics:
//   - In normal operation: exactly-once (position persisted on ack)
//   - During forced shutdown: at-least-once (soft checkpoint may lag behind acks)
//   - After rotation: exactly-once (rotation marker persisted immediately)
package io

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/warpstreamlabs/bento/public/service"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Time-based checkpoint interval: save position at least every 30 seconds
const timeBasedCheckpointInterval = 30 * time.Second

// splitKeepNewline is a custom scanner split function that keeps delimiters
var splitKeepNewline = func(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i+1], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// StreamingFileInputConfig holds configuration for the streaming file input
type StreamingFileInputConfig struct {
	Path               string        `json:"path"`
	StateDir           string        `json:"state_dir"`
	CheckpointInterval int           `json:"checkpoint_interval"`
	MaxBufferSize      int           `json:"max_buffer_size"`
	MaxLineSize        int           `json:"max_line_size"`
	ReadTimeout        time.Duration `json:"read_timeout"`
	ShutdownTimeout    time.Duration `json:"shutdown_timeout"`
	Debug              bool          `json:"debug"`
}

// FilePosition tracks the current position in a file
type FilePosition struct {
	FilePath   string       `json:"file_path"`
	Inode      uint64       `json:"inode"`
	ByteOffset atomic.Int64 `json:"-"`
	RawOffset  int64        `json:"byte_offset"`
	LineNumber atomic.Int64 `json:"-"`
	RawLineNum int64        `json:"line_number"`
	Timestamp  time.Time    `json:"timestamp"`
}

// Metrics holds counters for observability using OpenTelemetry
type Metrics struct {
	LinesReadCounter      metric.Int64Counter
	BytesReadCounter      metric.Int64Counter
	ErrorsCounter         metric.Int64Counter
	FileRotationsCounter  metric.Int64Counter
	StateWritesCounter    metric.Int64Counter
	BufferSaturationGauge metric.Int64UpDownCounter

	LinesRead     atomic.Int64
	BytesRead     atomic.Int64
	ErrorsCount   atomic.Int64
	FileRotations atomic.Int64
	StateWrites   atomic.Int64
}

// HealthStatus represents the health of the input
type HealthStatus struct {
	IsHealthy    bool
	LastError    string
	LastReadTime time.Time
	FileSize     int64
	ByteOffset   int64
	LineNumber   int64
}

// StreamingFileInput implements a robust streaming file input for Bento
type StreamingFileInput struct {
	config        StreamingFileInputConfig
	logger        *service.Logger
	position      *FilePosition
	positionMutex sync.RWMutex
	file          *os.File
	fileMu        sync.RWMutex
	reader        *bufio.Reader
	buffer        chan []byte
	bufferPool    sync.Pool
	stopCh        chan struct{}
	readLoopDone  chan struct{}
	wg            sync.WaitGroup
	statusMu      sync.RWMutex
	lastError     error
	lastReadTime  time.Time
	lastSaveTime  time.Time
	connected     bool
	connMutex     sync.RWMutex
	lastInode     uint64
	lastSize      atomic.Int64
	metrics       *Metrics
	ackCount      atomic.Int64
	inFlightCount atomic.Int64
	bufferClosed  atomic.Bool
	generation    atomic.Uint64
	watcher       *fsnotify.Watcher

	batchedLinesRead atomic.Int64
	batchedBytesRead atomic.Int64
	batchedErrors    atomic.Int64
	lastMetricsFlush time.Time
}

// NewStreamingFileInput creates a new streaming file input
func NewStreamingFileInput(cfg StreamingFileInputConfig, logger *service.Logger) (*StreamingFileInput, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("path is required")
	}
	if cfg.StateDir == "" {
		return nil, fmt.Errorf("state_dir is required")
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = 100
	}
	if cfg.MaxBufferSize <= 0 {
		cfg.MaxBufferSize = 1000
	}
	if cfg.MaxLineSize <= 0 {
		cfg.MaxLineSize = 1024 * 1024 // 1MB default
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}

	if err := os.MkdirAll(cfg.StateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	meter := otel.GetMeterProvider().Meter("streaming_file_input")
	metrics := &Metrics{}

	if linesCounter, err := meter.Int64Counter("streaming_file_input.lines_read",
		metric.WithDescription("Number of lines read from the file"),
		metric.WithUnit("1")); err == nil {
		metrics.LinesReadCounter = linesCounter
	}

	if bytesCounter, err := meter.Int64Counter("streaming_file_input.bytes_read",
		metric.WithDescription("Number of bytes read from the file"),
		metric.WithUnit("By")); err == nil {
		metrics.BytesReadCounter = bytesCounter
	}

	if errorsCounter, err := meter.Int64Counter("streaming_file_input.errors",
		metric.WithDescription("Number of errors encountered"),
		metric.WithUnit("1")); err == nil {
		metrics.ErrorsCounter = errorsCounter
	}

	if rotationsCounter, err := meter.Int64Counter("streaming_file_input.file_rotations",
		metric.WithDescription("Number of file rotations detected"),
		metric.WithUnit("1")); err == nil {
		metrics.FileRotationsCounter = rotationsCounter
	}

	if stateCounter, err := meter.Int64Counter("streaming_file_input.state_writes",
		metric.WithDescription("Number of position state writes"),
		metric.WithUnit("1")); err == nil {
		metrics.StateWritesCounter = stateCounter
	}

	if bufferGauge, err := meter.Int64UpDownCounter("streaming_file_input.buffer_saturation",
		metric.WithDescription("Current number of messages in the buffer"),
		metric.WithUnit("1")); err == nil {
		metrics.BufferSaturationGauge = bufferGauge
	}

	now := time.Now()
	sfi := &StreamingFileInput{
		config:       cfg,
		logger:       logger,
		buffer:       make(chan []byte, cfg.MaxBufferSize),
		stopCh:       make(chan struct{}),
		readLoopDone: make(chan struct{}),
		position: &FilePosition{
			FilePath: cfg.Path,
		},
		metrics:          metrics,
		lastReadTime:     now,
		lastSaveTime:     now,
		lastMetricsFlush: now,
		bufferPool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 0, 4096)
				return &b
			},
		},
	}

	if err := sfi.loadPosition(); err != nil {
		if logger != nil {
			logger.Warnf("Failed to load previous position: %v", err)
		}
	}

	return sfi, nil
}

func streamingFileInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Local").
		Summary("Robust streaming file input with automatic recovery and rotation handling").
		Description(`
Reads from a file continuously, similar to 'tail -f', but with several important improvements:

- Automatic recovery from crashes using persistent position tracking
- Seamless handling of file rotations
- At-least-once semantics with ack-based position updates
- Comprehensive metrics and observability
- No external process dependencies

The input maintains state in a JSON file to enable recovery from the exact position where it left off.
`).
		Field(service.NewStringField("path").
			Description("Path to the file to read from").
			Example("/var/log/app.log")).
		Field(service.NewStringField("state_dir").
			Description("Directory to store position tracking state").
			Default("/tmp/bento-streaming-file-state").
			Example("/tmp/bento-streaming-file-state")).
		Field(service.NewIntField("checkpoint_interval").
			Description("How often to persist position (in lines)").
			Default(100).
			Example(100)).
		Field(service.NewIntField("max_buffer_size").
			Description("Maximum number of lines to buffer").
			Default(1000).
			Example(1000)).
		Field(service.NewIntField("max_line_size").
			Description("Maximum line size in bytes to prevent OOM").
			Default(1048576).
			Example(1048576)).
		Field(service.NewBoolField("debug").
			Description("Enable debug logging").
			Default(false))
}

// logDebugf logs a debug message using Bento logger
func (sfi *StreamingFileInput) logDebugf(format string, args ...interface{}) {
	if sfi.logger != nil {
		sfi.logger.Debugf(format, args...)
	}
}

// logInfof logs an info message using Bento logger
func (sfi *StreamingFileInput) logInfof(format string, args ...interface{}) {
	if sfi.logger != nil {
		sfi.logger.Infof(format, args...)
	}
}

// logWarnf logs a warning message using Bento logger
func (sfi *StreamingFileInput) logWarnf(format string, args ...interface{}) {
	if sfi.logger != nil {
		sfi.logger.Warnf(format, args...)
	}
}

// logErrorf logs an error message using Bento logger
func (sfi *StreamingFileInput) logErrorf(format string, args ...interface{}) {
	if sfi.logger != nil {
		sfi.logger.Errorf(format, args...)
	}
}

// setLastError safely sets the last error
func (sfi *StreamingFileInput) setLastError(err error) {
	sfi.statusMu.Lock()
	sfi.lastError = err
	sfi.statusMu.Unlock()
}

// getLastError safely gets the last error
func (sfi *StreamingFileInput) getLastError() error {
	sfi.statusMu.RLock()
	defer sfi.statusMu.RUnlock()
	return sfi.lastError
}

// setLastReadNow safely sets the last read time to now
func (sfi *StreamingFileInput) setLastReadNow() {
	sfi.statusMu.Lock()
	sfi.lastReadTime = time.Now()
	sfi.statusMu.Unlock()
}

// getLastReadTime safely gets the last read time
func (sfi *StreamingFileInput) getLastReadTime() time.Time {
	sfi.statusMu.RLock()
	defer sfi.statusMu.RUnlock()
	return sfi.lastReadTime
}

// setLastSaveNow safely sets the last save time to now
func (sfi *StreamingFileInput) setLastSaveNow() {
	sfi.statusMu.Lock()
	sfi.lastSaveTime = time.Now()
	sfi.statusMu.Unlock()
}

// getLastSaveTime safely gets the last save time
func (sfi *StreamingFileInput) getLastSaveTime() time.Time {
	sfi.statusMu.RLock()
	defer sfi.statusMu.RUnlock()
	return sfi.lastSaveTime
}

// statePaths returns the state file path for this input
func (sfi *StreamingFileInput) statePaths() string {
	abs, _ := filepath.Abs(sfi.config.Path)
	sum := sha1.Sum([]byte(abs))
	return filepath.Join(sfi.config.StateDir, fmt.Sprintf("pos_%x.json", sum[:8]))
}

// Connect opens the file and starts reading
func (sfi *StreamingFileInput) Connect(ctx context.Context) error {
	sfi.connMutex.Lock()
	defer sfi.connMutex.Unlock()

	if sfi.connected {
		return nil
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before opening file: %s: %w", sfi.config.Path, ctx.Err())
	default:
	}

	file, err := os.Open(sfi.config.Path)
	if err != nil {
		return fmt.Errorf("failed to open file: %s: %w", sfi.config.Path, err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to stat file: %s: %w", sfi.config.Path, err)
	}

	currentInode, hasInode := inodeOf(stat)
	currentSize := stat.Size()

	sfi.lastSize.Store(currentSize)

	sfi.positionMutex.Lock()
	savedInode := sfi.position.Inode
	savedOffset := sfi.position.ByteOffset.Load()
	sfi.positionMutex.Unlock()

	shouldResume := false

	if hasInode && savedInode != 0 && savedInode == currentInode && savedOffset > 0 && savedOffset <= currentSize {
		shouldResume = true
	}

	sfi.positionMutex.Lock()
	if hasInode {
		sfi.position.Inode = currentInode
		sfi.lastInode = currentInode
	}

	if shouldResume {
		if _, err := file.Seek(savedOffset, 0); err != nil {
			sfi.position.ByteOffset.Store(0)
			sfi.position.LineNumber.Store(0)
		}
	} else {
		sfi.position.ByteOffset.Store(0)
		sfi.position.LineNumber.Store(0)
	}
	sfi.positionMutex.Unlock()

	sfi.fileMu.Lock()
	sfi.file = file
	sfi.reader = bufio.NewReader(file)
	sfi.fileMu.Unlock()

	// Setup the fsnotify watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	// Watch the file for changes
	if err := watcher.Add(sfi.config.Path); err != nil {
		watcher.Close()
		return fmt.Errorf("failed to watch file: %w", err)
	}

	// Also watch the parent directory for rotation detection
	parentDir := filepath.Dir(sfi.config.Path)
	if err := watcher.Add(parentDir); err != nil {
		sfi.logWarnf("Failed to watch parent directory '%s', rotation detection may be degraded: %v", parentDir, err)
	}

	sfi.watcher = watcher
	sfi.connected = true

	sfi.wg.Add(1)
	go sfi.metricsFlusher()

	sfi.wg.Add(1)
	go sfi.monitorFile(ctx)

	return nil
}

const maxConsecutivePanics = 10

// monitorFile is the primary goroutine for watching and reading the file using fsnotify.
func (sfi *StreamingFileInput) monitorFile(ctx context.Context) {
	defer sfi.wg.Done()
	defer close(sfi.readLoopDone)
	defer sfi.watcher.Close()

	// Do an initial drain of any existing file content
	sfi.drainAvailableData()

	// Health check ticker as a fallback for missed events (30 seconds)
	healthCheck := time.NewTicker(30 * time.Second)
	defer healthCheck.Stop()

	for {
		select {
		case event, ok := <-sfi.watcher.Events:
			if !ok {
				return
			}

			// A write event means new data is available
			if event.Has(fsnotify.Write) && event.Name == sfi.config.Path {
				sfi.drainAvailableData()
			}

			// A file was created in the directory - check if it's our target (rotation completion)
			if event.Has(fsnotify.Create) && event.Name == sfi.config.Path {
				sfi.handleRotation()
			}

			// Rename/Remove can indicate rotation - re-check the file state
			if event.Has(fsnotify.Rename) || event.Has(fsnotify.Remove) {
				// A short delay helps coalesce rapid filesystem events
				time.Sleep(100 * time.Millisecond)
				sfi.checkStateAndReact()
			}

		case <-healthCheck.C:
			// Infrequent health check to catch edge cases
			sfi.checkStateAndReact()

		case err, ok := <-sfi.watcher.Errors:
			if !ok {
				return
			}
			sfi.logErrorf("fsnotify watcher error: %v", err)
			sfi.incrementErrorsCountWithType("watcher_error")

		case <-sfi.stopCh:
			return

		case <-ctx.Done():
			return
		}
	}
}

// checkStateAndReact performs a stat check to detect rotation or truncation
func (sfi *StreamingFileInput) checkStateAndReact() {
	rotated, truncated, err := sfi.detectFileChanges()
	if err != nil {
		sfi.logWarnf("Error during state check: %v", err)
		return
	}
	if rotated {
		sfi.handleRotation()
	} else if truncated {
		sfi.handleTruncation()
	}
}

// detectFileChanges checks for rotation and truncation using inode comparison
func (sfi *StreamingFileInput) detectFileChanges() (rotated, truncated bool, err error) {
	currentStat, err := os.Stat(sfi.config.Path)
	if err != nil {
		// If the file doesn't exist, it has been rotated/removed
		if os.IsNotExist(err) {
			return true, false, nil
		}
		return false, false, err
	}

	currentInode, _ := inodeOf(currentStat)
	currentSize := currentStat.Size()

	sfi.positionMutex.RLock()
	lastInode := sfi.position.Inode
	lastOffset := sfi.position.ByteOffset.Load()
	sfi.positionMutex.RUnlock()

	// Rotation is detected if the inode has changed
	if currentInode != 0 && lastInode != 0 && currentInode != lastInode {
		return true, false, nil
	}

	// Truncation is detected if the inode is the same but the size is now smaller than our offset
	if currentInode == lastInode && currentSize < lastOffset {
		sfi.logWarnf("File truncation detected: current size=%d is less than last offset=%d", currentSize, lastOffset)
		return false, true, nil
	}

	return false, false, nil
}

// handleTruncation resets the position for the current file
func (sfi *StreamingFileInput) handleTruncation() error {
	sfi.logInfof("Handling file truncation, resetting position to zero")

	sfi.positionMutex.Lock()
	sfi.position.ByteOffset.Store(0)
	sfi.position.LineNumber.Store(0)
	sfi.positionMutex.Unlock()

	// Invalidate in-flight acks for the old file by incrementing a generation counter
	sfi.generation.Add(1)

	// Clear the buffer to discard any stale data from before truncation
	sfi.drainBufferChannel()

	// Seek the existing file handle back to the beginning
	sfi.fileMu.Lock()
	if sfi.file != nil {
		if _, err := sfi.file.Seek(0, 0); err != nil {
			sfi.logErrorf("Failed to seek to start after truncation, will reopen: %v", err)
			// Fallback to a full restart if seek fails
			err2 := sfi.reopenFileLocked()
			sfi.fileMu.Unlock()
			return err2
		}
		sfi.reader.Reset(sfi.file)
	}
	sfi.fileMu.Unlock()

	sfi.incrementErrorsCountWithType("file_truncated")

	// Persist the new zero offset immediately
	if err := sfi.savePositionDurable(context.Background()); err != nil {
		return err
	}

	// Drain any data from the truncated file
	sfi.fileMu.RLock()
	reader := sfi.reader
	sfi.fileMu.RUnlock()
	if reader != nil {
		sfi.drainAvailableDataWithoutRotationCheck(reader)
	}

	return nil
}

// handleRotation manages the full file rotation process
func (sfi *StreamingFileInput) handleRotation() error {
	sfi.logInfof("File rotation detected, handling transition.")

	sfi.fileMu.Lock()
	// First, drain and close the old file handle
	if sfi.file != nil {
		sfi.logDebugf("Draining remaining data from old file handle before closing.")
		sfi.drainAvailableDataWithoutRotationCheckLocked()
		sfi.file.Close()
		sfi.file = nil
		sfi.reader = nil
	}
	sfi.fileMu.Unlock()

	// Invalidate in-flight acks for the old file by incrementing a generation counter
	sfi.generation.Add(1)

	// Attempt to open the new file at the path, with retries in case it doesn't exist yet
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if err = sfi.reopenFile(); err == nil {
			break
		}
		if !os.IsNotExist(err) {
			// If it's not a "file not found" error, don't retry
			sfi.logErrorf("Failed to open new file after rotation: %v", err)
			// Still try to re-add the file to the watcher
			if sfi.watcher != nil {
				if err2 := sfi.watcher.Add(sfi.config.Path); err2 != nil {
					sfi.logWarnf("Failed to re-add file to watcher after rotation: %v", err2)
				}
			}
			return err
		}
		// File doesn't exist yet, wait a bit and retry
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		sfi.logErrorf("Failed to open new file after rotation after retries: %v", err)
		// Still try to re-add the file to the watcher
		if sfi.watcher != nil {
			if err2 := sfi.watcher.Add(sfi.config.Path); err2 != nil {
				sfi.logWarnf("Failed to re-add file to watcher after rotation: %v", err2)
			}
		}
		return err
	}

	// Re-add the file to the watcher since it was removed during rotation
	if sfi.watcher != nil {
		if err := sfi.watcher.Add(sfi.config.Path); err != nil {
			sfi.logWarnf("Failed to re-add file to watcher after rotation: %v", err)
		}
	}

	// Persist the new "zero" position immediately. This is a critical step.
	if err := sfi.savePositionDurable(context.Background()); err != nil {
		sfi.logErrorf("CRITICAL: Failed to persist new position after rotation: %v", err)
		return err
	}

	// Drain any existing data from the new file (without rotation detection to avoid recursion)
	sfi.fileMu.RLock()
	reader := sfi.reader
	sfi.fileMu.RUnlock()
	if reader != nil {
		sfi.drainAvailableDataWithoutRotationCheck(reader)
	}

	sfi.incrementFileRotations()
	sfi.logInfof("Successfully switched to new file after rotation.")
	return nil
}

// reopenFile opens the configured path and updates the position
func (sfi *StreamingFileInput) reopenFile() error {
	sfi.fileMu.Lock()
	defer sfi.fileMu.Unlock()
	return sfi.reopenFileLocked()
}

// reopenFileLocked opens the configured path (assumes lock is held)
func (sfi *StreamingFileInput) reopenFileLocked() error {
	file, err := os.Open(sfi.config.Path)
	if err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	newInode, _ := inodeOf(info)

	sfi.positionMutex.Lock()
	sfi.position.Inode = newInode
	sfi.position.ByteOffset.Store(0)
	sfi.position.LineNumber.Store(0)
	sfi.positionMutex.Unlock()

	sfi.file = file
	sfi.reader = bufio.NewReader(file)
	return nil
}

// drainAvailableData drains any remaining data from the file
func (sfi *StreamingFileInput) drainAvailableData() {
	sfi.fileMu.RLock()
	file := sfi.file
	reader := sfi.reader
	sfi.fileMu.RUnlock()

	if reader == nil || file == nil {
		return
	}

	sfi.drainAvailableDataWithReader(reader)
}

// drainAvailableDataLocked drains data (assumes lock is held)
func (sfi *StreamingFileInput) drainAvailableDataLocked() {
	if sfi.reader == nil || sfi.file == nil {
		return
	}
	sfi.drainAvailableDataWithReader(sfi.reader)
}

// drainAvailableDataWithoutRotationCheckLocked drains data without rotation check (assumes lock is held)
// This is used to avoid recursive rotation detection
func (sfi *StreamingFileInput) drainAvailableDataWithoutRotationCheckLocked() {
	if sfi.reader == nil || sfi.file == nil {
		return
	}
	sfi.drainAvailableDataWithoutRotationCheck(sfi.reader)
}

// drainAvailableDataWithReader reads and buffers available data from the reader
func (sfi *StreamingFileInput) drainAvailableDataWithReader(reader *bufio.Reader) {
	// Check for truncation/rotation before reading
	rotated, truncated, err := sfi.detectFileChanges()
	if err != nil {
		sfi.logErrorf("Error detecting file changes: %v", err)
	}
	if rotated {
		sfi.handleRotation()
		return
	}
	if truncated {
		sfi.handleTruncation()
		return
	}

	sfi.drainAvailableDataWithoutRotationCheck(reader)
}

// drainBufferChannel drains all pending data from the buffer channel
// This is used to clear stale data when truncation is detected
func (sfi *StreamingFileInput) drainBufferChannel() {
	for {
		select {
		case lineBytes, ok := <-sfi.buffer:
			if !ok {
				return
			}
			// Return the buffer to the pool
			sfi.bufferPool.Put(&lineBytes)
			if sfi.metrics != nil && sfi.metrics.BufferSaturationGauge != nil {
				sfi.metrics.BufferSaturationGauge.Add(context.Background(), -1)
			}
		default:
			// No more data in the buffer
			return
		}
	}
}

// drainAvailableDataWithoutRotationCheck reads and buffers available data without checking for rotation
// This is used after rotation to avoid recursive rotation detection
func (sfi *StreamingFileInput) drainAvailableDataWithoutRotationCheck(reader *bufio.Reader) {
	scanner := bufio.NewScanner(reader)
	maxScanTokenSize := sfi.config.MaxLineSize + 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxScanTokenSize)
	scanner.Split(splitKeepNewline)

	for scanner.Scan() {
		lineBytes := scanner.Bytes()
		if len(lineBytes) == 0 {
			continue
		}

		bufPtr := sfi.bufferPool.Get().(*[]byte)
		*bufPtr = append((*bufPtr)[:0], lineBytes...)

		if sfi.bufferClosed.Load() {
			sfi.bufferPool.Put(bufPtr)
			return
		}

		select {
		case sfi.buffer <- *bufPtr:
			if sfi.metrics != nil && sfi.metrics.BufferSaturationGauge != nil {
				sfi.metrics.BufferSaturationGauge.Add(context.Background(), 1)
			}
			sfi.incrementLinesRead()
			sfi.incrementBytesRead(int64(len(lineBytes)))
			sfi.setLastReadNow()
		case <-sfi.stopCh:
			sfi.bufferPool.Put(bufPtr)
			return
		}
	}

	if err := scanner.Err(); err != nil && err != bufio.ErrTooLong {
		sfi.logWarnf("Error while draining data: %v", err)
	}
}

func (sfi *StreamingFileInput) updatePositionOnAck(delta int64) {
	sfi.position.ByteOffset.Add(delta)
	sfi.position.LineNumber.Add(1)
}

func (sfi *StreamingFileInput) savePositionDurable(ctx context.Context) error {
	sfi.positionMutex.Lock()
	posToSave := &FilePosition{
		FilePath:   sfi.position.FilePath,
		Inode:      sfi.position.Inode,
		RawOffset:  sfi.position.ByteOffset.Load(),
		RawLineNum: sfi.position.LineNumber.Load(),
		Timestamp:  time.Now(),
	}
	sfi.positionMutex.Unlock()

	stateFile := sfi.statePaths()

	data, err := json.Marshal(posToSave)
	if err != nil {
		return fmt.Errorf("failed to marshal position: %w", err)
	}

	doneCh := make(chan error, 1)
	opCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		select {
		case <-opCtx.Done():
			doneCh <- opCtx.Err()
			return
		default:
		}

		f, err := os.CreateTemp(sfi.config.StateDir, "pos-*.tmp")
		if err != nil {
			select {
			case doneCh <- fmt.Errorf("failed to create temporary position file: %w", err):
			default:
			}
			return
		}
		defer f.Close()

		select {
		case <-opCtx.Done():
			os.Remove(f.Name())
			doneCh <- opCtx.Err()
			return
		default:
		}

		if _, err := f.Write(data); err != nil {
			os.Remove(f.Name())
			select {
			case doneCh <- fmt.Errorf("failed to write position data: %w", err):
			default:
			}
			return
		}

		if err := f.Sync(); err != nil {
			os.Remove(f.Name())
			select {
			case doneCh <- fmt.Errorf("failed to sync position file: %w", err):
			default:
			}
			return
		}

		if err := os.Rename(f.Name(), stateFile); err != nil {
			os.Remove(f.Name())
			select {
			case doneCh <- fmt.Errorf("failed to rename position file: %w", err):
			default:
			}
			return
		}

		d, err := os.Open(sfi.config.StateDir)
		if err != nil {
			select {
			case doneCh <- fmt.Errorf("failed to open state directory for sync: %w", err):
			default:
			}
			return
		}
		defer d.Close()

		if err := d.Sync(); err != nil {
			select {
			case doneCh <- fmt.Errorf("failed to sync state directory: %w", err):
			default:
			}
			return
		}

		select {
		case doneCh <- nil:
		default:
		}
	}()

	select {
	case <-ctx.Done():
		cancel()
		select {
		case <-doneCh:
		case <-time.After(100 * time.Millisecond):
		}
		return fmt.Errorf("context cancelled while saving position: %w", ctx.Err())
	case err := <-doneCh:
		return err
	}
}

func (sfi *StreamingFileInput) loadPosition() error {
	stateFile := sfi.statePaths()
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			sfi.logDebugf("No previous position file found, starting from beginning")
			return nil
		}
		return fmt.Errorf("failed to read position file: %w", err)
	}

	loadedPos := &FilePosition{FilePath: sfi.config.Path}
	if err := json.Unmarshal(data, loadedPos); err != nil {
		sfi.logWarnf("Failed to unmarshal position file, starting from beginning: %v", err)
		sfi.position = &FilePosition{FilePath: sfi.config.Path}
		return nil
	}

	if loadedPos.FilePath != sfi.config.Path {
		sfi.logWarnf("Position file is for different file (%s vs %s), starting from beginning", loadedPos.FilePath, sfi.config.Path)
		sfi.position = &FilePosition{FilePath: sfi.config.Path}
		return nil
	}

	if loadedPos.RawOffset < 0 {
		sfi.logWarnf("Invalid byte offset %d, starting from beginning", loadedPos.RawOffset)
		loadedPos.RawOffset = 0
	}

	sfi.position.FilePath = loadedPos.FilePath
	sfi.position.Inode = loadedPos.Inode
	sfi.position.ByteOffset.Store(loadedPos.RawOffset)
	sfi.position.LineNumber.Store(loadedPos.RawLineNum)
	sfi.position.Timestamp = loadedPos.Timestamp

	if sfi.config.Debug {
		sfi.logDebugf("Loaded position: line=%d, offset=%d", loadedPos.RawLineNum, loadedPos.RawOffset)
	}

	return nil
}

// Read returns the next message from the buffer
func (sfi *StreamingFileInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	sfi.connMutex.RLock()
	connected := sfi.connected
	sfi.connMutex.RUnlock()

	if !connected {
		return nil, nil, service.ErrNotConnected
	}

	// Check for file changes (rotation/truncation) before reading
	sfi.checkStateAndReact()

	readCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		readCtx, cancel = context.WithTimeout(ctx, sfi.config.ReadTimeout)
		defer cancel()
	}

	select {
	case <-readCtx.Done():
		return nil, nil, readCtx.Err()
	case <-sfi.stopCh:
		select {
		case lineBytes, ok := <-sfi.buffer:
			if !ok {
				return nil, nil, io.EOF
			}
			if sfi.metrics != nil && sfi.metrics.BufferSaturationGauge != nil {
				sfi.metrics.BufferSaturationGauge.Add(context.Background(), -1)
			}
			delta := int64(len(lineBytes))
			msg := service.NewMessage(lineBytes)
			sfi.inFlightCount.Add(1)

			pubGen := sfi.generation.Load()
			sfi.positionMutex.RLock()
			pubInode := sfi.position.Inode
			sfi.positionMutex.RUnlock()

			return msg, func(ackCtx context.Context, ackErr error) error {
				defer sfi.inFlightCount.Add(-1)
				defer sfi.bufferPool.Put(&lineBytes)

				if ackErr != nil {
					sfi.incrementErrorsCountWithType("ack_error")
					return nil
				}

				currGen := sfi.generation.Load()
				sfi.positionMutex.RLock()
				currInode := sfi.position.Inode
				sfi.positionMutex.RUnlock()
				if currGen != pubGen || currInode != pubInode {
					return nil
				}

				sfi.updatePositionOnAck(delta)

				shouldCheckpoint := false
				if sfi.ackCount.Add(1)%int64(sfi.config.CheckpointInterval) == 0 {
					shouldCheckpoint = true
				} else if time.Since(sfi.getLastSaveTime()) > timeBasedCheckpointInterval {
					shouldCheckpoint = true
				}

				if shouldCheckpoint {
					if err := sfi.savePositionDurable(ackCtx); err != nil {
						sfi.logWarnf("Failed to save position: %v", err)
					} else {
						sfi.setLastSaveNow()
						sfi.incrementStateWrites()
					}
				}
				return nil
			}, nil
		default:
			return nil, nil, io.EOF
		}
	case lineBytes, ok := <-sfi.buffer:
		if !ok {
			return nil, nil, io.EOF
		}

		if sfi.metrics != nil && sfi.metrics.BufferSaturationGauge != nil {
			sfi.metrics.BufferSaturationGauge.Add(context.Background(), -1)
		}

		delta := int64(len(lineBytes))
		msg := service.NewMessage(lineBytes)

		pubGen := sfi.generation.Load()
		sfi.positionMutex.RLock()
		pubInode := sfi.position.Inode
		sfi.positionMutex.RUnlock()

		sfi.inFlightCount.Add(1)

		return msg, func(ackCtx context.Context, ackErr error) error {
			defer sfi.inFlightCount.Add(-1)
			defer sfi.bufferPool.Put(&lineBytes)

			if ackErr != nil {
				sfi.incrementErrorsCountWithType("ack_error")
				return nil
			}

			currGen := sfi.generation.Load()
			sfi.positionMutex.RLock()
			currInode := sfi.position.Inode
			sfi.positionMutex.RUnlock()
			if currGen != pubGen || currInode != pubInode {
				sfi.logWarnf("Ignoring ack from stale generation (gen: %d->%d, inode: %d->%d)",
					pubGen, currGen, pubInode, currInode)
				return nil
			}

			sfi.updatePositionOnAck(delta)

			shouldCheckpoint := false
			if sfi.ackCount.Add(1)%int64(sfi.config.CheckpointInterval) == 0 {
				shouldCheckpoint = true
			} else if time.Since(sfi.getLastSaveTime()) > timeBasedCheckpointInterval {
				shouldCheckpoint = true
			}

			if shouldCheckpoint {
				if err := sfi.savePositionDurable(ackCtx); err != nil {
					sfi.logWarnf("Failed to save position: %v", err)
				} else {
					sfi.setLastSaveNow()
					sfi.incrementStateWrites()
				}
			}
			return nil
		}, nil
	}
}

// Close closes the file and stops reading
func (sfi *StreamingFileInput) Close(ctx context.Context) error {
	sfi.connMutex.Lock()
	if !sfi.connected {
		sfi.connMutex.Unlock()
		return nil
	}
	sfi.connected = false
	sfi.connMutex.Unlock()

	select {
	case <-sfi.stopCh:
	default:
		close(sfi.stopCh)
	}

	sfi.fileMu.Lock()
	if sfi.file != nil {
		sfi.file.Close()
		sfi.file = nil
	}
	sfi.fileMu.Unlock()

	select {
	case <-sfi.readLoopDone:
	case <-ctx.Done():
		sfi.logWarnf("Close context cancelled before read loop finished")
		return ctx.Err()
	case <-time.After(5 * time.Second):
		sfi.logWarnf("Read loop did not finish within 5 seconds")
	}

	if sfi.bufferClosed.CompareAndSwap(false, true) {
		close(sfi.buffer)
	}

	shutdownTimeout := 30 * time.Second
	if sfi.config.ShutdownTimeout > 0 {
		shutdownTimeout = sfi.config.ShutdownTimeout
	}

	drainDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		defer close(drainDone)

		for {
			if sfi.inFlightCount.Load() == 0 {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-drainDone:
		sfi.logInfof("All in-flight messages acknowledged")
	case <-time.After(shutdownTimeout):
		remaining := sfi.inFlightCount.Load()
		sfi.logErrorf("CRITICAL: Shutdown timeout with %d in-flight messages. Persisting soft checkpoint.", remaining)
		if sfi.metrics.ErrorsCounter != nil {
			sfi.metrics.ErrorsCounter.Add(context.Background(), remaining,
				metric.WithAttributes(
					attribute.String("error_type", "shutdown_timeout"),
					attribute.String("file", sfi.config.Path)))
		}
		softCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := sfi.savePositionDurable(softCtx); err != nil {
			sfi.logWarnf("Failed to persist soft checkpoint on shutdown timeout: %v", err)
		} else {
			sfi.logInfof("Soft checkpoint persisted with %d in-flight messages", remaining)
		}
	case <-ctx.Done():
		sfi.logWarnf("Close context cancelled with %d in-flight messages", sfi.inFlightCount.Load())
	}

	sfi.wg.Wait()

	sfi.fileMu.Lock()
	sfi.file = nil
	sfi.fileMu.Unlock()

	if err := sfi.savePositionDurable(ctx); err != nil {
		sfi.logErrorf("Failed to save final position: %v", err)
		return err
	}

	sfi.logInfof("Streaming file input closed successfully")
	return nil
}

// Metrics & Observability Methods

func (sfi *StreamingFileInput) incrementLinesRead() {
	sfi.metrics.LinesRead.Add(1)
	sfi.batchedLinesRead.Add(1)
}

func (sfi *StreamingFileInput) incrementBytesRead(bytes int64) {
	sfi.metrics.BytesRead.Add(bytes)
	sfi.batchedBytesRead.Add(bytes)
}

func (sfi *StreamingFileInput) incrementErrorsCount() {
	sfi.incrementErrorsCountWithType("unknown")
}

func (sfi *StreamingFileInput) incrementErrorsCountWithType(errorType string) {
	sfi.metrics.ErrorsCount.Add(1)
	sfi.batchedErrors.Add(1)
	if sfi.metrics.ErrorsCounter != nil {
		sfi.metrics.ErrorsCounter.Add(context.Background(), 1,
			metric.WithAttributes(
				attribute.String("file", sfi.config.Path),
				attribute.String("error_type", errorType)))
	}
}

func (sfi *StreamingFileInput) incrementFileRotations() {
	sfi.metrics.FileRotations.Add(1)
	if sfi.metrics.FileRotationsCounter != nil {
		sfi.metrics.FileRotationsCounter.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("file", sfi.config.Path)))
	}
}

func (sfi *StreamingFileInput) incrementStateWrites() {
	sfi.metrics.StateWrites.Add(1)
	if sfi.metrics.StateWritesCounter != nil {
		sfi.metrics.StateWritesCounter.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("file", sfi.config.Path)))
	}
}

func (sfi *StreamingFileInput) flushBatchedMetrics() {
	now := time.Now()
	if now.Sub(sfi.lastMetricsFlush) < 50*time.Millisecond {
		return
	}
	sfi.lastMetricsFlush = now

	lines := sfi.batchedLinesRead.Swap(0)
	if lines > 0 && sfi.metrics.LinesReadCounter != nil {
		sfi.metrics.LinesReadCounter.Add(context.Background(), lines,
			metric.WithAttributes(attribute.String("file", sfi.config.Path)))
	}

	bytes := sfi.batchedBytesRead.Swap(0)
	if bytes > 0 && sfi.metrics.BytesReadCounter != nil {
		sfi.metrics.BytesReadCounter.Add(context.Background(), bytes,
			metric.WithAttributes(attribute.String("file", sfi.config.Path)))
	}
}

func (sfi *StreamingFileInput) metricsFlusher() {
	defer sfi.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sfi.flushBatchedMetrics()
		case <-sfi.stopCh:
			sfi.flushBatchedMetrics()
			return
		}
	}
}

func init() {
	err := service.RegisterInput("streaming_file", streamingFileInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
			path, err := pConf.FieldString("path")
			if err != nil {
				return nil, err
			}
			stateDir, err := pConf.FieldString("state_dir")
			if err != nil {
				return nil, err
			}
			checkpointInterval, err := pConf.FieldInt("checkpoint_interval")
			if err != nil {
				return nil, err
			}
			maxBufferSize, err := pConf.FieldInt("max_buffer_size")
			if err != nil {
				return nil, err
			}
			maxLineSize, err := pConf.FieldInt("max_line_size")
			if err != nil {
				return nil, err
			}
			debug, err := pConf.FieldBool("debug")
			if err != nil {
				return nil, err
			}

			cfg := StreamingFileInputConfig{
				Path:               path,
				StateDir:           stateDir,
				CheckpointInterval: checkpointInterval,
				MaxBufferSize:      maxBufferSize,
				MaxLineSize:        maxLineSize,
				Debug:              debug,
			}

			return NewStreamingFileInput(cfg, res.Logger())
		})
	if err != nil {
		panic(err)
	}
}

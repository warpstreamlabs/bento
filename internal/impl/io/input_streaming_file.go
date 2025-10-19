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
	IdleTimeout        time.Duration `json:"idle_timeout"`
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
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 30 * time.Second
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
		Field(service.NewDurationField("idle_timeout").
			Description("Timeout waiting for new data before checking for rotation").
			Default("30s").
			Example("30s")).
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

	sfi.connected = true

	sfi.wg.Add(1)
	go sfi.metricsFlusher()

	sfi.wg.Add(1)
	go sfi.readLoop(ctx)

	return nil
}

const maxConsecutivePanics = 10

func (sfi *StreamingFileInput) readLoop(ctx context.Context) {
	defer sfi.wg.Done()
	defer close(sfi.readLoopDone)

	panicCount := 0
	panicBackoff := 1 * time.Second
	maxPanicBackoff := 30 * time.Second

	for {
		if panicCount >= maxConsecutivePanics {
			sfi.logErrorf("CRITICAL: Max consecutive panics (%d) reached, stopping read loop", maxConsecutivePanics)
			sfi.setLastError(fmt.Errorf("read loop stopped after %d consecutive panics", panicCount))
			return
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					panicCount++
					sfi.logErrorf("PANIC in readLoop (count: %d/%d): %v", panicCount, maxConsecutivePanics, r)
					sfi.setLastError(fmt.Errorf("panic: %v", r))
					sfi.metrics.ErrorsCount.Add(1)

					backoff := time.Duration(panicCount) * panicBackoff
					if backoff > maxPanicBackoff {
						backoff = maxPanicBackoff
					}
					time.Sleep(backoff)
				}
			}()
			sfi.readLoopIteration(ctx)
		}()

		select {
		case <-ctx.Done():
			return
		case <-sfi.stopCh:
			return
		default:
		}

		if sfi.getLastError() == nil {
			panicCount = 0
		}
	}
}

func (sfi *StreamingFileInput) readLoopIteration(ctx context.Context) {
	ticker := time.NewTicker(sfi.config.IdleTimeout)
	defer ticker.Stop()

	backoff := 10 * time.Millisecond
	maxBackoff := 500 * time.Millisecond
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sfi.stopCh:
			return
		case <-ticker.C:
			sfi.checkFileRotationAndReturn(ctx)
			sfi.flushBatchedMetrics()
		default:
		}

		sfi.fileMu.RLock()
		file := sfi.file
		reader := sfi.reader
		sfi.fileMu.RUnlock()

		if reader == nil || file == nil {
			return
		}

		// Set read deadline to allow timeout-based interruption
		if err := file.SetReadDeadline(time.Now().Add(sfi.config.ReadTimeout)); err != nil {
			sfi.logWarnf("Failed to set read deadline: %v", err)
		}

		scanner := bufio.NewScanner(reader)
		maxScanTokenSize := 100 * 1024 * 1024
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, maxScanTokenSize)
		scanner.Split(splitKeepNewline)

		for {
			select {
			case <-ctx.Done():
				return
			case <-sfi.stopCh:
				return
			default:
			}

			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					if os.IsTimeout(err) {
						break
					}
					if err == bufio.ErrTooLong {
						sfi.logErrorf("Line exceeds 100MB limit, skipping to next line")
						sfi.incrementErrorsCountWithType("line_too_long")
						for {
							b, e := reader.ReadByte()
							if e != nil || b == '\n' {
								break
							}
						}
						scanner = bufio.NewScanner(reader)
						scanner.Buffer(make([]byte, 0, 64*1024), maxScanTokenSize)
						scanner.Split(splitKeepNewline)
						continue
					}
					sfi.setLastError(err)
					return
				}
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(backoff)
				select {
				case <-timer.C:
					if backoff < maxBackoff {
						backoff *= 2
					}
					break
				case <-sfi.stopCh:
					return
				case <-ctx.Done():
					return
				}
				break
			}

			backoff = 10 * time.Millisecond
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
			case <-ctx.Done():
				sfi.bufferPool.Put(bufPtr)
				return
			}
		}
	}
}

func (sfi *StreamingFileInput) checkFileRotationAndReturn(_ context.Context) bool {
	stat, err := os.Stat(sfi.config.Path)
	if err != nil {
		sfi.logWarnf("Failed to stat file for rotation check: %v", err)
		return false
	}

	rotationDetected := false
	reason := ""

	if stat.Size() < sfi.lastSize.Load() {
		rotationDetected = true
		reason = fmt.Sprintf("size decreased from %d to %d bytes", sfi.lastSize.Load(), stat.Size())
	}

	if currentInode, hasInode := inodeOf(stat); hasInode {
		if sfi.lastInode != 0 && currentInode != sfi.lastInode {
			rotationDetected = true
			reason = fmt.Sprintf("inode changed from %d to %d", sfi.lastInode, currentInode)
		}
		sfi.lastInode = currentInode
	}

	if rotationDetected {
		sfi.logInfof("File rotation detected (%s), reopening file", reason)
		sfi.incrementFileRotations()
		sfi.generation.Add(1)

		file, err := os.Open(sfi.config.Path)
		if err != nil {
			sfi.logErrorf("Failed to reopen file after rotation: %v", err)
			sfi.incrementErrorsCount()
			return true
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			sfi.logErrorf("Failed to stat reopened file: %v", err)
			sfi.incrementErrorsCount()
			return true
		}

		newReader := bufio.NewReader(file)

		sfi.fileMu.Lock()
		oldFile := sfi.file
		sfi.file = file
		sfi.reader = newReader
		sfi.fileMu.Unlock()

		if oldFile != nil {
			oldFile.Close()
		}

		sfi.positionMutex.Lock()
		sfi.position.ByteOffset.Store(0)
		sfi.position.LineNumber.Store(0)
		if newInode, hasInode := inodeOf(stat); hasInode {
			sfi.position.Inode = newInode
			sfi.lastInode = newInode
		}
		sfi.positionMutex.Unlock()

		sfi.lastSize.Store(stat.Size())
		sfi.logInfof("Successfully reopened file after rotation")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := sfi.savePositionDurable(ctx); err != nil {
			sfi.logWarnf("Failed to persist rotation marker: %v", err)
		} else {
			sfi.setLastSaveNow()
			sfi.incrementStateWrites()
		}

		return true
	} else {
		sfi.lastSize.Store(stat.Size())
	}
	return false
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
			idleTimeout, err := pConf.FieldDuration("idle_timeout")
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
				IdleTimeout:        idleTimeout,
				Debug:              debug,
			}

			return NewStreamingFileInput(cfg, res.Logger())
		})
	if err != nil {
		panic(err)
	}
}

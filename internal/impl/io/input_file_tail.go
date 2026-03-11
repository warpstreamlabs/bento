package io

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ftiFieldPath              = "path"
	ftiFieldPollInterval      = "poll_interval"
	ftiFieldMaxLineBufferSize = "line_buffer"
	ftiFieldStartPosition     = "start_position"
)

func fileTailInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Local").
		Summary("Tails a file").
		Version("1.15.0").
		Description(`
Reads lines from a file continuously with handling of log rotation.

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- file_tail_path 
- file_tail_position 
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).
		`).
		Field(service.NewStringField(ftiFieldPath).
			Description("Path to file to consume lines from.").
			Example("./folder/file.log")).
		Field(service.NewDurationField(ftiFieldPollInterval).
			Description("The time between checks for new lines.").
			Default("1s").
			Example("1s").
			Example("200ms")).
		Field(service.NewIntField(ftiFieldMaxLineBufferSize).
			Description("Number of lines to buffer internally before backpressure is applied.").
			Default(1000).
			Advanced()).
		Field(service.NewStringEnumField(ftiFieldStartPosition, []string{"start", "end"}...).
			Description("Where to begin reading the file from. `start` will consume all existing lines in the file, `end` will only consume new lines after input has started.").
			Default("start")).
		Field(service.NewAutoRetryNacksToggleField())
}

func init() {
	err := service.RegisterInput(
		"file_tail",
		fileTailInputSpec(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (in service.Input, err error) {
			in, err = NewFileTailInput(pConf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(pConf, in)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type FileTailInput struct {
	tail tail

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

func NewFileTailInput(pConf *service.ParsedConfig, mgr *service.Resources) (*FileTailInput, error) {
	path, err := pConf.FieldString(ftiFieldPath)
	if err != nil {
		return nil, err
	}

	pollInterval, err := pConf.FieldDuration(ftiFieldPollInterval)
	if err != nil {
		return nil, err
	}

	lineBufferSize, err := pConf.FieldInt(ftiFieldMaxLineBufferSize)
	if err != nil {
		return nil, err
	}

	startPosition, err := pConf.FieldString(ftiFieldStartPosition)
	if err != nil {
		return nil, err
	}

	tailOpts := []tailOpt{
		withPollInterval(pollInterval),
		withLineChanBufferSize(lineBufferSize),
		withStartPosition(startPosition),
		withLogger(mgr.Logger()),
	}

	tail, err := newTail(path, mgr.FS(), tailOpts...)
	if err != nil {
		return nil, err
	}

	return &FileTailInput{
		tail:    tail,
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}, nil
}

func (fti *FileTailInput) Connect(ctx context.Context) error {
	if fti.shutSig.IsHardStopSignalled() {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	fti.tail.cancel = cancel

	go fti.tail.watch(ctx)

	return nil
}

func (fti *FileTailInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case err, ok := <-fti.tail.errChan:
		if !ok {
			return nil, nil, service.ErrNotConnected
		}
		fti.logger.Error(err.Error())
		return nil, nil, service.ErrNotConnected

	case tl, ok := <-fti.tail.lineChan:
		if !ok {
			return nil, nil, service.ErrNotConnected
		}

		msg := service.NewMessage(tl.line)
		msg.MetaSet("file_tail_path", fti.tail.path)
		msg.MetaSet("file_tail_position", strconv.Itoa(int(tl.position)))

		return msg, func(ctx context.Context, res error) error {
			return nil
		}, nil

	case <-ctx.Done():
		return nil, nil, ctx.Err()

	case <-fti.shutSig.HardStopChan():
		return nil, nil, nil
	}
}

func (fti *FileTailInput) Close(ctx context.Context) error {
	if fti.shutSig.IsHardStopSignalled() {
		return nil
	}
	fti.shutSig.TriggerHardStop()
	fti.tail.cancel()

	select {
	case <-fti.tail.doneChan:
		close(fti.tail.errChan)
		close(fti.tail.lineChan)
		defer close(fti.tail.doneChan)
		fti.tail.file.Close()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

//------------------------------------------------------------------------------

type tail struct {
	path         string
	pollInterval time.Duration

	fs       ifs.FS
	file     *os.File
	fileInfo os.FileInfo

	logger *service.Logger

	reader *bufio.Reader

	lineChan chan tailLine
	errChan  chan error
	doneChan chan struct{}

	cancel context.CancelFunc
}

type tailLine struct {
	line     []byte
	position int64
}

type tailOpt func(*tail) error

func withPollInterval(pollInterval time.Duration) tailOpt {
	return func(t *tail) error {
		t.pollInterval = pollInterval
		return nil
	}
}

func withLineChanBufferSize(bs int) tailOpt {
	return func(t *tail) error {
		t.lineChan = make(chan tailLine, bs)
		return nil
	}
}

func withStartPosition(sp string) tailOpt {
	return func(t *tail) error {
		if sp == "end" {
			_, err := t.file.Seek(0, io.SeekEnd)
			return err
		}
		return nil
	}
}

func withLogger(logger *service.Logger) tailOpt {
	return func(t *tail) error {
		t.logger = logger
		return nil
	}
}

func newTail(path string, fs ifs.FS, opts ...tailOpt) (tail, error) {
	file, err := fs.Open(path)
	if err != nil {
		return tail{}, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return tail{}, err
	}

	reader := bufio.NewReader(file)

	osFile, ok := file.(*os.File)
	if !ok {
		return tail{}, fmt.Errorf("expected *os.File, got %T", file)
	}

	t := &tail{
		path:         path,
		pollInterval: time.Second,

		fs:       fs,
		file:     osFile,
		fileInfo: fileInfo,

		reader:   reader,
		lineChan: make(chan tailLine, 1000),
		errChan:  make(chan error),
		doneChan: make(chan struct{}),
	}

	for _, o := range opts {
		err := o(t)
		if err != nil {
			file.Close()
			return tail{}, err
		}
	}

	return *t, nil
}

func (t *tail) watch(ctx context.Context) {
	ticker := time.NewTicker(t.pollInterval)
	defer ticker.Stop()

	for {

		select {
		case <-ctx.Done():
			t.doneChan <- struct{}{}
			return
		default:
		}

		line, err := t.reader.ReadBytes('\n')
		line = bytes.TrimRight(line, "\r\n")

		if len(line) > 0 {
			pos, err := t.file.Seek(0, io.SeekCurrent)
			if err != nil {
				t.errChan <- err
				return
			}

			tl := tailLine{
				line:     line,
				position: pos,
			}

			select {
			case t.lineChan <- tl:
			case <-ctx.Done():
				t.doneChan <- struct{}{}
				return
			}
		}

		if err != nil {
			if err == io.EOF {

				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.doneChan <- struct{}{}
					return
				}

				if err := t.reopenIfMovedOrTruncated(); err != nil {
					t.errChan <- err
					return
				}

			} else {
				t.errChan <- fmt.Errorf("reader: %w", err)
				return
			}
		}
	}
}

func (t *tail) reopenIfMovedOrTruncated() error {
	tempInfo, err := t.fs.Stat(t.path)
	if err != nil {
		return fmt.Errorf("failed to get file stats for %s: %w", t.file.Name(), err)
	}

	pos, err := t.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to set seek on %s: %w", t.file.Name(), err)
	}

	var truncation bool
	var moved bool

	if !os.SameFile(t.fileInfo, tempInfo) {
		if t.logger != nil {
			t.logger.Debug(fmt.Sprintf("Handling rotation for %v", t.path))
		}
		moved = true
	}

	if pos > tempInfo.Size() && !moved {
		if t.logger != nil {
			t.logger.Debug(fmt.Sprintf("Handling truncation for %v", t.path))
		}
		truncation = true
	}

	if !truncation && !moved {
		return nil
	}

	err = t.handleMoveOrTruncation()
	if err != nil {
		return fmt.Errorf("failed to handle rotation for %s: %w", t.file.Name(), err)
	}

	return nil
}

func (t *tail) handleMoveOrTruncation() error {
	file, err := t.fs.Open(t.path)
	if err != nil {
		return err
	}

	fileInfo, err := t.fs.Stat(t.path)
	if err != nil {
		return err
	}

	t.file.Close()

	var ok bool
	t.file, ok = file.(*os.File)
	if !ok {
		return fmt.Errorf("expected *os.File, got %T", file)
	}

	t.fileInfo = fileInfo
	t.reader = bufio.NewReader(file)

	return nil
}

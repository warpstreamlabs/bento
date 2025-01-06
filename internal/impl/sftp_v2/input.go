package sftp

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/sftp"

	"github.com/warpstreamlabs/bento/public/service"
)

type Input struct {
	baseSftpIO
	chBytes chan []byte
	chErr   chan error
}

func (i *Input) streamPathIfIsFileCodecCsvLineWithHeaders(_ context.Context, f *sftp.File) error {
	lineCounter := 0
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return err
	}

	for {
		lineCounter++
		rec, err := reader.Read()

		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(rec) == 0 {
			slog.Warn("got zero bytes rec at line %v - assuming EOF", "line", strconv.Itoa(lineCounter))
			return nil
		}
		if len(rec) != len(header) {
			return fmt.Errorf("row %v does not have the same number of cols as header (row: %v x header: %v)", lineCounter, len(rec), len(header))
		}

		data := map[string]string{}

		for i, k := range header {
			data[k] = rec[i]
		}

		bs, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("converting json at line %v: %w", lineCounter, err)
		}

		i.chBytes <- bs
	}
}

func (i *Input) streamPathIfIsFileCodecLine(ctx context.Context, f *sftp.File) error {
	scan := bufio.NewScanner(f)

	for scan.Scan() {
		if ctx.Err() != nil {
			return nil
		}

		if err := scan.Err(); err != nil {
			return fmt.Errorf("error while scanning file: %w", err)
		}

		bs := scan.Bytes()

		i.chBytes <- bs
	}

	return nil
}

func (i *Input) streamPathIfIsFileCodecAllBytes(_ context.Context, f *sftp.File) error {

	bs, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("could not open file for reading from remote sftp server: %w", err)
	}

	i.chBytes <- bs

	return nil
}

func (i *Input) streamPathIfIsFile(ctx context.Context, fpath string) error {
	fStat, err := i.sftpClient.Stat(fpath)
	if err != nil {
		return fmt.Errorf("error retrieving file stat from sftp: %w", err)
	}

	if fStat.IsDir() {
		return nil
	}

	f, err := i.sftpClient.Open(fpath)
	if err != nil {
		return fmt.Errorf("could not open file for reading from remote sftp server: %w", err)
	}

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		slog.Debug("Finished processing.", "file", fpath, "duration", dur.String())
	}()

	switch i.config.codec {
	case CodecLines:
		return i.streamPathIfIsFileCodecLine(ctx, f)
	case CodecAllBytes:
		return i.streamPathIfIsFileCodecAllBytes(ctx, f)
	case CodecCsvLinesWithHeader:
		return i.streamPathIfIsFileCodecCsvLineWithHeaders(ctx, f)
	default:
		return fmt.Errorf("unknown codec %s", i.config.codec)
	}
}

func (i *Input) Connect(ctx context.Context) error {

	if err := i.baseConnect(ctx); err != nil {
		return fmt.Errorf("error connecting to remote sftp server: %w", err)
	}

	walker := i.sftpClient.Walk("/")

	pathRxs := make([]any, len(i.parsedPaths))

	var err error
	for idx := range i.parsedPaths {
		pathRxs[idx], err = regexp.Compile(i.parsedPaths[idx])
		if err != nil {
			pathRxs[idx] = i.parsedPaths[idx]
		}
	}

	go func() {
		// TODO: this could be improved if we filter inside sftp server.
		for _, pathRx := range pathRxs {
			rawPathRx := pathRx

			for walker.Step() {
				switch queryPath := rawPathRx.(type) {
				case *regexp.Regexp:
					if queryPath.MatchString(walker.Path()) {
						slog.Debug("Path matches", "regex", queryPath.String(), "path", walker.Path())

						if err := i.streamPathIfIsFile(ctx, walker.Path()); err != nil {
							i.chErr <- err
							return
						}
					} else {
						slog.Debug("Path DOES NOT match", "regex", queryPath.String(), "path", walker.Path())
					}

				case string:
					if queryPath == walker.Path() {
						slog.Debug("Path matches", "path", walker.Path())
						if err := i.streamPathIfIsFile(ctx, walker.Path()); err != nil {
							i.chErr <- err
							return
						}
					} else {
						slog.Debug("Path DOES NOT match", "path", walker.Path())
					}
				}
			}
		}

		i.chErr <- service.ErrEndOfInput
	}()

	return nil
}

func (i *Input) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case bs := <-i.chBytes:
		return service.NewMessage(bs), func(ctx context.Context, err error) error {
			return ctx.Err()
		}, nil

	// TODO: Check error handling best practices
	case err := <-i.chErr:
		slog.Warn("error processing", "err", err)
		return nil, nil, service.ErrEndOfInput
	case <-ctx.Done():
		return nil, nil, service.ErrEndOfInput
	}
}

func (i *Input) Close(ctx context.Context) error {
	return i.baseClose(ctx)
}

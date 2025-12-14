package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"

	"github.com/parquet-go/parquet-go"

	"github.com/warpstreamlabs/bento/public/service"
)

func parquetInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Local").
		Summary("Reads and decodes [Parquet files](https://parquet.apache.org/docs/) into a stream of structured messages.").
		Field(service.NewStringListField("paths").
			Description("A list of file paths to read from. Each file will be read sequentially until the list is exhausted, at which point the input will close. Glob patterns are supported, including super globs (double star).").
			Example("/tmp/foo.parquet").
			Example("/tmp/bar/*.parquet").
			Example("/tmp/data/**/*.parquet")).
		Field(service.NewIntField("batch_count").
			Description(`Optionally process records in batches. This can help to speed up the consumption of exceptionally large files. When the end of the file is reached the remaining records are processed as a (potentially smaller) batch.`).
			Default(1).
			Advanced()).
		Field(service.NewBoolField("strict_schema").
			Description("Whether to enforce strict Parquet schema validation. When set to false, allows reading files with non-standard schema structures (such as non-standard LIST formats). Disabling strict mode may reduce validation but increases compatibility.").
			Default(true).
			Advanced()).
		Field(service.NewAutoRetryNacksToggleField()).
		Description(`
This input uses [https://github.com/parquet-go/parquet-go](https://github.com/parquet-go/parquet-go), which is itself experimental. Therefore changes could be made into how this processor functions outside of major version releases.

By default any BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY value will be extracted as a byte slice (` + "`[]byte`" + `) unless the logical type is UTF8, in which case they are extracted as a string (` + "`string`" + `).

When a value extracted as a byte slice exists within a document which is later JSON serialized by default it will be base 64 encoded into strings, which is the default for arbitrary data fields. It is possible to convert these binary values to strings (or other data types) using Bloblang transformations such as ` + "`root.foo = this.foo.string()` or `root.foo = this.foo.encode(\"hex\")`" + `, etc.`).
		Version("1.0.0")
}

func init() {
	err := service.RegisterBatchInput(
		"parquet", parquetInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			in, err := newParquetInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, in)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newParquetInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	pathsList, err := conf.FieldStringList("paths")
	if err != nil {
		return nil, err
	}
	pathsRemaining, err := service.Globs(mgr.FS(), pathsList...)
	if err != nil {
		return nil, err
	}
	if len(pathsRemaining) == 0 {
		// Important to note that this could be intentional, e.g. running
		// Bento as a cron job on a directory.
		mgr.Logger().Warnf("Paths %v did not match any files", pathsList)
	}

	batchSize, err := conf.FieldInt("batch_count")
	if err != nil {
		return nil, err
	}
	if batchSize < 1 {
		return nil, fmt.Errorf("batch_size must be >0, got %v", batchSize)
	}

	strictSchema, err := conf.FieldBool("strict_schema")
	if err != nil {
		return nil, err
	}

	rdr := &parquetReader{
		batchSize:      batchSize,
		strictSchema:   strictSchema,
		pathsRemaining: pathsRemaining,
		log:            mgr.Logger(),
		mgr:            mgr,
	}
	return rdr, nil
}

type openParquetFile struct {
	schema         *parquet.Schema
	handle         fs.File
	rdr            *parquet.GenericReader[any]
	lenientRGs     []parquet.RowGroup
	lenientRGIdx   int
	lenientRowsRdr parquet.Rows
	strictSchema   bool
}

func (p *openParquetFile) Close() error {
	if p.strictSchema {
		_ = p.rdr.Close()
	} else if p.lenientRowsRdr != nil {
		_ = p.lenientRowsRdr.Close()
	}
	return p.handle.Close()
}

type parquetReader struct {
	mgr *service.Resources
	log *service.Logger

	batchSize      int
	strictSchema   bool
	pathsRemaining []string

	mut      sync.Mutex
	openFile *openParquetFile
}

func (r *parquetReader) Connect(ctx context.Context) error {
	return nil
}

func (r *parquetReader) getOpenFile() (*openParquetFile, error) {
	if r.openFile != nil {
		return r.openFile, nil
	}
	if len(r.pathsRemaining) == 0 {
		return nil, io.EOF
	}

	path := r.pathsRemaining[0]
	r.pathsRemaining = r.pathsRemaining[1:]

	fileHandle, err := r.mgr.FS().Open(path)
	if err != nil {
		return nil, err
	}

	readAtFileHandle, ok := fileHandle.(io.ReaderAt)
	if !ok {
		r.log.Warnf("Target filesystem does not support ReadAt, falling back to fully in-memory consumption, this may cause excessive memory usage.")
		allBytes, err := io.ReadAll(fileHandle)
		if err != nil {
			return nil, err
		}
		readAtFileHandle = bytes.NewReader(allBytes)
	}

	fileStats, err := fileHandle.Stat()
	if err != nil {
		_ = fileHandle.Close()
		return nil, err
	}

	inFile, err := parquet.OpenFile(readAtFileHandle, fileStats.Size())
	if err != nil {
		return nil, err
	}

	if r.strictSchema {
		rdr, err := newReaderWithoutPanic(inFile)
		if err != nil {
			return nil, err
		}

		r.openFile = &openParquetFile{
			schema:       rdr.Schema(),
			handle:       fileHandle,
			rdr:          rdr,
			strictSchema: true,
		}
	} else {
		r.openFile = &openParquetFile{
			schema:       inFile.Schema(),
			handle:       fileHandle,
			lenientRGs:   inFile.RowGroups(),
			lenientRGIdx: 0,
			strictSchema: false,
		}
	}

	r.log.Debugf("Consuming parquet data from file '%v'", path)
	return r.openFile, nil
}

func (r *parquetReader) closeOpenFile() error {
	if r.openFile == nil {
		return nil
	}
	err := r.openFile.Close()
	r.openFile = nil
	return err
}

func (r *parquetReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	rowBuf := make([]any, r.batchSize)
	var f *openParquetFile
	var n int

	for {
		var err error
		if f, err = r.getOpenFile(); err != nil {
			if errors.Is(err, io.EOF) {
				err = service.ErrEndOfInput
			}
			return nil, nil, err
		}

		if f.strictSchema {
			if n, err = readWithoutPanic(f.rdr, rowBuf); errors.Is(err, io.EOF) {
				// If we finished this file we close the handle and forget it so
				// that the next call moves on.
				if closeErr := f.Close(); closeErr != nil {
					r.log.Errorf("Failed to close file cleanly: %v", closeErr)
				}
				r.openFile = nil
			}
		} else {
			// Lenient mode: iterate through RowGroups
			for f.lenientRGIdx < len(f.lenientRGs) {
				// Open the current row group if needed
				if f.lenientRowsRdr == nil {
					f.lenientRowsRdr = f.lenientRGs[f.lenientRGIdx].Rows()
				}

				n, err = readLenient(f.lenientRowsRdr, f.schema, rowBuf)
				if errors.Is(err, io.EOF) {
					// Finished this row group, close it and move to next
					_ = f.lenientRowsRdr.Close()
					f.lenientRowsRdr = nil
					f.lenientRGIdx++
					if n > 0 {
						break // We got some rows, return them
					}
					// No rows in this batch, try next row group
					continue
				}
				if err != nil {
					return nil, nil, err
				}
				if n > 0 {
					break // Got rows, return them
				}
			}

			// If we've exhausted all row groups, close the file
			if f.lenientRGIdx >= len(f.lenientRGs) && n == 0 {
				if closeErr := f.Close(); closeErr != nil {
					r.log.Errorf("Failed to close file cleanly: %v", closeErr)
				}
				r.openFile = nil
				err = io.EOF
			}
		}

		// If we got rows then break and yield them.
		if n > 0 {
			break
		}

		// Otherwise, unless the error is critical, we try again with the next
		// file. If the err indicates a different issue than reaching the end
		// then we escalate it, consumption will still continue on the next call
		// but this gives the parent reader a chance to rate limit etc.
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
	}

	resBatch := make(service.MessageBatch, n)
	for i := 0; i < n; i++ {
		newMsg := service.NewMessage(nil)
		newMsg.SetStructuredMut(rowBuf[i])
		resBatch[i] = newMsg
	}

	return resBatch, func(ctx context.Context, err error) error { return nil }, nil
}

func (r *parquetReader) Close(ctx context.Context) error {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.closeOpenFile()
}

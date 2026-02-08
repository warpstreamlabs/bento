package io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fileProcessorFieldOperation = "operation"
	fileProcessorFieldPath      = "path"
	fileProcessorFieldContent   = "content"
	fileProcessorFieldDest      = "dest"
)

func fileProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Local").
		Summary(`Performs operations (read, write, delete, move, stat) on files.`).
		Description(`
This processor allows you to perform various file operations based on message content. The operation is specified by the 'operation' field, and paths can be dynamically generated using interpolation functions.

### Operations

- **read**: Read file content into the message
- **write**: Write message content to a file
- **delete**: Delete a file
- **move**: Move/rename a file
- **stat**: Get file information without reading content

### Metadata

When reading or getting file info (stat), this processor adds the following metadata fields:

`+"```text"+`
- file_path: The path of the file
- file_size: The size of the file in bytes
- file_mod_time_unix: File modification time as Unix timestamp
- file_mod_time: File modification time in RFC3339 format
- file_name: The name of the file
- file_is_dir: Whether the file is a directory (true/false)
- file_mode: File permissions and mode
`+"```"+``).
		Fields(
			service.NewStringEnumField(fileProcessorFieldOperation, "read", "write", "delete", "move", "stat").
				Description("The file operation to perform.").
				Examples("read", "write", "delete", "move", "stat"),
			service.NewInterpolatedStringField(fileProcessorFieldPath).
				Description("The source file path. Supports interpolation for dynamic paths.").
				Examples(
					"/tmp/data.txt",
					"/tmp/${! json(\"document.id\") }.txt",
				).LintRule(`if this == "" { [ "'path' must be set to a non-empty string" ] }`),
			service.NewInterpolatedStringField(fileProcessorFieldContent).
				Description("The content to write when operation is 'write'. Supports interpolation.").
				Optional().
				Examples(
					"Hello World",
					"${! content() }",
					"${! json(\"document.content\") }",
				),
			service.NewInterpolatedStringField(fileProcessorFieldDest).
				Description("The destination path for 'move' operation. Supports interpolation.").
				Optional().
				Examples(
					"/tmp/backup/${! json(\"document.id\") }.txt",
				),
		).LintRule(`root = match {
      this.operation == "write" && !this.exists("content") => [ "'content' must be set when operation is 'write'" ],
      this.operation == "move" && !this.exists("dest") => [ "'dest' must be set when operation is 'move'" ],
    }`)
}

func init() {
	err := service.RegisterProcessor("file", fileProcessorSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			return fileProcessorFromParsed(pConf, res)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type fileProcessorConfig struct {
	Operation string
	Path      *service.InterpolatedString
	Content   *service.InterpolatedString
	Dest      *service.InterpolatedString
}

func fileProcessorConfigFromParsed(pConf *service.ParsedConfig) (conf fileProcessorConfig, err error) {
	if conf.Operation, err = pConf.FieldString(fileProcessorFieldOperation); err != nil {
		return
	}
	if conf.Path, err = pConf.FieldInterpolatedString(fileProcessorFieldPath); err != nil {
		return
	}
	if conf.Content, err = pConf.FieldInterpolatedString(fileProcessorFieldContent); err != nil {
		// Content is optional for operations other than write
		if conf.Operation != "write" {
			conf.Content = nil
		}
	}
	if conf.Dest, err = pConf.FieldInterpolatedString(fileProcessorFieldDest); err != nil {
		// Dest is optional for operations other than move
		if conf.Operation != "move" {
			conf.Dest = nil
			err = nil
		}
	}
	return
}

//------------------------------------------------------------------------------

type fileProcessor struct {
	log  *service.Logger
	nm   *service.Resources
	conf fileProcessorConfig
}

func fileProcessorFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*fileProcessor, error) {
	pConf, err := fileProcessorConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	return &fileProcessor{
		log:  nm.Logger(),
		nm:   nm,
		conf: pConf,
	}, nil
}

func (p *fileProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	switch p.conf.Operation {
	case "read":
		return p.processRead(ctx, msg)
	case "write":
		return p.processWrite(ctx, msg)
	case "delete":
		return p.processDelete(ctx, msg)
	case "move":
		return p.processMove(ctx, msg)
	case "stat":
		return p.processStat(ctx, msg)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", p.conf.Operation)
	}
}

func (p *fileProcessor) processRead(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	path, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	file, err := p.nm.FS().Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", path, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for '%s': %w", path, err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file '%s': %w", path, err)
	}

	newMsg := msg.Copy()
	newMsg.SetBytes(content)

	newMsg.MetaSetMut("file_path", path)
	newMsg.MetaSetMut("file_size", fileInfo.Size())
	newMsg.MetaSetMut("file_mod_time_unix", fileInfo.ModTime().Unix())
	newMsg.MetaSetMut("file_mod_time", fileInfo.ModTime().Format(time.RFC3339))

	return service.MessageBatch{newMsg}, nil
}

func (p *fileProcessor) processWrite(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	path, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	contentStr, err := p.conf.Content.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("content interpolation error: %w", err)
	}

	if err := p.nm.FS().MkdirAll(filepath.Dir(path), fs.FileMode(0o777)); err != nil {
		return nil, fmt.Errorf("failed to create directory for '%s': %w", path, err)
	}

	file, err := p.nm.FS().OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(0o666))
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s' for writing: %w", path, err)
	}
	defer file.Close()

	writer, ok := file.(io.Writer)
	if !ok {
		return nil, errors.New("failed to open a writable file")
	}

	if _, err := writer.Write([]byte(contentStr)); err != nil {
		return nil, fmt.Errorf("failed to write to file '%s': %w", path, err)
	}

	return service.MessageBatch{msg}, nil
}

func (p *fileProcessor) processDelete(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	path, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	if err := p.nm.FS().Remove(path); err != nil {
		return nil, fmt.Errorf("failed to delete file '%s': %w", path, err)
	}

	return service.MessageBatch{msg}, nil
}

func (p *fileProcessor) processMove(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	if p.conf.Dest == nil {
		return nil, errors.New("destination path is required for move operation")
	}

	srcPath, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("source path interpolation error: %w", err)
	}
	srcPath = filepath.Clean(srcPath)

	destPath, err := p.conf.Dest.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("destination path interpolation error: %w", err)
	}
	destPath = filepath.Clean(destPath)

	if err := p.nm.FS().MkdirAll(filepath.Dir(destPath), fs.FileMode(0o777)); err != nil {
		return nil, fmt.Errorf("failed to create directory for '%s': %w", destPath, err)
	}

	srcFile, err := p.nm.FS().Open(srcPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file '%s': %w", srcPath, err)
	}
	defer srcFile.Close()

	content, err := io.ReadAll(srcFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read source file '%s': %w", srcPath, err)
	}

	destFile, err := p.nm.FS().OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(0o666))
	if err != nil {
		return nil, fmt.Errorf("failed to open destination file '%s': %w", destPath, err)
	}
	defer destFile.Close()

	writer, ok := destFile.(io.Writer)
	if !ok {
		return nil, errors.New("failed to open a writable destination file")
	}

	if _, err := writer.Write(content); err != nil {
		return nil, fmt.Errorf("failed to write to destination file '%s': %w", destPath, err)
	}

	if err := p.nm.FS().Remove(srcPath); err != nil {
		return nil, fmt.Errorf("failed to delete source file '%s': %w", srcPath, err)
	}

	return service.MessageBatch{msg}, nil
}

func (p *fileProcessor) processStat(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	path, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	file, err := p.nm.FS().Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", path, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for '%s': %w", path, err)
	}

	newMsg := msg.Copy()

	// Add file metadata
	newMsg.MetaSetMut("file_path", path)
	newMsg.MetaSetMut("file_size", fileInfo.Size())
	newMsg.MetaSetMut("file_mod_time_unix", fileInfo.ModTime().Unix())
	newMsg.MetaSetMut("file_mod_time", fileInfo.ModTime().Format(time.RFC3339))
	newMsg.MetaSetMut("file_name", fileInfo.Name())
	newMsg.MetaSetMut("file_is_dir", fileInfo.IsDir())
	newMsg.MetaSetMut("file_mode", fileInfo.Mode().String())

	return service.MessageBatch{newMsg}, nil
}

func (p *fileProcessor) Close(ctx context.Context) error {
	return nil
}

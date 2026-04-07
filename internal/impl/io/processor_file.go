package io

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fileProcessorFieldOperation = "operation"
	fileProcessorFieldPath      = "path"
	fileProcessorFieldDest      = "destination_path"
	fileProcessorFieldScanner   = "scanner"

	// Operation types
	fileProcessorOpRead   = "read"
	fileProcessorOpWrite  = "write"
	fileProcessorOpDelete = "delete"
	fileProcessorOpMove   = "move"
	fileProcessorOpRename = "rename"
	fileProcessorOpStat   = "stat"
)

func fileProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Local").
		Summary(fmt.Sprintf(`Performs operations (%s, %s, %s, %s, %s, %s) on files.`, fileProcessorOpRead, fileProcessorOpWrite, fileProcessorOpDelete, fileProcessorOpMove, fileProcessorOpRename, fileProcessorOpStat)).
		Description(`
This processor allows you to perform various file operations based on message content. The operation is specified by the 'operation' field, and paths can be dynamically generated using interpolation functions.

### Operations

- **read**: Read content at 'path' into the message
- **write**: Write message content to 'path'
- **delete**: Delete file at 'path'
- **move**: Move a file at 'path' to 'destination_path'
- **rename**: Rename/move a file at 'path' to 'destination_path' using [os.Rename](https://pkg.go.dev/os#Rename).
- **stat**: Get file information from 'path' without reading content

### move vs rename
You may want to use the rename operation instead of using move and therefore avoid copying bytes.
However you should be aware that there are OS-specific restrictions with this approach and it will not work across different filesystems. 

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
			service.NewStringEnumField(fileProcessorFieldOperation, fileProcessorOpRead, fileProcessorOpWrite, fileProcessorOpDelete, fileProcessorOpMove, fileProcessorOpRename, fileProcessorOpStat).
				Description("The file operation to perform."),
			service.NewInterpolatedStringField(fileProcessorFieldPath).
				Description("The path used for reads, writes, deletes and stat. Source path for move and rename.").
				Examples(
					"/tmp/data.txt",
					"/tmp/${! json(\"document.id\") }.txt",
				).LintRule(`if this == "" { [ "'path' must be set to a non-empty string" ] }`),
			service.NewInterpolatedStringField(fileProcessorFieldDest).
				Description("The destination path for 'move' and 'rename' operations.").
				Optional().
				Examples(
					"/tmp/backup/${! json(\"document.id\") }.txt",
				),
			service.NewScannerField(fileProcessorFieldScanner).
				Description("The scanner to use for reading files.").
				Advanced().
				Optional(),
		).LintRule(`root = match {
      (this.operation == "` + fileProcessorOpMove + `" || this.operation == "` + fileProcessorOpRename + `") && !this.exists("` + fileProcessorFieldDest + `") => [ "'` + fileProcessorFieldDest + `' must be set when operation is '` + fileProcessorOpMove + `' or '` + fileProcessorOpRename + `'" ],
      this.operation == "` + fileProcessorOpRead + `" && !this.exists("` + fileProcessorFieldScanner + `") => [ "'` + fileProcessorFieldScanner + `' must be set when operation is '` + fileProcessorOpRead + `'" ],
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
	Operation       string
	Path            *service.InterpolatedString
	DestinationPath *service.InterpolatedString
}

func fileProcessorConfigFromParsed(pConf *service.ParsedConfig) (conf fileProcessorConfig, err error) {
	if conf.Operation, err = pConf.FieldString(fileProcessorFieldOperation); err != nil {
		return
	}
	if conf.Path, err = pConf.FieldInterpolatedString(fileProcessorFieldPath); err != nil {
		return
	}
	if conf.DestinationPath, err = pConf.FieldInterpolatedString(fileProcessorFieldDest); err != nil {
		// DestinationPath is optional for operations other than move and rename
		if conf.Operation != fileProcessorOpMove && conf.Operation != fileProcessorOpRename {
			conf.DestinationPath = nil
			err = nil
		}
	}

	return
}

//------------------------------------------------------------------------------

type fileProcessor struct {
	log     *service.Logger
	nm      *service.Resources
	scanner *service.OwnedScannerCreator
	conf    fileProcessorConfig
}

func fileProcessorFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*fileProcessor, error) {
	pConf, err := fileProcessorConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	// Scanner is required for read operations
	var scan *service.OwnedScannerCreator
	if pConf.Operation == fileProcessorOpRead {
		scan, err = conf.FieldScanner(fileProcessorFieldScanner)
		if err != nil {
			return nil, err
		}
	}

	return &fileProcessor{
		log:     nm.Logger(),
		nm:      nm,
		scanner: scan,
		conf:    pConf,
	}, nil
}

func (p *fileProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	switch p.conf.Operation {
	case fileProcessorOpRead:
		return p.processRead(ctx, msg)
	case fileProcessorOpWrite:
		return p.processWrite(msg)
	case fileProcessorOpDelete:
		return p.processDelete(msg)
	case fileProcessorOpMove:
		return p.processMove(ctx, msg)
	case fileProcessorOpRename:
		return p.processRename(msg)
	case fileProcessorOpStat:
		return p.processStat(msg)
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

	details := service.NewScannerSourceDetails()
	details.SetName(path)

	scanner, err := p.scanner.Create(file, func(ctx context.Context, err error) error {
		return nil
	}, details)
	if err != nil {
		return nil, fmt.Errorf("failed to create scanner for file '%s': %w", path, err)
	}
	defer scanner.Close(ctx)

	var allMessages service.MessageBatch

	// Process all batches from scanner until EOF
	for {
		parts, _, err := scanner.NextBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, component.ErrTimeout
			}
			if err == io.EOF {
				// End of file reached
				break
			}
			return nil, fmt.Errorf("failed to read from scanner for file '%s': %w", path, err)
		}

		// Create a copy of the original message for each part
		for _, part := range parts {
			newMsg := msg.Copy()
			partBytes, err := part.AsBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to get bytes from part: %w", err)
			}
			newMsg.SetBytes(partBytes)
			addFileMetadata(newMsg, path, fileInfo)

			allMessages = append(allMessages, newMsg)
		}
	}

	// If no messages were created (empty file), create one with just metadata
	if len(allMessages) == 0 {
		newMsg := msg.Copy()
		addFileMetadata(newMsg, path, fileInfo)
		return service.MessageBatch{newMsg}, nil
	}

	return allMessages, nil
}

func (p *fileProcessor) processWrite(msg *service.Message) (service.MessageBatch, error) {
	path, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	content, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	if err := p.nm.FS().MkdirAll(filepath.Dir(path), fs.FileMode(0o777)); err != nil {
		return nil, fmt.Errorf("failed to create directory for '%s': %w", path, err)
	}

	// Use atomic write pattern: write to temp file, then rename
	tempFile, err := generateTempFileName(path)
	if err != nil {
		return nil, err
	}
	file, err := p.nm.FS().OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(0o666))
	if err != nil {
		return nil, fmt.Errorf("failed to open temporary file '%s' for writing: %w", tempFile, err)
	}

	writer, ok := file.(io.Writer)
	if !ok {
		file.Close()
		_ = p.nm.FS().Remove(tempFile)
		return nil, errors.New("failed to open a writable file")
	}

	// Write content to temporary file
	if _, err := writer.Write(content); err != nil {
		file.Close()
		_ = p.nm.FS().Remove(tempFile)
		return nil, fmt.Errorf("failed to write to temporary file '%s': %w", tempFile, err)
	}

	// Close file before rename to ensure all data is flushed
	if err := file.Close(); err != nil {
		_ = p.nm.FS().Remove(tempFile)
		return nil, fmt.Errorf("failed to close temporary file '%s': %w", tempFile, err)
	}

	if err := os.Rename(tempFile, path); err != nil {
		_ = p.nm.FS().Remove(tempFile)
		return nil, fmt.Errorf("failed to rename temporary file '%s' to '%s': %w", tempFile, path, err)
	}

	return service.MessageBatch{msg}, nil
}

func (p *fileProcessor) processDelete(msg *service.Message) (service.MessageBatch, error) {
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
	if p.conf.DestinationPath == nil {
		return nil, errors.New("destination path is required for " + fileProcessorOpMove + " operation")
	}

	srcPath, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("source path interpolation error: %w", err)
	}
	srcPath = filepath.Clean(srcPath)

	destPath, err := p.conf.DestinationPath.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("destination path interpolation error: %w", err)
	}
	destPath = filepath.Clean(destPath)

	return p.atomicCopyAndDelete(srcPath, destPath, msg)
}

func (p *fileProcessor) processRename(msg *service.Message) (service.MessageBatch, error) {
	if p.conf.DestinationPath == nil {
		return nil, errors.New("destination path is required for " + fileProcessorOpRename + " operation")
	}

	srcPath, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("source path interpolation error: %w", err)
	}
	srcPath = filepath.Clean(srcPath)

	destPath, err := p.conf.DestinationPath.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("destination path interpolation error: %w", err)
	}
	destPath = filepath.Clean(destPath)

	if err := os.Rename(srcPath, destPath); err != nil {
		return nil, fmt.Errorf("failed to rename file from '%s' to '%s': %w", srcPath, destPath, err)
	}

	return service.MessageBatch{msg}, nil
}

// atomicCopyAndDelete performs an atomic copy from src to dest and then deletes src
// This ensures that either the operation completes fully or leaves the source intact
func (p *fileProcessor) atomicCopyAndDelete(srcPath, destPath string, msg *service.Message) (service.MessageBatch, error) {
	if err := p.nm.FS().MkdirAll(filepath.Dir(destPath), fs.FileMode(0o777)); err != nil {
		return nil, fmt.Errorf("failed to create directory for '%s': %w", destPath, err)
	}

	tempFile, err := generateTempFileName(destPath)
	if err != nil {
		return nil, err
	}
	srcFile, err := p.nm.FS().Open(srcPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file '%s': %w", srcPath, err)
	}
	defer srcFile.Close()

	destFile, err := p.nm.FS().OpenFile(tempFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(0o666))
	if err != nil {
		return nil, fmt.Errorf("failed to open temporary destination file '%s': %w", tempFile, err)
	}

	writer, ok := destFile.(io.Writer)
	if !ok {
		destFile.Close()
		_ = p.nm.FS().Remove(tempFile)
		return nil, errors.New("failed to open a writable destination file")
	}

	if _, err := io.Copy(writer, srcFile); err != nil {
		destFile.Close()
		_ = p.nm.FS().Remove(tempFile)
		return nil, fmt.Errorf("failed to write to temporary destination file '%s': %w", tempFile, err)
	}

	if err := destFile.Close(); err != nil {
		_ = p.nm.FS().Remove(tempFile)
		return nil, fmt.Errorf("failed to close temporary destination file '%s': %w", tempFile, err)
	}

	if err := os.Rename(tempFile, destPath); err != nil {
		_ = p.nm.FS().Remove(tempFile)
		return nil, fmt.Errorf("failed to rename temporary file '%s' to '%s': %w", tempFile, destPath, err)
	}

	// Only delete source after destination is successfully created
	if err := p.nm.FS().Remove(srcPath); err != nil {
		// If source deletion fails, we have both files but destination is complete
		// This is better than losing data
		p.log.Warnf("Failed to delete source file '%s' after successful copy to '%s': %v", srcPath, destPath, err)
	}

	return service.MessageBatch{msg}, nil
}

func (p *fileProcessor) processStat(msg *service.Message) (service.MessageBatch, error) {
	path, err := p.conf.Path.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	fileInfo, err := p.nm.FS().Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for '%s': %w", path, err)
	}

	newMsg := msg.Copy()

	addFileMetadata(newMsg, path, fileInfo)

	return service.MessageBatch{newMsg}, nil
}

func addFileMetadata(msg *service.Message, path string, fileInfo fs.FileInfo) {
	msg.MetaSetMut("file_path", path)
	msg.MetaSetMut("file_size", fileInfo.Size())
	msg.MetaSetMut("file_mod_time_unix", fileInfo.ModTime().Unix())
	msg.MetaSetMut("file_mod_time", fileInfo.ModTime().Format(time.RFC3339))
	msg.MetaSetMut("file_name", fileInfo.Name())
	msg.MetaSetMut("file_is_dir", fileInfo.IsDir())
	msg.MetaSetMut("file_mode", fileInfo.Mode().String())
}

// generateTempFileName generates a unique temporary file name to avoid collisions
func generateTempFileName(basePath string) (string, error) {
	// Generate 8 random bytes and encode as hex (16 characters)
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes for temp file: %w", err)
	}
	randomSuffix := hex.EncodeToString(randomBytes)

	return basePath + ".tmp_" + randomSuffix, nil
}

func (p *fileProcessor) Close(ctx context.Context) error {
	return nil
}

package sftp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/sftp"
	"golang.org/x/exp/slog"

	"github.com/warpstreamlabs/bento/public/service"
)

type Output struct {
	baseSftpIO
	outFile *sftp.File
}

func (o *Output) Connect(ctx context.Context) error {
	if err := o.baseConnect(ctx); err != nil {
		return fmt.Errorf("error connecting to remote sftp for writing: %w", err)
	}

	dirName := filepath.Dir(o.parsedPaths[0])

	_ = o.sftpClient.MkdirAll(dirName)

	var err error
	if err := o.sftpClient.Remove(o.parsedPaths[0]); err != nil {
		slog.Debug("Error ensuring file is removed: " + err.Error())
	}

	o.outFile, err = o.sftpClient.OpenFile(o.parsedPaths[0], os.O_WRONLY|os.O_TRUNC|os.O_CREATE)
	if err != nil {
		return fmt.Errorf("error creating file for writing: %w", err)
	}

	return nil
}

func (o *Output) Write(_ context.Context, message *service.Message) error {
	bs, err := message.AsBytes()
	if err != nil {
		return fmt.Errorf("error converting message to bytes: %w", err)
	}
	_, err = o.outFile.Write(bs)
	if err != nil {
		return fmt.Errorf("could not write to file: %w", err)
	}

	if o.config.codec == CodecLines {
		slog.Debug("Adding newlines for codec==lines")
		_, err = o.outFile.Write([]byte("\n"))
		if err != nil {
			return fmt.Errorf("could not write to file: %w", err)
		}
	}

	return nil
}

func (o *Output) Close(ctx context.Context) error {
	if err := o.outFile.Close(); err != nil {
		return err
	}

	return o.baseClose(ctx)
}

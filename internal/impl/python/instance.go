package python

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing/fstest"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

func (p *pythonProcessor) newInstance(ctx context.Context) (*pythonInstance, error) {
	fs := fstest.MapFS{
		pythonEntrypointFile: &fstest.MapFile{Data: pythonEntrypoint},
		pythonExecScriptFile: &fstest.MapFile{Data: []byte(p.script)},
	}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errBuf := &bytes.Buffer{}

	cfg := wazero.NewModuleConfig().
		WithFS(fs).
		WithStdin(inR).WithStdout(outW).WithStderr(errBuf).
		WithArgs("python", filepath.Join("/", pythonEntrypointFile), strings.Join(p.imports, ","))

	var mod api.Module

	instantiateErr := make(chan error, 1)
	go func() {
		var err error
		// this is a blocking call, so we have to deploy in a goroutine
		mod, err = p.runtime.InstantiateModule(ctx, p.compiled, cfg)
		if err != nil {
			instantiateErr <- fmt.Errorf("instantiation failed: %w", err)
			outW.Close()
		}
	}()

	handshakeErr := make(chan error, 1)
	go func() {
		buf := make([]byte, len(pythonReadySignal))
		if _, err := io.ReadFull(outR, buf); err != nil {
			handshakeErr <- fmt.Errorf("failed to read handshake: %w", err)
			return
		}
		if string(buf) != pythonReadySignal {
			handshakeErr <- fmt.Errorf("wrong signal: %s", string(buf))
			return
		}
		handshakeErr <- nil
	}()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-instantiateErr:
		return nil, err
	case err := <-handshakeErr:
		if err != nil {
			p.logger.Errorf("python handshake failed (stderr): %s)")
			return nil, fmt.Errorf("handshake failed: %w", err)
		}
	}

	return &pythonInstance{
		Module:    mod,
		stdinW:    inW,
		stdoutR:   outR,
		stderrBuf: errBuf,
	}, nil
}

//------------------------------------------------------------------------------

type pythonInstance struct {
	api.Module
	stdinW    io.WriteCloser
	stdoutR   io.Reader
	stderrBuf *bytes.Buffer
}

func (pi *pythonInstance) runRequest(input []byte) (stdout []byte, stderr []byte, err error) {
	if err := binary.Write(pi.stdinW, binary.BigEndian, uint32(len(input))); err != nil {
		return nil, pi.stderrBuf.Bytes(), err
	}
	if _, err := pi.stdinW.Write(input); err != nil {
		return nil, pi.stderrBuf.Bytes(), err
	}

	header := make([]byte, 5)
	if _, err := io.ReadFull(pi.stdoutR, header); err != nil {
		return nil, pi.stderrBuf.Bytes(), fmt.Errorf("pipe read error: %w", err)
	}

	status, respLen := header[0], binary.BigEndian.Uint32(header[1:])

	payload := make([]byte, respLen)
	if _, err := io.ReadFull(pi.stdoutR, payload); err != nil {
		return nil, pi.stderrBuf.Bytes(), fmt.Errorf("pipe read error: %w", err)
	}

	stderr = pi.stderrBuf.Bytes()
	if status == pythonFailureStatus {
		return nil, stderr, fmt.Errorf("python error: %s", string(payload))
	}

	return payload, stderr, nil
}

func (pi *pythonInstance) Close(ctx context.Context) error {
	pi.stdinW.Close()
	if pi.Module == nil {
		return nil
	}
	return pi.Module.Close(ctx)
}

package io

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

func socketRequestProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Connects to a (tcp/udp/unix) server, sends a message, and optionally reads a response using a scanner.`).
		Description(`This processor connects to a specified network address, sends interpolated data, and optionally reads a response using scanner component. If a scanner is configured, it reads exactly one batch from the scanner. If no scanner is configured, it acts as a send-only processor.`).
		Categories("Network").
		Fields(
			service.NewStringEnumField("network", "tcp", "udp", "unix").
				Description("A network type to connect as.").
				Default("tcp"),
			service.NewInterpolatedStringField("address").
				Description("The address to connect to. Supports Bloblang interpolation.").
				Examples("127.0.0.1:6000", `${! this.ip }`),
			service.NewInterpolatedStringField("data").
				Description("The data to send. Supports Bloblang interpolation. Can be empty if you only want to read a response or just establish a connection.").
				Default(""),
			service.NewDurationField("write_timeout").
				Description("The maximum time to wait for the data to be written before timing out. Prevents the pipeline from hanging if the network drops. If omitted, it relies solely on the pipeline's context deadline.").
				Default("5s").
				Optional(),
			service.NewScannerField("scanner").
				Description("The scanner to use to read the response. If omitted, the processor will only send the message and not read any response.").
				Optional(),
		)
}

func init() {
	err := service.RegisterProcessor("socket_request", socketRequestProcessorSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newSocketRequestProcessorFromParsed(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

type socketRequestProcessor struct {
	network      string
	address      *service.InterpolatedString
	data         *service.InterpolatedString
	writeTimeout time.Duration
	scanner      *service.OwnedScannerCreator
	log          *service.Logger
}

func newSocketRequestProcessorFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*socketRequestProcessor, error) {
	p := &socketRequestProcessor{
		log: mgr.Logger(),
	}
	var err error
	if p.network, err = pConf.FieldString("network"); err != nil {
		return nil, err
	}
	if p.address, err = pConf.FieldInterpolatedString("address"); err != nil {
		return nil, err
	}
	if p.data, err = pConf.FieldInterpolatedString("data"); err != nil {
		return nil, err
	}
	if pConf.Contains("write_timeout") {
		if p.writeTimeout, err = pConf.FieldDuration("write_timeout"); err != nil {
			return nil, err
		}
	}
	if pConf.Contains("scanner") {
		if p.scanner, err = pConf.FieldScanner("scanner"); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *socketRequestProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	addr, err := p.address.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate address: %w", err)
	}
	dataBytes, err := p.data.TryBytes(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate data: %w", err)
	}

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, p.network, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s://%s: %w", p.network, addr, err)
	}
	defer conn.Close()

	if p.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(p.writeTimeout))
	} else if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	}

	_, err = io.Copy(conn, bytes.NewReader(dataBytes))

	if err != nil {
		return nil, fmt.Errorf("failed to write data to %s://%s: %w", p.network, addr, err)
	}

	if p.scanner != nil {
		details := service.NewScannerSourceDetails()
		details.SetName(fmt.Sprintf("%s://%s", p.network, addr))

		sc, err := p.scanner.Create(conn, func(ctx context.Context, err error) error {
			return nil
		}, details)

		if err != nil {
			return nil, fmt.Errorf("failed to create scanner: %w", err)
		}

		defer sc.Close(ctx)

		batch, ackFn, err := sc.NextBatch(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read from scanner: %w", err)
		}

		if ackFn != nil {
			_ = ackFn(ctx, nil)
		}

		if len(batch) == 0 {
			return nil, fmt.Errorf("scanner returned an empty batch")
		}

		_ = batch.WalkWithBatchedErrors(func(i int, m *service.Message) error {
			return msg.MetaWalkMut(func(key string, value any) error {
				_, exists := m.MetaGetMut(key)
				if !exists {
					m.MetaSetMut(key, value)
				}

				return nil
			})
		})

		return batch, nil
	}

	return []*service.Message{msg}, nil
}

func (p *socketRequestProcessor) Close(ctx context.Context) error {
	if p.scanner != nil {
		return p.scanner.Close(ctx)
	}
	return nil
}

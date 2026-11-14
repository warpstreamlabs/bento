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

func socketSendOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Connects to a (tcp/udp/unix) server, sends a single message with interpolated data, and disconnects directly.`).
		Description(`This output connects to a specified network address for each incoming message, sends the interpolated data, and immediately disconnects. It is useful for simple fire-and-forget TCP/UDP communication where connection pooling is not required or desired.`).
		Categories("Network").
		Fields(
			service.NewStringEnumField("network", "tcp", "udp", "unix").
				Description("A network type to connect as.").
				Default("tcp"),
			service.NewInterpolatedStringField("address").
				Description("The address to connect to. Supports Bloblang interpolation.").
				Examples("127.0.0.1:6000", `${! this.ip }`),
			service.NewInterpolatedStringField("data").
				Description("The data to send. Supports Bloblang interpolation.").
				Examples("hello", `${! this.data }`),
			service.NewDurationField("write_timeout").
				Description("The maximum time to wait for the data to be written before timing out. Prevents the pipeline from hanging if the network drops. If omitted, it relies solely on the pipeline's context deadline.").
				Default("5s").
				Optional(),
		)
}

func init() {
	err := service.RegisterOutput("socket_send", socketSendOutputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
		maxInFlight = 1
		out, err = newSocketSendWriterFromParsed(conf, mgr)
		return
	})
	if err != nil {
		panic(err)
	}
}

type socketSendWriter struct {
	network      string
	address      *service.InterpolatedString
	data         *service.InterpolatedString
	writeTimeout time.Duration
	log          *service.Logger
}

func newSocketSendWriterFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*socketSendWriter, error) {
	w := &socketSendWriter{
		log: mgr.Logger(),
	}
	var err error
	if w.network, err = pConf.FieldString("network"); err != nil {
		return nil, err
	}
	if w.address, err = pConf.FieldInterpolatedString("address"); err != nil {
		return nil, err
	}
	if w.data, err = pConf.FieldInterpolatedString("data"); err != nil {
		return nil, err
	}
	if pConf.Contains("write_timeout") {
		if w.writeTimeout, err = pConf.FieldDuration("write_timeout"); err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (s *socketSendWriter) Connect(ctx context.Context) error {
	return nil
}

func (s *socketSendWriter) Write(ctx context.Context, msg *service.Message) error {
	addr, err := s.address.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate address: %w", err)
	}

	dataBytes, err := s.data.TryBytes(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate data: %w", err)
	}

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, s.network, addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s://%s: %w", s.network, addr, err)
	}
	defer conn.Close()

	if s.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
	} else if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	}

	_, err = io.Copy(conn, bytes.NewReader(dataBytes))
	if err != nil {
		return fmt.Errorf("failed to write data to %s://%s: %w", s.network, addr, err)
	}

	return nil
}

func (s *socketSendWriter) Close(ctx context.Context) error {
	return nil
}

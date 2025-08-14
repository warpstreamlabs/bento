package grpc

import (
	"context"
	"crypto/tls"
	"sync"

	pb "github.com/warpstreamlabs/bento/internal/impl/grpc/pb/proto"

	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
)

type grpcOutput struct {
	address   string
	resources *service.Resources

	conn   *grpc.ClientConn
	client pb.BentoIngressClient

	dialCfg clientOpts
	useTLS  bool

	mu     sync.Mutex
	stream pb.BentoIngress_PublishClient
	log    *service.Logger
}

func newGRPCOutput(address string, tlsConf *tls.Config, dial clientOpts, res *service.Resources) (service.Output, error) {
	return &grpcOutput{address: address, resources: res, dialCfg: dial, useTLS: tlsConf != nil, log: res.Logger()}, nil
}

func (g *grpcOutput) Connect(ctx context.Context) error {
	if g.conn != nil {
		return nil
	}
	opts, err := buildDialOptions(ctx, g.useTLS, g.dialCfg)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(g.address, opts...)
	if err != nil {
		if g.log != nil {
			g.log.With("address", g.address, "use_tls", g.useTLS, "err", err).Error("grpc dial failed")
		}
		return err
	}
	g.conn = conn
	g.client = pb.NewBentoIngressClient(conn)
	if g.log != nil {
		g.log.With("address", g.address, "use_tls", g.useTLS).Info("grpc output connected")
	}
	return nil
}

func (g *grpcOutput) Write(ctx context.Context, msg *service.Message) error {
	if g.client == nil {
		return service.ErrNotConnected
	}

	dataBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}
	frame := &pb.Frame{Data: dataBytes}
	_ = msg.MetaWalkMut(func(k string, v any) error {
		if frame.Metadata == nil {
			frame.Metadata = map[string]string{}
		}
		if s, ok := v.(string); ok {
			frame.Metadata[k] = s
		}
		return nil
	})

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stream == nil {
		s, err := g.client.Publish(ctx)
		if err != nil {
			if g.log != nil {
				g.log.With("err", err).Error("grpc publish stream open failed")
			}
			return err
		}
		g.stream = s
	}
	if err := g.stream.Send(frame); err != nil {
		// reset and try once more
		_, _ = g.stream.CloseAndRecv()
		g.stream = nil
		s, err2 := g.client.Publish(ctx)
		if err2 != nil {
			if g.log != nil {
				g.log.With("err", err2).Error("grpc publish stream reopen failed")
			}
			return err
		}
		g.stream = s
		if err3 := g.stream.Send(frame); err3 != nil {
			if g.log != nil {
				g.log.With("err", err3).Error("grpc publish send failed after reopen")
			}
			return err3
		}
	}
	return nil
}

func (g *grpcOutput) Close(ctx context.Context) error {
	g.mu.Lock()
	if g.stream != nil {
		_, _ = g.stream.CloseAndRecv()
		g.stream = nil
	}
	g.mu.Unlock()
	if g.conn != nil {
		_ = g.conn.Close()
		g.conn = nil
	}
	return nil
}

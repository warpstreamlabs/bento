package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"sync"
	"time"

	pb "github.com/warpstreamlabs/bento/internal/impl/grpc/pb/proto"

	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
)

type grpcInput struct {
	address    string
	consumerID string
	resources  *service.Resources

	conn   *grpc.ClientConn
	client pb.BentoIngressClient
	once   sync.Once

	recvCh    chan *pb.Frame
	errCh     chan error
	subCancel context.CancelFunc
	subMu     sync.Mutex
	done      bool

	dialCfg clientOpts
	useTLS  bool
	log     *service.Logger
}

func newGRPCInput(address, consumerID string, tlsConf *tls.Config, dial clientOpts, res *service.Resources) (service.Input, error) {
	gi := &grpcInput{
		address:    address,
		consumerID: consumerID,
		resources:  res,
		dialCfg:    dial,
		useTLS:     tlsConf != nil,
		log:        res.Logger(),
	}
	return gi, nil
}

func (g *grpcInput) Connect(ctx context.Context) error {
	if g.conn != nil {
		return nil
	}
	// Build options using helper (tls/headers/keepalive/etc.).
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
		g.log.With("address", g.address, "use_tls", g.useTLS).Info("grpc input connected")
	}
	// Prepare channels
	g.recvCh = make(chan *pb.Frame, 64)
	g.errCh = make(chan error, 1)
	return nil
}

func (g *grpcInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if g.client == nil {
		return nil, nil, errors.New("grpc not connected")
	}
	if g.done {
		return nil, nil, service.ErrEndOfInput
	}
	// Ensure subscribe loop started
	g.ensureSubscribeLoop()

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case err := <-g.errCh:
		if errors.Is(err, io.EOF) {
			g.subMu.Lock()
			g.done = true
			g.subMu.Unlock()
			return nil, nil, service.ErrEndOfInput
		}
		return nil, nil, err
	case frame := <-g.recvCh:
		if frame == nil {
			// Channel closed due to stream end
			g.subMu.Lock()
			g.done = true
			g.subMu.Unlock()
			return nil, nil, service.ErrEndOfInput
		}
		msg := service.NewMessage(frame.Data)
		for k, v := range frame.Metadata {
			msg.MetaSetMut(k, v)
		}
		ack := func(ctx context.Context, err error) error { return nil }
		return msg, ack, nil
	}
}

func (g *grpcInput) ensureSubscribeLoop() {
	g.subMu.Lock()
	alreadyRunning := g.subCancel != nil
	g.subMu.Unlock()
	if alreadyRunning {
		return
	}

	g.subMu.Lock()
	// Double-check
	if g.subCancel != nil {
		g.subMu.Unlock()
		return
	}
	subCtx, cancel := context.WithCancel(context.Background())
	g.subCancel = cancel
	client := g.client
	recvCh := g.recvCh
	errCh := g.errCh
	consumerID := g.consumerID
	g.subMu.Unlock()

	go func() {
		stream, err := client.Subscribe(subCtx, &pb.SubscribeRequest{ConsumerId: consumerID})
		if err != nil {
			// report error and exit
			select {
			case errCh <- err:
			default:
			}
			return
		}
		for {
			frame, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					select {
					case errCh <- io.EOF:
					default:
					}
				} else {
					select {
					case errCh <- err:
					default:
					}
				}
				close(recvCh)
				return
			}
			// non-blocking send with backpressure via buffered chan
			recvCh <- frame
		}
	}()
}

func (g *grpcInput) Close(ctx context.Context) error {
	var err error
	g.once.Do(func() {
		if g.subCancel != nil {
			g.subCancel()
		}
		if g.conn != nil {
			cErr := g.conn.Close()
			if cErr != nil {
				err = cErr
			}
			g.conn = nil
		}
	})
	// Allow time for close
	_ = ctx
	time.Sleep(5 * time.Millisecond)
	return err
}

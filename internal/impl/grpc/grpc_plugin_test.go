package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/warpstreamlabs/bento/internal/impl/grpc/pb/proto"

	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
)

type testServer struct {
	pb.UnimplementedBentoIngressServer
	lastFrame *pb.Frame
}

func (t *testServer) Publish(stream pb.BentoIngress_PublishServer) error {
	var count uint64
	for {
		f, err := stream.Recv()
		if err != nil {
			// EOF ends the stream
			break
		}
		t.lastFrame = f
		count++
	}
	return stream.SendAndClose(&pb.PublishResponse{Accepted: count})
}

func (t *testServer) Subscribe(req *pb.SubscribeRequest, stream pb.BentoIngress_SubscribeServer) error {
	// Send a single frame then finish
	f := &pb.Frame{Data: []byte("hello from server"), Metadata: map[string]string{"x": "y"}}
	if err := stream.Send(f); err != nil {
		return err
	}
	return nil
}

func startTestGRPCServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	ts := &testServer{}
	pb.RegisterBentoIngressServer(s, ts)

	done := make(chan struct{})
	go func() {
		_ = s.Serve(lis)
		close(done)
	}()

	return lis.Addr().String(), func() {
		s.GracefulStop()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			s.Stop()
		}
	}
}

func TestGRPCOutput_Publish(t *testing.T) {
	addr, cleanup := startTestGRPCServer(t)
	defer cleanup()

	out, err := newGRPCOutput(addr, nil, clientOpts{}, service.MockResources())
	if err != nil {
		t.Fatalf("newGRPCOutput: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := out.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	msg := service.NewMessage([]byte("hello"))
	msg.MetaSetMut("k", "v")
	if err := out.Write(ctx, msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = out.Close(context.Background())
}

func TestGRPCInput_Subscribe(t *testing.T) {
	addr, cleanup := startTestGRPCServer(t)
	defer cleanup()

	in, err := newGRPCInput(addr, "tester", nil, clientOpts{}, service.MockResources())
	if err != nil {
		t.Fatalf("newGRPCInput: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := in.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	msg, ack, err := in.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	body, _ := msg.AsBytes()
	if got := string(body); got == "" {
		t.Fatalf("unexpected empty message body")
	}
	if err := ack(ctx, nil); err != nil {
		t.Fatalf("ack: %v", err)
	}
	_ = in.Close(context.Background())
}

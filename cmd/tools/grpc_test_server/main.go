package main

import (
	"context"
	"io"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

// Manual registration for bidi service using google.protobuf.Struct
func chatStreamHandler(_ interface{}, stream grpc.ServerStream) error {
	for {
		in := new(structpb.Struct)
		if err := stream.RecvMsg(in); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// Enrich response with server metadata and timestamp so client logs are visible
		enriched := map[string]interface{}{}
		for k, v := range in.AsMap() {
			enriched[k] = v
		}
		enriched["server"] = "grpc_test_server"
		enriched["ts"] = time.Now().Format(time.RFC3339Nano)
		enriched["note"] = "bidi echo"
		out, _ := structpb.NewStruct(enriched)
		if err := stream.SendMsg(out); err != nil {
			return err
		}
	}
}

var chatServiceDesc = grpc.ServiceDesc{
	ServiceName: "chat.Chat",
	HandlerType: (*interface{})(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       chatStreamHandler,
		ServerStreams: true,
		ClientStreams: true,
	}},
}

// Server-stream-only echo service using Struct
func echoStreamHandler(_ interface{}, stream grpc.ServerStream) error {
	in := new(structpb.Struct)
	if err := stream.RecvMsg(in); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	// emit a few echoes and finish
	for i := 0; i < 3; i++ {
		if err := stream.SendMsg(in); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

var echoServiceDesc = grpc.ServiceDesc{
	ServiceName: "echo.Echo",
	HandlerType: (*interface{})(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       echoStreamHandler,
		ServerStreams: true,
		ClientStreams: false,
	}},
}

// Client-stream-only ingest service: counts messages and returns a final result
func ingestStreamHandler(_ interface{}, stream grpc.ServerStream) error {
	count := 0
	for {
		in := new(structpb.Struct)
		if err := stream.RecvMsg(in); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		count++
	}
	// respond once with count
	out, _ := structpb.NewStruct(map[string]interface{}{"count": count})
	return stream.SendMsg(out)
}

var ingestServiceDesc = grpc.ServiceDesc{
	ServiceName: "ingest.Ingest",
	HandlerType: (*interface{})(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		Handler:       ingestStreamHandler,
		ServerStreams: false,
		ClientStreams: true,
	}},
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()

	// Health service
	hs := health.NewServer()
	// Set default service to SERVING
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	healthpb.RegisterHealthServer(s, hs)

	// Reflection for dynamic clients
	reflection.Register(s)

	// Register manual chat service
	s.RegisterService(&chatServiceDesc, nil)
	// Register echo server-stream-only service
	s.RegisterService(&echoServiceDesc, nil)
	// Register ingest client-stream-only service
	s.RegisterService(&ingestServiceDesc, nil)

	log.Println("grpc test server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// Ensure unused import of context isn't optimized out in newer toolchains
var _ = context.Background

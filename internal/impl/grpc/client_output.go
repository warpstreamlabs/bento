package grpc

import (
	"context"
	"fmt"
	"io"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/warpstreamlabs/bento/public/service"
	_ "google.golang.org/grpc/health"
	grpcmd "google.golang.org/grpc/metadata"
)

const (
	grpcClientOutputBatching = "batching"
	grpcClientOutputPropRes  = "propagate_response"
	grpcClientOutputMetadata = "metadata"
)

const grpcClientOutputDescription = `

### Expected Message Format

Either the field ` + "`reflection` or `proto_files`" + ` must be supplied, which will provide the protobuf schema Bento will use to marshall the Bento message into protobuf.

### Metadata

The ` + "`metadata`" + ` field allows you to attach key/value pairs as gRPC metadata (headers) to outgoing requests. Values support [interpolation functions](/docs/configuration/interpolation#bloblang-queries).

How metadata is applied depends on the ` + "`rpc_type`:" + `

- ` + "`" + rpcTypeUnary + "`" + `: Metadata is evaluated **per message**. Each message in a batch can produce different metadata values via interpolation.` + `
- ` + "`" + rpcTypeServerStream + "`" + `: Metadata is evaluated **per message**, since each message opens its own server stream.` + `
- ` + "`" + rpcTypeClientStream + "`" + `: Metadata is evaluated from the **first message in the batch** only. gRPC metadata is sent as headers when the stream is opened, so it cannot vary per message within a single stream.` + `
- ` + "`" + rpcTypeBidi + "`" + `: Same as ` + "`client_stream`" + ` — metadata is evaluated from the **first message in the batch**.` + `

:::caution
For ` + "`client_stream`" + ` and ` + "`bidi`" + ` RPC types, gRPC metadata is a stream-level concept (sent as HTTP/2 headers at stream creation). If you use interpolation functions that produce different values per message (e.g. ` + "`${! json(\"id\") }`" + `), only the value from the **first message** in the batch will be used for the entire stream. This is a gRPC protocol limitation, not a Bento limitation.
:::

### Propagating Responses

It's possible to propagate the response(s) from each gRPC method invocation back to the input source by
setting ` + "`" + `propagate_response` + "` to `true`." + ` Only inputs that support [synchronous responses](/docs/guides/sync_responses)
are able to make use of these propagated responses. Also the  ` + "`" + `rpc_type` + "`" + `effects the behavior of what is returned via a sync_response:

- ` + "`" + rpcTypeUnary + "`" + `: The response propagated is a single message.` + `
- ` + "`" + rpcTypeClientStream + "`" + `: The response propagated is a single message.` + `
- ` + "`" + rpcTypeServerStream + "`" + `: The response propagated is a batch of messages.` + `
- ` + "`" + rpcTypeBidi + "`" + `: Any inbound message from the server is discarded.` + `
`

func grpcClientOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Sends messages to a GRPC server.").
		Description(grpcClientOutputDescription).
		Categories("network").
		Example("HTTP <--> gRPC Reverse Proxy", "Use Bento as a reverse proxy to translate HTTP requests into gRPC calls and return the response", `
input:
  http_server:
    path: /post

output:
  grpc_client:
    address: localhost:51286
    service: helloworld.Greeter
    method: SayHello
    propagate_response: true
    reflection: true
`).
		Fields(grpcCommonFieldSpec()...).
		Fields(
			service.NewInterpolatedStringMapField(grpcClientOutputMetadata).
				Description("A map of metadata key/value pairs to add to gRPC requests. For `unary` and `server_stream` RPC types, metadata is evaluated per message. For `client_stream` and `bidi` RPC types, metadata is evaluated from the first message in the batch only, since gRPC stream metadata is sent once at stream creation.").
				Example(map[string]any{
					"application":  "bento",
					"x-request-id": `${!metadata("request_id")}`,
				}).
				Default(map[string]any{}).
				Optional(),
			service.NewBoolField(grpcClientOutputPropRes).
				Description("Whether responses from the server should be [propagated back](/docs/guides/sync_responses) to the input.").
				Default(false).
				Advanced(),
			oAuth2FieldSpec(),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(grpcClientOutputBatching),
		).LintRule(
		`root = match {
  this.rpc_type == "bidi" && this.propagate_response == true => "cannot set propagate_response to true when rpc_type is bidi",
  this.reflection == false && (!this.exists("proto_files") || this.proto_files.length() == 0) => "reflection must be true or proto_files must be populated"
}`,
	)
}

func init() {
	err := service.RegisterBatchOutput("grpc_client", grpcClientOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}

			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}

			out, err = newGrpcClientWriterFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type grpcClientOutput struct {
	grpcCommonConfig
	metadata     map[string]*service.InterpolatedString
	propResponse bool
}

func newGrpcClientWriterFromParsed(conf *service.ParsedConfig, _ *service.Resources) (*grpcClientOutput, error) {
	gcc, err := grpcCommonConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	propResponse, err := conf.FieldBool(grpcClientOutputPropRes)
	if err != nil {
		return nil, err
	}
	md, err := conf.FieldInterpolatedStringMap(grpcClientOutputMetadata)
	if err != nil {
		return nil, err
	}

	writer := &grpcClientOutput{
		grpcCommonConfig: gcc,
		propResponse:     propResponse,
		metadata:         md,
	}

	return writer, nil
}

//------------------------------------------------------------------------------

func (gco *grpcClientOutput) WriteBatch(ctx context.Context, msgBatch service.MessageBatch) error {
	if gco.conn == nil || gco.method == nil {
		return service.ErrNotConnected
	}

	var err error
	switch gco.rpcType {
	case rpcTypeUnary:
		err = gco.unaryHandler(ctx, msgBatch)
	case rpcTypeClientStream:
		err = gco.clientStreamHandler(ctx, msgBatch)
	case rpcTypeServerStream:
		err = gco.serverStreamHandler(ctx, msgBatch)
	case rpcTypeBidi:
		err = gco.bidirectionalHandler(ctx, msgBatch)
	}
	if err != nil {
		return err
	}

	return nil
}

func (gco *grpcClientOutput) Close(ctx context.Context) (err error) {
	if gco.reflectClient != nil {
		gco.reflectClient.Reset()
	}
	if gco.conn != nil {
		return gco.conn.Close()
	}
	return nil
}

func (gco *grpcClientOutput) contextWithMetadata(ctx context.Context, msg *service.Message) (context.Context, error) {
	if len(gco.metadata) == 0 {
		return ctx, nil
	}
	pairs := make([]string, 0, len(gco.metadata)*2)
	for k, v := range gco.metadata {
		s, err := v.TryString(msg)
		if err != nil {
			return ctx, fmt.Errorf("metadata %q interpolation error: %w", k, err)
		}
		pairs = append(pairs, k, s)
	}
	return grpcmd.AppendToOutgoingContext(ctx, pairs...), nil
}

func (gco *grpcClientOutput) unaryHandler(ctx context.Context, msgBatch service.MessageBatch) error {
	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(msgBatch, err)
		}
		batchErr.Failed(i, err)
	}

	for i, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		request := dynamic.NewMessage(gco.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			batchErrFailed(i, err)
			continue
		}

		rpcCtx, err := gco.contextWithMetadata(ctx, msg)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		resProtoMessage, err := gco.stub.InvokeRpc(rpcCtx, gco.method, request)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		if !gco.propResponse {
			continue
		}

		dynMsg, ok := resProtoMessage.(*dynamic.Message)
		if !ok {
			batchErrFailed(i, fmt.Errorf("expected dynamic.Message but got %T", resProtoMessage))
			continue
		}

		jsonBytes, err := dynMsg.MarshalJSON()
		if err != nil {
			batchErrFailed(i, fmt.Errorf("failed to marshal proto response to JSON: %w", err))
			continue
		}

		responseMsg := msg.Copy()
		responseMsg.SetBytes(jsonBytes)

		responseBatch := service.MessageBatch{responseMsg}
		if err := responseBatch.AddSyncResponse(); err != nil {
			batchErrFailed(i, err)
			continue
		}
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	return nil
}

func (gco *grpcClientOutput) clientStreamHandler(ctx context.Context, msgBatch service.MessageBatch) error {
	rpcCtx, err := gco.contextWithMetadata(ctx, msgBatch[0])
	if err != nil {
		return err
	}

	clientStream, err := gco.stub.InvokeRpcClientStream(rpcCtx, gco.method)
	if err != nil {
		return err
	}

	for _, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		request := dynamic.NewMessage(gco.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			return err
		}

		err = clientStream.SendMsg(request)
		if err != nil {
			return err
		}
	}

	resProtoMessage, err := clientStream.CloseAndReceive()
	if err != nil {
		return err
	}

	if !gco.propResponse {
		return nil
	}

	dynMsg, ok := resProtoMessage.(*dynamic.Message)
	if !ok {
		return fmt.Errorf("expected dynamic.Message but got %T", resProtoMessage)
	}

	jsonBytes, err := dynMsg.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal proto response to JSON: %w", err)
	}

	responseMsg := msgBatch[0].Copy()
	responseMsg.SetBytes(jsonBytes)

	if err := responseMsg.AddSyncResponse(); err != nil {
		return err
	}

	return nil
}

func (gco *grpcClientOutput) serverStreamHandler(ctx context.Context, msgBatch service.MessageBatch) error {
	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(msgBatch, err)
		}
		batchErr.Failed(i, err)
	}

msgLoop:
	for i, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		request := dynamic.NewMessage(gco.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			batchErrFailed(i, err)
			continue
		}

		rpcCtx, err := gco.contextWithMetadata(ctx, msg)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		serverStream, err := gco.stub.InvokeRpcServerStream(rpcCtx, gco.method, request)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		if !gco.propResponse {
			for {
				_, err := serverStream.RecvMsg()
				if err == io.EOF {
					break
				}
				if err != nil {
					batchErrFailed(i, fmt.Errorf("failed to receive from server stream: %w", err))
					continue msgLoop
				}
			}
			continue
		}

		responseBatch := service.MessageBatch{}

		for {
			resProtoMessage, err := serverStream.RecvMsg()
			if err == io.EOF {
				if len(responseBatch) > 0 {
					if err := responseBatch.AddSyncResponse(); err != nil {
						batchErrFailed(i, err)
						continue msgLoop
					}
				}
				break
			}

			if err != nil {
				batchErrFailed(i, fmt.Errorf("failed to receive from server stream: %w", err))
				continue msgLoop
			}

			if dynMsg, ok := resProtoMessage.(*dynamic.Message); ok {
				jsonBytes, err := dynMsg.MarshalJSON()
				if err != nil {
					batchErrFailed(i, fmt.Errorf("failed to marshal proto response to JSON: %w", err))
					continue msgLoop
				}

				responseMsg := msg.Copy()
				responseMsg.SetBytes(jsonBytes)
				responseBatch = append(responseBatch, responseMsg)
			}
		}
	}
	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	return nil
}

func (gco *grpcClientOutput) bidirectionalHandler(ctx context.Context, msgBatch service.MessageBatch) error {
	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(msgBatch, err)
		}
		batchErr.Failed(i, err)
	}

	rpcCtx, err := gco.contextWithMetadata(ctx, msgBatch[0])
	if err != nil {
		return err
	}

	bidi, err := gco.stub.InvokeRpcBidiStream(rpcCtx, gco.method)
	if err != nil {
		return err
	}

	for i, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		request := dynamic.NewMessage(gco.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			batchErrFailed(i, err)
			continue
		}

		err = bidi.SendMsg(request)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	if err := bidi.CloseSend(); err != nil {
		return err
	}
	return nil
}

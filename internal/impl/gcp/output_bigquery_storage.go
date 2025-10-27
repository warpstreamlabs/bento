package gcp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/warpstreamlabs/bento/public/service"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func gcpBigQueryWriteAPIConfig() *service.ConfigSpec {

	return service.NewConfigSpec().
		Stable().
		Categories("GCP", "Services").
		Version("1.3.0").
		Summary(`Sends messages as new rows to a Google Cloud BigQuery table using the BigQuery Storage Write API.`).
		Description(`
You can use the Storage Write API to stream records into BigQuery in real time or to batch process an arbitrarily large number of records and commit them in a single atomic operation.
:::caution BigQuery API Limitation
The [AppendRows](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest) request is limited to 10 MB.

If you experience issues with this limitation, tweak the component's batch policy using the ` + "`batching`" + ` field. You can read more at [Message Batching](https://warpstreamlabs.github.io/bento/docs/configuration/batching/#performance).
:::
`).
		Field(service.NewStringField("project").Description("The project ID of the dataset to insert data to. If not set, it will be inferred from the credentials or read from the GOOGLE_CLOUD_PROJECT environment variable.").Default("")).
		Field(service.NewStringField("dataset").Description("The BigQuery Dataset ID.")).
		Field(service.NewInterpolatedStringField("table").Description(`:::caution Interpolation of Message Batches
It is assumed that the first message in the batch will resolve the bloblang query and that string will be used for all messages in the batch.
:::
The table to insert messages to.`)).
		Field(service.NewObjectField("endpoint",
			service.NewURLField("http").Description("The endpoint used to create the BigQuery client.").Default(""),
			service.NewURLField("grpc").Description("The endpoint used to create the BigQuery Storage API client.").Default(""),
		).Description("Used to overwrite the default gRPC and HTTP BigQuery endpoints.").Optional().Advanced()).
		Field(service.NewStringAnnotatedEnumField("stream_type", map[string]string{
			string(managedwriter.DefaultStream): "DefaultStream most closely mimics the legacy bigquery tabledata.insertAll semantics. Successful inserts are committed immediately, and there's no tracking offsets as all writes go into a `default` stream that always exists for a table.",
			// TODO: Add support for these types following expected flows in https://cloud.google.com/bigquery/docs/write-api#overview
			// string(managedwriter.CommittedStream): "CommittedStream appends data immediately, but creates a discrete stream for the work so that offset tracking can be used to track writes.",
			// string(managedwriter.BufferedStream):  "BufferedStream is a form of checkpointed stream, that allows you to advance the offset of visible rows via Flush operations.",
			// string(managedwriter.PendingStream):   "PendingStream is a stream in which no data is made visible to readers until the stream is finalized and committed explicitly.",
		}).Description(`:::caution Storage API Stream Types
Only ` + "`DEFAULT`" + ` stream types are currently enabled. Future versions will see support extended to ` + "`COMMITTED`, `BUFFERED`, and `PENDING`." + `
:::
sets the type of stream this write client is managing.`).Default(string(managedwriter.DefaultStream)).Advanced()).
		Field(service.NewStringAnnotatedEnumField("message_format", map[string]string{
			"json":     "Messages are in JSON format (default)",
			"protobuf": "Messages are in protobuf format",
		}).Description("Format of incoming messages").Default("json")).
		Field(service.NewBatchPolicyField("batching").Advanced().LintRule(`root = if this.byte_size >= 1000000 { "the amount of bytes in a batch cannot exceed 10 MB" }`)).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of message batches to have in flight at a given time. Increase this to improve throughput.").
			Default(64).Advanced())
}

func bigQueryStorageWriterConfigFromParsed(pConf *service.ParsedConfig) (conf bigQueryStorageWriterConfig, err error) {
	if conf.projectID, err = pConf.FieldString("project"); err != nil {
		return
	}

	if conf.projectID == "" {
		conf.projectID = bigquery.DetectProjectID
	}

	if conf.datasetID, err = pConf.FieldString("dataset"); err != nil {
		return
	}
	if conf.tableID, err = pConf.FieldInterpolatedString("table"); err != nil {
		return
	}

	if tableID, isStatic := conf.tableID.Static(); isStatic {
		// Always return the same string value if static
		conf.getTableID = func(_ *service.Message) (string, error) {
			return tableID, nil
		}
	} else {
		// Interpolate the tableID using the built-in TryString function if dynamic
		conf.getTableID = conf.tableID.TryString
	}

	if conf.httpEndpoint, err = pConf.FieldString("endpoint", "http"); err != nil {
		return
	}

	if conf.grpcEndpoint, err = pConf.FieldString("endpoint", "grpc"); err != nil {
		return
	}

	streamType, err := pConf.FieldString("stream_type")
	if err != nil {
		return
	}

	switch streamType {
	case string(managedwriter.DefaultStream):
		conf.streamType = managedwriter.DefaultStream
	default:
		err = fmt.Errorf("unknown stream type: %s", streamType)
		return
	}

	if conf.messageFormat, err = pConf.FieldString("message_format"); err != nil {
		return
	}

	return
}

func newBigQueryStorageOutput(
	conf bigQueryStorageWriterConfig,
	log *service.Logger,
) (*bigQueryStorageWriter, error) {
	g := &bigQueryStorageWriter{
		conf: conf,
		log:  log,
	}
	return g, nil
}

func init() {
	err := service.RegisterBatchOutput(
		"gcp_bigquery_write_api", gcpBigQueryWriteAPIConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (output service.BatchOutput, batchPol service.BatchPolicy, maxInFlight int, err error) {
			if batchPol, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			log := mgr.Logger()
			var gconf bigQueryStorageWriterConfig
			if gconf, err = bigQueryStorageWriterConfigFromParsed(conf); err != nil {
				return
			}
			output, err = newBigQueryStorageOutput(gconf, log)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type streamWithDescriptor struct {
	stream            *managedwriter.ManagedStream
	messageDescriptor protoreflect.MessageDescriptor
}

type bigQueryStorageWriter struct {
	conf bigQueryStorageWriterConfig
	log  *service.Logger

	client        *bigquery.Client
	storageClient *managedwriter.Client

	streamCacheLock sync.Mutex
	streams         map[string]*streamWithDescriptor
}

type bigQueryStorageWriterConfig struct {
	projectID string
	datasetID string
	tableID   *service.InterpolatedString

	// getTableID interpolates a tableID string if dynamic, else it just returns a static tableID string
	getTableID func(*service.Message) (string, error)

	httpEndpoint string
	grpcEndpoint string

	streamType managedwriter.StreamType

	messageFormat string

	// Not implemented: tableSchema holds an explicitly defined BigQuery table schema.
	tableSchema bigquery.Schema
}

func (bq *bigQueryStorageWriter) Connect(ctx context.Context) error {

	var bqClientOpts []option.ClientOption
	if bq.conf.httpEndpoint != "" {
		bqClientOpts = append(bqClientOpts, option.WithoutAuthentication(), option.WithEndpoint(bq.conf.httpEndpoint))
	}

	client, err := bigquery.NewClient(ctx, bq.conf.projectID, bqClientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	var storageClientOpts []option.ClientOption
	if bq.conf.grpcEndpoint != "" {
		conn, err := grpc.NewClient(bq.conf.grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}

		storageClientOpts = append(storageClientOpts, option.WithoutAuthentication(), option.WithEndpoint(bq.conf.grpcEndpoint), option.WithGRPCConn(conn))
	}

	storageClient, err := managedwriter.NewClient(ctx, bq.conf.projectID, storageClientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	bq.client = client
	bq.storageClient = storageClient
	bq.streams = make(map[string]*streamWithDescriptor)

	return nil
}

func (bq *bigQueryStorageWriter) Close(_ context.Context) error {
	var err error
	if bq.client != nil {
		err = errors.Join(err, bq.client.Close())

	}

	if bq.storageClient != nil {
		err = errors.Join(err, bq.client.Close())
	}

	return err
}

func (bq *bigQueryStorageWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if bq.client == nil || bq.storageClient == nil {
		return service.ErrNotConnected
	}

	tableID, err := bq.conf.tableID.TryString(batch[0])
	if err != nil {
		return err
	}

	streamDescriptorPair, err := bq.getManagedStreamForTable(ctx, tableID)
	if err != nil {
		return err
	}

	stream := streamDescriptorPair.stream
	messageDescriptor := streamDescriptorPair.messageDescriptor

	var result *managedwriter.AppendResult
	rowData := make([][]byte, len(batch))
	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		var protoBytes []byte
		switch bq.conf.messageFormat {
		case "json":
			protoMessage := dynamicpb.NewMessage(messageDescriptor)
			if err := protojson.Unmarshal(msgBytes, protoMessage); err != nil {
				return fmt.Errorf("failed to Unmarshal message for item %d: err: %w, sampleEvent: %s", i, err, string(msgBytes))
			}

			// Marshal to proto bytes for BigQuery
			protoBytes, err = proto.Marshal(protoMessage)
			if err != nil {
				return fmt.Errorf("failed to marshal proto bytes for item %d: %w", i, err)
			}
		case "protobuf":
			protoBytes = msgBytes
		}

		rowData[i] = protoBytes
	}

	if result, err = stream.AppendRows(ctx, rowData); err != nil {
		return fmt.Errorf("single-row append failed: %w", err)
	}

	// Wait for the result to indicate ready, then validate.
	if _, err := result.GetResult(ctx); err != nil {
		fullResponse, _ := result.FullResponse(ctx)
		if fullResponse != nil && len(fullResponse.RowErrors) > 0 {
			err = fmt.Errorf("original error: %w, sample row error: (code=%v): %v", err, fullResponse.RowErrors[0].Code, fullResponse.RowErrors[0].Message)
		}

		return fmt.Errorf("result error for last send: %w", err)
	}

	return nil
}

//------------------------------------------------------------------------------

func (bq *bigQueryStorageWriter) getManagedStreamForTable(ctx context.Context, tableID string) (*streamWithDescriptor, error) {
	destTable := managedwriter.TableParentFromParts(bq.conf.projectID, bq.conf.datasetID, tableID)
	defStreamName := destTable + "/streams/_default"

	bq.streamCacheLock.Lock()
	defer bq.streamCacheLock.Unlock()
	if stream, ok := bq.streams[destTable]; ok {
		return stream, nil
	}

	resp, err := bq.storageClient.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{
		Name: defStreamName,
		View: storagepb.WriteStreamView_FULL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get write stream: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(resp.GetTableSchema(), "root")
	if err != nil {
		return nil, fmt.Errorf("failed to adapt storage schema proto descriptor: %w", err)
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("could not create valid MessageDescriptor for proto: %w", err)
	}

	descriptorProto, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize MessageDescriptor proto: %w", err)
	}

	stream, err := bq.storageClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(destTable),
		managedwriter.WithType(bq.conf.streamType),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new ManagedStream type [%s] for table [%s]: %w", destTable, string(bq.conf.streamType), err)
	}

	managedStreamPair := &streamWithDescriptor{
		stream:            stream,
		messageDescriptor: messageDescriptor,
	}

	bq.streams[destTable] = managedStreamPair

	messageDescriptorString := prototext.Format(protodesc.ToFileDescriptorProto(messageDescriptor.ParentFile()))
	bq.log.Infof("loaded new bigquery schema for table: %s, schema: %s", destTable, messageDescriptorString)

	return managedStreamPair, nil
}

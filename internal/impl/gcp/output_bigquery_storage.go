package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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
		Field(service.NewBoolField("auto_add_missing_columns").
			Description("Automatically add missing columns to the BigQuery table when a schema mismatch error is detected. When enabled, the component will detect missing fields, update the table schema, and retry the write operation. **Note:** This feature only works when `message_format` is set to `json`.").
			Default(false).
			Advanced()).
		Field(service.NewIntField("max_schema_update_retries").
			Description("Maximum number of times to retry a write operation after updating the table schema. Prevents infinite loops if schema updates fail repeatedly.").
			Default(3).
			Advanced()).
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
		conf.getTableID = func(_ *service.Message) (string, error) {
			return tableID, nil
		}
	} else {
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

	if conf.autoAddMissingColumns, err = pConf.FieldBool("auto_add_missing_columns"); err != nil {
		return
	}

	if conf.maxSchemaUpdateRetries, err = pConf.FieldInt("max_schema_update_retries"); err != nil {
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

	tableUpdateLock sync.Mutex
	tablesUpdating  map[string]*sync.Mutex
}

type bigQueryStorageWriterConfig struct {
	projectID string
	datasetID string
	tableID   *service.InterpolatedString

	getTableID func(*service.Message) (string, error)

	httpEndpoint string
	grpcEndpoint string

	streamType managedwriter.StreamType

	messageFormat string

	autoAddMissingColumns  bool
	maxSchemaUpdateRetries int
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
	bq.tablesUpdating = make(map[string]*sync.Mutex)

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

	destTable := managedwriter.TableParentFromParts(bq.conf.projectID, bq.conf.datasetID, tableID)

	bq.tableUpdateLock.Lock()
	tableLock, updating := bq.tablesUpdating[destTable]
	bq.tableUpdateLock.Unlock()

	if updating {
		tableLock.Lock()
		//nolint:staticcheck // SA2001: Empty critical section is intentional - waiting for schema update to complete
		tableLock.Unlock()
	}

	maxSchemaUpdateAttempts := 1
	if bq.conf.autoAddMissingColumns && bq.conf.messageFormat == "json" {
		maxSchemaUpdateAttempts = bq.conf.maxSchemaUpdateRetries
	}

	var lastErr error
	for attempt := 0; attempt < maxSchemaUpdateAttempts; attempt++ {
		err := bq.writeBatchAttempt(ctx, tableID, batch)
		if err == nil {
			return nil
		}

		lastErr = err

		if bq.conf.autoAddMissingColumns && bq.conf.messageFormat == "json" && isSchemaError(err) {
			bq.log.Warnf("Schema mismatch detected on attempt %d/%d: %v", attempt+1, maxSchemaUpdateAttempts, err)

			if updateErr := bq.handleSchemaEvolution(ctx, tableID, batch, err); updateErr != nil {
				bq.log.Errorf("Failed to update schema: %v", updateErr)
				return fmt.Errorf("schema update failed: %w (original error: %v)", updateErr, err)
			}

			bq.log.Infof("Schema updated successfully, retrying write (attempt %d/%d)", attempt+2, maxSchemaUpdateAttempts)

			destTable := managedwriter.TableParentFromParts(bq.conf.projectID, bq.conf.datasetID, tableID)
			bq.streamCacheLock.Lock()
			if stream, ok := bq.streams[destTable]; ok {
				if stream.stream != nil {
					_ = stream.stream.Close()
				}
				delete(bq.streams, destTable)
			}
			bq.streamCacheLock.Unlock()

			continue
		}

		return err
	}

	return fmt.Errorf("max schema update retries (%d) exceeded: %w", maxSchemaUpdateAttempts, lastErr)
}

func (bq *bigQueryStorageWriter) writeBatchAttempt(ctx context.Context, tableID string, batch service.MessageBatch) error {
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
			convertedBytes, err := bq.convertTimestampsToBigQueryFormat(msgBytes)
			if err != nil {
				return fmt.Errorf("failed to convert timestamps for item %d: %w", i, err)
			}

			sanitizedBytes, err := bq.sanitizeJSONFieldNames(convertedBytes)
			if err != nil {
				return fmt.Errorf("failed to sanitize field names for item %d: %w", i, err)
			}

			normalizedBytes, err := bq.normalizeNumbersToStrings(sanitizedBytes, messageDescriptor)
			if err != nil {
				return fmt.Errorf("failed to normalize numbers to strings for item %d: %w", i, err)
			}

			protoMessage := dynamicpb.NewMessage(messageDescriptor)
			if err := protojson.Unmarshal(normalizedBytes, protoMessage); err != nil {
				return fmt.Errorf("failed to Unmarshal message for item %d: err: %w, sampleEvent: %s", i, err, string(normalizedBytes))
			}

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
		if isStreamClosedError(err) {
			destTable := managedwriter.TableParentFromParts(bq.conf.projectID, bq.conf.datasetID, tableID)
			bq.streamCacheLock.Lock()
			if cachedStream, ok := bq.streams[destTable]; ok {
				if cachedStream.stream != nil {
					_ = cachedStream.stream.Close()
				}
				delete(bq.streams, destTable)
			}
			bq.streamCacheLock.Unlock()
		}
		return fmt.Errorf("single-row append failed: %w", err)
	}

	if _, err := result.GetResult(ctx); err != nil {
		fullResponse, _ := result.FullResponse(ctx)
		if fullResponse != nil && len(fullResponse.RowErrors) > 0 {
			err = fmt.Errorf("original error: %w, sample row error: (code=%v): %v", err, fullResponse.RowErrors[0].Code, fullResponse.RowErrors[0].Message)
		}

		if isStreamClosedError(err) {
			destTable := managedwriter.TableParentFromParts(bq.conf.projectID, bq.conf.datasetID, tableID)
			bq.streamCacheLock.Lock()
			if cachedStream, ok := bq.streams[destTable]; ok {
				if cachedStream.stream != nil {
					_ = cachedStream.stream.Close()
				}
				delete(bq.streams, destTable)
			}
			bq.streamCacheLock.Unlock()
		}

		return fmt.Errorf("result error for last send: %w", err)
	}

	return nil
}

func (bq *bigQueryStorageWriter) convertTimestampsToBigQueryFormat(jsonBytes []byte) ([]byte, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	if err := bq.convertTimestampsInMapToBigQueryFormat(data); err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

func (bq *bigQueryStorageWriter) convertTimestampsInMapToBigQueryFormat(data map[string]interface{}) error {
	for key, value := range data {
		switch v := value.(type) {
		case string:
			if isTimestampField(key, v) {
				bqTimestamp, err := convertToBigQueryTimestampFormat(v)
				if err != nil {
					return fmt.Errorf("failed to convert timestamp field %s: %w", key, err)
				}
				data[key] = bqTimestamp
			}
		case map[string]interface{}:
			if err := bq.convertTimestampsInMapToBigQueryFormat(v); err != nil {
				return err
			}
		case []interface{}:
			for i, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					if err := bq.convertTimestampsInMapToBigQueryFormat(itemMap); err != nil {
						return err
					}
				} else if itemStr, ok := item.(string); ok {
					if isTimestampField(key, itemStr) {
						bqTimestamp, err := convertToBigQueryTimestampFormat(itemStr)
						if err != nil {
							return fmt.Errorf("failed to convert timestamp in array field %s[%d]: %w", key, i, err)
						}
						v[i] = bqTimestamp
					}
				}
			}
		}
	}
	return nil
}

func convertToBigQueryTimestampFormat(timestampStr string) (int64, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05.999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.999999999Z",
		"2006-01-02T15:04:05.999999Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}

	var t time.Time
	var err error
	for _, format := range formats {
		t, err = time.Parse(format, timestampStr)
		if err == nil {
			return t.UnixMicro(), nil
		}
	}

	return 0, fmt.Errorf("unable to parse timestamp '%s' with any known format: %w", timestampStr, err)
}

func (bq *bigQueryStorageWriter) getManagedStreamForTable(ctx context.Context, tableID string) (*streamWithDescriptor, error) {
	destTable := managedwriter.TableParentFromParts(bq.conf.projectID, bq.conf.datasetID, tableID)
	defStreamName := destTable + "/streams/_default"

	bq.streamCacheLock.Lock()
	if stream, ok := bq.streams[destTable]; ok {
		bq.streamCacheLock.Unlock()
		return stream, nil
	}
	bq.streamCacheLock.Unlock()

	maxRetries := 5
	var resp *storagepb.WriteStream
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			bq.log.Infof("Retrying GetWriteStream for table %s (attempt %d/%d) after %v", tableID, attempt+1, maxRetries, backoff)
			time.Sleep(backoff)
		}

		resp, err = bq.storageClient.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{
			Name: defStreamName,
			View: storagepb.WriteStreamView_FULL,
		})

		if err == nil {
			break // Success!
		}

		if st, ok := status.FromError(err); ok && st.Code() == 5 { // Code 5 = NotFound
			if attempt < maxRetries-1 {
				bq.log.Warnf("Table/stream not found (propagation delay), will retry: %v", err)
				continue
			}
		}

		return nil, fmt.Errorf("failed to get write stream: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get write stream after %d retries: %w", maxRetries, err)
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

	bq.streamCacheLock.Lock()
	bq.streams[destTable] = managedStreamPair
	bq.streamCacheLock.Unlock()

	messageDescriptorString := prototext.Format(protodesc.ToFileDescriptorProto(messageDescriptor.ParentFile()))
	bq.log.Infof("loaded new bigquery schema for table: %s, schema: %s", destTable, messageDescriptorString)

	return managedStreamPair, nil
}

func isSchemaError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	schemaErrorPatterns := []string{
		"no matching field found",
		"schema mismatch",
		"field not found",
		"unknown field",
		"cannot find field",
		"failed to unmarshal",
	}

	lowerErr := strings.ToLower(errMsg)
	for _, pattern := range schemaErrorPatterns {
		if strings.Contains(lowerErr, pattern) {
			return true
		}
	}

	if st, ok := status.FromError(err); ok {
		return st.Code() == 3
	}

	return false
}

func isStreamClosedError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	streamClosedPatterns := []string{
		"eof",
		"connection reset",
		"broken pipe",
		"stream closed",
		"connection closed",
		"transport is closing",
	}

	for _, pattern := range streamClosedPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

func (bq *bigQueryStorageWriter) handleSchemaEvolution(ctx context.Context, tableID string, batch service.MessageBatch, originalErr error) error {
	if bq.conf.messageFormat != "json" {
		return fmt.Errorf("schema evolution is only supported for JSON format, current format is %s: %w", bq.conf.messageFormat, originalErr)
	}

	destTable := managedwriter.TableParentFromParts(bq.conf.projectID, bq.conf.datasetID, tableID)

	bq.tableUpdateLock.Lock()
	tableLock, exists := bq.tablesUpdating[destTable]
	if !exists {
		tableLock = &sync.Mutex{}
		bq.tablesUpdating[destTable] = tableLock
	}
	bq.tableUpdateLock.Unlock()

	tableLock.Lock()
	defer tableLock.Unlock()

	bq.streamCacheLock.Lock()
	if stream, ok := bq.streams[destTable]; ok {
		if stream.stream != nil {
			_ = stream.stream.Close()
		}
		delete(bq.streams, destTable)
	}
	bq.streamCacheLock.Unlock()

	table := bq.client.Dataset(bq.conf.datasetID).Table(tableID)

	if len(batch) == 0 {
		return errors.New("empty batch, cannot infer schema")
	}

	allFieldsMap := make(map[string]interface{})
	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("failed to get message bytes from batch item %d: %w", i, err)
		}

		var msgData map[string]interface{}
		if err := json.Unmarshal(msgBytes, &msgData); err != nil {
			bq.log.Warnf("Failed to unmarshal message %d for schema inference, skipping: %v", i, err)
			continue
		}

		sanitizedMsgData := bq.sanitizeMapFieldNames(msgData)
		bq.mergeFields(allFieldsMap, sanitizedMsgData)
	}

	if len(allFieldsMap) == 0 {
		return errors.New("no valid messages in batch for schema inference")
	}

	maxETagRetries := 3
	for etagRetry := 0; etagRetry < maxETagRetries; etagRetry++ {
		if etagRetry > 0 {
			bq.log.Infof("Retrying schema update (ETag retry %d/%d)", etagRetry+1, maxETagRetries)
			time.Sleep(time.Duration(etagRetry) * 500 * time.Millisecond)
		}

		metadata, err := table.Metadata(ctx)
		if err != nil {
			return fmt.Errorf("failed to get table metadata: %w", err)
		}

		currentSchema := metadata.Schema

		existingFieldNames := make([]string, len(currentSchema))
		for i, field := range currentSchema {
			existingFieldNames[i] = field.Name
		}

		missingFields := bq.findMissingFields(currentSchema, allFieldsMap)
		if len(missingFields) == 0 {
			if etagRetry == 0 {
				analyzedFields := make([]string, 0, len(allFieldsMap))
				for k := range allFieldsMap {
					analyzedFields = append(analyzedFields, k)
				}
				existingFields := make([]string, 0, len(currentSchema))
				for _, f := range currentSchema {
					existingFields = append(existingFields, f.Name)
				}

				bq.log.Warnf("=== SCHEMA MISMATCH DEBUG ===")
				bq.log.Warnf("Analyzed %d fields from batch (lowercase): %v", len(analyzedFields), analyzedFields)
				bq.log.Warnf("Existing %d fields in schema (original casing): %v", len(existingFields), existingFields)

				existingLowerMap := make(map[string]bool)
				for _, f := range currentSchema {
					existingLowerMap[strings.ToLower(f.Name)] = true
				}

				actuallyMissing := make([]string, 0)
				for _, analyzedField := range analyzedFields {
					if !existingLowerMap[strings.ToLower(analyzedField)] {
						actuallyMissing = append(actuallyMissing, analyzedField)
					}
				}

				if len(actuallyMissing) > 0 {
					bq.log.Errorf("BUG DETECTED: Found %d actually missing fields that weren't detected: %v", len(actuallyMissing), actuallyMissing)
				}

				bq.log.Warnf("Original error: %v", originalErr)
				return fmt.Errorf("no missing fields detected, but schema error occurred: %w", originalErr)
			}
			bq.log.Infof("No missing fields found on retry %d, schema may have been updated by another process", etagRetry+1)
			break
		}

		bq.log.Infof("Detected %d field update(s) needed", len(missingFields))

		newSchema := bq.mergeSchemas(currentSchema, missingFields)

		bq.dumpSchema(newSchema, "  ")

		update := bigquery.TableMetadataToUpdate{
			Schema: newSchema,
		}

		if _, err := table.Update(ctx, update, metadata.ETag); err != nil {
			if strings.Contains(err.Error(), "Precondition check failed") || strings.Contains(err.Error(), "412") {
				if etagRetry < maxETagRetries-1 {
					bq.log.Warnf("Precondition failed (ETag mismatch), retrying with fresh metadata...")
					continue
				}
			}
			return fmt.Errorf("failed to update table schema: %w", err)
		}

		bq.log.Infof("Successfully added %d field(s) to table %s", len(missingFields), tableID)
		break
	}

	bq.log.Infof("Waiting for BigQuery schema propagation...")
	defStreamName := destTable + "/streams/_default"

	maxWaitTime := 10 * time.Second
	pollInterval := 500 * time.Millisecond
	deadline := time.Now().Add(maxWaitTime)

	for time.Now().Before(deadline) {
		resp, err := bq.storageClient.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{
			Name: defStreamName,
			View: storagepb.WriteStreamView_FULL,
		})
		if err != nil {
			bq.log.Warnf("Failed to verify schema propagation: %v", err)
			break
		}

		descriptor, err := adapt.StorageSchemaToProto2Descriptor(resp.GetTableSchema(), "root")
		if err == nil {
			if messageDesc, ok := descriptor.(protoreflect.MessageDescriptor); ok {
				metadata, err := bq.client.Dataset(bq.conf.datasetID).Table(tableID).Metadata(ctx)
				if err == nil {
					missingFields := bq.findMissingFields(metadata.Schema, allFieldsMap)
					if len(missingFields) == 0 {
						allFieldsPresent := true
						for fieldName := range allFieldsMap {
							if messageDesc.Fields().ByName(protoreflect.Name(fieldName)) == nil {
								allFieldsPresent = false
								break
							}
						}

						if allFieldsPresent {
							bq.log.Infof("Schema propagation confirmed - all new fields present")
							bq.streamCacheLock.Lock()
							if stream, ok := bq.streams[destTable]; ok {
								if stream.stream != nil {
									_ = stream.stream.Close()
								}
								delete(bq.streams, destTable)
							}
							bq.streamCacheLock.Unlock()
							return nil
						}
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}

	bq.log.Warnf("Schema propagation verification timed out, proceeding anyway")
	bq.streamCacheLock.Lock()
	if stream, ok := bq.streams[destTable]; ok {
		if stream.stream != nil {
			_ = stream.stream.Close()
		}
		delete(bq.streams, destTable)
	}
	bq.streamCacheLock.Unlock()
	return nil
}

func (bq *bigQueryStorageWriter) findMissingFields(currentSchema bigquery.Schema, msgData map[string]interface{}) []*bigquery.FieldSchema {
	var missingFields []*bigquery.FieldSchema

	existingFieldsMap := make(map[string]*bigquery.FieldSchema)
	for _, field := range currentSchema {
		normalizedName := strings.ToLower(field.Name)
		existingFieldsMap[normalizedName] = field
	}

	for fieldName, value := range msgData {
		sanitizedName := sanitizeFieldName(fieldName)

		existingField, fieldExists := existingFieldsMap[sanitizedName]

		if !fieldExists {
			fieldSchema := inferBigQueryFieldSchema(sanitizedName, value)
			if fieldSchema != nil {
				missingFields = append(missingFields, fieldSchema)
			}
		} else if existingField.Type == bigquery.RecordFieldType && existingField.Schema != nil {
			var nestedData map[string]interface{}

			if existingField.Repeated {
				if valueArray, ok := value.([]interface{}); ok && len(valueArray) > 0 {
					nestedData, _ = valueArray[0].(map[string]interface{})
				}
			} else {
				nestedData, _ = value.(map[string]interface{})
			}

			if nestedData != nil {
				nestedMissing := bq.findMissingFields(existingField.Schema, nestedData)
				if len(nestedMissing) > 0 {
					mergedNestedSchema := bq.mergeSchemas(existingField.Schema, nestedMissing)

					updatedField := &bigquery.FieldSchema{
						Name:        sanitizedName,
						Type:        existingField.Type,
						Repeated:    existingField.Repeated,
						Required:    existingField.Required,
						Description: existingField.Description,
						Schema:      mergedNestedSchema,
					}
					missingFields = append(missingFields, updatedField)
				}
			}
		}
	}

	return missingFields
}

func (bq *bigQueryStorageWriter) mergeSchemas(existingSchema bigquery.Schema, newFields []*bigquery.FieldSchema) bigquery.Schema {
	existingFieldsMap := make(map[string]*bigquery.FieldSchema)
	for i, field := range existingSchema {
		normalizedName := strings.ToLower(field.Name)
		existingFieldsMap[normalizedName] = existingSchema[i]
	}

	result := make(bigquery.Schema, 0, len(existingSchema))
	for _, field := range existingSchema {
		normalizedField := &bigquery.FieldSchema{
			Name:        strings.ToLower(field.Name), // Normalize to lowercase
			Type:        field.Type,
			Repeated:    field.Repeated,
			Required:    field.Required,
			Description: field.Description,
			Schema:      field.Schema,
		}
		result = append(result, normalizedField)
	}

	processedNewFields := make(map[string]bool)

	for i, existingField := range result {
		normalizedExistingName := strings.ToLower(existingField.Name)
		for _, newField := range newFields {
			normalizedNewName := strings.ToLower(newField.Name)

			if normalizedNewName == normalizedExistingName {
				if newField.Type == bigquery.RecordFieldType && existingField.Type == bigquery.RecordFieldType {
					result[i] = &bigquery.FieldSchema{
						Name:        normalizedNewName, // Use normalized lowercase name
						Type:        existingField.Type,
						Repeated:    existingField.Repeated,
						Required:    existingField.Required,
						Description: existingField.Description,
						Schema:      newField.Schema, // newField.Schema already contains merged nested fields
					}
					processedNewFields[normalizedNewName] = true
					bq.log.Debugf("Merged nested fields into existing RECORD field '%s'", normalizedNewName)
					break
				}
			}
		}
	}

	for _, newField := range newFields {
		normalizedNewName := strings.ToLower(newField.Name)
		if !processedNewFields[normalizedNewName] {
			if _, exists := existingFieldsMap[normalizedNewName]; !exists {
				normalizedNewField := &bigquery.FieldSchema{
					Name:        normalizedNewName,
					Type:        newField.Type,
					Repeated:    newField.Repeated,
					Required:    newField.Required,
					Description: newField.Description,
					Schema:      newField.Schema,
				}
				result = append(result, normalizedNewField)
				bq.log.Debugf("Added new field '%s' (Type=%s, Repeated=%v)", normalizedNewName, newField.Type, newField.Repeated)
			} else {
				bq.log.Debugf("Skipping field '%s' - already exists in schema", normalizedNewName)
			}
		}
	}

	bq.log.Debugf("mergeSchemas result: %d fields", len(result))
	for _, f := range result {
		bq.log.Debugf("  result: %s (type=%s, repeated=%v)", f.Name, f.Type, f.Repeated)
	}

	return result
}

func (bq *bigQueryStorageWriter) dumpSchema(schema bigquery.Schema, indent string) {
	for _, field := range schema {
		if field.Type == bigquery.RecordFieldType && field.Schema != nil {
			bq.log.Debugf("%s%s (RECORD, repeated=%v) {", indent, field.Name, field.Repeated)
			bq.dumpSchema(field.Schema, indent+"  ")
			bq.log.Debugf("%s}", indent)
		} else {
			bq.log.Debugf("%s%s (type=%s, repeated=%v)", indent, field.Name, field.Type, field.Repeated)
		}
	}
}

func (bq *bigQueryStorageWriter) mergeFields(target, source map[string]interface{}) {
	targetKeysLower := make(map[string]string) // lowercase -> original key
	for key := range target {
		targetKeysLower[strings.ToLower(key)] = key
	}

	for key, value := range source {
		sanitizedKey := sanitizeFieldName(key)
		sanitizedKeyLower := strings.ToLower(sanitizedKey)

		existingKey, exists := targetKeysLower[sanitizedKeyLower]
		if exists {
			existingValue := target[existingKey]

			if existingMap, ok := existingValue.(map[string]interface{}); ok {
				if sourceMap, ok := value.(map[string]interface{}); ok {
					bq.mergeFields(existingMap, sourceMap)
					continue
				}
			}
			if existingArray, ok := existingValue.([]interface{}); ok {
				if sourceArray, ok := value.([]interface{}); ok {
					if len(existingArray) > 0 && len(sourceArray) > 0 {
						if existingItemMap, ok := existingArray[0].(map[string]interface{}); ok {
							if sourceItemMap, ok := sourceArray[0].(map[string]interface{}); ok {
								bq.mergeFields(existingItemMap, sourceItemMap)
								continue
							}
						}
					}
				}
				continue
			}
			bq.log.Debugf("Skipping duplicate field '%s' (already exists as '%s')", sanitizedKey, existingKey)
		} else {
			target[sanitizedKey] = value
			targetKeysLower[sanitizedKeyLower] = sanitizedKey
		}
	}
}

func sanitizeFieldName(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "-", "_"))
}

func (bq *bigQueryStorageWriter) sanitizeJSONFieldNames(jsonBytes []byte) ([]byte, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	sanitizedData := bq.sanitizeMapFieldNames(data)

	return json.Marshal(sanitizedData)
}

func (bq *bigQueryStorageWriter) sanitizeMapFieldNames(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range data {
		sanitizedKey := sanitizeFieldName(key)

		switch v := value.(type) {
		case map[string]interface{}:
			if len(v) == 0 {
				continue
			}
			sanitizedNested := bq.sanitizeMapFieldNames(v)
			if len(sanitizedNested) > 0 {
				result[sanitizedKey] = sanitizedNested
			}
		case []interface{}:
			sanitizedArray := make([]interface{}, 0, len(v))
			for _, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					if len(itemMap) == 0 {
						continue
					}
					sanitizedItem := bq.sanitizeMapFieldNames(itemMap)
					if len(sanitizedItem) > 0 {
						sanitizedArray = append(sanitizedArray, sanitizedItem)
					}
				} else {
					sanitizedArray = append(sanitizedArray, item)
				}
			}
			if len(sanitizedArray) > 0 {
				result[sanitizedKey] = sanitizedArray
			}
		default:
			result[sanitizedKey] = value
		}
	}

	return result
}

func (bq *bigQueryStorageWriter) normalizeNumbersToStrings(jsonBytes []byte, messageDescriptor protoreflect.MessageDescriptor) ([]byte, error) {
	decoder := json.NewDecoder(strings.NewReader(string(jsonBytes)))
	decoder.UseNumber()

	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}

	normalizedData := bq.convertNumbersToStringsInMap(data, messageDescriptor)

	return json.Marshal(normalizedData)
}

func (bq *bigQueryStorageWriter) convertNumbersToStringsInMap(data map[string]interface{}, descriptor protoreflect.MessageDescriptor) map[string]interface{} {
	if descriptor == nil {
		return data
	}

	for key, value := range data {
		fieldDesc := descriptor.Fields().ByName(protoreflect.Name(key))
		if fieldDesc == nil {
			continue // Field not in schema
		}

		switch v := value.(type) {
		case json.Number:
			if fieldDesc.Kind() == protoreflect.StringKind {
				data[key] = v.String()
			}
		case map[string]interface{}:
			if fieldDesc.Kind() == protoreflect.MessageKind && !fieldDesc.IsMap() {
				data[key] = bq.convertNumbersToStringsInMap(v, fieldDesc.Message())
			}
		case []interface{}:
			if fieldDesc.IsList() && fieldDesc.Kind() == protoreflect.MessageKind {
				nestedDesc := fieldDesc.Message()
				for i, item := range v {
					if itemMap, ok := item.(map[string]interface{}); ok {
						v[i] = bq.convertNumbersToStringsInMap(itemMap, nestedDesc)
					}
				}
			}
		}
	}

	return data
}

func inferBigQueryFieldSchema(name string, value interface{}) *bigquery.FieldSchema {
	if value == nil {
		return &bigquery.FieldSchema{
			Name:     name,
			Type:     bigquery.StringFieldType,
			Required: false,
		}
	}

	switch v := value.(type) {
	case bool:
		return &bigquery.FieldSchema{
			Name:     name,
			Type:     bigquery.BooleanFieldType,
			Required: false,
		}
	case float64:
		return &bigquery.FieldSchema{
			Name:     name,
			Type:     bigquery.FloatFieldType,
			Required: false,
		}
	case string:
		fieldType := bigquery.StringFieldType
		if isTimestampField(name, v) {
			fieldType = bigquery.TimestampFieldType
		}
		return &bigquery.FieldSchema{
			Name:     name,
			Type:     fieldType,
			Required: false,
		}
	case map[string]interface{}:
		if len(v) == 0 {
			return nil
		}

		nestedSchema := make([]*bigquery.FieldSchema, 0)
		for nestedName, nestedValue := range v {
			sanitizedNestedName := sanitizeFieldName(nestedName)
			nestedField := inferBigQueryFieldSchema(sanitizedNestedName, nestedValue)
			if nestedField != nil {
				nestedSchema = append(nestedSchema, nestedField)
			}
		}

		if len(nestedSchema) == 0 {
			return nil
		}

		return &bigquery.FieldSchema{
			Name:     name,
			Type:     bigquery.RecordFieldType,
			Schema:   nestedSchema,
			Required: false,
		}
	case []interface{}:
		if len(v) > 0 {
			firstElemSchema := inferBigQueryFieldSchema(name, v[0])
			if firstElemSchema != nil {
				firstElemSchema.Repeated = true
				return firstElemSchema
			}
		}
		return &bigquery.FieldSchema{
			Name:     name,
			Type:     bigquery.StringFieldType,
			Repeated: true,
			Required: false,
		}
	default:
		return &bigquery.FieldSchema{
			Name:     name,
			Type:     bigquery.StringFieldType,
			Required: false,
		}
	}
}

func isTimestampField(name string, value string) bool {
	if len(value) < 19 {
		return false
	}

	if value[4] == '-' && value[7] == '-' && value[10] == 'T' &&
		value[13] == ':' && value[16] == ':' {
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05.999999999Z07:00",
			"2006-01-02T15:04:05.999999Z07:00",
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02T15:04:05.999999999Z",
			"2006-01-02T15:04:05.999999Z",
			"2006-01-02T15:04:05Z",
		}

		for _, format := range formats {
			if _, err := time.Parse(format, value); err == nil {
				return true
			}
		}
	}

	if value[4] == '-' && value[7] == '-' && value[10] == ' ' &&
		value[13] == ':' && value[16] == ':' {
		formats := []string{
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
		}

		for _, format := range formats {
			if _, err := time.Parse(format, value); err == nil {
				return true
			}
		}
	}

	return false
}

// Package gcp provides Google Cloud Platform integrations for Bento.
// This file implements a Cloud Spanner Change Data Capture (CDC) input component
// that consumes change events from Spanner change streams.
package gcp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/Jeffail/shutdown"
	types "github.com/warpstreamlabs/bento/internal/impl/gcp/types"
	"github.com/warpstreamlabs/bento/public/service"
	"golang.org/x/sync/errgroup"
)

// TODO(gregfurman): Implement caching and checkpointing mechanism

const (
	// Spanner CDC Input Fields
	cdcFieldSpannerDSN        = "spanner_dsn"
	cdcFieldStreamName        = "stream_name"
	cdcFieldStartTime         = "start_time"
	cdcFieldEndTime           = "end_time"
	cdcFieldHeartbeatInterval = "heartbeat_interval"

	metadataTimestamp   = "gcp_spanner_commit_timestamp"
	metadataModType     = "gcp_spanner_cdc_mod_type"
	metadataTableName   = "gcp_spanner_table_name"
	metadataServerTxnID = "gcp_spanner_cdc_server_transaction_id"
	metadataRecordSeq   = "gcp_spanner_cdc_record_sequence"
)

// cdcConfig holds the configuration for the Spanner CDC input component
type cdcConfig struct {
	SpannerDSN           string        // DSN for the source Spanner database
	SpannerMetadataDSN   string        // DSN for the metadata tracking database
	SpannerMetadataTable string        // Table name for storing partition metadata
	StreamName           string        // Name of the change stream to consume
	StartTime            *time.Time    // Optional start time for reading changes
	EndTime              *time.Time    // Optional end time for reading changes
	HeartbeatInterval    time.Duration // Interval between partition heartbeats
}

func cdcConfigFromParsed(pConf *service.ParsedConfig) (conf cdcConfig, err error) {
	if conf.SpannerDSN, err = pConf.FieldString(cdcFieldSpannerDSN); err != nil {
		return
	}

	if conf.StreamName, err = pConf.FieldString(cdcFieldStreamName); err != nil {
		return
	}

	toPtr := func(in time.Time) *time.Time {
		return &in
	}

	conf.StartTime = toPtr(time.Now())
	if pConf.Contains(cdcFieldStartTime) {
		var startTimeString string
		if startTimeString, err = pConf.FieldString(cdcFieldStartTime); err != nil {
			return
		}
		var startTime time.Time
		if startTime, err = time.Parse(time.RFC3339, startTimeString); err != nil {
			return
		}
		conf.StartTime = toPtr(startTime)
	}

	if pConf.Contains(cdcFieldEndTime) {
		var endTimeString string
		if endTimeString, err = pConf.FieldString(cdcFieldEndTime); err != nil {
			return
		}

		var endTime time.Time
		if endTime, err = time.Parse(time.RFC3339, endTimeString); err != nil {
			return
		}
		conf.EndTime = &endTime
	}

	if conf.HeartbeatInterval, err = pConf.FieldDuration(cdcFieldHeartbeatInterval); err != nil {
		return
	}

	return
}

func spannerCdcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("1.8.0").
		Categories("Services", "GCP").
		Summary(`Consumes Spanner Change Stream Events from a GCP Spanner instance.`).
		Description(`
For information on how to set up credentials check out [this guide](https://cloud.google.com/docs/authentication/production).

### Event Data Structure
The data structure of the events emitted by this input can be found here:
* [google](https://cloud.google.com/spanner/docs/change-streams/details#data-change-records)

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- gcp_spanner_commit_timestamp - The time the records were committed in spanner.
- gcp_spanner_cdc_mod_type - The type of modification that occurred (INSERT, UPDATE, DELETE).
- gcp_spanner_table_name - The name of the table that was modified.
- gcp_spanner_cdc_server_transaction_id - The server transaction ID of the change.
- gcp_spanner_cdc_record_sequence - The sequence number of the record in the change stream.
`+"```"+`

`).
		Fields(
			service.NewStringField(cdcFieldSpannerDSN).
				Description("The dsn for spanner from where to read the changestream.").
				Example("projects/{projectId}/instances/{instanceId}/databases/{databaseName}"),
			service.NewStringField(cdcFieldStreamName).
				Description("The name of the stream to track changes on."),
			service.NewDurationField(cdcFieldHeartbeatInterval).
				Description("An optional field to configure the heartbeat interval for partitions.").
				Default((time.Second * 3).String()).
				Optional(),
			service.NewStringField(cdcFieldStartTime).
				Description("An optional field to define the start point to read from the changestreams, timestamp format should conform to RFC3339, for details on valid start times please see [this document](https://cloud.google.com/spanner/docs/change-streams#data-retention)").
				Example(time.RFC3339).
				Optional(),
			service.NewStringField(cdcFieldEndTime).
				Description("An optional field to define the end time to read from the changestreams, timestamp format should conform to RFC3339").
				Example(time.RFC3339).
				Optional(),
		)
}

func init() {
	err := service.RegisterInput("gcp_spanner_cdc", spannerCdcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			pConf, err := cdcConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return newGcpSpannerCDCInput(pConf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

// gcpSpannerCDCInput implements a Bento input component that reads from Spanner change streams
type gcpSpannerCDCInput struct {
	conf         cdcConfig       // Configuration for this input
	streamClient *spanner.Client // Client for reading change stream data
	cdcMut       sync.RWMutex    // RWMutex for thread-safe operations
	log          *service.Logger // Logger instance

	recordsCh chan changeRecord // Channel that holds all records

	partitionTokens map[string]struct{} // Track processed partition tokens
	partitionLock   sync.RWMutex        // Mutex for thread-safe operations

	shutdownSig *shutdown.Signaller // Signals to begin shutting down components
}

type changeRecord struct {
	data    *types.DataChangeRecord
	mod     *types.Mod
	modType string
}

// newGcpSpannerCDCInput creates a new Spanner CDC input instance
func newGcpSpannerCDCInput(conf cdcConfig, res *service.Resources) (*gcpSpannerCDCInput, error) {
	return &gcpSpannerCDCInput{
		conf:            conf,
		log:             res.Logger(),
		shutdownSig:     shutdown.NewSignaller(),
		recordsCh:       make(chan changeRecord),
		partitionTokens: make(map[string]struct{}),
	}, nil
}

// Connect initializes the connection to the Spanner change stream
func (c *gcpSpannerCDCInput) Connect(ctx context.Context) error {
	c.cdcMut.Lock()
	defer c.cdcMut.Unlock()

	if c.streamClient != nil {
		return nil
	}

	select {
	case <-c.shutdownSig.HasStoppedChan():
		return service.ErrNotConnected
	default:
	}

	var err error
	c.streamClient, err = spanner.NewClient(ctx, c.conf.SpannerDSN)
	if err != nil {
		return err
	}

	if c.recordsCh == nil {
		c.recordsCh = make(chan changeRecord)
	}

	go c.loop()

	return nil
}

// Read retrieves the next message from the change stream
func (c *gcpSpannerCDCInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	c.cdcMut.RLock()
	if c.streamClient == nil || c.recordsCh == nil {
		c.cdcMut.RUnlock()
		return nil, nil, service.ErrNotConnected
	}
	recordsCh := c.recordsCh
	c.cdcMut.RUnlock()

	var (
		record changeRecord
		open   bool
	)

	select {
	case record, open = <-recordsCh:
		if !open {
			return nil, nil, service.ErrEndOfInput
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-c.shutdownSig.HardStopChan():
		return nil, nil, service.ErrEndOfInput
	}

	out, err := record.mod.ToMap()
	if err != nil {
		return nil, nil, err
	}

	msg := service.NewMessage(nil)

	msg.SetStructuredMut(out)
	msg.MetaSetMut(metadataTimestamp, time.Time(record.data.CommitTimestamp).Format(time.RFC3339Nano))
	msg.MetaSetMut(metadataModType, record.modType)
	msg.MetaSetMut(metadataTableName, record.data.TableName)
	msg.MetaSetMut(metadataServerTxnID, record.data.ServerTransactionId)
	msg.MetaSetMut(metadataRecordSeq, record.data.RecordSequence)

	return msg, func(ctx context.Context, res error) error {
		return nil
	}, nil
}

func (c *gcpSpannerCDCInput) Close(ctx context.Context) error {
	c.shutdownSig.TriggerHardStop()

	c.cdcMut.Lock()
	defer c.cdcMut.Unlock()

	if c.shutdownSig.IsHardStopSignalled() {
		return nil
	}

	if c.recordsCh != nil {
		close(c.recordsCh)
		c.recordsCh = nil
	}

	if c.streamClient != nil {
		c.streamClient.Close()
		c.streamClient = nil
	}
	return nil
}

func (c *gcpSpannerCDCInput) loop() {
	ctx, cancel := c.shutdownSig.HardStopCtx(context.Background())
	defer cancel()

	errgrp, errCtx := errgroup.WithContext(ctx)
	errgrp.Go(func() error {
		return c.readPartition(errCtx, errgrp, nil, c.conf.StartTime)
	})

	if err := errgrp.Wait(); err != nil && err != context.Canceled {
		c.log.Errorf("Error in readPartition: %v", err)
	}
}

func (c *gcpSpannerCDCInput) readPartition(ctx context.Context, errgrp *errgroup.Group, partitionToken *string, startTimestamp *time.Time) error {
	if partitionToken != nil {
		token := *partitionToken
		if c.isPartitionTracked(token) {
			defer func() {
				c.removePartition(token)
			}()
		}
	}

	if startTimestamp == nil {
		now := time.Now()
		startTimestamp = &now
	}

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`
SELECT ChangeRecord
FROM READ_%s (
    @start_timestamp,
    @end_timestamp,
    @partition_token,
    @heartbeat_milliseconds
)`, c.conf.StreamName),
		Params: map[string]interface{}{
			"start_timestamp":        startTimestamp.UTC(),
			"end_timestamp":          c.conf.EndTime,
			"partition_token":        partitionToken,
			"heartbeat_milliseconds": c.conf.HeartbeatInterval.Milliseconds(),
		},
	}

	txn := c.streamClient.Single()
	defer txn.Close()

	return txn.Query(ctx, stmt).Do(func(row *spanner.Row) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var records []*types.ChangeStreamRecord
		if err := row.ColumnByName("ChangeRecord", &records); err != nil {
			return err
		}

		for _, record := range records {
			for _, dataChange := range record.DataChangeRecord {
				for _, mod := range dataChange.Mods {
					changeRec := changeRecord{
						data:    dataChange,
						mod:     mod,
						modType: dataChange.ModType,
					}

					select {
					case c.recordsCh <- changeRec:
					case <-ctx.Done():
						return ctx.Err()
					}

				}
			}

			for _, childPartitionsRecord := range record.ChildPartitionsRecord {
				for _, childPartition := range childPartitionsRecord.ChildPartitions {
					childStartTime := time.Time(childPartitionsRecord.StartTimestamp)
					childToken := childPartition.Token

					if isTracked := c.trackPartition(childToken); isTracked {
						continue
					}

					errgrp.Go(func() error {
						return c.readPartition(ctx, errgrp, &childToken, &childStartTime)
					})
				}
			}
		}
		return nil
	})
}

func (c *gcpSpannerCDCInput) isPartitionTracked(token string) bool {
	c.partitionLock.RLock()
	defer c.partitionLock.RUnlock()
	_, exists := c.partitionTokens[token]
	return exists
}

func (c *gcpSpannerCDCInput) trackPartition(token string) bool {
	if c.isPartitionTracked(token) {
		return true
	}

	c.partitionLock.Lock()
	defer c.partitionLock.Unlock()
	if _, ok := c.partitionTokens[token]; ok {
		return true
	}

	c.partitionTokens[token] = struct{}{}
	return false
}

func (c *gcpSpannerCDCInput) removePartition(token string) {
	if !c.isPartitionTracked(token) {
		return
	}

	c.partitionLock.Lock()
	defer c.partitionLock.Unlock()
	delete(c.partitionTokens, token)
}

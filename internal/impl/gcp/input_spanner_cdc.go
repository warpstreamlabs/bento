// Package gcp provides Google Cloud Platform integrations for Bento.
// This file implements a Cloud Spanner Change Data Capture (CDC) input component
// that consumes change events from Spanner change streams.
package gcp

import (
	"context"
	"sync"
	"time"

	spanner "cloud.google.com/go/spanner"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/google/uuid"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// Spanner CDC Input Fields
	cdcFieldSpannerDSN           = "spanner_dsn"
	cdcFieldSpannerMetadataTable = "metadata_table"
	cdcFieldSpannerMetadataDSN   = "spanner_metadata_dsn"
	cdcFieldStreamName           = "stream_name"
	cdcFieldStartTime            = "start_time"
	cdcFieldEndTime              = "end_time"
	cdcFieldHeartbeatInterval    = "heartbeat_interval"
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
	if conf.SpannerMetadataTable, err = pConf.FieldString(cdcFieldSpannerMetadataTable); err != nil {
		return
	}
	if conf.SpannerMetadataDSN, err = pConf.FieldString(cdcFieldSpannerMetadataDSN); err != nil {
		return
	}
	if conf.StreamName, err = pConf.FieldString(cdcFieldStreamName); err != nil {
		return
	}
	if pConf.Contains(cdcFieldStartTime) {
		var startTimeString string
		if startTimeString, err = pConf.FieldString(cdcFieldStartTime); err != nil {
			return
		}
		var startTime time.Time
		if startTime, err = time.Parse(time.RFC3339, startTimeString); err != nil {
			return
		}
		conf.StartTime = func(in time.Time) *time.Time {
			return &in
		}(startTime)
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
		conf.EndTime = func(in time.Time) *time.Time {
			return &in
		}(endTime)
	}

	if conf.HeartbeatInterval, err = pConf.FieldDuration(cdcFieldHeartbeatInterval); err != nil {
		return
	}

	return
}

func spannerCdcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "GCP").
		Summary(`Consumes spanner Change Stream Events from a GCP Spanner instance.`).
		Description(`
For information on how to set up credentials check out [this guide](https://cloud.google.com/docs/authentication/production).

This Input uses [screamer](https://github.com/anicoll/screamer) for the reading and tracking of partitions within spanner.
Currently does not support Postgresql Dialect for the Spanner CDC.
`).
		Fields(
			service.NewStringField(cdcFieldSpannerDSN).
				Description("The dsn for spanner from where to read the changestream.").
				Example("projects/{projectId}/instances/{instanceId}/databases/{databaseName}"),
			service.NewStringField(cdcFieldSpannerMetadataDSN).
				Description("The dsn for the metadata table to track partition reads. (can be same as spanner_dsn)").
				Example("projects/{projectId}/instances/{instanceId}/databases/{databaseName}"),
			service.NewStringField(cdcFieldSpannerMetadataTable).
				Description("The table name you want to use for tracking partition metadata.").
				Example("table_metadata").
				Example("stream_metadata"),
			service.NewDurationField(cdcFieldHeartbeatInterval).
				Description("An optional field to configure the heartbeat interval for partitions.").
				Default(time.Second*3),
			service.NewStringField(cdcFieldStartTime).
				Description("An optional field to define the start point to read from the changestreams, for details on valid start times please see [this document](https://cloud.google.com/spanner/docs/change-streams#data-retention)").
				Example(time.RFC3339).
				Optional(),
			service.NewStringField(cdcFieldEndTime).
				Description("An optional field to define the end time to read from the changestreams").
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

type subscriber interface {
	Subscribe(ctx context.Context, consumer screamer.Consumer) error
}

// gcpSpannerCDCInput implements a Bento input component that reads from Spanner change streams
type gcpSpannerCDCInput struct {
	conf           cdcConfig          // Configuration for this input
	runnerID       string             // Unique ID for this consumer instance
	streamClient   *spanner.Client    // Client for reading change stream data
	metadataClient *spanner.Client    // Client for metadata operations
	closeFunc      context.CancelFunc // Function to cancel ongoing operations
	cdcMut         sync.Mutex         // Mutex for thread-safe operations
	subscriber     subscriber         // Change stream subscriber
	log            *service.Logger    // Logger instance
	consumer       consumer           // Message consumer implementation
}

// consumer implements the message consumption interface
type consumer struct {
	msgQueue chan []byte // Channel for queuing received messages
}

// Consume implements the message consumption callback
func (c consumer) Consume(data []byte) error {
	return nil
}

// newGcpSpannerCDCInput creates a new Spanner CDC input instance
func newGcpSpannerCDCInput(conf cdcConfig, res *service.Resources) (*gcpSpannerCDCInput, error) {
	ctx := context.Background()
	metadataClient, err := spanner.NewClient(ctx, conf.SpannerMetadataDSN)
	if err != nil {
		return nil, err
	}
	streamClient, err := spanner.NewClient(ctx, conf.SpannerDSN)
	if err != nil {
		return nil, err
	}

	runnerID := uuid.NewString()

	ps := partitionstorage.NewSpanner(metadataClient, conf.SpannerMetadataTable)
	if err := ps.RegisterRunner(ctx, runnerID); err != nil {
		return nil, err
	}

	opts := []screamer.Option{}
	if conf.StartTime != nil {
		opts = append(opts, screamer.WithStartTimestamp(*conf.StartTime))
	}
	if conf.EndTime != nil {
		opts = append(opts, screamer.WithEndTimestamp(*conf.EndTime))
	}
	subscriber := screamer.NewSubscriber(streamClient, conf.StreamName, runnerID, ps, opts...)

	return &gcpSpannerCDCInput{
		conf:           conf,
		log:            res.Logger(),
		runnerID:       runnerID,
		subscriber:     subscriber,
		metadataClient: metadataClient,
		streamClient:   streamClient,
		consumer: consumer{
			msgQueue: make(chan []byte, 1000),
		},
	}, nil
}

// Connect initializes the connection to the Spanner change stream
func (c *gcpSpannerCDCInput) Connect(ctx context.Context) error {
	c.cdcMut.Lock()
	defer c.cdcMut.Unlock()
	if c.subscriber == nil {
		return service.ErrNotConnected
	}

	subCtx, cancel := context.WithCancel(context.Background())
	c.closeFunc = cancel

	go func() {
		rerr := c.subscriber.Subscribe(subCtx, c.consumer)

		if rerr != nil && rerr != context.Canceled {
			c.log.Errorf("Subscription error: %v\n", rerr)
		}
		c.cdcMut.Lock()
		close(c.consumer.msgQueue)
		c.closeFunc = nil
		c.cdcMut.Unlock()
	}()
	return nil
}

// Read retrieves the next message from the change stream
func (c *gcpSpannerCDCInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	var data []byte
	var open bool
	select {
	case data, open = <-c.consumer.msgQueue:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	if !open {
		return nil, nil, service.ErrNotConnected
	}

	msg := service.NewMessage(data)

	return msg, func(ctx context.Context, res error) error {
		return nil
	}, nil
}

// Close cleanly shuts down the input component
func (c *gcpSpannerCDCInput) Close(ctx context.Context) error {
	c.cdcMut.Lock()
	defer c.cdcMut.Unlock()

	if c.closeFunc != nil {
		c.closeFunc()
		c.closeFunc = nil
	}
	if c.metadataClient != nil {
		c.metadataClient.Close()
	}
	if c.streamClient != nil {
		c.streamClient.Close()
	}
	return nil
}

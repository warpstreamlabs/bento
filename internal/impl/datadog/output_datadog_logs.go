package datadog

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/Jeffail/shutdown"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ddoFieldAPIKey          = "api_key"
	ddoFieldSite            = "site"
	ddoFieldDDSource        = "source"
	ddoFieldDDTags          = "tags"
	ddoFieldHostname        = "hostname"
	ddoFieldService         = "service"
	ddoFieldTimestamp       = "timestamp"
	ddoFieldStatus          = "status"
	ddoFieldContentEncoding = "content_encoding"
	ddoFieldBatching        = "batching"
	ddoFieldMaxInFlight     = "max_in_flight"
	ddoFieldEndpoint        = "endpoint"
)

func init() {
	err := service.RegisterBatchOutput(
		"datadog_logs", datadogLogOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (output service.BatchOutput, batchPol service.BatchPolicy, maxInFlight int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(ddoFieldBatching); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt(ddoFieldMaxInFlight); err != nil {
				return
			}
			output, err = newDatadogLogWriterFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

func datadogLogOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Sends log messages to the Datadog Logs API.").
		Description(`Submits log entries to Datadog using the HTTP Logs intake API.

### Limits

- Maximum payload size (uncompressed): 5 MB
- Maximum size for a single log: 1 MB
- Maximum number of logs per batch: 1,000 entries

Logs exceeding 1 MB are truncated by Datadog but still accepted (2xx). Payloads exceeding 5 MB are rejected with a 413.

:::warning
Log events with a timestamp older than 18 hours in the past will be rejected.
:::

### Authentication

Set `+"`api_key`"+` explicitly or via the `+"`DD_API_KEY`"+` environment variable.`).
		Fields(
			service.NewStringField(ddoFieldAPIKey).
				Secret().
				Description("The Datadog API key. If unset, falls back to the `DD_API_KEY` environment variable.").
				Optional(),
			service.NewStringField(ddoFieldSite).
				Description("The Datadog site to send logs to. If unset, falls back to the `DD_SITE` environment variable, then `datadoghq.com`.").
				Optional().
				Examples("datadoghq.com", "datadoghq.eu", "us3.datadoghq.com", "us5.datadoghq.com").
				Advanced(),
			service.NewInterpolatedStringField(ddoFieldDDSource).
				Description("The source of the log, used for log processing rules.").
				Optional(),
			service.NewInterpolatedStringField(ddoFieldDDTags).
				Description("A comma-separated list of tags to attach to the log.").
				Example(`env:${!json("environment")},version:${!json("version")}`).
				Optional(),
			service.NewInterpolatedStringField(ddoFieldHostname).
				Description("The hostname of the machine that produced the log.").
				Optional(),
			service.NewInterpolatedStringField(ddoFieldService).
				Description("The name of the service that generated the log.").
				Optional(),
			service.NewInterpolatedStringField(ddoFieldStatus).
				Description("The status of the log (e.g. info, warn, error).").
				Optional(),
			service.NewInterpolatedStringField(ddoFieldTimestamp).
				Description("The timestamp of the log in epoch milliseconds. Defaults to the current time.").
				Optional(),
			service.NewStringEnumField(ddoFieldContentEncoding, "gzip", "identity", "deflate").
				Description("HTTP content encoding used to compress log payloads.").
				Default("gzip"),
			service.NewURLField(ddoFieldEndpoint).
				Description("Override the API's destination endpoint with a custom host. Protocol scheme defaults to 'http'.").
				Optional(),
			service.NewBatchPolicyField(ddoFieldBatching),
			service.NewOutputMaxInFlightField(),
		)
}

type datadogLogWriterConfig struct {
	apiKey   string
	site     string
	endpoint string

	contentEncoding datadogV2.ContentEncoding

	ddsource  *service.InterpolatedString
	ddtags    *service.InterpolatedString
	hostname  *service.InterpolatedString
	service   *service.InterpolatedString
	status    *service.InterpolatedString
	timestamp *service.InterpolatedString
}

func (c *datadogLogWriterConfig) buildDDContext(ctx context.Context) context.Context {
	submitCtx := datadog.NewDefaultContext(ctx)
	if c.site != "" {
		submitCtx = context.WithValue(submitCtx, datadog.ContextServerVariables, map[string]string{
			"site": c.site,
		})
	}
	if c.apiKey != "" {
		submitCtx = context.WithValue(submitCtx, datadog.ContextAPIKeys, map[string]datadog.APIKey{
			"apiKeyAuth": {Key: c.apiKey},
		})
	}
	return submitCtx
}

type datadogLogWriter struct {
	client *datadogV2.LogsApi

	shutSig *shutdown.Signaller

	writerConf datadogLogWriterConfig
	ddConf     *datadog.Configuration
	log        *service.Logger

	mu sync.RWMutex
}

func newDatadogLogWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*datadogLogWriter, error) {
	ddConf := datadog.NewConfiguration()

	var wconf datadogLogWriterConfig
	var err error

	if conf.Contains(ddoFieldAPIKey) {
		if wconf.apiKey, err = conf.FieldString(ddoFieldAPIKey); err != nil {
			return nil, err
		}
	}

	if conf.Contains(ddoFieldSite) {
		if wconf.site, err = conf.FieldString(ddoFieldSite); err != nil {
			return nil, err
		}
	}

	var encoding string
	if encoding, err = conf.FieldString(ddoFieldContentEncoding); err != nil {
		return nil, err
	}
	enc, err := datadogV2.NewContentEncodingFromValue(encoding)
	if err != nil {
		return nil, err
	}
	wconf.contentEncoding = *enc

	if conf.Contains(ddoFieldDDSource) {
		if wconf.ddsource, err = conf.FieldInterpolatedString(ddoFieldDDSource); err != nil {
			return nil, err
		}
	}

	if conf.Contains(ddoFieldDDTags) {
		if wconf.ddtags, err = conf.FieldInterpolatedString(ddoFieldDDTags); err != nil {
			return nil, err
		}
	}

	if conf.Contains(ddoFieldHostname) {
		if wconf.hostname, err = conf.FieldInterpolatedString(ddoFieldHostname); err != nil {
			return nil, err
		}
	}

	if conf.Contains(ddoFieldService) {
		if wconf.service, err = conf.FieldInterpolatedString(ddoFieldService); err != nil {
			return nil, err
		}
	}

	if conf.Contains(ddoFieldTimestamp) {
		if wconf.timestamp, err = conf.FieldInterpolatedString(ddoFieldTimestamp); err != nil {
			return nil, err
		}
	}

	if conf.Contains(ddoFieldStatus) {
		if wconf.status, err = conf.FieldInterpolatedString(ddoFieldStatus); err != nil {
			return nil, err
		}
	}

	if conf.Contains(ddoFieldEndpoint) {
		raw, err := conf.FieldString(ddoFieldEndpoint)
		if err != nil {
			return nil, err
		}

		if !strings.Contains(raw, "://") {
			raw = "http://" + raw
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid endpoint %q: %w", raw, err)
		}

		mgr.Logger().Warnf("Setting '%s' will override the destination of DataDog to %s", ddoFieldEndpoint, u.String())

		ddConf.Host = u.Host
		ddConf.Scheme = u.Scheme
	}

	return &datadogLogWriter{
		ddConf:     ddConf,
		writerConf: wconf,
		log:        mgr.Logger(),
		shutSig:    shutdown.NewSignaller(),
	}, nil
}

func (d *datadogLogWriter) Connect(_ context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.client != nil {
		return nil
	}

	apiClient := datadog.NewAPIClient(d.ddConf)
	d.client = datadogV2.NewLogsApi(apiClient)
	return nil
}

func (d *datadogLogWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	d.mu.RLock()
	if d.client == nil {
		d.mu.RUnlock()
		return service.ErrNotConnected
	}
	d.mu.RUnlock()

	ctx, cancel := d.shutSig.SoftStopCtx(ctx)
	defer cancel()

	items := make([]datadogV2.HTTPLogItem, 0, len(batch))

	var (
		ddsourceExec,
		ddtagsExec,
		hostnameExec,
		serviceExec,
		statusExec,
		timestampExec *service.MessageBatchInterpolationExecutor
	)
	if d.writerConf.ddsource != nil {
		ddsourceExec = batch.InterpolationExecutor(d.writerConf.ddsource)
	}
	if d.writerConf.ddtags != nil {
		ddtagsExec = batch.InterpolationExecutor(d.writerConf.ddtags)
	}
	if d.writerConf.hostname != nil {
		hostnameExec = batch.InterpolationExecutor(d.writerConf.hostname)
	}
	if d.writerConf.service != nil {
		serviceExec = batch.InterpolationExecutor(d.writerConf.service)
	}
	if d.writerConf.status != nil {
		statusExec = batch.InterpolationExecutor(d.writerConf.status)
	}
	if d.writerConf.timestamp != nil {
		timestampExec = batch.InterpolationExecutor(d.writerConf.timestamp)
	}

	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(batch, err)
		}
		batchErr.Failed(i, err)
	}

	for i, msg := range batch {
		contents, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, fmt.Errorf("message %d reading body: %w", i, err))
			continue
		}
		item := datadogV2.HTTPLogItem{
			Message:              string(contents),
			AdditionalProperties: make(map[string]interface{}, 2),
		}
		if err := exec(i, ddsourceExec, item.SetDdsource); err != nil {
			batchErrFailed(i, fmt.Errorf("message %d resolving ddsource: %w", i, err))
			continue
		}

		if err := exec(i, ddtagsExec, item.SetDdtags); err != nil {
			batchErrFailed(i, fmt.Errorf("message %d resolving ddtags: %w", i, err))
			continue
		}
		if err := exec(i, hostnameExec, item.SetHostname); err != nil {
			batchErrFailed(i, fmt.Errorf("message %d resolving hostname: %w", i, err))
			continue
		}
		if err := exec(i, serviceExec, item.SetService); err != nil {
			batchErrFailed(i, fmt.Errorf("message %d resolving service: %w", i, err))
			continue
		}

		if err := exec(i, timestampExec, func(v string) {
			if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
				item.AdditionalProperties["timestamp"] = ts
			}
		}); err != nil {
			batchErrFailed(i, fmt.Errorf("message %d resolving timestamp: %w", i, err))
			continue
		}

		if err := exec(i, statusExec, func(v string) {
			item.AdditionalProperties["status"] = v
		}); err != nil {
			batchErrFailed(i, fmt.Errorf("message %d resolving service: %w", i, err))
			continue
		}

		items = append(items, item)
	}

	if len(items) > 0 {
		ddCtx := d.writerConf.buildDDContext(ctx)
		opts := datadogV2.NewSubmitLogOptionalParameters().WithContentEncoding(d.writerConf.contentEncoding)

		_, resp, err := d.client.SubmitLog(
			ddCtx,
			items,
			*opts,
		)
		if resp != nil {
			defer resp.Body.Close()
		}

		if err != nil {
			oaiErr, ok := err.(datadog.GenericOpenAPIError)
			if !ok {
				return err
			}

			ddErr, ok := oaiErr.Model().(datadogV2.HTTPLogErrors)
			if !ok {
				return err
			}

			for i, e := range ddErr.GetErrors() {
				d.log.Debugf(`SubmitLogs call failed with error[%d]: status=%s title="%s" detail="%s"`, i, e.GetStatus(), e.GetTitle(), e.GetDetail())
			}

			return err
		}
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	return nil
}

func (d *datadogLogWriter) Close(_ context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil
	}

	d.shutSig.TriggerHardStop()
	d.client = nil
	return nil
}

func exec(i int, executor *service.MessageBatchInterpolationExecutor, set func(v string)) error {
	if executor == nil {
		return nil
	}
	result, err := executor.TryString(i)
	if err != nil {
		return err
	}
	set(result)
	return nil
}

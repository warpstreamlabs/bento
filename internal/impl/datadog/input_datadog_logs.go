package datadog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ddiFieldAPIKey          = "api_key"
	ddiFieldAppKey          = "app_key"
	ddiFieldSite            = "site"
	ddiFieldQuery           = "query"
	ddiFieldFrom            = "from"
	ddiFieldTo              = "to"
	ddiFieldStartFromOldest = "start_from_oldest"
	ddiFieldIndexes         = "indexes"
	ddiFieldPageLimit       = "page_limit"

	ddiFieldHTTP         = "http_client"
	ddiFieldRetryEnabled = "retry_enabled"
	ddiFieldMaxRetries   = "max_retries"
	ddiFieldTimeout      = "timeout"
)

func init() {
	err := service.RegisterInput(
		"datadog_logs", datadogLogInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newDatadogLogReaderFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func datadogLogInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Reads log entries from the Datadog Logs API.").
		Description(`Queries the Datadog Logs search API and emits one batch of messages per page of results.

When a page is exhausted and no `+"`to`"+` is configured, the input slides the time window forward to the last seen log timestamp and waits `+"`poll_interval`"+` before querying again, providing a live tail behaviour.

### Metadata

`+"```text"+`
- datadog_log_host
- datadog_log_service
- datadog_log_tags
- datadog_log_status
- datadog_log_timestamp
- datadog_log_<attribute_key>
`+"```"+`

### Authentication

Set `+"`api_key`"+` and `+"`app_key`"+` explicitly or via the `+"`DD_API_KEY`"+` and `+"`DD_APP_KEY`"+` environment variables. Both keys are required to query logs.`).
		Fields(
			service.NewStringField(ddiFieldAPIKey).
				Secret().
				Description("Datadog API key. Falls back to the `DD_API_KEY` environment variable if unset.").
				Optional(),
			service.NewStringField(ddiFieldAppKey).
				Secret().
				Description("Datadog application key. Falls back to the `DD_APP_KEY` environment variable if unset. Required for querying logs.").
				Optional(),
			service.NewStringField(ddiFieldSite).
				Description("Datadog site to query. Falls back to `DD_SITE`, then `datadoghq.com`.").
				Optional().
				Examples("datadoghq.com", "datadoghq.eu", "us3.datadoghq.com", "us5.datadoghq.com").
				Advanced(),
			service.NewStringField(ddiFieldQuery).
				Description("Log search query.").
				Default("*"),
			service.NewStringField(ddiFieldFrom).
				Description("Start of the query time window as an RFC 3339 timestamp. Defaults to 15 minutes before the input connects.").
				Optional(),
			service.NewStringField(ddiFieldTo).
				Description("End of the query time window as an RFC 3339 timestamp. If unset the window slides forward continuously.").
				Optional(),
			service.NewBoolField(ddiFieldStartFromOldest).
				Description("When true, logs are returned oldest-first (ascending). When false, newest-first (descending).").
				Default(true).
				Advanced(),
			service.NewStringListField(ddiFieldIndexes).
				Description("Log indexes to search. Defaults to all indexes.").
				Optional().
				Advanced(),
			service.NewIntField(ddiFieldPageLimit).
				Description("Maximum number of log entries per page request.").
				Default(100).
				Advanced(),
			service.NewObjectField(ddiFieldHTTP,
				service.NewBoolField(ddiFieldRetryEnabled).
					Description("Enable automatic retries on API errors.").
					Default(true).
					Advanced(),
				service.NewIntField(ddiFieldMaxRetries).
					Description("Maximum number of retries per request.").
					Default(3).
					Advanced(),
				service.NewDurationField(ddiFieldTimeout).
					Description("Maximum total time to spend retrying a single request.").
					Default("60s").
					Advanced(),
			).Advanced(),
		)
}

//------------------------------------------------------------------------------

type datadogLogReaderConfig struct {
	apiKey string
	appKey string
	site   string

	query         string
	from          time.Time
	to            time.Time
	sortDirection datadogV2.LogsSort
	indices       []string

	limit int32

	ddConf *datadog.Configuration
}

func buildDDContext(ctx context.Context, conf datadogLogReaderConfig) context.Context {
	ddCtx := datadog.NewDefaultContext(ctx)
	if conf.site != "" {
		ddCtx = context.WithValue(ddCtx, datadog.ContextServerVariables, map[string]string{
			"site": conf.site,
		})
	}
	keys := map[string]datadog.APIKey{}
	if conf.apiKey != "" {
		keys["apiKeyAuth"] = datadog.APIKey{Key: conf.apiKey}
	}
	if conf.appKey != "" {
		keys["appKeyAuth"] = datadog.APIKey{Key: conf.appKey}
	}
	if len(keys) > 0 {
		ddCtx = context.WithValue(ddCtx, datadog.ContextAPIKeys, keys)
	}
	return ddCtx
}

func newDatadogLogReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*datadogLogReader, error) {
	var c datadogLogReaderConfig
	var err error

	if conf.Contains(ddiFieldAPIKey) {
		if c.apiKey, err = conf.FieldString(ddiFieldAPIKey); err != nil {
			return nil, err
		}
	}
	if conf.Contains(ddiFieldAppKey) {
		if c.appKey, err = conf.FieldString(ddiFieldAppKey); err != nil {
			return nil, err
		}
	}
	if conf.Contains(ddiFieldSite) {
		if c.site, err = conf.FieldString(ddiFieldSite); err != nil {
			return nil, err
		}
	}
	if c.query, err = conf.FieldString(ddiFieldQuery); err != nil {
		return nil, err
	}
	if conf.Contains(ddiFieldFrom) {
		fromStr, err := conf.FieldString(ddiFieldFrom)
		if err != nil {
			return nil, err
		}
		if c.from, err = time.Parse(time.RFC3339, fromStr); err != nil {
			return nil, fmt.Errorf("field %s: %w", ddiFieldFrom, err)
		}
	}
	if conf.Contains(ddiFieldTo) {
		toStr, err := conf.FieldString(ddiFieldTo)
		if err != nil {
			return nil, err
		}
		if c.to, err = time.Parse(time.RFC3339, toStr); err != nil {
			return nil, fmt.Errorf("field %s: %w", ddiFieldTo, err)
		}
	}
	startFromOldest, err := conf.FieldBool(ddiFieldStartFromOldest)
	if err != nil {
		return nil, err
	}
	if startFromOldest {
		c.sortDirection = datadogV2.LOGSSORT_TIMESTAMP_ASCENDING
	} else {
		c.sortDirection = datadogV2.LOGSSORT_TIMESTAMP_DESCENDING
	}

	if conf.Contains(ddiFieldIndexes) {
		if c.indices, err = conf.FieldStringList(ddiFieldIndexes); err != nil {
			return nil, err
		}
	}
	limit, err := conf.FieldInt(ddiFieldPageLimit)
	if err != nil {
		return nil, err
	}
	c.limit = int32(limit)

	httpConf := conf.Namespace(ddiFieldHTTP)

	retryEnabled, err := httpConf.FieldBool(ddiFieldRetryEnabled)
	if err != nil {
		return nil, err
	}
	maxRetries, err := httpConf.FieldInt(ddiFieldMaxRetries)
	if err != nil {
		return nil, err
	}
	timeout, err := httpConf.FieldDuration(ddiFieldTimeout)
	if err != nil {
		return nil, err
	}

	c.ddConf = datadog.NewConfiguration()
	c.ddConf.RetryConfiguration = datadog.RetryConfiguration{
		EnableRetry:       retryEnabled,
		MaxRetries:        maxRetries,
		BackOffMultiplier: 2,
		BackOffBase:       2,
		HTTPRetryTimeout:  timeout,
	}

	return newDatadogLogReader(c, mgr.Logger())
}

//------------------------------------------------------------------------------

type datadogLogReader struct {
	client *datadogV2.LogsApi

	log  *service.Logger
	conf datadogLogReaderConfig

	logChan chan datadogV2.Log

	mu      sync.RWMutex
	shutSig *shutdown.Signaller
}

func newDatadogLogReader(conf datadogLogReaderConfig, log *service.Logger) (*datadogLogReader, error) {
	return &datadogLogReader{
		conf:    conf,
		log:     log,
		logChan: make(chan datadogV2.Log, conf.limit),
		shutSig: shutdown.NewSignaller(),
	}, nil
}

func (r *datadogLogReader) loop(client *datadogV2.LogsApi, conf datadogLogReaderConfig) {
	if client == nil {
		return
	}

	defer r.shutSig.TriggerHasStopped()

	ctx, cancel := r.shutSig.SoftStopCtx(context.Background())
	defer cancel()
	defer close(r.logChan)

	var cursor string
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		req := buildRequest(cursor, conf)
		ddCtx := buildDDContext(ctx, conf)

		resp, _, err := client.ListLogs(ddCtx, *datadogV2.NewListLogsOptionalParameters().WithBody(req))
		if err != nil {
			r.log.Errorf("Datadog API error: %v", err)
			continue
		}

		if meta := resp.GetMeta(); meta.Page != nil {
			cursor = meta.Page.GetAfter()
		}

		logs := resp.GetData()
		for _, l := range logs {
			select {
			case r.logChan <- l:
			case <-ctx.Done():
				return
			}
		}

		if len(logs) == 0 {
			if !conf.to.IsZero() {
				r.log.Infof("No more logs found between %s and %s.", conf.from, conf.to)
				return
			}
		}
	}
}

func (r *datadogLogReader) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client != nil {
		return nil
	}

	r.client = datadogV2.NewLogsApi(datadog.NewAPIClient(r.conf.ddConf))

	go r.loop(r.client, r.conf)

	return nil
}

func (r *datadogLogReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	r.mu.RLock()
	if r.client == nil {
		r.mu.RUnlock()
		return nil, nil, service.ErrNotConnected
	}
	r.mu.RUnlock()

	select {
	case item, ok := <-r.logChan:
		if !ok {
			return nil, nil, service.ErrEndOfInput
		}

		return toMessage(item), func(ctx context.Context, err error) error {
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (r *datadogLogReader) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.client == nil {
		r.mu.Unlock()
		return nil
	}

	r.client = nil
	r.mu.Unlock()

	r.shutSig.TriggerSoftStop()
	select {
	case <-r.shutSig.HasStoppedChan():
	case <-ctx.Done():
	}
	return nil
}

//------------------------------------------------------------------------------

func buildRequest(cursor string, conf datadogLogReaderConfig) datadogV2.LogsListRequest {
	filter := datadogV2.LogsQueryFilter{
		Query: datadog.PtrString(conf.query),
		From:  datadog.PtrString(conf.from.Format(time.RFC3339)),
	}
	if !conf.to.IsZero() {
		filter.To = datadog.PtrString(conf.to.Format(time.RFC3339))
	}
	if len(conf.indices) > 0 {
		filter.Indexes = conf.indices
	}

	page := datadogV2.LogsListRequestPage{
		Limit: datadog.PtrInt32(conf.limit),
	}
	if cursor != "" {
		page.Cursor = datadog.PtrString(cursor)
	}

	return datadogV2.LogsListRequest{
		Filter: &filter,
		Page:   &page,
		Sort:   &conf.sortDirection,
	}
}

func toMessage(item datadogV2.Log) *service.Message {
	msg := service.NewMessage(nil)
	attr := item.GetAttributes()

	if logMsg := attr.GetMessage(); logMsg != "" {
		msg.SetBytes([]byte(logMsg))
	}

	if host := attr.GetHost(); host != "" {
		msg.MetaSetMut("datadog_log_host", host)
	}

	if svc := attr.GetService(); svc != "" {
		msg.MetaSetMut("datadog_log_service", svc)
	}

	if tags := attr.GetTags(); len(tags) > 0 {
		msg.MetaSetMut("datadog_log_tags", strings.Join(tags, ","))
	}

	if status := attr.GetStatus(); status != "" {
		msg.MetaSetMut("datadog_log_status", status)
	}

	if ts := attr.GetTimestamp(); !ts.IsZero() {
		msg.MetaSetMut("datadog_log_timestamp", ts.Format(time.RFC3339))
	}

	for key, value := range attr.GetAttributes() {
		msg.MetaSetMut(fmt.Sprintf("datadog_log_%s", key), value)
	}

	return msg
}

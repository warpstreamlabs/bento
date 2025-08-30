package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esutil"
	"github.com/warpstreamlabs/bento/internal/retries"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	esoV2FieldURLs                  = "urls"
	esoV2FieldDiscoverNodesOnStart  = "discover_nodes_on_start"
	esoV2FieldDiscoverNodesInterval = "discover_nodes_interval"
	esoV2FieldID                    = "id"
	esoV2FieldAction                = "action"
	esoV2FieldIndex                 = "index"
	esoV2FieldPipeline              = "pipeline"
	esoV2FieldRouting               = "routing"
	esoV2FieldTimeout               = "timeout"
	esoV2FieldTLS                   = "tls"
	esoV2FieldAuth                  = "basic_auth"
	esoV2FieldAuthEnabled           = "enabled"
	esoV2FieldAuthUsername          = "username"
	esoV2FieldAuthPassword          = "password"
	esoV2FieldCompressRequestBody   = "compress_request_body"
	esoV2FieldBatching              = "batching"
	esoV2FieldRetryOnStatus         = "retry_on_status"
)

func init() {
	err := service.RegisterBatchOutput("elasticsearch_v2", OutputSpecV2(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(esoV2FieldBatching); err != nil {
				return
			}
			out, err = EsoOutputConstructor(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

func OutputSpecV2() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("1.11.0").
		Categories("Services").
		Summary(`Publishes messages into an Elasticsearch index. If the index does not exist then it is created with a dynamic mapping.
:::warning UNIQUE MESSAGES PER BATCH
This component makes use of the [BulkIndexer](https://pkg.go.dev/github.com/elastic/go-elasticsearch/v9@v9.0.0/esutil#BulkIndexer) - this will error if an attempt is made to update the same document twice - therefore it is recommended that you ensure each message in the batch has a unique id.
:::`).
		Description(`
Both the `+"`id` and `index`"+` fields can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages these interpolations are performed per message part.
`).
		Fields(
			service.NewStringListField(esoV2FieldURLs).
				Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"http://localhost:9200"}),
			service.NewInterpolatedStringField(esoV2FieldIndex).
				Description("The index to place messages."),
			service.NewInterpolatedStringField(esoV2FieldAction).
				Default("index").
				Description("The action to take on the document. This field must resolve to one of the following action types: `create`, `index`, `update`, `upsert` or `delete`.").
				Advanced(),
			service.NewStringField(esoV2FieldPipeline).
				Default("").
				Description("An optional pipeline id to preprocess incoming documents.").
				Advanced(),
			service.NewInterpolatedStringField(esoV2FieldID).
				Description("The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").
				Default(`${!count("elastic_ids")}-${!timestamp_unix()}`),
			service.NewInterpolatedStringField(esoV2FieldRouting).
				Description("The routing key to use for the document.").
				Advanced().
				Default(""),
			service.NewBoolField(esoV2FieldDiscoverNodesOnStart).
				Default(false).
				Description("Discover nodes when initializing the client."),
			service.NewDurationField(esoV2FieldDiscoverNodesInterval).
				Default("0s").
				Description("Discover nodes periodically."),
			service.NewDurationField(esoV2FieldTimeout).
				Description("The maximum time to wait before abandoning a request (and trying again).").
				Advanced().
				Default("5s"),
			service.NewTLSToggledField(esoV2FieldTLS),
			service.NewOutputMaxInFlightField(),
			service.NewObjectField(esoV2FieldAuth,
				service.NewBoolField(esoV2FieldAuthEnabled).
					Description("Whether to use basic authentication in requests.").
					Default(false),
				service.NewStringField(esoV2FieldAuthUsername).
					Description("A username to authenticate as.").
					Default(""),
				service.NewStringField(esoV2FieldAuthPassword).
					Description("A password to authenticate with.").
					Default("").Secret(),
			).Description("Allows you to specify basic authentication.").
				Advanced().
				Optional(),
			service.NewBatchPolicyField(esoV2FieldBatching),
			service.NewBoolField(esoV2FieldCompressRequestBody).
				Description("Enable gzip compression on the request side.").
				Advanced().
				Default(false),
			service.NewIntListField(esoV2FieldRetryOnStatus).
				Default([]int{502, 503, 504}).
				Description("HTTP Status codes that should be retried."),
		).
		Fields(retries.CommonRetryBackOffFields(0, "1s", "5s", "30s")...)
}

//------------------------------------------------------------------------------

type esoV2Config struct {
	clientConfig elasticsearch.Config

	timeout time.Duration

	indexExpr   *service.InterpolatedString
	actionExpr  *service.InterpolatedString
	pipeline    string
	idExpr      *service.InterpolatedString
	routingExpr *service.InterpolatedString
}

func esoV2ConfigFromParsed(pConf *service.ParsedConfig) (conf esoV2Config, err error) {
	if conf.clientConfig.Addresses, err = pConf.FieldStringList(esoV2FieldURLs); err != nil {
		return
	}
	if conf.indexExpr, err = pConf.FieldInterpolatedString(esoV2FieldIndex); err != nil {
		return
	}
	if conf.actionExpr, err = pConf.FieldInterpolatedString(esoV2FieldAction); err != nil {
		return
	}
	if conf.pipeline, err = pConf.FieldString(esoV2FieldPipeline); err != nil {
		return
	}
	if conf.idExpr, err = pConf.FieldInterpolatedString(esoV2FieldID); err != nil {
		return
	}
	if conf.routingExpr, err = pConf.FieldInterpolatedString(esoV2FieldRouting); err != nil {
		return
	}
	if conf.clientConfig.DiscoverNodesOnStart, err = pConf.FieldBool(esoV2FieldDiscoverNodesOnStart); err != nil {
		return
	}
	if conf.clientConfig.DiscoverNodesInterval, err = pConf.FieldDuration(esoV2FieldDiscoverNodesInterval); err != nil {
		return
	}
	if conf.timeout, err = pConf.FieldDuration(esoV2FieldTimeout); err != nil {
		return
	}
	if conf.clientConfig.RetryOnStatus, err = pConf.FieldIntList(esoV2FieldRetryOnStatus); err != nil {
		return
	}
	if conf.clientConfig.MaxRetries, err = pConf.FieldInt("max_retries"); err != nil {
		return
	}
	if conf.clientConfig.MaxRetries != 0 {
		var backoffCtor func() backoff.BackOff
		if backoffCtor, err = retries.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
			return
		}

		boff := backoffCtor()

		retryBackoffFunc := func(i int) time.Duration {
			if i == 1 {
				boff.Reset()
			}
			return boff.NextBackOff()
		}

		conf.clientConfig.RetryBackoff = retryBackoffFunc
	}

	authConf := pConf.Namespace(esoV2FieldAuth)
	if enabled, _ := authConf.FieldBool(esoV2FieldAuthEnabled); enabled {
		if conf.clientConfig.Username, err = authConf.FieldString(esoV2FieldAuthUsername); err != nil {
			return
		}
		if conf.clientConfig.Password, err = authConf.FieldString(esoV2FieldAuthPassword); err != nil {
			return
		}
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(esoV2FieldTLS)
	if err != nil {
		return
	}

	if tlsEnabled {
		conf.clientConfig.Transport = &http.Transport{
			TLSClientConfig: tlsConf,
		}
	}

	if conf.clientConfig.CompressRequestBody, err = pConf.FieldBool(esoV2FieldCompressRequestBody); err != nil {
		return
	}

	return
}

//------------------------------------------------------------------------------

type EsOutput struct {
	log  *service.Logger
	conf esoV2Config

	client *elasticsearch.Client
}

func EsoOutputConstructor(pConf *service.ParsedConfig, mgr *service.Resources) (*EsOutput, error) {
	conf, err := esoV2ConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	return &EsOutput{
		log:  mgr.Logger(),
		conf: conf,
	}, nil
}

func (eso *EsOutput) Connect(ctx context.Context) error {
	if eso.client != nil {
		return nil
	}

	client, err := elasticsearch.NewClient(eso.conf.clientConfig)
	if err != nil {
		return err
	}

	res, err := client.Info()
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return errors.New(strconv.Itoa(res.StatusCode))
	}

	eso.client = client

	return nil
}

func (eso *EsOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if eso.client == nil {
		return service.ErrNotConnected
	}

	var errorMu sync.Mutex
	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(batch, err)
		}
		batchErr.Failed(i, err)
	}

	batchInterpolator := eso.newInterpolationExecutor(batch)

	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: eso.client,
		OnError: func(ctx context.Context, err error) {
			eso.log.Errorf("Bulk Indexer Error: %w", err)
		},
		Timeout: eso.conf.timeout,
	})
	if err != nil {
		return err
	}

	for i, msg := range batch {
		index, action, id, routing, err := batchInterpolator.exec(i)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		switch action {
		case "update", "upsert":
			updateBody := map[string]any{
				"doc":           json.RawMessage(msgBytes),
				"doc_as_upsert": action == "upsert",
			}
			updateBodyBytes, err := json.Marshal(updateBody)
			if err != nil {
				batchErrFailed(i, err)
				continue
			}
			err = indexer.Add(ctx, esutil.BulkIndexerItem{
				Index:      index,
				Action:     "update",
				DocumentID: id,
				Routing:    routing,
				Body:       bytes.NewReader(updateBodyBytes),
				OnSuccess:  onSuccessHandler(eso),
				OnFailure:  onFailureHandler(&errorMu, i, batchErrFailed),
			})
			if err != nil {
				batchErrFailed(i, err)
			}
		case "delete":
			err = indexer.Add(ctx, esutil.BulkIndexerItem{
				Index:      index,
				Action:     action,
				DocumentID: id,
				Routing:    routing,
				OnSuccess:  onSuccessHandler(eso),
				OnFailure:  onFailureHandler(&errorMu, i, batchErrFailed),
			})
			if err != nil {
				batchErrFailed(i, err)
			}
		case "index", "create":
			err = indexer.Add(ctx, esutil.BulkIndexerItem{
				Index:      index,
				Action:     action,
				DocumentID: id,
				Routing:    routing,
				Body:       bytes.NewReader(msgBytes),
				OnSuccess:  onSuccessHandler(eso),
				OnFailure:  onFailureHandler(&errorMu, i, batchErrFailed),
			})
			if err != nil {
				batchErrFailed(i, err)
			}
		default:
			batchErrFailed(i, fmt.Errorf("elasticsearch action '%s' is not allowed", action))
		}
	}

	err = indexer.Close(ctx)
	if err != nil {
		return err
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	return nil
}

func (eso *EsOutput) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

func (eso *EsOutput) newInterpolationExecutor(batch service.MessageBatch) (ie interpolationExecutor) {
	ie.indexExecutor = batch.InterpolationExecutor(eso.conf.indexExpr)
	ie.actionExecutor = batch.InterpolationExecutor(eso.conf.actionExpr)
	ie.idExecutor = batch.InterpolationExecutor(eso.conf.idExpr)
	ie.routingExecutor = batch.InterpolationExecutor(eso.conf.routingExpr)
	return ie
}

type interpolationExecutor struct {
	indexExecutor   *service.MessageBatchInterpolationExecutor
	actionExecutor  *service.MessageBatchInterpolationExecutor
	idExecutor      *service.MessageBatchInterpolationExecutor
	routingExecutor *service.MessageBatchInterpolationExecutor
}

func (ie *interpolationExecutor) exec(i int) (index, action, id, routing string, err error) {
	index, err = ie.indexExecutor.TryString(i)
	if err != nil {
		return
	}
	action, err = ie.actionExecutor.TryString(i)
	if err != nil {
		return
	}
	id, err = ie.idExecutor.TryString(i)
	if err != nil {
		return
	}
	routing, err = ie.routingExecutor.TryString(i)
	if err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

func onSuccessHandler(eso *EsOutput) func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
	return func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
		eso.log.Tracef("Processed %s/%s", res.Index, res.DocumentID)
	}
}

func onFailureHandler(errorMu *sync.Mutex, i int, batchErrFailed func(i int, err error)) func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
	return func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
		errorMu.Lock()
		defer errorMu.Unlock()

		if err != nil {
			batchErrFailed(i, err)
		} else {
			if res.Error.Type != "" {
				err := errors.New(res.Error.Type + " " + res.Error.Reason)
				batchErrFailed(i, err)
			}
		}
	}
}

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
	"github.com/warpstreamlabs/bento/public/bloblang"
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
	esoV2FieldAPIKey                = "api_key"
	esoV2FieldAuthEnabled           = "enabled"
	esoV2FieldAuthUsername          = "username"
	esoV2FieldAuthPassword          = "password"
	esoV2FieldCompressRequestBody   = "compress_request_body"
	esoV2FieldBatching              = "batching"
	esoV2FieldRetryOnStatus         = "retry_on_status"
	esoV2FieldRetryOnConflict       = "retry_on_conflict"
	esoV2FieldNumWorkers            = "num_workers"
	esoV2FieldUpdate                = "update"
	esoV2FieldDocMapping            = "doc_mapping"
	esoV2FieldUpsertMapping         = "upsert_mapping"
	esoV2FieldScript                = "script"
	esoV2FieldScriptSource          = "source"
	esoV2FieldScriptLang            = "lang"
	esoV2FieldScriptParamsMapping   = "params_mapping"
	esoV2FieldScriptedUpsert        = "scripted_upsert"
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
				Description("A list of URLs to connect to.").
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
			service.NewStringField(esoV2FieldAPIKey).
				Description("Allows you to specify a Base64-encoded token for authorization; if set, overrides basic auth.").
				Optional(),
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
			service.NewIntField(esoV2FieldNumWorkers).
				Description("The number of workers of the [BulkIndexer](https://pkg.go.dev/github.com/elastic/go-elasticsearch/v9/esutil#BulkIndexer) used for each batch. 0 uses the library default, the number of CPUs.").
				Advanced().
				Default(0).
				LintRule(`root = if this < 0 { [ "num_workers must be 0 or more" ] }`),
			service.NewObjectField(esoV2FieldUpdate,
				service.NewIntField(esoV2FieldRetryOnConflict).
					Description("The number of times Elasticsearch retries the update internally when it hits a version conflict, e.g. from concurrent writes to the same document, including two upserts creating the same new document. Not sent when unset.").
					Optional().
					LintRule(`root = if this < 0 { [ "retry_on_conflict must be 0 or more" ] }`),
				service.NewBloblangField(esoV2FieldDocMapping).
					Description("A mapping, executed on each message, whose result is sent as the partial `doc` merged into the existing document, instead of the message itself. Combined with `upsert_mapping` this allows sending only some fields as updates while indexing the full message when the document does not exist yet. If the mapping deletes the message, the message itself is sent, as if this field was not set. Cannot be combined with `script`.").
					Example(`root.status = this.status
root.updated_at = now()`).
					Optional(),
				service.NewBloblangField(esoV2FieldUpsertMapping).
					Description("A mapping, executed on each message, whose result is the document indexed when the document does not exist yet (Elasticsearch's `upsert` document); an existing document gets the update instead. This allows fields that are only set when a document is created. With the `upsert` action it replaces the default, the message itself. If the mapping deletes the message, it behaves as if this field was not set.").
					Example(`root = this
root.first_seen = now()`).
					Optional(),
				service.NewObjectField(esoV2FieldScript,
					service.NewStringField(esoV2FieldScriptSource).
						Description("The script source.").
						Example(`ctx._source.counter = (ctx._source.counter ?: 0) + params.count`),
					service.NewStringEnumField(esoV2FieldScriptLang, "painless").
						Description("The script language.").
						Default("painless"),
					service.NewBloblangField(esoV2FieldScriptParamsMapping).
						Description("A mapping, executed on each message, whose result is passed to the script as `params`. If unset, or if the mapping deletes the message, the message itself is passed as `params`.").
						Optional(),
				).
					Description("Update the document with a script instead of merging the message into it. When the document does not exist yet, the `upsert` action indexes the `upsert_mapping` result, or else the message itself; the `update` action only does so when `upsert_mapping` is set. Cannot be combined with `doc_mapping`.").
					Optional(),
				service.NewBoolField(esoV2FieldScriptedUpsert).
					Description("Run the `script` also when the document does not exist yet, starting from the document that would otherwise be indexed (or an empty one) instead of indexing it as is. Requires `script`.").
					Default(false),
			).
				Description("Options for the `update` and `upsert` actions, mirroring the Elasticsearch update API. Ignored for other actions.").
				Advanced().
				Optional(),
		).
		Fields(retries.CommonRetryBackOffFields(0, "1s", "5s", "30s")...).
		Example(
			"Fields set only on creation",
			"Fold a stream of events into one document per session: the first event creates the document with all its fields, later events only update the progress fields, so `started_at` keeps its first value.",
			`
output:
  elasticsearch_v2:
    urls: [ http://localhost:9200 ]
    index: sessions
    id: ${! this.session_id }
    action: upsert
    update:
      doc_mapping: |
        root.progress = this.progress
        root.last_event_at = this.timestamp
      upsert_mapping: |
        root = this
        root.started_at = this.timestamp
`,
		).
		Example(
			"Never decrease a value",
			"Keep the highest value seen, even when an older message arrives after a newer one. The first message for a document is indexed as is.",
			`
output:
  elasticsearch_v2:
    urls: [ http://localhost:9200 ]
    index: sessions
    id: ${! this.session_id }
    action: upsert
    update:
      script:
        source: 'ctx._source.progress = Math.max(ctx._source.progress, params.progress)'
        params_mapping: 'root.progress = this.progress'
`,
		).
		LintRule(`root = [
  if this.update.script.source.type() == "string" && this.update.doc_mapping.type() == "string" { "fields update.script and update.doc_mapping cannot be combined, a scripted update sends no partial document" } else { null },
  if this.update.scripted_upsert.or(false) == true && this.update.script.source.type() != "string" { "field update.scripted_upsert requires field update.script" } else { null },
].filter(v -> v != null)`)
}

//------------------------------------------------------------------------------

type esoV2Config struct {
	clientConfig elasticsearch.Config

	timeout         time.Duration
	retryOnConflict *int
	numWorkers      int
	docMapping      *bloblang.Executor
	upsertMapping   *bloblang.Executor
	hasScript       bool
	scriptSource    string
	scriptLang      string
	scriptParams    *bloblang.Executor
	scriptedUpsert  bool

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
	if conf.numWorkers, err = pConf.FieldInt(esoV2FieldNumWorkers); err != nil {
		return
	}
	if conf.numWorkers < 0 {
		err = fmt.Errorf("field %s must be 0 or more, got %d", esoV2FieldNumWorkers, conf.numWorkers)
		return
	}
	if pConf.Contains(esoV2FieldUpdate) {
		if err = conf.parseUpdate(pConf.Namespace(esoV2FieldUpdate)); err != nil {
			return
		}
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
	if pConf.Contains(esoV2FieldAPIKey) {
		if conf.clientConfig.APIKey, err = pConf.FieldString(esoV2FieldAPIKey); err != nil {
			return
		}
	}

	if conf.clientConfig.CompressRequestBody, err = pConf.FieldBool(esoV2FieldCompressRequestBody); err != nil {
		return
	}

	return
}

// parseUpdate reads the update object. The combination checks repeat the
// spec's lint rules, as lint only runs on `bento lint`.
func (conf *esoV2Config) parseUpdate(uConf *service.ParsedConfig) (err error) {
	if uConf.Contains(esoV2FieldRetryOnConflict) {
		var retryOnConflict int
		if retryOnConflict, err = uConf.FieldInt(esoV2FieldRetryOnConflict); err != nil {
			return
		}
		if retryOnConflict < 0 {
			return fmt.Errorf("field %s must be 0 or more, got %d", esoV2FieldRetryOnConflict, retryOnConflict)
		}
		conf.retryOnConflict = &retryOnConflict
	}
	if uConf.Contains(esoV2FieldDocMapping) {
		if conf.docMapping, err = uConf.FieldBloblang(esoV2FieldDocMapping); err != nil {
			return
		}
	}
	if uConf.Contains(esoV2FieldUpsertMapping) {
		if conf.upsertMapping, err = uConf.FieldBloblang(esoV2FieldUpsertMapping); err != nil {
			return
		}
	}
	if uConf.Contains(esoV2FieldScript) {
		if conf.docMapping != nil {
			return fmt.Errorf("fields %s and %s cannot be combined, a scripted update sends no partial document", esoV2FieldDocMapping, esoV2FieldScript)
		}
		scriptConf := uConf.Namespace(esoV2FieldScript)
		conf.hasScript = true
		if conf.scriptSource, err = scriptConf.FieldString(esoV2FieldScriptSource); err != nil {
			return
		}
		if conf.scriptLang, err = scriptConf.FieldString(esoV2FieldScriptLang); err != nil {
			return
		}
		if scriptConf.Contains(esoV2FieldScriptParamsMapping) {
			if conf.scriptParams, err = scriptConf.FieldBloblang(esoV2FieldScriptParamsMapping); err != nil {
				return
			}
		}
	}
	if conf.scriptedUpsert, err = uConf.FieldBool(esoV2FieldScriptedUpsert); err != nil {
		return
	}
	if conf.scriptedUpsert && !conf.hasScript {
		return fmt.Errorf("field %s requires field %s", esoV2FieldScriptedUpsert, esoV2FieldScript)
	}
	return nil
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
		errorMu.Lock()
		defer errorMu.Unlock()
		if batchErr == nil {
			batchErr = service.NewBatchError(batch, err)
		}
		batchErr.Failed(i, err)
	}

	batchInterpolator := eso.newInterpolationExecutor(batch)

	updateExecs := eso.newUpdateExecutors(batch)

	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: eso.client,
		OnError: func(ctx context.Context, err error) {
			eso.log.Errorf("Bulk Indexer Error: %v", err)
		},
		OnFlushStart: func(context.Context) context.Context { return ctx },
		Timeout:      eso.conf.timeout,
		NumWorkers:   eso.conf.numWorkers,
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
			updateBody, err := eso.updateBody(i, action, msgBytes, updateExecs)
			if err != nil {
				batchErrFailed(i, err)
				continue
			}
			updateBodyBytes, err := json.Marshal(updateBody)
			if err != nil {
				batchErrFailed(i, err)
				continue
			}
			err = indexer.Add(ctx, esutil.BulkIndexerItem{
				Index:           index,
				Action:          "update",
				DocumentID:      id,
				Routing:         routing,
				RetryOnConflict: eso.conf.retryOnConflict,
				Body:            bytes.NewReader(updateBodyBytes),
				OnSuccess:       onSuccessHandler(eso),
				OnFailure:       onFailureHandler(i, batchErrFailed),
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
				OnFailure:  onFailureHandler(i, batchErrFailed),
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
				OnFailure:  onFailureHandler(i, batchErrFailed),
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

// updateExecutors holds the per-batch executors of the update mappings, nil
// when the mapping is not set.
type updateExecutors struct {
	doc          *service.MessageBatchBloblangExecutor
	upsert       *service.MessageBatchBloblangExecutor
	scriptParams *service.MessageBatchBloblangExecutor
}

func (eso *EsOutput) newUpdateExecutors(batch service.MessageBatch) (ue updateExecutors) {
	if eso.conf.docMapping != nil {
		ue.doc = batch.BloblangExecutor(eso.conf.docMapping)
	}
	if eso.conf.upsertMapping != nil {
		ue.upsert = batch.BloblangExecutor(eso.conf.upsertMapping)
	}
	if eso.conf.scriptParams != nil {
		ue.scriptParams = batch.BloblangExecutor(eso.conf.scriptParams)
	}
	return ue
}

// queryOr returns the mapping result for message i, or fallback when the
// mapping is not set or deletes the message.
func queryOr(exec *service.MessageBatchBloblangExecutor, i int, fallback json.RawMessage) (json.RawMessage, error) {
	if exec == nil {
		return fallback, nil
	}
	msg, err := exec.Query(i)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return fallback, nil
	}
	return msg.AsBytes()
}

// updateBody builds the body of a bulk update item for message i: a partial
// document or a script, plus the document to index when it does not exist
// yet (the upsert_mapping result, if any).
func (eso *EsOutput) updateBody(i int, action string, msgBytes []byte, ue updateExecutors) (map[string]any, error) {
	upsert, err := queryOr(ue.upsert, i, nil)
	if err != nil {
		return nil, err
	}
	if eso.conf.hasScript {
		return eso.scriptUpdateBody(i, action, msgBytes, upsert, ue)
	}
	return docUpdateBody(i, action, msgBytes, upsert, ue)
}

func docUpdateBody(i int, action string, msgBytes, upsert json.RawMessage, ue updateExecutors) (map[string]any, error) {
	doc, err := queryOr(ue.doc, i, msgBytes)
	if err != nil {
		return nil, err
	}
	if upsert != nil {
		// doc_as_upsert would make Elasticsearch ignore the upsert document
		return map[string]any{"doc": doc, "upsert": upsert}, nil
	}
	return map[string]any{"doc": doc, "doc_as_upsert": action == "upsert"}, nil
}

func (eso *EsOutput) scriptUpdateBody(i int, action string, msgBytes, upsert json.RawMessage, ue updateExecutors) (map[string]any, error) {
	params, err := queryOr(ue.scriptParams, i, msgBytes)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"script": map[string]any{
			"source": eso.conf.scriptSource,
			"lang":   eso.conf.scriptLang,
			"params": params,
		},
	}
	// the upsert action indexes the message itself when there is no
	// upsert_mapping result, like a partial-document upsert does
	if upsert == nil && action == "upsert" {
		upsert = msgBytes
	}
	if eso.conf.scriptedUpsert {
		body["scripted_upsert"] = true
		// scripted_upsert runs the script against the upsert document, so one is required
		if upsert == nil {
			upsert = json.RawMessage(`{}`)
		}
	}
	if upsert != nil {
		body["upsert"] = upsert
	}
	return body, nil
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

func onFailureHandler(i int, batchErrFailed func(i int, err error)) func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
	return func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
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

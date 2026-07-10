package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	espFieldURLs         = "urls"
	espFieldAction       = "action"
	espFieldIndex        = "index"
	espFieldID           = "id"
	espFieldArgsMapping  = "args_mapping"
	espFieldTLS          = "tls"
	espFieldAuth         = "basic_auth"
	espFieldAuthEnabled  = "enabled"
	espFieldAuthUsername = "username"
	espFieldAuthPassword = "password"
	espFieldAPIKey       = "api_key"
)

func ProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Summary("Queries Elasticsearch and replaces messages with the result.").
		Description(`
Runs a search or document lookup against Elasticsearch for each message in a
batch and replaces the message content with the result.

If the query fails the message is left unchanged and the error is set on the
message — it can be caught downstream with
`+"[`catch`](/docs/components/processors/catch)"+` or handled via
`+"[`try`](/docs/configuration/error_handling)"+`.

## Actions

`+"### `search`"+`

Executes a search query against the given index. The query body is built by
`+"`args_mapping`"+`, which must evaluate to an object containing a valid
[Elasticsearch Query DSL](https://www.elastic.co/docs/explore-analyze/query-filter/languages/querydsl)
document. The result written to the message is an array of `+"`_source`"+` objects,
one per hit — equivalent to how `+"`sql_raw`"+` returns an array of rows.

`+"### `get`"+`

Retrieves a single document by ID. The `+"`id`"+` field is required and supports
interpolation. The result is the `+"`_source`"+` object of the matched document.

`+"### `delete`"+`

Deletes the document identified by `+"`id`"+` from the given index. The message
content is left unchanged (equivalent to `+"`exec_only: true`"+` in `+"`sql_raw`"+`).

### Search Metadata
- `+"`es_index`"+`
- `+"`es_took_ms`"+`
- `+"`es_result_count`"+`
- `+"`es_total_hits`"+`

### Get Metadata
- `+"`es_index`"+`
- `+"`es_id`"+`
- `+"`es_found`"+`

### Delete Metadata
- `+"`es_index`"+`
- `+"`es_id`"+`
- `+"`es_delete_result`"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

## Result mapping

By default the processor overwrites the entire message with the Elasticsearch
result. To merge the result into an existing message or extract a subset of
fields, wrap this processor in a
`+"[`branch`](/docs/components/processors/branch)"+` and use its `+"`result_map`"+`.
`).
		LintRule(`
      root = if (
  this.action == "get" ||
  this.action == "delete"
) && (!this.exists("id") || this.id == "") {
  [ "field 'id' is required when action is '%v'".format(this.action) ]
}
    `).
		Fields(
			service.NewStringListField(espFieldURLs).
				Description("A list of Elasticsearch URLs to connect to. When using an environment variable, provide URLs as a YAML array rather than a single comma-separated string. For example, `ES_URLS=http://es1:9200,http://es2:9200` will be interpreted as a single URL and may result in a malformed address error.").
				Example([]string{"http://localhost:9200"}),
			service.NewStringEnumField(espFieldAction, "search", "get", "delete").
				Description("The operation to perform against Elasticsearch.").
				Default("search"),
			service.NewInterpolatedStringField(espFieldIndex).
				Description("The index to query."),
			service.NewInterpolatedStringField(espFieldID).
				Description("The document ID. Required for `get` and `delete` actions. Supports interpolation.").
				Optional(),
			service.NewBloblangField(espFieldArgsMapping).
				Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) that produces the request body for `search` actions. The mapping must evaluate to an object containing a valid Elasticsearch Query DSL document.").
				Example(`root = { "query": { "term": { "user_id": this.user_id } }`).
				Example(`root = {
  "query": {
    "bool": {
      "must": [
        { "match": { "status": this.status } },
        { "range": { "created_at": { "gte": this.since } } }
      ]
    }
  },
  "size": 10
}`).
				Optional(),
			service.NewTLSToggledField(espFieldTLS),
			service.NewObjectField(espFieldAuth,
				service.NewBoolField(espFieldAuthEnabled).
					Description("Whether to use basic authentication in requests.").
					Default(false),
				service.NewStringField(espFieldAuthUsername).
					Description("A username to authenticate as.").
					Default(""),
				service.NewStringField(espFieldAuthPassword).
					Description("A password to authenticate with.").
					Default("").Secret(),
			).Description("Allows you to specify basic authentication.").
				Advanced().
				Optional(),
			service.NewStringField(espFieldAPIKey).
				Description("A Base64-encoded token for authorization; if set, overrides basic auth.").
				Optional(),
		).
		Example(
			"Enrich message with document lookup",
			`For each incoming message, fetch the matching Elasticsearch document by ID and merge its fields into the message using a `+"`branch`"+` processor.`,
			`
pipeline:
  processors:
    - branch:
        processors:
          - elasticsearch:
              urls:
                - http://localhost:9200
              action: get
              index: users
              id: ${! this.user_id }
        result_map: root = this.assign(result)
`,
		).
		Example(
			"Search and attach hits as a field",
			"Run a search built from the message and attach the hits array under a new key.",
			`
pipeline:
  processors:
    - branch:
        processors:
          - elasticsearch:
              urls:
                - http://localhost:9200
              action: search
              index: orders
              args_mapping: |
                root = {
                  "query": {
                    "term": { "customer_id": this.customer_id }
                  },
                  "size": 5
                }
        result_map: root.recent_orders = this
`,
		).
		Example(
			"Delete a document (exec-only)",
			"Remove a document from Elasticsearch. The message is passed through unchanged.",
			`
pipeline:
  processors:
    - elasticsearch:
        urls:
          - http://localhost:9200
        action: delete
        index: sessions
        id: ${! this.session_id }
`,
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"elasticsearch",
		ProcessorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type esProcessorConfig struct {
	clientConfig elasticsearch.Config

	action    string
	indexExpr *service.InterpolatedString
	idExpr    *service.InterpolatedString

	argsMapping *bloblang.Executor
}

func esProcessorConfigFromParsed(pConf *service.ParsedConfig) (conf esProcessorConfig, err error) {
	if conf.clientConfig.Addresses, err = pConf.FieldStringList(espFieldURLs); err != nil {
		return
	}

	if conf.action, err = pConf.FieldString(espFieldAction); err != nil {
		return
	}

	if conf.indexExpr, err = pConf.FieldInterpolatedString(espFieldIndex); err != nil {
		return
	}

	if pConf.Contains(espFieldID) {
		if conf.idExpr, err = pConf.FieldInterpolatedString(espFieldID); err != nil {
			return
		}
	}

	if pConf.Contains(espFieldArgsMapping) {
		if conf.argsMapping, err = pConf.FieldBloblang(espFieldArgsMapping); err != nil {
			return
		}
	}

	authConf := pConf.Namespace(espFieldAuth)
	if enabled, _ := authConf.FieldBool(espFieldAuthEnabled); enabled {
		if conf.clientConfig.Username, err = authConf.FieldString(espFieldAuthUsername); err != nil {
			return
		}
		if conf.clientConfig.Password, err = authConf.FieldString(espFieldAuthPassword); err != nil {
			return
		}
	}

	if pConf.Contains(espFieldAPIKey) {
		if conf.clientConfig.APIKey, err = pConf.FieldString(espFieldAPIKey); err != nil {
			return
		}
	}

	tlsConf, tlsEnabled, tlsErr := pConf.FieldTLSToggled(espFieldTLS)
	if tlsErr != nil {
		err = tlsErr
		return
	}
	if tlsEnabled {
		conf.clientConfig.Transport = &http.Transport{
			TLSClientConfig: tlsConf,
		}
	}

	return
}

type EsProcessor struct {
	log    *service.Logger
	conf   esProcessorConfig
	client *elasticsearch.Client
}

func NewProcessorFromConfig(pConf *service.ParsedConfig, mgr *service.Resources) (*EsProcessor, error) {
	conf, err := esProcessorConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	client, err := elasticsearch.NewClient(conf.clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}
	return &EsProcessor{
		log:    mgr.Logger(),
		conf:   conf,
		client: client,
	}, nil
}

func (p *EsProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	indexExecutor := batch.InterpolationExecutor(p.conf.indexExpr)
	var idExecutor *service.MessageBatchInterpolationExecutor
	if p.conf.idExpr != nil {
		idExecutor = batch.InterpolationExecutor(p.conf.idExpr)
	}
	var argsExecutor *service.MessageBatchBloblangExecutor
	if p.conf.argsMapping != nil {
		argsExecutor = batch.BloblangExecutor(p.conf.argsMapping)
	}

	batch = batch.Copy()

	for i, msg := range batch {
		index, err := indexExecutor.TryString(i)
		if err != nil {
			msg.SetError(fmt.Errorf("index interpolation error: %w", err))
			continue
		}

		switch p.conf.action {
		case "search":
			if err := p.processSearch(ctx, i, msg, index, argsExecutor); err != nil {
				p.log.Debugf("Elasticsearch search failed: %v", err)
				msg.SetError(err)
			}
		case "get":
			if idExecutor == nil {
				msg.SetError(errors.New("action 'get' requires 'id' field"))
				continue
			}
			id, err := idExecutor.TryString(i)
			if err != nil {
				p.log.Debugf("ID interpolation error: %v", err)
				msg.SetError(fmt.Errorf("id interpolation error: %w", err))
				continue
			}
			if err := p.processGet(ctx, msg, index, id); err != nil {
				p.log.Debugf("Elasticsearch get failed: %v", err)
				msg.SetError(err)
			}
		case "delete":
			if idExecutor == nil {
				msg.SetError(errors.New("action 'delete' requires 'id' field"))
				continue
			}
			id, err := idExecutor.TryString(i)
			if err != nil {
				p.log.Debugf("ID interpolation error: %v", err)
				msg.SetError(fmt.Errorf("id interpolation error: %w", err))
				continue
			}
			if err := p.processDelete(ctx, msg, index, id); err != nil {
				p.log.Debugf("Elasticsearch delete failed: %v", err)
				msg.SetError(err)
			}
		default:
			msg.SetError(fmt.Errorf("elasticsearch processor: unknown action %q", p.conf.action))
		}
	}

	return []service.MessageBatch{batch}, nil
}

func (p *EsProcessor) Close(context.Context) error {
	return nil
}

func (p *EsProcessor) processSearch(ctx context.Context, i int, msg *service.Message, index string, argsExecutor *service.MessageBatchBloblangExecutor) error {
	var bodyBytes []byte

	if argsExecutor != nil {
		resMsg, err := argsExecutor.Query(i)
		if err != nil {
			return fmt.Errorf("args_mapping failed: %w", err)
		}
		structured, err := resMsg.AsStructured()
		if err != nil {
			return fmt.Errorf("args_mapping returned non-structured result: %w", err)
		}
		if bodyBytes, err = json.Marshal(structured); err != nil {
			return fmt.Errorf("failed to marshal search body: %w", err)
		}
	}

	res, err := p.client.Search(
		p.client.Search.WithContext(ctx),
		p.client.Search.WithIndex(index),
		p.client.Search.WithBody(bytes.NewReader(bodyBytes)),
	)
	if err != nil {
		return fmt.Errorf("elasticsearch search request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch search returned error: %s", res.Status())
	}

	respBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read elasticsearch search response: %w", err)
	}

	return extractSearchHitsAndMeta(msg, index, respBytes)
}

func (p *EsProcessor) processGet(ctx context.Context, msg *service.Message, index, id string) error {
	res, err := p.client.Get(index, id,
		p.client.Get.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("elasticsearch get request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch get returned error %s for document id %q", res.Status(), id)
	}

	respBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read elasticsearch get response: %w", err)
	}

	return extractGetSourceAndMeta(msg, index, id, respBytes)
}

func (p *EsProcessor) processDelete(ctx context.Context, msg *service.Message, index, id string) error {
	res, err := p.client.Delete(
		index,
		id,
		p.client.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("elasticsearch delete request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf(
			"elasticsearch delete returned error %s for document id %q",
			res.Status(),
			id,
		)
	}

	var response struct {
		Result string `json:"result"`
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode delete response: %w", err)
	}

	msg.MetaSetMut("es_index", index)
	msg.MetaSetMut("es_id", id)
	msg.MetaSetMut("es_delete_result", response.Result)

	return nil
}

func extractSearchHitsAndMeta(msg *service.Message, index string, body []byte) error {
	var response struct {
		Took int `json:"took"`
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse elasticsearch search response: %w", err)
	}

	msg.MetaSetMut("es_index", index)
	msg.MetaSetMut("es_took_ms", response.Took)
	msg.MetaSetMut("es_result_count", len(response.Hits.Hits))
	msg.MetaSetMut("es_total_hits", response.Hits.Total.Value)

	sources := make([]any, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		var src any
		if err := json.Unmarshal(hit.Source, &src); err != nil {
			return fmt.Errorf("failed to parse _source from search hit: %w", err)
		}
		sources = append(sources, src)
	}

	msg.SetStructuredMut(sources)
	return nil
}

func extractGetSourceAndMeta(msg *service.Message, index, id string, body []byte) error {
	var response struct {
		Source json.RawMessage `json:"_source"`
		Found  bool            `json:"found"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse elasticsearch get response: %w", err)
	}

	msg.MetaSetMut("es_index", index)
	msg.MetaSetMut("es_id", id)
	msg.MetaSetMut("es_found", response.Found)

	var src any
	if err := json.Unmarshal(response.Source, &src); err != nil {
		return fmt.Errorf("failed to parse _source from get response: %w", err)
	}

	msg.SetStructuredMut(src)
	return nil
}

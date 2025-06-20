package elasticsearch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esutil"
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
		Categories("services").
		Summary(`Publishes messages into an Elasticsearch index. If the index does not exist then it is created with a dynamic mapping.`).
		Description(`TODO`).
		Fields(
			service.NewStringListField(esoV2FieldURLs).
				Description("").
				Example([]string{"http://localhost:9200"}),
			service.NewInterpolatedStringField(esoV2FieldIndex).
				Description(""),
			service.NewInterpolatedStringEnumField(esoV2FieldAction, "create", "delete", "index", "update").
				Default("index").
				Description(""),
			service.NewStringField(esoV2FieldPipeline).
				Default("").
				Description(""),
			service.NewInterpolatedStringField(esoV2FieldID).
				Description(""),
			service.NewInterpolatedStringField(esoV2FieldRouting).
				Default("").
				Description(""),
			service.NewBoolField(esoV2FieldDiscoverNodesOnStart).
				Default(false).
				Description(""),
			service.NewDurationField(esoV2FieldDiscoverNodesInterval).
				Default("0s").
				Description(""),
			service.NewDurationField(esoV2FieldTimeout).
				Default("0s").
				Description(""),
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
		)
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

	batchInterpolator := eso.newInterpolationExecutor(&batch)

	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: eso.client,
		OnError: func(ctx context.Context, err error) {
			log.Printf("Bulk Indexer Error: %s\n", err)
		},
		Timeout: eso.conf.timeout,
	})
	if err != nil {
		return err
	}

	for i, msg := range batch {
		index, action, id, routing, err := batchInterpolator.exec(i)
		if err != nil {
			eso.log.Errorf("Failed to execute bloblang interpolation: %v\n", err)
			msg.SetError(err)
			continue
		}

		msgBytes, err := msg.AsBytes()
		if err != nil {
			eso.log.Errorf("Failed to get message bytes: %v\n", err)
			msg.SetError(err)
			continue
		}

		switch action {
		case "delete":
			err = indexer.Add(ctx, esutil.BulkIndexerItem{
				Index:      index,
				Action:     action,
				DocumentID: id,
				Routing:    routing,
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					eso.log.Trace(fmt.Sprintf("Indexed %s/%s", res.Index, res.DocumentID))
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						msg.SetError(err)
					} else {
						if res.Error.Type != "" {
							log.Printf("%s:%s", res.Error.Type, res.Error.Reason)
						} else {
							log.Printf("%s/%s %s (%d)", res.Index, res.DocumentID, res.Result, res.Status)
						}
					}
				},
			})
			if err != nil {
				eso.log.Errorf("Failed to add message to elasticsearch indexer: %v\n", err)
				msg.SetError(err)
			}
		default:
			err = indexer.Add(ctx, esutil.BulkIndexerItem{
				Index:      index,
				Action:     action,
				DocumentID: id,
				Routing:    routing,
				Body:       bytes.NewReader(msgBytes),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					eso.log.Trace(fmt.Sprintf("Indexed %s/%s", res.Index, res.DocumentID))
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						msg.SetError(err)
					} else {
						if res.Error.Type != "" {
							log.Printf("%s:%s", res.Error.Type, res.Error.Reason)
						} else {
							log.Printf("%s/%s %s (%d)", res.Index, res.DocumentID, res.Result, res.Status)
						}
					}
				},
			})
			if err != nil {
				eso.log.Errorf("Failed to add message to elasticsearch indexer: %v\n", err)
				msg.SetError(err)
			}
		}
	}

	err = indexer.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (eso *EsOutput) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

func (eso *EsOutput) newInterpolationExecutor(batch *service.MessageBatch) (ie interpolationExecutor) {
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

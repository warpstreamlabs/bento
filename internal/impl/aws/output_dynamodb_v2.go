package aws

import (
	"context"
	"fmt"

	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// DynamoDB Output Fields
	ddboFieldTableV2            = "table"
	ddboFieldJSONMapColumnsV2   = "json_map_columns"
	ddboFieldJSONMapDataTypesV2 = "json_map_datatypes"
	//ddboFieldOmitIfEmptyV2        = "omit_if_empty"
	//ddboFieldTTLV2                = "ttl"
	//ddboFieldTTLKeyV2             = "ttl_key"
	//ddboFieldDeleteV2             = "delete"
	//ddboFieldDeleteConditionV2    = "condition"
	//ddboFieldDeletePartitionKeyV2 = "partition_key"
	//ddboFieldDeleteSortKeyV2      = "sort_key"
	ddboFieldBatchingV2 = "batching"
	//ddboFieldJSONNumberTypeV2 = "json_number_type"

	//ddboJSONNumberTypeStringV2 = "string"
	//ddboJSONNumberTypeNumberV2 = "number"
)

func dynamoDBOutputSpecV2() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "AWS").
		Version("1.20.0").
		Fields(
			service.NewStringField(ddboFieldTableV2).
				Description("The table to store messages in."),
			service.NewStringMapField(ddboFieldJSONMapColumnsV2).
				Description("A map of column keys to [field paths](/docs/configuration/field_paths) pointing to value data within messages.").
				Default(map[string]any{}).
				Example(map[string]any{
					"user":           "path.to.user",
					"whole_document": ".",
				}).
				Example(map[string]string{
					"": ".",
				}),
			service.NewStringMapField(ddboFieldJSONMapDataTypesV2).
				Description("").
				Default(map[string]any{}).
				Example(map[string]any{
					"user":           "S",
					"whole_document": "N",
				}).
				Example(map[string]string{
					"": "S",
				}),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(ddboFieldBatchingV2)).
		Fields(config.SessionFields()...)
}

func init() {
	err := service.RegisterBatchOutput("aws_dynamodb_v2", dynamoDBOutputSpecV2(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(ddboFieldBatchingV2); err != nil {
				return
			}
			out, err = newDynamoDBWriterV2FromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type dynamoDBWriterV2 struct {
	client dynamoDBAPI

	table            string
	jsonMapColumns   map[string]string
	jsonMapDataTypes map[string]string

	aConf aws.Config
}

func newDynamoDBWriterV2FromParsed(conf *service.ParsedConfig, _ *service.Resources) (*dynamoDBWriterV2, error) {
	table, err := conf.FieldString(ddboFieldTableV2)
	if err != nil {
		return nil, err
	}

	jsonMapColumns, err := conf.FieldStringMap(ddboFieldJSONMapColumnsV2)
	if err != nil {
		return nil, err
	}

	jsonMapDataTypes, err := conf.FieldStringMap(ddboFieldJSONMapDataTypesV2)
	if err != nil {
		return nil, err
	}

	aConf, err := GetSession(context.TODO(), conf)
	if err != nil {
		return nil, err
	}

	return &dynamoDBWriterV2{
		table:            table,
		jsonMapColumns:   jsonMapColumns,
		jsonMapDataTypes: jsonMapDataTypes,
		aConf:            aConf,
	}, nil
}

func (ddw *dynamoDBWriterV2) Connect(ctx context.Context) error {
	if ddw.client != nil {
		return nil
	}

	ddw.client = dynamodb.NewFromConfig(ddw.aConf)
	return nil
}

func (ddw *dynamoDBWriterV2) WriteBatch(ctx context.Context, msgBatch service.MessageBatch) error {
	writeReqs := make([]types.WriteRequest, 0, len(msgBatch))

	for _, msg := range msgBatch {
		wr, err := ddw.addPutRequest(msg)
		if err != nil {
			return err
		}
		writeReqs = append(writeReqs, wr)
	}

	for start := 0; start < len(writeReqs); start += 25 {
		chunk := writeReqs[start:min(start+25, len(writeReqs))]
		_, err := ddw.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				ddw.table: chunk,
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (ddw *dynamoDBWriterV2) Close(ctx context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

func (ddw *dynamoDBWriterV2) addPutRequest(msg *service.Message) (x types.WriteRequest, err error) {
	jRoot, err := msg.AsStructured()
	if err != nil {
		return types.WriteRequest{}, err
	}
	gRoot := gabs.Wrap(jRoot)

	items := map[string]types.AttributeValue{}

	for k, v := range ddw.jsonMapColumns {
		typ := ddw.jsonMapDataTypes[k]

		cont := gRoot.Path(v)
		val := fmt.Sprintf("%v", cont.Data())

		av := stringToDynAttr(val, typ)
		items[k] = av
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: items,
		},
	}, nil
}

func stringToDynAttr(val string, typ string) types.AttributeValue {
	switch typ {
	case "S":
		return &types.AttributeValueMemberS{
			Value: val,
		}
	case "N":
		return &types.AttributeValueMemberN{
			Value: val,
		}
	default:
		panic("NOT IMPLEMENTED")
	}
}

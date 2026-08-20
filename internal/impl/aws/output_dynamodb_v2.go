package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

var (
	ErrItemTooBig            = errors.New("Item too big")
	ErrPartitionKeyTooBig    = errors.New("Partition key too big")
	ErrPartitionKeyNotUnique = errors.New("Keys not unique in message batch")
	ErrSortKeyTooBig         = errors.New("Sort key too big")
)

const (
	// DynamoDB Output Fields
	ddboFieldTableV2            = "table"
	ddboFieldParititionKeyV2    = "partition_key"
	ddboFieldSortKeyV2          = "sort_key"
	ddboFieldJSONMapColumnsV2   = "json_map_columns"
	ddboFieldJSONMapDataTypesV2 = "json_map_datatypes"
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
			service.NewStringField(ddboFieldParititionKeyV2).
				Description("The name of the partition key column.").
				Default(""),
			service.NewStringField(ddboFieldSortKeyV2).
				Description("The name of the sort key column (if any).").
				Default(""),
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
	partitionKey     string
	sortKey          string
	jsonMapColumns   map[string]string
	jsonMapDataTypes map[string]string

	aConf aws.Config
}

func newDynamoDBWriterV2FromParsed(conf *service.ParsedConfig, _ *service.Resources) (*dynamoDBWriterV2, error) {
	table, err := conf.FieldString(ddboFieldTableV2)
	if err != nil {
		return nil, err
	}

	partitionKey, err := conf.FieldString(ddboFieldParititionKeyV2)
	if err != nil {
		return nil, err
	}

	sortKey, err := conf.FieldString(ddboFieldSortKeyV2)
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
		partitionKey:     partitionKey,
		sortKey:          sortKey,
		jsonMapColumns:   jsonMapColumns,
		jsonMapDataTypes: jsonMapDataTypes,
		aConf:            aConf,
	}, nil
}

func (ddw *dynamoDBWriterV2) Connect(ctx context.Context) error {
	if ddw.client != nil {
		return nil
	}

	client := dynamodb.NewFromConfig(ddw.aConf)
	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &ddw.table,
	})
	if err != nil {
		return err
	} else if out == nil || out.Table == nil || out.Table.TableStatus != types.TableStatusActive {
		return fmt.Errorf("dynamodb table '%s' must be active", ddw.table)
	}

	// if the partition_key is supplied check it, if the sort_key is supplied check that too
	for _, v := range out.Table.KeySchema {
		if v.KeyType == "HASH" {
			if ddw.partitionKey != "" && *v.AttributeName != ddw.partitionKey {
				return fmt.Errorf("supplied partition_key doesn't match Table schema")
			}
		}
		if v.KeyType == "RANGE" {
			if ddw.sortKey != "" && *v.AttributeName != ddw.sortKey {
				return fmt.Errorf("supplied sort_key doesn't match Table schema")
			}
		}
	}

	ddw.client = client

	return nil
}

func (ddw *dynamoDBWriterV2) WriteBatch(ctx context.Context, msgBatch service.MessageBatch) error {
	writeReqs := make([]types.WriteRequest, 0, len(msgBatch))

	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(msgBatch, err)
		}
		batchErr.Failed(i, err)
	}

	keys := []string{}
	for i, msg := range msgBatch {
		jRoot, err := msg.AsStructured()
		if err != nil {
			return err
		}
		gRoot := gabs.Wrap(jRoot)

		pCont := gRoot.Path(ddw.partitionKey)
		val := fmt.Sprintf("%v", pCont.Data())

		sCont := gRoot.Path(ddw.sortKey)
		val = val + fmt.Sprintf("%v", sCont.Data())

		if slices.Contains(keys, val) {
			return ErrPartitionKeyNotUnique
		} else {
			keys = append(keys, val)
		}

		var wr types.WriteRequest
		if len(ddw.jsonMapDataTypes) == 0 {
			wr, err = ddw.addPutRequestInferTypes(gRoot)
		} else {
			wr, err = ddw.addPutRequest(gRoot)
		}
		if err != nil {
			if errors.Is(err, ErrItemTooBig) || errors.Is(err, ErrPartitionKeyTooBig) || errors.Is(err, ErrSortKeyTooBig) {
				batchErrFailed(i, err)
			} else {
				return err
			}
		} else {
			writeReqs = append(writeReqs, wr)
		}
	}

	// wait group - keep track of errors
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

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	return nil
}

func (ddw *dynamoDBWriterV2) Close(ctx context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

func (ddw *dynamoDBWriterV2) addPutRequest(gRoot *gabs.Container) (x types.WriteRequest, err error) {
	attrValues := map[string]types.AttributeValue{}

	itemSize := 0
	for k, v := range ddw.jsonMapColumns {

		typ := ddw.jsonMapDataTypes[k]

		cont := gRoot.Path(v)
		val := fmt.Sprintf("%v", cont.Data())
		itemSize += len(val)
		if itemSize >= 400000 {
			return types.WriteRequest{}, ErrItemTooBig
		}

		if ddw.partitionKey == k && len(val) > 2048 {
			return types.WriteRequest{}, ErrPartitionKeyTooBig
		}
		if ddw.sortKey == k && len(val) > 1024 {
			return types.WriteRequest{}, ErrSortKeyTooBig
		}

		av := stringToDynAttr(val, typ)
		attrValues[k] = av
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: attrValues,
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

//------------------------------------------------------------------------------

func (ddw *dynamoDBWriterV2) addPutRequestInferTypes(gRoot *gabs.Container) (x types.WriteRequest, err error) {
	attrValues := map[string]types.AttributeValue{}
	for k, v := range ddw.jsonMapColumns {
		if attr, err := jsonToMap_(v, gRoot); err == nil {
			// if k == "" {
			// 	// if mv, ok := attr.(*types.AttributeValueMemberM); ok {
			// 	// 	maps.Copy(items, mv.Value)
			// 	// } else {
			// 	// 	items[k] = attr
			// 	// }
			// } else {
			attrValues[k] = attr
			// }
		} else {
			//d.log.Warnf("Unable to extract JSON map path '%v' from document: %v", v, err)
			return types.WriteRequest{}, err
		}
	}

	return types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: attrValues,
		},
	}, nil
}

func jsonToMap_(path string, root *gabs.Container) (types.AttributeValue, error) {
	cont := root
	if path != "" {
		cont = root.Path(path)
	}
	if cont == nil {
		return nil, fmt.Errorf("path '%v' not found in document", path)
	}
	return anyToAttributeValue_(cont.Data()), nil
}

func anyToAttributeValue_(root any) types.AttributeValue {
	switch v := root.(type) {
	// TODO: additional cases
	case map[string]any:
		m := make(map[string]types.AttributeValue, len(v))
		for k, v2 := range v {
			m[k] = anyToAttributeValue_(v2)
		}
		return &types.AttributeValueMemberM{
			Value: m,
		}
	case []any:
		l := make([]types.AttributeValue, len(v))
		for i, v2 := range v {
			l[i] = anyToAttributeValue_(v2)
		}
		return &types.AttributeValueMemberL{
			Value: l,
		}
	case string:
		return &types.AttributeValueMemberS{
			Value: v,
		}
	case json.Number:
		return &types.AttributeValueMemberN{
			Value: v.String(),
		}
	case float64:
		return &types.AttributeValueMemberN{
			Value: strconv.FormatFloat(v, 'f', -1, 64),
		}
	case int:
		return &types.AttributeValueMemberN{
			Value: strconv.Itoa(v),
		}
	case int64:
		return &types.AttributeValueMemberN{
			Value: strconv.Itoa(int(v)),
		}
	case bool:
		return &types.AttributeValueMemberBOOL{
			Value: v,
		}
	case nil:
		return &types.AttributeValueMemberNULL{
			Value: true,
		}
	}
	return &types.AttributeValueMemberS{
		Value: fmt.Sprintf("%v", root),
	}
}

//------------------------------------------------------------------------------

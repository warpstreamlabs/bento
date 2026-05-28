package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterInput(
		"redis_scan", redisScanInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newRedisScanInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

const (
	matchFieldName         = "match"
	dataTypeFieldName      = "data_type"
	valueFormatFieldName   = "value_format"
	scanCountFieldName     = "scan_count"
	hashScanCountFieldName = "hash_scan_count"

	redisScanDataTypeString = "string"
	redisScanDataTypeHash   = "hash"

	redisScanValueFormatStructured = "structured"
	redisScanValueFormatRaw        = "raw"

	redisScanMetaKey       = "redis_key"
	redisScanMetaHashField = "redis_hash_field"
)

func redisScanInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Scans the set of keys in the current selected database and gets their values, using the Scan and Get commands.`).
		Description(`Optionally, iterates only elements matching a blob-style pattern. For example:
- ` + "`*foo*`" + ` iterates only keys which contain ` + "`foo`" + ` in it.
- ` + "`foo*`" + ` iterates only keys starting with ` + "`foo`" + `.

This input generates a message for each key value pair in the following format:

` + "```json" + `
{"key":"foo","value":"bar"}
` + "```" + `

When ` + "`data_type`" + ` is set to ` + "`hash`" + ` this input treats matched keys as Redis hashes. It uses HSCAN to
iterate over each field and its value.

By default, ` + "`value_format`" + ` is ` + "`structured`" + ` in order to preserve the original ` + "`redis_scan`" + ` message shape
of ` + "`{\"key\":\"...\",\"value\":\"...\"}`" + `. Set it to ` + "`raw`" + ` to emit Redis values as raw message payloads.
Raw messages set Redis identity fields as metadata, using ` + "`redis_key`" + ` for Redis keys and
` + "`redis_hash_field`" + ` for hash fields.
`).
		Categories("Services")
	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	return spec.
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewStringField(matchFieldName).
			Description("Iterates only elements matching the optional glob-style pattern. By default, it matches all elements.").
			Example("*").
			Example("1*").
			Example("foo*").
			Example("foo").
			Example("*4*").
			Default("")).
		Field(service.NewStringEnumField(dataTypeFieldName, redisScanDataTypeString, redisScanDataTypeHash).
			Description("The Redis data type to read for each matched key. When set to `hash`, matched keys are scanned with HSCAN and each hash field value is emitted as an individual message.").
			Default(redisScanDataTypeString)).
		Field(service.NewStringEnumField(valueFormatFieldName, redisScanValueFormatStructured, redisScanValueFormatRaw).
			Description("The Bento message shape to emit for Redis values. The `structured` format preserves the original redis_scan key/value object shape, while `raw` emits Redis values as message payloads and stores Redis identity in metadata.").
			Default(redisScanValueFormatStructured)).
		Field(service.NewIntField(scanCountFieldName).
			Description("An optional Redis SCAN count hint for key scanning. A value of 0 preserves the Redis default scan behaviour.").
			Default(0).
			Advanced()).
		Field(service.NewIntField(hashScanCountFieldName).
			Description("An optional Redis HSCAN count hint for hash field scanning. A value of 0 preserves the Redis default scan behaviour.").
			Default(0).
			Advanced())
}

func newRedisScanInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}
	match, err := conf.FieldString(matchFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", matchFieldName, err)
	}
	dataType, err := conf.FieldString(dataTypeFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", dataTypeFieldName, err)
	}
	valueFormat, err := conf.FieldString(valueFormatFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", valueFormatFieldName, err)
	}
	scanCount, err := conf.FieldInt(scanCountFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", scanCountFieldName, err)
	}
	hashScanCount, err := conf.FieldInt(hashScanCountFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", hashScanCountFieldName, err)
	}
	r := &redisScanReader{
		client:        client,
		match:         match,
		dataType:      dataType,
		valueFormat:   valueFormat,
		scanCount:     int64(scanCount),
		hashScanCount: int64(hashScanCount),
		log:           mgr.Logger(),
	}
	return r, nil
}

type redisScanReader struct {
	match         string
	dataType      string
	valueFormat   string
	scanCount     int64
	hashScanCount int64

	client redis.UniversalClient

	iter           *redis.ScanIterator
	hashIter       *redis.ScanIterator
	currentHashKey string

	log *service.Logger
}

func (r *redisScanReader) Connect(ctx context.Context) error {
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	r.iter = r.client.Scan(context.Background(), 0, r.match, r.scanCount).Iterator()
	return r.iter.Err()
}

func (r *redisScanReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if r.dataType == redisScanDataTypeHash {
		return r.readHash(ctx)
	}
	return r.readString(ctx)
}

// readString preserves the original redis_scan output shape by default for
// backwards compatibility: a structured object of {"key":"...","value":"..."}.
// That shape works well for text Redis string values, but can be awkward for
// arbitrary binary values such as protobuf bytes because downstream binary
// decoders usually expect the message payload itself to contain the encoded
// bytes.
func (r *redisScanReader) readString(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if !r.iter.Next(ctx) {
		if err := r.iter.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, service.ErrEndOfInput
	}
	key := r.iter.Val()
	res := r.client.Get(ctx, key)
	if err := res.Err(); err != nil {
		return nil, nil, err
	}
	return r.newStringMessage(key, res.Val())
}

func (r *redisScanReader) newStringMessage(key, value string) (*service.Message, service.AckFunc, error) {
	return r.newValueMessage(
		value,
		map[string]any{
			"key":   key,
			"value": value,
		},
		map[string]string{
			redisScanMetaKey: key,
		},
	)
}

// readHash can emit each hash field as either a structured object or as a raw
// value payload. Raw output aligns with Redis list/pubsub inputs and preserves
// binary values exactly, while structured output gives text values an envelope
// close to the legacy {"key":"...","value":"..."} redis_scan shape.
func (r *redisScanReader) readHash(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for {
		if r.hashIter != nil {
			if r.hashIter.Next(ctx) {
				field := r.hashIter.Val()
				if !r.hashIter.Next(ctx) {
					if err := r.hashIter.Err(); err != nil {
						return nil, nil, err
					}
					return nil, nil, fmt.Errorf("redis HSCAN returned field without value for key %q", r.currentHashKey)
				}

				// HSCAN returns field/value pairs, so the value is the next
				// iterator element. It's read via the same proto reader as
				// HGET, so it's byte-exact for binary values (e.g. protobuf) —
				// no need for a separate HGET round-trip per field.
				value := r.hashIter.Val()
				return r.newHashMessage(r.currentHashKey, field, value)
			}
			if err := r.hashIter.Err(); err != nil {
				return nil, nil, err
			}
			r.hashIter = nil
			r.currentHashKey = ""
		}

		if !r.iter.Next(ctx) {
			if err := r.iter.Err(); err != nil {
				return nil, nil, err
			}
			return nil, nil, service.ErrEndOfInput
		}

		r.currentHashKey = r.iter.Val()
		r.hashIter = r.client.HScan(ctx, r.currentHashKey, 0, "", r.hashScanCount).Iterator()
		if err := r.hashIter.Err(); err != nil {
			return nil, nil, err
		}
	}
}

func (r *redisScanReader) newHashMessage(key, field, value string) (*service.Message, service.AckFunc, error) {
	return r.newValueMessage(
		value,
		map[string]any{
			"key":   key,
			"field": field,
			"value": value,
		},
		map[string]string{
			redisScanMetaKey:       key,
			redisScanMetaHashField: field,
		},
	)
}

func (r *redisScanReader) newValueMessage(value string, structured map[string]any, metadata map[string]string) (*service.Message, service.AckFunc, error) {
	var msg *service.Message
	if r.valueFormat == redisScanValueFormatRaw {
		msg = service.NewMessage([]byte(value))
		for k, v := range metadata {
			msg.MetaSetMut(k, v)
		}
	} else {
		msg = service.NewMessage(nil)
		msg.SetStructuredMut(structured)
	}
	return msg, func(ctx context.Context, err error) error {
		return err
	}, nil
}

func (r *redisScanReader) Close(ctx context.Context) (err error) {
	return r.client.Close()
}

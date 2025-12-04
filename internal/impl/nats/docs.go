package nats

import (
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	kvFieldBucket = "bucket"
)

func connectionNameDescription() string {
	return `### Connection Name

When monitoring and managing a production NATS system, it is often useful to
know which connection a message was send/received from. This can be achieved by
setting the connection name option when creating a NATS connection.

Bento will automatically set the connection name based off the label of the given
NATS component, so that monitoring tools between NATS and bento can stay in sync.
`
}

func inputTracingDocs() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewExtractTracingSpanMappingField().Version("1.0.0"),
		service.NewRootSpanWithLinkField().Version("1.14.0"),
	}
}

func outputTracingDocs() *service.ConfigField {
	return service.NewInjectTracingSpanMappingField().Version("1.0.0")
}
func Docs(natsComponentType string, extraFields ...*service.ConfigField) []*service.ConfigField {
	// TODO: Use `slices.Concat()` after switching to Go 1.22
	bucketName := "my_bucket"
	if natsComponentType == "KV" {
		bucketName = "my_kv_bucket"
	}
	fields := append(
		connectionHeadFields(),
		[]*service.ConfigField{
			service.NewStringField(kvFieldBucket).
				Description("The name of the " + natsComponentType + " bucket.").Example(bucketName),
		}...,
	)
	fields = append(fields, extraFields...)
	fields = append(fields, connectionTailFields()...)

	return fields
}

package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	s3pFieldBucket             = "bucket"
	s3pFieldKey                = "key"
	s3pFieldForcePathStyleURLs = "force_path_style_urls"
	s3pFieldDeleteObjects      = "delete_objects"
)

func s3pSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Performs an S3 GetObject operation using the `bucket` + `key` provided in the config and replaces the original message parts with the content retrieved from S3.").
		Version("1.4.0").
		Description(`
This `+"`aws_s3`"+` processor is offered as an alternative to [streaming-objects-on-upload-with-sqs](/docs/components/inputs/aws_s3#streaming-objects-on-upload-with-sqs).

This `+"`aws_s3`"+` processor may be preferable to the `+"`aws_s3`"+` input with the field `+"`sqs`"+` in the following situations:

- You require data from the SQS message as well the S3 Object Data
- You are using a similar pattern to the [streaming-objects-on-upload-with-sqs](/docs/components/inputs/aws_s3#streaming-objects-on-upload-with-sqs) but with a different queue technology such as RabbitMQ
- You need to access some data from S3 as part of the processing stage of your config

In order to keep the original payload instead of replacing it entirely, you can use the [branch](/docs/components/processors/branch) processor.

## Scanner

Note that this processor is _odd_ because it has a [scanner](/docs/components/scanners/about) field.
This means that depending on the scanner used, it can change the amount of messages.
Therefore if you plan on using it inside a [branch](/docs/components/processors/branch) processor, you would need a scanner that doesn't alter the number of messages, such as the default `+"`to_the_end`"+` scanner.

## Metadata

This input adds the following metadata fields to each message:

`+"```"+`
- s3_key
- s3_bucket
- s3_last_modified_unix
- s3_last_modified (RFC3339)
- s3_content_type
- s3_content_encoding
- s3_content_length
- s3_version_id
- All user defined metadata
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries). Note that user defined metadata is case insensitive within AWS, and it is likely that the keys will be received in a capitalized form, if you wish to make them consistent you can map all metadata keys to lower or uppercase using a Bloblang mapping such as `+"`meta = meta().map_each_key(key -> key.lowercase())`"+`.`).
		Categories("Services", "AWS").
		Example(
			"Amazon SQS Extended Client Library",
			`This example shows how to create a Bento config that will allow connecting to SQS queues that are using the `+"[Amazon SQS Extended Client Library](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html)",
			`
input:
  label: sqs_extended_client

  aws_sqs:
    url: https://sqs.${AWS_REGION}.amazonaws.com/${AWS_ACCOUNT_ID}/sqs-extended-client-queue

  processors:
    - switch: 
        # check it's a large message:
        - check: this.0 == "com.amazon.sqs.javamessaging.MessageS3Pointer" 
        # use the aws_s3 processor to download it 
          processors:
            - aws_s3:
                bucket: ${! this.1.s3BucketName }
                key: ${! this.1.s3Key }
`).
		Example(
			"Connection to S3 API-Compatable services",
			`This example shows how to connect to "S3 API-Compatable services" - for instance minio.`,
			`
pipeline:
  processors: 
    - aws_s3: 
        bucket: mybucket
        key: events-json-stream/1756305057033860000.json
        endpoint: http://localhost:9000
        force_path_style_urls: true
        region: us-east-1
        credentials:
          id: minioadmin
          secret: minioadmin
`).
		Fields(
			service.NewInterpolatedStringField(s3pFieldBucket).
				Description("The bucket to perform the GetObject operation on."),
			service.NewInterpolatedStringField(s3pFieldKey).
				Description("The key of the object you wish to retrive."),
			service.NewBoolField(s3pFieldForcePathStyleURLs).
				Description("Forces the client API to use path style URLs for downloading keys, which is often required when connecting to custom endpoints.").
				Default(false),
			service.NewBoolField(s3pFieldDeleteObjects).
				Description("Whether to delete downloaded objects from the bucket once they are processed. Note: the S3 Object will be deleted from AWS as soon as this processor has consumed the object.").
				Version("1.5.0").
				Default(false).
				Advanced(),
		).
		Fields(config.SessionFields()...).
		Fields(service.NewScannerField("scanner").
			Description("The [scanner](/docs/components/scanners/about) by which the stream of bytes consumed will be broken out into individual messages. Scanners are useful for processing large sources of data without holding the entirety of it within memory. For example, the `csv` scanner allows you to process individual CSV rows without loading the entire CSV file in memory at once.").
			Default(map[string]any{"to_the_end": map[string]any{}}).
			Optional())
}

func s3pConstructor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	awsConf, err := GetSession(context.TODO(), conf)
	if err != nil {
		return nil, err
	}

	bucket, err := conf.FieldInterpolatedString(s3pFieldBucket)
	if err != nil {
		return nil, err
	}

	key, err := conf.FieldInterpolatedString(s3pFieldKey)
	if err != nil {
		return nil, err
	}

	forcePathStyleURLs, err := conf.FieldBool(s3pFieldForcePathStyleURLs)
	if err != nil {
		return nil, err
	}

	deleteObjects, err := conf.FieldBool(s3iFieldDeleteObjects)
	if err != nil {
		return nil, err
	}

	scanner, err := conf.FieldScanner("scanner")
	if err != nil {
		return nil, err
	}

	return &s3Processor{
		aconf:              awsConf,
		Bucket:             bucket,
		Key:                key,
		ForcePathStyleURLs: forcePathStyleURLs,
		DeleteObjects:      deleteObjects,
		Scanner:            scanner,
	}, nil
}

func init() {
	err := service.RegisterBatchProcessor("aws_s3", s3pSpec(), s3pConstructor)
	if err != nil {
		panic(err)
	}
}

type s3Processor struct {
	aconf aws.Config

	Bucket *service.InterpolatedString
	Key    *service.InterpolatedString

	ForcePathStyleURLs bool
	DeleteObjects      bool

	Scanner *service.OwnedScannerCreator
}

func (p *s3Processor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {

	var bucketExecutor *service.MessageBatchInterpolationExecutor
	if p.Bucket != nil {
		bucketExecutor = batch.InterpolationExecutor(p.Bucket)
	}

	var keyExecutor *service.MessageBatchInterpolationExecutor
	if p.Key != nil {
		keyExecutor = batch.InterpolationExecutor(p.Key)
	}

	client := s3.NewFromConfig(p.aconf, func(o *s3.Options) {
		o.UsePathStyle = p.ForcePathStyleURLs
	})

	var resBatches []service.MessageBatch

	for i, msg := range batch {

		bucket, err := bucketExecutor.TryString(i)
		if err != nil {
			msg.SetError(fmt.Errorf("s3 bucket interpolation error: %w", err))
			continue
		}
		key, err := keyExecutor.TryString(i)
		if err != nil {
			msg.SetError(fmt.Errorf("s3 key interpolation error: %w", err))
			continue
		}

		obj, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			msg.SetError(err)
			continue
		}

		if p.DeleteObjects {
			defer func() {
				if err != nil {
					return
				}

				_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: &bucket,
					Key:    &key,
				})
				if err != nil {
					msg.SetError(err)
				}
			}()
		}

		details := service.NewScannerSourceDetails()
		details.SetName(bucket + "/" + key)

		scanner, err := p.Scanner.Create(obj.Body, nil, details)
		if err != nil {
			msg.SetError(fmt.Errorf("error creating scanner: %w", err))
			continue
		}

		for {
			var resBatch service.MessageBatch
			resBatch, _, err := scanner.NextBatch(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}

			p.getMetadata(bucket, key, obj, resBatch)
			resBatches = append(resBatches, resBatch)
		}
	}
	return resBatches, nil
}

func (p *s3Processor) Close(context.Context) error {
	return nil
}

func (p *s3Processor) getMetadata(bucket string, key string, obj *s3.GetObjectOutput, batch service.MessageBatch) {
	for _, part := range batch {
		part.MetaSetMut("s3_key", key)
		part.MetaSetMut("s3_bucket", bucket)
		if obj.LastModified != nil {
			part.MetaSetMut("s3_last_modified", obj.LastModified.Format(time.RFC3339))
			part.MetaSetMut("s3_last_modified_unix", obj.LastModified.Unix())
		}
		if obj.ContentType != nil {
			part.MetaSetMut("s3_content_type", *obj.ContentType)
		}
		if obj.ContentEncoding != nil {
			part.MetaSetMut("s3_content_encoding", *obj.ContentEncoding)
		}
		if obj.ContentLength != nil {
			part.MetaSetMut("s3_content_length", *obj.ContentLength)
		}
		if obj.VersionId != nil && *obj.VersionId != "null" {
			part.MetaSetMut("s3_version_id", *obj.VersionId)
		}
		for k, v := range obj.Metadata {
			part.MetaSetMut(k, v)
		}
	}
}

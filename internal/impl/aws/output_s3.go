package aws

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// S3 Output Fields
	s3oFieldBucket                  = "bucket"
	s3oFieldForcePathStyleURLs      = "force_path_style_urls"
	s3oFieldPath                    = "path"
	s3oFieldTags                    = "tags"
	s3oFieldContentType             = "content_type"
	s3oFieldContentEncoding         = "content_encoding"
	s3oFieldCacheControl            = "cache_control"
	s3oFieldContentDisposition      = "content_disposition"
	s3oFieldContentLanguage         = "content_language"
	s3oFieldWebsiteRedirectLocation = "website_redirect_location"
	s3oFieldMetadata                = "metadata"
	s3oFieldStorageClass            = "storage_class"
	s3oFieldTimeout                 = "timeout"
	s3oFieldKMSKeyID                = "kms_key_id"
	s3oFieldServerSideEncryption    = "server_side_encryption"
	s3oFieldBatching                = "batching"
)

type s3TagPair struct {
	key   string
	value *service.InterpolatedString
}

type s3oConfig struct {
	Bucket string

	Path                    *service.InterpolatedString
	Tags                    []s3TagPair
	ContentType             *service.InterpolatedString
	ContentEncoding         *service.InterpolatedString
	CacheControl            *service.InterpolatedString
	ContentDisposition      *service.InterpolatedString
	ContentLanguage         *service.InterpolatedString
	WebsiteRedirectLocation *service.InterpolatedString
	Metadata                *service.MetadataExcludeFilter
	StorageClass            *service.InterpolatedString
	Timeout                 time.Duration
	KMSKeyID                string
	ServerSideEncryption    string
	UsePathStyle            bool

	aconf aws.Config
}

func s3oConfigFromParsed(pConf *service.ParsedConfig) (conf s3oConfig, err error) {
	if conf.Bucket, err = pConf.FieldString(s3oFieldBucket); err != nil {
		return
	}

	if conf.UsePathStyle, err = pConf.FieldBool(s3oFieldForcePathStyleURLs); err != nil {
		return
	}

	if conf.Path, err = pConf.FieldInterpolatedString(s3oFieldPath); err != nil {
		return
	}

	var tagMap map[string]*service.InterpolatedString
	if tagMap, err = pConf.FieldInterpolatedStringMap(s3oFieldTags); err != nil {
		return
	}

	conf.Tags = make([]s3TagPair, 0, len(tagMap))
	for k, v := range tagMap {
		conf.Tags = append(conf.Tags, s3TagPair{key: k, value: v})
	}
	sort.Slice(conf.Tags, func(i, j int) bool {
		return conf.Tags[i].key < conf.Tags[j].key
	})

	if conf.ContentType, err = pConf.FieldInterpolatedString(s3oFieldContentType); err != nil {
		return
	}
	if conf.ContentEncoding, err = pConf.FieldInterpolatedString(s3oFieldContentEncoding); err != nil {
		return
	}
	if conf.CacheControl, err = pConf.FieldInterpolatedString(s3oFieldCacheControl); err != nil {
		return
	}
	if conf.ContentDisposition, err = pConf.FieldInterpolatedString(s3oFieldContentDisposition); err != nil {
		return
	}
	if conf.ContentLanguage, err = pConf.FieldInterpolatedString(s3oFieldContentLanguage); err != nil {
		return
	}
	if conf.WebsiteRedirectLocation, err = pConf.FieldInterpolatedString(s3oFieldWebsiteRedirectLocation); err != nil {
		return
	}
	if conf.Metadata, err = pConf.FieldMetadataExcludeFilter(s3oFieldMetadata); err != nil {
		return
	}
	if conf.StorageClass, err = pConf.FieldInterpolatedString(s3oFieldStorageClass); err != nil {
		return
	}
	if conf.Timeout, err = pConf.FieldDuration(s3oFieldTimeout); err != nil {
		return
	}
	if conf.KMSKeyID, err = pConf.FieldString(s3oFieldKMSKeyID); err != nil {
		return
	}
	if conf.ServerSideEncryption, err = pConf.FieldString(s3oFieldServerSideEncryption); err != nil {
		return
	}
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}

	// Starting on v1.29 of the github.com/aws/aws-sdk-go-v2/config package,
	// the default value for RequestChecksumCalculation is RequestChecksumCalculationWhenSupported.
	// This breaks the behavior of the aws_s3 output during multipart uploads, since we're not adding
	// a checksum to the request. We're setting it back to RequestChecksumCalculationWhenRequired.
	// See https://github.com/aws/aws-sdk-go-v2/discussions/2960#discussioncomment-12077210
	conf.aconf.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired

	return
}

func s3oOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("1.0.0").
		Categories("Services", "AWS").
		Summary(`Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded with the path specified with the `+"`path`"+` field.`).
		Description(`
In order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Tags

The tags field allows you to specify key/value pairs to attach to objects as tags, where the values support [interpolation functions](/docs/configuration/interpolation#bloblang-queries):

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz
    tags:
      Key1: Value1
      Timestamp: ${!metadata("Timestamp").string()}
`+"```"+`

### Credentials

By default Bento will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).

### Batching

It's common to want to upload messages to S3 as batched archives, the easiest way to do this is to batch your messages at the output level and join the batch of messages with an `+"[`archive`](/docs/components/processors/archive)"+` and/or `+"[`compress`](/docs/components/processors/compress)"+` processor.

For example, if we wished to upload messages as a .tar.gz archive of documents we could achieve that with the following config:

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz
    batching:
      count: 100
      period: 10s
      processors:
        - archive:
            format: tar
        - compress:
            algorithm: gzip
`+"```"+`

Alternatively, if we wished to upload JSON documents as a single large document containing an array of objects we can do that with:

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.json
    batching:
      count: 100
      processors:
        - archive:
            format: json_array
`+"```"+``+service.OutputPerformanceDocs(true, false)).
		Example(
			"Connection to S3 API-Compatable services",
			`This example shows how to connect to "S3 API-Compatable services" - for instance minio.`,
			`
output:
  aws_s3: 
    bucket: mybucket
    path: "events-json-stream/${!timestamp_unix_nano()}.json"
    endpoint: http://localhost:9000
    force_path_style_urls: true
    region: us-east-1
    credentials:
      id: minioadmin
      secret: minioadmin
`).
		Fields(
			service.NewStringField(s3oFieldBucket).
				Description("The bucket to upload messages to."),
			service.NewInterpolatedStringField(s3oFieldPath).
				Description("The path of each message to upload.").
				Default(`${!count("files")}-${!timestamp_unix_nano()}.txt`).
				Example(`${!count("files")}-${!timestamp_unix_nano()}.txt`).
				Example(`${!metadata("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`),
			service.NewInterpolatedStringMapField(s3oFieldTags).
				Description("Key/value pairs to store with the object as tags.").
				Default(map[string]any{}).
				Example(map[string]any{
					"Key1":      "Value1",
					"Timestamp": `${!metadata("Timestamp")}`,
				}),
			service.NewInterpolatedStringField(s3oFieldContentType).
				Description("The content type to set for each object.").
				Default("application/octet-stream"),
			service.NewInterpolatedStringField(s3oFieldContentEncoding).
				Description("An optional content encoding to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldCacheControl).
				Description("The cache control to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldContentDisposition).
				Description("The content disposition to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldContentLanguage).
				Description("The content language to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldWebsiteRedirectLocation).
				Description("The website redirect location to set for each object.").
				Default("").
				Advanced(),
			service.NewMetadataExcludeFilterField(s3oFieldMetadata).
				Description("Specify criteria for which metadata values are attached to objects as headers."),
			service.NewInterpolatedStringEnumField(s3oFieldStorageClass,
				"STANDARD", "REDUCED_REDUNDANCY", "GLACIER", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "DEEP_ARCHIVE",
			).
				Description("The storage class to set for each object.").
				Default("STANDARD").
				Advanced(),
			service.NewStringField(s3oFieldKMSKeyID).
				Description("An optional server side encryption key.").
				Default("").
				Advanced(),
			service.NewStringField(s3oFieldServerSideEncryption).
				Description("An optional server side encryption algorithm.").
				Version("1.0.0").
				Default("").
				Advanced(),
			service.NewBoolField(s3oFieldForcePathStyleURLs).
				Description("Forces the client API to use path style URLs, which helps when connecting to custom endpoints.").
				Advanced().
				Default(false),
			service.NewOutputMaxInFlightField(),
			service.NewDurationField(s3oFieldTimeout).
				Description("The maximum period to wait on an upload before abandoning it and reattempting.").
				Advanced().
				Default("5s"),
			service.NewBatchPolicyField(s3oFieldBatching),
		).
		Fields(config.SessionFields()...)
}

func init() {
	err := service.RegisterBatchOutput("aws_s3", s3oOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(koFieldBatching); err != nil {
				return
			}
			var wConf s3oConfig
			if wConf, err = s3oConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newAmazonS3Writer(wConf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type amazonS3Writer struct {
	conf     s3oConfig
	uploader *manager.Uploader
	log      *service.Logger
}

func newAmazonS3Writer(conf s3oConfig, mgr *service.Resources) (*amazonS3Writer, error) {
	a := &amazonS3Writer{
		conf: conf,
		log:  mgr.Logger(),
	}
	return a, nil
}

func (a *amazonS3Writer) Connect(ctx context.Context) error {
	if a.uploader != nil {
		return nil
	}

	client := s3.NewFromConfig(a.conf.aconf, func(o *s3.Options) {
		o.UsePathStyle = a.conf.UsePathStyle
	})
	a.uploader = manager.NewUploader(client)
	return nil
}

func (a *amazonS3Writer) WriteBatch(wctx context.Context, msg service.MessageBatch) error {
	if a.uploader == nil {
		return service.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(wctx, a.conf.Timeout)
	defer cancel()

	return msg.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		metadata := map[string]string{}
		_ = a.conf.Metadata.WalkMut(m, func(k string, v any) error {
			metadata[k] = bloblang.ValueToString(v)
			return nil
		})

		var contentEncoding *string
		ce, err := msg.TryInterpolatedString(i, a.conf.ContentEncoding)
		if err != nil {
			return fmt.Errorf("content encoding interpolation: %w", err)
		}
		if ce != "" {
			contentEncoding = aws.String(ce)
		}
		var cacheControl *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.CacheControl); err != nil {
			return fmt.Errorf("cache control interpolation: %w", err)
		}
		if ce != "" {
			cacheControl = aws.String(ce)
		}
		var contentDisposition *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.ContentDisposition); err != nil {
			return fmt.Errorf("content disposition interpolation: %w", err)
		}
		if ce != "" {
			contentDisposition = aws.String(ce)
		}
		var contentLanguage *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.ContentLanguage); err != nil {
			return fmt.Errorf("content language interpolation: %w", err)
		}
		if ce != "" {
			contentLanguage = aws.String(ce)
		}
		var websiteRedirectLocation *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.WebsiteRedirectLocation); err != nil {
			return fmt.Errorf("website redirect location interpolation: %w", err)
		}
		if ce != "" {
			websiteRedirectLocation = aws.String(ce)
		}

		key, err := msg.TryInterpolatedString(i, a.conf.Path)
		if err != nil {
			return fmt.Errorf("key interpolation: %w", err)
		}

		contentType, err := msg.TryInterpolatedString(i, a.conf.ContentType)
		if err != nil {
			return fmt.Errorf("content type interpolation: %w", err)
		}

		storageClass, err := msg.TryInterpolatedString(i, a.conf.StorageClass)
		if err != nil {
			return fmt.Errorf("storage class interpolation: %w", err)
		}

		mBytes, err := m.AsBytes()
		if err != nil {
			return err
		}

		uploadInput := &s3.PutObjectInput{
			Bucket:                  &a.conf.Bucket,
			Key:                     aws.String(key),
			Body:                    bytes.NewReader(mBytes),
			ContentType:             aws.String(contentType),
			ContentEncoding:         contentEncoding,
			CacheControl:            cacheControl,
			ContentDisposition:      contentDisposition,
			ContentLanguage:         contentLanguage,
			WebsiteRedirectLocation: websiteRedirectLocation,
			StorageClass:            types.StorageClass(storageClass),
			Metadata:                metadata,
		}

		// Prepare tags, escaping keys and values to ensure they're valid query string parameters.
		if len(a.conf.Tags) > 0 {
			tags := make([]string, len(a.conf.Tags))
			for j, pair := range a.conf.Tags {
				tagStr, err := msg.TryInterpolatedString(i, pair.value)
				if err != nil {
					return fmt.Errorf("tag %v interpolation: %w", pair.key, err)
				}
				tags[j] = url.QueryEscape(pair.key) + "=" + url.QueryEscape(tagStr)
			}
			uploadInput.Tagging = aws.String(strings.Join(tags, "&"))
		}

		if a.conf.KMSKeyID != "" {
			uploadInput.ServerSideEncryption = types.ServerSideEncryptionAwsKms
			uploadInput.SSEKMSKeyId = &a.conf.KMSKeyID
		}

		// NOTE: This overrides the ServerSideEncryption set above. We need this to preserve
		// backwards compatibility, where it is allowed to only set kms_key_id in the config and
		// the ServerSideEncryption value of "aws:kms" is implied.
		if a.conf.ServerSideEncryption != "" {
			uploadInput.ServerSideEncryption = types.ServerSideEncryption(a.conf.ServerSideEncryption)
		}

		if _, err := a.uploader.Upload(ctx, uploadInput); err != nil {
			return err
		}
		return nil
	})
}

func (a *amazonS3Writer) Close(context.Context) error {
	return nil
}

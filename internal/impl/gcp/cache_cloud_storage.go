package gcp

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/public/service"
)

func gcpCloudStorageCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Summary(`Use a Google Cloud Storage bucket as a cache.`).
		Description(`It is not possible to atomically upload cloud storage objects exclusively when the target does not already exist, therefore this cache is not suitable for deduplication.`).
		Field(service.NewStringField("bucket").
			Description("The Google Cloud Storage bucket to store items in.")).
		Field(service.NewStringField("content_type").
			Description("Optional field to explicitly set the Content-Type.").Optional()).
		Field(service.NewStringField("prefix").
			Description("An optional string to prefix item keys with in order to prevent collisions with similar services. The prefix is also used to scope key listings, and is stripped from the keys returned.").
			Advanced().
			Default(""))

	return spec
}

func init() {
	err := service.RegisterCache(
		"gcp_cloud_storage", gcpCloudStorageCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newGcpCloudStorageCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newGcpCloudStorageCacheFromConfig(parsedConf *service.ParsedConfig) (*gcpCloudStorageCache, error) {
	bucket, err := parsedConf.FieldString("bucket")
	if err != nil {
		return nil, err
	}

	contentType := ""
	if parsedConf.Contains("content_type") {
		contentType, err = parsedConf.FieldString("content_type")
		if err != nil {
			return nil, err
		}
	}

	prefix, err := parsedConf.FieldString("prefix")
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return &gcpCloudStorageCache{
		bucketHandle: client.Bucket(bucket),
		contentType:  contentType,
		prefix:       prefix,
	}, nil
}

//------------------------------------------------------------------------------

type gcpCloudStorageCache struct {
	bucketHandle *storage.BucketHandle
	contentType  string
	prefix       string
}

func (c *gcpCloudStorageCache) object(key string) *storage.ObjectHandle {
	return c.bucketHandle.Object(c.prefix + key)
}

func (c *gcpCloudStorageCache) Get(ctx context.Context, key string) ([]byte, error) {
	reader, err := c.object(key).NewReader(ctx)
	if err != nil {
		// Check if the object does not exist and return the proper error
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, service.ErrKeyNotFound
		}
		return nil, err
	}

	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *gcpCloudStorageCache) Exists(ctx context.Context, key string) (bool, error) {
	_, err := c.object(key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *gcpCloudStorageCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	writer := c.object(key).NewWriter(ctx)

	if c.contentType != "" {
		writer.ContentType = c.contentType
	}

	_, err := writer.Write(value)
	if err != nil {
		return err
	}

	return writer.Close()
}

func (c *gcpCloudStorageCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	objectHandle := c.object(key)

	// Check if the object already exists
	_, err := objectHandle.Attrs(ctx)
	if err == nil {
		return service.ErrKeyAlreadyExists
	}

	writer := objectHandle.NewWriter(ctx)

	if c.contentType != "" {
		writer.ContentType = c.contentType
	}

	_, err = writer.Write(value)
	if err != nil {
		return err
	}

	return writer.Close()
}

func (c *gcpCloudStorageCache) Delete(ctx context.Context, key string) error {
	return c.object(key).Delete(ctx)
}

func (c *gcpCloudStorageCache) Keys(ctx context.Context) service.KeyIterator {
	// readAhead matches the storage client's default page size so that the next
	// page can be fetched while the current one is yielded.
	var query *storage.Query
	if c.prefix != "" {
		query = &storage.Query{Prefix: c.prefix}
	}

	const readAhead = 1000
	return cache.PrefetchKeys(ctx, readAhead, func(ctx context.Context, emit func(string) bool) error {
		it := c.bucketHandle.Objects(ctx, query)
		for {
			attrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return nil
			}
			if err != nil {
				return err
			}
			// The prefix is stripped so that yielded keys round-trip through
			// the other cache methods.
			if !emit(strings.TrimPrefix(attrs.Name, c.prefix)) {
				return nil
			}
		}
	})
}

func (c *gcpCloudStorageCache) Close(ctx context.Context) error {
	return nil
}

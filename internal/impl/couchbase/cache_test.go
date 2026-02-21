package couchbase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationCouchbaseCache(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	template := `
cache_resources:
  - label: testcache
    couchbase:
      url: couchbase://localhost:$PORT
      username: $USER
      password: $PASS
      bucket: $ID
`

	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(servicePort),
		integration.CacheTestOptVarSet("USER", username),
		integration.CacheTestOptVarSet("PASS", password),
		integration.CacheTestOptPreTest(func(tb testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
			require.NoError(tb, createBucket(ctx, servicePort, vars.ID))
			tb.Cleanup(func() {
				require.NoError(tb, removeBucket(ctx, servicePort, vars.ID))
			})
		}),
	)
}

func getCluster(port string) (*gocb.Cluster, error) {
	return gocb.Connect(fmt.Sprintf("couchbase://localhost:%v", port), gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
}

func removeBucket(ctx context.Context, port, bucket string) error {
	cluster, err := getCluster(port)
	if err != nil {
		return err
	}

	return cluster.Buckets().DropBucket(bucket, &gocb.DropBucketOptions{
		Context: ctx,
	})
}

func createBucket(ctx context.Context, port, bucket string) error {
	cluster, err := getCluster(port)
	if err != nil {
		return err
	}

	err = cluster.Buckets().CreateBucket(gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:       bucket,
			RAMQuotaMB: 100, // smallest value and allow max 10 running bucket with cluster-ramsize 1024 from setup script
			BucketType: gocb.CouchbaseBucketType,
		},
	}, nil)
	if err != nil {
		return err
	}

	for range 6 { // try six times
		time.Sleep(time.Second)
		err = cluster.Bucket(bucket).WaitUntilReady(time.Second*10, &gocb.WaitUntilReadyOptions{Context: ctx})
		if err == nil {
			break
		}
	}

	return err
}

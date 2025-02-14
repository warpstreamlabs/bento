package pure

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/message"
)

func TestLocalRateLimitConfErrors(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`count: -1`, nil)
	require.NoError(t, err)

	_, err = newLocalRatelimitFromConfig(conf)
	require.Error(t, err)

	conf, err = localRatelimitConfig().ParseYAML(`byte_size: -1`, nil)
	require.NoError(t, err)

	_, err = newLocalRatelimitFromConfig(conf)
	require.Error(t, err)

	// This will fail as byte_size is set to 0 by default and we cannot have count=0 and byte_size=0
	conf, err = localRatelimitConfig().ParseYAML(`count: 0`, nil)
	require.NoError(t, err)

	_, err = newLocalRatelimitFromConfig(conf)
	require.Error(t, err)

	_, err = localRatelimitConfig().ParseYAML(`interval: nope`, nil)
	require.NoError(t, err)

	_, err = newLocalRatelimitFromConfig(conf)
	require.Error(t, err)
}

func TestLocalRateLimitBasic(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`
count: 10
interval: 1s
`, nil)
	require.NoError(t, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		exceeded := rl.Add(ctx, nil)
		assert.False(t, exceeded)

		period, _ := rl.Access(ctx)
		assert.LessOrEqual(t, period, time.Duration(0))
	}

	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

func TestLocalRateLimitBytesBasic(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`
byte_size: 100
interval: 1s
`, nil)
	require.NoError(t, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	msgBytes := make([][]byte, 10)

	for i := 0; i < len(msgBytes); i++ {
		msgBytes[i] = make([]byte, 10)
	}
	batch := message.QuickBatch(msgBytes)

	for _, msg := range batch {
		assert.False(t, rl.Add(ctx, msg))
		period, _ := rl.Access(ctx)
		assert.LessOrEqual(t, period, time.Duration(0))
	}

	assert.True(t, rl.Add(ctx, batch[0]), "Expected rate limit to be reached")
	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

func TestLocalRateLimitRefresh(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`
count: 10
interval: 10ms
`, nil)
	require.NoError(t, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		assert.False(t, rl.Add(ctx, nil))
		period, _ := rl.Access(ctx)
		if period > 0 {
			t.Errorf("Period above zero: %v", period)
		}
	}

	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}

	<-time.After(time.Millisecond * 15)

	for i := 0; i < 10; i++ {
		assert.False(t, rl.Add(ctx, nil))
		period, _ := rl.Access(ctx)
		if period != 0 {
			t.Errorf("Rate limited on get %v", i)
		}
	}

	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

func TestLocalRateLimitRefreshBytes(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`
byte_size: 100
interval: 10ms
`, nil)
	require.NoError(t, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	msgBytes := make([][]byte, 10)

	for i := 0; i < len(msgBytes); i++ {
		msgBytes[i] = make([]byte, 10)
	}
	batch := message.QuickBatch(msgBytes)

	for _, msg := range batch {
		assert.False(t, rl.Add(ctx, msg))
		period, _ := rl.Access(ctx)
		assert.LessOrEqual(t, period, time.Duration(0))
	}

	assert.True(t, rl.Add(ctx, batch[0]), "Expected rate limit to be reached")
	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}

	<-time.After(time.Millisecond * 15)

	for i, msg := range batch {
		assert.False(t, rl.Add(ctx, msg))
		period, _ := rl.Access(ctx)
		if period != 0 {
			t.Errorf("Rate limited on get %v", i)
		}
	}

	assert.True(t, rl.Add(ctx, batch[0]))
	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

func TestLocalRateLimitRefreshBytesAndCount(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`
byte_size: 100
count: 15
interval: 10ms
`, nil)
	require.NoError(t, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	msgWith10Bytes := message.NewPart(make([]byte, 10))
	msgWith5Bytes := message.NewPart(make([]byte, 5))

	for i := 0; i < 10; i++ {
		assert.False(t, rl.Add(ctx, msgWith10Bytes))
		period, _ := rl.Access(ctx)
		assert.LessOrEqual(t, period, time.Duration(0))
	}

	assert.Equal(t, 5, rl.bucket)
	assert.Equal(t, 0, rl.byteBucket)

	// Rate limit on 11th request since byte_size is reached
	assert.True(t, rl.Add(ctx, msgWith10Bytes), "Expected rate limit to be reached")
	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}

	<-time.After(time.Millisecond * 15)

	for i := 0; i < 15; i++ {
		assert.False(t, rl.Add(ctx, msgWith5Bytes))
		period, _ := rl.Access(ctx)
		if period != 0 {
			t.Errorf("Rate limited on get %v", i)
		}
	}

	assert.Equal(t, 0, rl.bucket)
	assert.Equal(t, 25, rl.byteBucket)

	// rate limit on 16th request since count is reached
	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

//------------------------------------------------------------------------------

func BenchmarkRateLimit(b *testing.B) {
	/* A rate limit is typically going to be protecting a networked resource
	 * where the request will likely be measured at least in hundreds of
	 * microseconds. It would be reasonable to assume the rate limit might be
	 * shared across tens of components.
	 *
	 * Therefore, we can probably sit comfortably with lock contention across
	 * one hundred or so parallel components adding an overhead of single digit
	 * microseconds. Since this benchmark doesn't take into account the actual
	 * request duration after receiving a rate limit I've set the number of
	 * components to ten in order to compensate.
	 */
	b.ReportAllocs()

	nParallel := 10
	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(nParallel)

	conf, err := localRatelimitConfig().ParseYAML(`
count: 1000
interval: 1ns
`, nil)
	require.NoError(b, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(b, err)

	ctx := context.Background()

	for i := 0; i < nParallel; i++ {
		go func() {
			<-startChan
			for j := 0; j < b.N; j++ {
				rl.Add(ctx, nil)
				period, _ := rl.Access(ctx)
				if period > 0 {
					time.Sleep(period)
				}
			}
			wg.Done()
		}()
	}

	b.ResetTimer()
	close(startChan)
	wg.Wait()
}

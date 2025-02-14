package pure

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

func localRatelimitConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`The local rate limit is a simple X every Y type rate limit that can be shared across any number of components within the pipeline but does not support distributed rate limits across multiple running instances of Bento.`).
		Field(service.NewIntField("count").
			Description("The maximum number of requests to allow for a given period of time. If `0` disables count based rate-limiting.").
			Default(1000).LintRule(`root = if this < 0 { [ "count cannot be less than zero" ] }`)).
		Field(service.NewIntField("byte_size").
			Description("The maximum number of bytes to allow for a given period of time. If `0` disables byte_size based rate-limiting.").
			Default(0).LintRule(`root = if this < 0 { [ "byte_size cannot be less than zero" ] }`)).
		Field(service.NewDurationField("interval").
			Description("The time window to limit requests by.").
			Default("1s"))

	return spec
}

func init() {
	err := service.RegisterRateLimit(
		"local", localRatelimitConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			return newLocalRatelimitFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newLocalRatelimitFromConfig(conf *service.ParsedConfig) (*localRatelimit, error) {
	count, err := conf.FieldInt("count")
	if err != nil {
		return nil, err
	}
	byteSize, err := conf.FieldInt("byte_size")
	if err != nil {
		return nil, err
	}
	interval, err := conf.FieldDuration("interval")
	if err != nil {
		return nil, err
	}
	return newLocalRatelimit(count, byteSize, interval)
}

//------------------------------------------------------------------------------

type localRatelimit struct {
	mut           sync.Mutex // TODO: We should rather be using atomics as opposed to locking
	bucket        int
	byteBucket    int
	exceededLimit bool
	lastRefresh   time.Time

	size     int
	byteSize int
	period   time.Duration
}

func newLocalRatelimit(count, byteSize int, interval time.Duration) (*localRatelimit, error) {
	if byteSize < 0 || count < 0 {
		return nil, errors.New("neither byte size nor count can be negative")
	}

	if byteSize == 0 && count == 0 {
		return nil, errors.New("either count or byte size must be larger than zero")
	}

	return &localRatelimit{
		bucket:        count,
		byteBucket:    byteSize,
		exceededLimit: false,
		lastRefresh:   time.Now(),

		size:     count,
		byteSize: byteSize,
		period:   interval,
	}, nil
}

func (r *localRatelimit) Access(ctx context.Context) (time.Duration, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	// Rate limiting count is enabled
	if r.size > 0 {
		r.bucket--
	}

	if r.bucket < 0 {
		r.exceededLimit = true
	}

	if !r.exceededLimit {
		return 0, nil
	}

	remaining := r.period - time.Since(r.lastRefresh)
	if remaining > 0 {
		return remaining, nil
	}

	// The interval has passed so reset
	r.refresh()
	return 0, nil
}

// Add increments the observation or message-bytes counter. Returns true if this
// triggers the conditions of the rate-limiter.
func (r *localRatelimit) Add(ctx context.Context, parts ...*message.Part) bool {
	r.mut.Lock()
	defer r.mut.Unlock()

	if time.Since(r.lastRefresh) >= r.period {
		r.refresh()
	}

	if r.exceededLimit {
		return true
	}

	// Rate limiting bytes is enabled
	if r.byteSize > 0 {
		for i := 0; i < len(parts); i++ {
			if parts[i] == nil {
				continue
			}

			if r.byteBucket -= len(parts[i].AsBytes()); r.byteBucket < 0 {
				r.exceededLimit = true
				break
			}
		}
	}

	return r.exceededLimit
}

func (r *localRatelimit) refresh() {
	r.byteBucket = r.byteSize
	r.bucket = r.size
	r.lastRefresh = time.Now()
	r.exceededLimit = false
}

func (r *localRatelimit) Close(ctx context.Context) error {
	return nil
}

package service

import (
	"context"
	"time"

	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/message"
)

// RateLimit is an interface implemented by Bento rate limits.
type RateLimit interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(context.Context) (time.Duration, error)

	Closer
}

// MessageAwareRateLimit is an interface implemented by Bento rate limits that require message awareness
type MessageAwareRateLimit interface {
	// Add a new *message.Part to the rate limited resource. Returns true when an
	// additional message part triggers rate-limiting or if the resource is currently
	// being rate-limited.
	Add(ctx context.Context, msg ...*Message) bool

	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(context.Context) (time.Duration, error)

	Closer
}

//------------------------------------------------------------------------------

func newAirGapRateLimit(c RateLimit, stats metrics.Type) ratelimit.V1 {
	return ratelimit.MetricsForRateLimit(c, stats)
}

func newAirGapMessageAwareRateLimit(rl MessageAwareRateLimit, stats metrics.Type) ratelimit.MessageAwareRateLimit {
	agrl := &airGapMessageAwareRateLimit{r: rl}
	return ratelimit.MetricsForMessageAwareRateLimit(agrl, stats)
}

//------------------------------------------------------------------------------

// Implements types.MessageAwareRateLimit.
type airGapMessageAwareRateLimit struct {
	r MessageAwareRateLimit
}

func (a *airGapMessageAwareRateLimit) Add(ctx context.Context, msgs ...*message.Part) bool {
	imsgs := make([]*Message, len(msgs))
	for i, msg := range msgs {
		imsgs[i] = NewInternalMessage(msg)
	}
	return a.r.Add(ctx, imsgs...)
}

func (a *airGapMessageAwareRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return a.r.Access(ctx)
}

func (a *airGapMessageAwareRateLimit) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}

//------------------------------------------------------------------------------

// Implements RateLimit around a types.RateLimit.
type reverseAirGapRateLimit struct {
	r ratelimit.V1
}

func newReverseAirGapRateLimit(r ratelimit.V1) *reverseAirGapRateLimit {
	return &reverseAirGapRateLimit{r}
}

func (a *reverseAirGapRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return a.r.Access(ctx)
}

func (a *reverseAirGapRateLimit) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}

//------------------------------------------------------------------------------

// Implements MessageAwareRateLimit around a types.MessageAwareRateLimiter.
type reverseAirGapMessageAwareRateLimit struct {
	r ratelimit.MessageAwareRateLimit
}

func newReverseAirGapMessageAwareRateLimit(r ratelimit.MessageAwareRateLimit) *reverseAirGapMessageAwareRateLimit {
	return &reverseAirGapMessageAwareRateLimit{r}
}
func (a *reverseAirGapMessageAwareRateLimit) Add(ctx context.Context, msgs ...*Message) bool {
	parts := make([]*message.Part, len(msgs))
	for i, msg := range msgs {
		parts[i] = msg.part
	}
	return a.r.Add(ctx, parts...)
}

func (a *reverseAirGapMessageAwareRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return a.r.Access(ctx)
}

func (a *reverseAirGapMessageAwareRateLimit) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}

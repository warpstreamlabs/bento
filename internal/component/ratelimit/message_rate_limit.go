package ratelimit

import (
	"context"
	"time"

	"github.com/warpstreamlabs/bento/internal/message"
)

// MessageAwareRateLimiter extends RateLimiter with message-specific rate limiting
type MessageAwareRateLimit interface {
	// Add a new message part to the rate limiter. Returns true if this part triggers
	// the conditions of the rate-limiter.
	Add(ctx context.Context, parts ...*message.Part) bool

	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(ctx context.Context) (time.Duration, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

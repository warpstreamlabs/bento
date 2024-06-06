package mock_test

import (
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

var _ ratelimit.V1 = mock.RateLimit(nil)

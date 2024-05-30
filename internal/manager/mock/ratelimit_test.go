package mock_test

import (
	"github.com/warpstreamlabs/bento/v4/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/v4/internal/manager/mock"
)

var _ ratelimit.V1 = mock.RateLimit(nil)

package mock_test

import (
	"github.com/warpstreamlabs/bento/v1/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
)

var _ ratelimit.V1 = mock.RateLimit(nil)

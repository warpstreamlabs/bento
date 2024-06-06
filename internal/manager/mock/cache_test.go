package mock_test

import (
	"github.com/warpstreamlabs/bento/v1/internal/component/cache"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
)

var _ cache.V1 = &mock.Cache{}

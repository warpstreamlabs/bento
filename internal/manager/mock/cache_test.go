package mock_test

import (
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

var _ cache.V1 = &mock.Cache{}

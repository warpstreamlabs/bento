package mock_test

import (
	"github.com/warpstreamlabs/bento/v4/internal/component/input"
	"github.com/warpstreamlabs/bento/v4/internal/manager/mock"
)

var _ input.Streamed = &mock.Input{}

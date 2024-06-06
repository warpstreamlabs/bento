package mock_test

import (
	"github.com/warpstreamlabs/bento/v1/internal/component/input"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
)

var _ input.Streamed = &mock.Input{}

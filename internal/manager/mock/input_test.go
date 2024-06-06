package mock_test

import (
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

var _ input.Streamed = &mock.Input{}

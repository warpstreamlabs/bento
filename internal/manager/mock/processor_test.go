package mock_test

import (
	"github.com/warpstreamlabs/bento/v1/internal/component/processor"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
)

var _ processor.V1 = mock.Processor(nil)

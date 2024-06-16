package mock_test

import (
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

var _ processor.V1 = mock.Processor(nil)

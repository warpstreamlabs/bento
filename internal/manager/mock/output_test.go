package mock_test

import (
	"github.com/warpstreamlabs/bento/v1/internal/component/output"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
)

var _ output.Sync = mock.OutputWriter(nil)

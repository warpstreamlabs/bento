package mock_test

import (
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

var _ output.Sync = mock.OutputWriter(nil)

package mock_test

import (
	"github.com/warpstreamlabs/bento/v4/internal/component/output"
	"github.com/warpstreamlabs/bento/v4/internal/manager/mock"
)

var _ output.Sync = mock.OutputWriter(nil)

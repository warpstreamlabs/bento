package mock_test

import (
	"github.com/warpstreamlabs/bento/v4/internal/bundle"
	"github.com/warpstreamlabs/bento/v4/internal/manager/mock"
)

var _ bundle.NewManagement = &mock.Manager{}

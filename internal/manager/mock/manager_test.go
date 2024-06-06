package mock_test

import (
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

var _ bundle.NewManagement = &mock.Manager{}

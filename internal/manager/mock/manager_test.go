package mock_test

import (
	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/manager/mock"
)

var _ bundle.NewManagement = &mock.Manager{}

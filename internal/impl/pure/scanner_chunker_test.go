package pure_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/v4/internal/component/scanner/testutil"
	"github.com/warpstreamlabs/bento/v4/public/service"
)

func TestLinesChunkerSuite(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  chunker:
    size: 4
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`abcdefghijklmnopqrstuvwxyz`), "abcd", "efgh", "ijkl", "mnop", "qrst", "uvwx", "yz")
}

package pure_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/scanner/testutil"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestCSVScannerDefault(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a,b,c
a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`),
		`{"a":"a1","b":"b1","c":"c1"}`,
		`{"a":"a2","b":"b2","c":"c2"}`,
		`{"a":"a3","b":"b3","c":"c3"}`,
		`{"a":"a4","b":"b4","c":"c4"}`,
	)
}

func TestCSVScannerCustomDelim(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    custom_delimiter: '|'
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a|b|c
a1|b1|c1
a2|b2|c2
a3|b3|c3
a4|b4|c4
`),
		`{"a":"a1","b":"b1","c":"c1"}`,
		`{"a":"a2","b":"b2","c":"c2"}`,
		`{"a":"a3","b":"b3","c":"c3"}`,
		`{"a":"a4","b":"b4","c":"c4"}`,
	)
}

func TestCSVScannerNoHeaderRow(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    parse_header_row: false
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`),
		`["a1","b1","c1"]`,
		`["a2","b2","c2"]`,
		`["a3","b3","c3"]`,
		`["a4","b4","c4"]`,
	)
}

func TestCSVScannerExpectedHeaders(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    expected_headers:
      - a
      - b
      - c
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a,b,c
a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`),
		`{"a":"a1","b":"b1","c":"c1"}`,
		`{"a":"a2","b":"b2","c":"c2"}`,
		`{"a":"a3","b":"b3","c":"c3"}`,
		`{"a":"a4","b":"b4","c":"c4"}`,
	)
}

func TestCSVScannerExpectedHeadersErr(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    expected_headers:
      - a
      - b
      - c
      - d
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	buf := io.NopCloser(bytes.NewReader([]byte(`a,b,c
a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`)))

	_, err = rdr.Create(buf, func(ctx context.Context, err error) error {
		return nil
	}, &service.ScannerSourceDetails{})
	require.ErrorContains(t, err, "expected_headers don't match file contents")
}

func TestCSVScannerExpectedNumberOfFields(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    expected_number_of_fields: 3
`, nil)

	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a,b,c
a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`),
		`{"a":"a1","b":"b1","c":"c1"}`,
		`{"a":"a2","b":"b2","c":"c2"}`,
		`{"a":"a3","b":"b3","c":"c3"}`,
		`{"a":"a4","b":"b4","c":"c4"}`,
	)
}

func TestCSVScannerExpectedNumberOfErr(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    expected_number_of_fields: 4
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	buf := io.NopCloser(bytes.NewReader([]byte(`a,b,c
a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`)))

	_, err = rdr.Create(buf, func(ctx context.Context, err error) error {
		return nil
	}, &service.ScannerSourceDetails{})
	require.ErrorContains(t, err, "wrong number of fields")
}

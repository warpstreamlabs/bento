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

func TestCSVScannerRejectsExpectedHeadersWhenHeaderParsingDisabled(t *testing.T) {
	confSpec := service.NewConfigSpec().
		Field(service.NewScannerField("test"))

	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    parse_header_row: false
    expected_headers:
      - a
      - b
      - c
      - d
`, nil)
	require.NoError(t, err, "YAML with parse_header_row=false & expected_headers should parse")

	// Attempt to build the scanner — this should hit the runtime check
	_, err = pConf.FieldScanner("test")

	require.ErrorContains(
		t,
		err,
		"parse_header_row=false but expected_headers is set; headers won’t be checked",
		"Expected an error when header parsing is disabled but expected_headers is set",
	)
}

// Ensures CSV scanner succeeds when header parsing is enabled and expected headers match.
func TestCSVScannerSuccess_HeaderEnabledWithExpectedHeaders(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    parse_header_row: true
    expected_headers:
      - a
      - b
      - c
`, nil)

	require.NoError(t, err, "YAML with parse_header_row=true & expected_headers should parse")

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err, "FieldScanner should succeed when headers are enabled and match")

	// feed it a matching header + two rows, and verify we get back structured messages
	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a,b,c
foo1,bar1,baz1
foo2,bar2,baz2
`),
		`{"a":"foo1","b":"bar1","c":"baz1"}`,
		`{"a":"foo2","b":"bar2","c":"baz2"}`,
	)
}

func TestCSVScannerExpectedHeadersLintRuleErr(t *testing.T) {
	builder := service.NewStreamBuilder()

	err := builder.SetYAML(`
input:
  file:
    paths: ["./data.csv"]
    scanner:
      csv:
        parse_header_row: false
        expected_headers: ["one", "two", "three"]
output:
  stdout: {}
`)
	require.ErrorContains(t, err, "expected_headers is set but parse_header_row is false")
}

func TestCSVScannerExpectedHeadersLintRuleHappy(t *testing.T) {
	builder := service.NewStreamBuilder()

	err := builder.SetYAML(`
input:
  file:
    paths: ["./data.csv"]
    scanner:
      csv:
        parse_header_row: true
        expected_headers: ["one", "two", "three"]
output:
  stdout: {}
`)
	require.NoError(t, err)
}

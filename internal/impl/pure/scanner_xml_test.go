package pure_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/scanner/testutil"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestXMLScannerDefault(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  xml_documents:
    operator: to_json
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`<a>a0</a>
<a>a1</a>
<a>a2</a>
<a>a3</a>
<a>a4</a>
`),
		`{"a":"a0"}`,
		`{"a":"a1"}`,
		`{"a":"a2"}`,
		`{"a":"a3"}`,
		`{"a":"a4"}`,
	)
}

func TestXMLScannerBadData(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  xml_documents:
    operator: to_json
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`<a>a0</a>invalid
<a>a1</a>
`),
		`{"a":"a0"}`,
		`{"a":"a1"}`,
	)
}

func TestXMLScannerFormatted(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  xml_documents:
    operator: to_json
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`<a>
		a0
	</a>
<a>
	a1
</a>
<a>
	a2
</a>
<a>
	a3
</a>
<a>
	a4
</a>
`),
		`{"a":"a0"}`,
		`{"a":"a1"}`,
		`{"a":"a2"}`,
		`{"a":"a3"}`,
		`{"a":"a4"}`,
	)
}

func TestXMLScannerNested(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  xml_documents:
    operator: to_json
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`<a><b>ab0</b></a>
<a><b>ab1</b></a>
<a><b>ab2</b></a>
<a><b>ab3</b></a>
<a><b>ab4</b></a>
`),
		`{"a":{"b":"ab0"}}`,
		`{"a":{"b":"ab1"}}`,
		`{"a":{"b":"ab2"}}`,
		`{"a":{"b":"ab3"}}`,
		`{"a":{"b":"ab4"}}`,
	)
}

func TestXMLScannerNestedAndFormatted(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  xml_documents:
    operator: to_json
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`<a>
	<b>
		ab0
	</b>
</a>
<a>
	<b>
		ab1
	</b>
</a>
<a>
	<b>
		ab2
	</b>
</a>
<a>
	<b>
		ab3
	</b>
</a>
<a>
	<b>
		ab4
	</b>
</a>
`),
		`{"a":{"b":"ab0"}}`,
		`{"a":{"b":"ab1"}}`,
		`{"a":{"b":"ab2"}}`,
		`{"a":{"b":"ab3"}}`,
		`{"a":{"b":"ab4"}}`,
	)
}

func TestXMLScannerMultipleValuesAndFormatted(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  xml_documents:
    operator: to_json
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`<a>
	<b>
		ab0
	</b>
	<c>
		ac0
	</c>
</a>
<a>
	<b>
		ab1
	</b>
	<c>
		ac1
	</c>
</a>
<a>
	<b>
		ab2
	</b>
	<c>
		ac2
	</c>
</a>
`),
		`{"a":{"b":"ab0","c":"ac0"}}`,
		`{"a":{"b":"ab1","c":"ac1"}}`,
		`{"a":{"b":"ab2","c":"ac2"}}`,
	)
}

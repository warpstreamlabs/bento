package pure

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"slices"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	scsvFieldCustomDelimiter        = "custom_delimiter"
	scsvFieldParseHeaderRow         = "parse_header_row"
	scsvFieldLazyQuotes             = "lazy_quotes"
	scsvFieldContinueOnError        = "continue_on_error"
	scsvFieldExpectedHeaders        = "expected_headers"
	scsvFieldExpectedNumberOfFields = "expected_number_of_fields"
)

func csvScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Consume comma-separated values row by row, including support for custom delimiters.").
		Description(`
### Metadata

This scanner adds the following metadata to each message:

- `+"`csv_row`"+` The index of each row, beginning at 0.

`).
		Fields(
			service.NewStringField(scsvFieldCustomDelimiter).
				Description("Use a provided custom delimiter instead of the default comma.").
				Optional(),
			service.NewBoolField(scsvFieldParseHeaderRow).
				Description("Whether to reference the first row as a header row. If set to true the output structure for messages will be an object where field keys are determined by the header row. Otherwise, each message will consist of an array of values from the corresponding CSV row.").
				Default(true),
			service.NewBoolField(scsvFieldLazyQuotes).
				Description("If set to `true`, a quote may appear in an unquoted field and a non-doubled quote may appear in a quoted field.").
				Default(false),
			service.NewBoolField(scsvFieldContinueOnError).
				Description("If a row fails to parse due to any error emit an empty message marked with the error and then continue consuming subsequent rows when possible. This can sometimes be useful in situations where input data contains individual rows which are malformed. However, when a row encounters a parsing error it is impossible to guarantee that following rows are valid, as this indicates that the input data is unreliable and could potentially emit misaligned rows.").
				Default(false),
			service.NewStringListField(scsvFieldExpectedHeaders).
				Description("An optional list of expected headers in the header row. If provided, the scanner will check the file contents and emit an error if any expected headers don't match.").
				Example([]string{"first_name", "last_name", "age"}).
				Version("1.6.0").
				Optional(),
			service.NewIntField(scsvFieldExpectedNumberOfFields).
				Description("The number of expected fields in the csv file.").
				Version("1.6.0").
				LintRule(`root = if this < 1 { [ "`+scsvFieldExpectedNumberOfFields+` must be at least 1" ] }`).
				Optional(),
		).LintRule(`
root = if this.parse_header_row == false && this.expected_headers.or([]).length() > 0 {
  "expected_headers is set but parse_header_row is false"
}
`)
}

func init() {
	err := service.RegisterBatchScannerCreator("csv", csvScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return csvScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func csvScannerFromParsed(conf *service.ParsedConfig) (l *csvScannerCreator, err error) {
	l = &csvScannerCreator{}
	if conf.Contains(scsvFieldCustomDelimiter) {
		if l.customDelim, err = conf.FieldString(scsvFieldCustomDelimiter); err != nil {
			return
		}
	}
	if l.parseHeaderRow, err = conf.FieldBool(scsvFieldParseHeaderRow); err != nil {
		return
	}
	if l.lazyQuotes, err = conf.FieldBool(scsvFieldLazyQuotes); err != nil {
		return
	}
	if l.continueOnError, err = conf.FieldBool(scsvFieldContinueOnError); err != nil {
		return
	}
	if conf.Contains(scsvFieldExpectedHeaders) {
		if l.expectedHeaders, err = conf.FieldStringList(scsvFieldExpectedHeaders); err != nil {
			return
		}
	}
	// Runtime‐guard: reject expected_headers when header parsing is off
	if parseHeaderRow, err := conf.FieldBool(scsvFieldParseHeaderRow); err == nil && !parseHeaderRow {
		if expectedHeaders, err := conf.FieldStringList(scsvFieldExpectedHeaders); err == nil && len(expectedHeaders) > 0 {
			return nil, errors.New(
				"parse_header_row=false but expected_headers is set; " +
					"headers won’t be checked. Either set parse_header_row=true or remove expected_headers",
			)
		}
	}
	if conf.Contains(scsvFieldExpectedNumberOfFields) {
		if l.expectedNumberOfFields, err = conf.FieldInt(scsvFieldExpectedNumberOfFields); err != nil {
			return
		}
	}
	return
}

type csvScannerCreator struct {
	customDelim            string
	parseHeaderRow         bool
	lazyQuotes             bool
	continueOnError        bool
	expectedHeaders        []string
	expectedNumberOfFields int
}

func (c *csvScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	cRdr := csv.NewReader(rdr)
	cRdr.LazyQuotes = c.lazyQuotes
	if c.customDelim != "" {
		cRdr.Comma = []rune(c.customDelim)[0]
	}
	cRdr.FieldsPerRecord = c.expectedNumberOfFields

	var headers []string
	if c.parseHeaderRow {
		tmpHeaders, err := cRdr.Read()
		if err != nil {
			return nil, err
		}
		if len(c.expectedHeaders) > 0 && slices.Compare(c.expectedHeaders, tmpHeaders) != 0 {
			return nil, errors.New("expected_headers don't match file contents")
		}
		headers = append([]string{}, tmpHeaders...)
	}

	return service.AutoAggregateBatchScannerAcks(&csvScanner{
		r:               rdr,
		c:               cRdr,
		headers:         headers,
		continueOnError: c.continueOnError,
	}, aFn), nil
}

func (c *csvScannerCreator) Close(context.Context) error {
	return nil
}

type csvScanner struct {
	c *csv.Reader
	r io.ReadCloser

	headers         []string
	row             int
	continueOnError bool
}

func (c *csvScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if c.r == nil {
		return nil, io.EOF
	}

	recordStrs, err := c.c.Read()
	if err != nil {
		if errors.Is(err, io.EOF) || !c.continueOnError {
			return nil, err
		}
	}

	msg := service.NewMessage(nil)
	msg.MetaSetMut("csv_row", c.row)
	if err != nil {
		msg.SetError(err)
	}
	if len(c.headers) > 0 {
		a := make(map[string]any, len(recordStrs))
		for i, v := range recordStrs {
			if len(c.headers) > i {
				a[c.headers[i]] = v
			}
		}
		msg.SetStructuredMut(a)
	} else {
		a := make([]any, len(recordStrs))
		for i, v := range recordStrs {
			a[i] = v
		}
		msg.SetStructuredMut(a)
	}
	c.row++

	return service.MessageBatch{msg}, nil
}

func (c *csvScanner) Close(ctx context.Context) error {
	if c.r == nil {
		return nil
	}
	return c.r.Close()
}

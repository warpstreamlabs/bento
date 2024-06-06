package pure

import (
	"context"
	"fmt"
	"strconv"
	"time"

	syslog "github.com/influxdata/go-syslog/v3"
	"github.com/influxdata/go-syslog/v3/rfc3164"
	"github.com/influxdata/go-syslog/v3/rfc5424"

	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/component/interop"
	"github.com/warpstreamlabs/bento/v1/internal/component/processor"
	"github.com/warpstreamlabs/bento/v1/internal/log"
	"github.com/warpstreamlabs/bento/v1/internal/message"
	"github.com/warpstreamlabs/bento/v1/public/service"
)

const (
	plpFieldFormat       = "format"
	plpFieldCodec        = "codec"
	plpFieldBestEffort   = "best_effort"
	plpFieldWithRFC3339  = "allow_rfc3339"
	plpFieldWithYear     = "default_year"
	plpFieldWithTimezone = "default_timezone"
)

func parseLogSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Parsing").
		Stable().
		Summary(`Parses common log [formats](#formats) into [structured data](#codecs). This is easier and often much faster than `+"[`grok`](/docs/components/processors/grok)"+`.`).
		Footnotes(`
## Codecs

Currently the only supported structured data codec is `+"`json`"+`.

## Formats

### `+"`syslog_rfc5424`"+`

Attempts to parse a log following the [Syslog rfc5424](https://tools.ietf.org/html/rfc5424) spec. The resulting structured document may contain any of the following fields:

- `+"`message`"+` (string)
- `+"`timestamp`"+` (string, RFC3339)
- `+"`facility`"+` (int)
- `+"`severity`"+` (int)
- `+"`priority`"+` (int)
- `+"`version`"+` (int)
- `+"`hostname`"+` (string)
- `+"`procid`"+` (string)
- `+"`appname`"+` (string)
- `+"`msgid`"+` (string)
- `+"`structureddata`"+` (object)

### `+"`syslog_rfc3164`"+`

Attempts to parse a log following the [Syslog rfc3164](https://tools.ietf.org/html/rfc3164) spec. The resulting structured document may contain any of the following fields:

- `+"`message`"+` (string)
- `+"`timestamp`"+` (string, RFC3339)
- `+"`facility`"+` (int)
- `+"`severity`"+` (int)
- `+"`priority`"+` (int)
- `+"`hostname`"+` (string)
- `+"`procid`"+` (string)
- `+"`appname`"+` (string)
- `+"`msgid`"+` (string)
`).
		Fields(
			service.NewStringEnumField(plpFieldFormat, "syslog_rfc5424", "syslog_rfc3164").
				Description("A common log [format](#formats) to parse."),
			service.NewBoolField(plpFieldBestEffort).
				Description("Still returns partially parsed messages even if an error occurs.").
				Advanced().
				Default(true),
			service.NewBoolField(plpFieldWithRFC3339).
				Description("Also accept timestamps in rfc3339 format while parsing. Applicable to format `syslog_rfc3164`.").
				Advanced().
				Default(true),
			service.NewStringField(plpFieldWithYear).
				Description("Sets the strategy used to set the year for rfc3164 timestamps. Applicable to format `syslog_rfc3164`. When set to `current` the current year will be set, when set to an integer that value will be used. Leave this field empty to not set a default year at all.").
				Advanced().
				Default("current"),
			service.NewStringField(plpFieldWithTimezone).
				Description("Sets the strategy to decide the timezone for rfc3164 timestamps. Applicable to format `syslog_rfc3164`. This value should follow the [time.LoadLocation](https://golang.org/pkg/time/#LoadLocation) format.").
				Advanced().
				Default("UTC"),
			service.NewStringField(plpFieldCodec).Deprecated(),
		)
}

type parseLogConfig struct {
	Format       string
	Codec        string
	BestEffort   bool
	WithRFC3339  bool
	WithYear     string
	WithTimezone string
}

func init() {
	err := service.RegisterBatchProcessor(

		"parse_log", parseLogSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			var c parseLogConfig
			var err error

			if c.Format, err = conf.FieldString(plpFieldFormat); err != nil {
				return nil, err
			}
			if c.BestEffort, err = conf.FieldBool(plpFieldBestEffort); err != nil {
				return nil, err
			}
			if c.WithRFC3339, err = conf.FieldBool(plpFieldWithRFC3339); err != nil {
				return nil, err
			}
			if c.WithYear, err = conf.FieldString(plpFieldWithYear); err != nil {
				return nil, err
			}
			if c.WithTimezone, err = conf.FieldString(plpFieldWithTimezone); err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newParseLog(c, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedProcessor("parse_log", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type parserFormat func(body []byte) (map[string]any, error)

func parserRFC5424(bestEffort bool) parserFormat {
	var opts []syslog.MachineOption
	if bestEffort {
		opts = append(opts, rfc5424.WithBestEffort())
	}
	p := rfc5424.NewParser(opts...)

	return func(body []byte) (map[string]any, error) {
		resGen, err := p.Parse(body)
		if err != nil {
			return nil, err
		}
		res := resGen.(*rfc5424.SyslogMessage)

		resMap := make(map[string]any)
		if res.Message != nil {
			resMap["message"] = *res.Message
		}
		if res.Timestamp != nil {
			resMap["timestamp"] = res.Timestamp.Format(time.RFC3339Nano)
		}
		if res.Facility != nil {
			resMap["facility"] = *res.Facility
		}
		if res.Severity != nil {
			resMap["severity"] = *res.Severity
		}
		if res.Priority != nil {
			resMap["priority"] = *res.Priority
		}
		if res.Version != 0 {
			resMap["version"] = res.Version
		}
		if res.Hostname != nil {
			resMap["hostname"] = *res.Hostname
		}
		if res.ProcID != nil {
			resMap["procid"] = *res.ProcID
		}
		if res.Appname != nil {
			resMap["appname"] = *res.Appname
		}
		if res.MsgID != nil {
			resMap["msgid"] = *res.MsgID
		}
		if res.StructuredData != nil {
			structuredData := make(map[string]any, len(*res.StructuredData))
			for key, dataItem := range *res.StructuredData {
				elements := make(map[string]any, len(dataItem))
				for itemKey, itemVal := range dataItem {
					elements[itemKey] = itemVal
				}
				structuredData[key] = elements
			}
			resMap["structureddata"] = structuredData
		}

		return resMap, nil
	}
}

func parserRFC3164(bestEffort, wrfc3339 bool, year, tz string) (parserFormat, error) {
	var opts []syslog.MachineOption
	if bestEffort {
		opts = append(opts, rfc3164.WithBestEffort())
	}
	if wrfc3339 {
		opts = append(opts, rfc3164.WithRFC3339())
	}
	switch year {
	case "current":
		opts = append(opts, rfc3164.WithYear(rfc3164.CurrentYear{}))
	case "":
		// do nothing
	default:
		iYear, err := strconv.Atoi(year)
		if err != nil {
			return nil, fmt.Errorf("failed to convert year %s into integer:  %v", year, err)
		}
		opts = append(opts, rfc3164.WithYear(rfc3164.Year{YYYY: iYear}))
	}
	if tz != "" {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return nil, fmt.Errorf("failed to lookup timezone %s - %v", loc, err)
		}
		opts = append(opts, rfc3164.WithTimezone(loc))
	}

	p := rfc3164.NewParser(opts...)

	return func(body []byte) (map[string]any, error) {
		resGen, err := p.Parse(body)
		if err != nil {
			return nil, err
		}
		res := resGen.(*rfc3164.SyslogMessage)

		resMap := make(map[string]any)
		if res.Message != nil {
			resMap["message"] = *res.Message
		}
		if res.Timestamp != nil {
			resMap["timestamp"] = res.Timestamp.Format(time.RFC3339Nano)
		}
		if res.Facility != nil {
			resMap["facility"] = *res.Facility
		}
		if res.Severity != nil {
			resMap["severity"] = *res.Severity
		}
		if res.Priority != nil {
			resMap["priority"] = *res.Priority
		}
		if res.Hostname != nil {
			resMap["hostname"] = *res.Hostname
		}
		if res.ProcID != nil {
			resMap["procid"] = *res.ProcID
		}
		if res.Appname != nil {
			resMap["appname"] = *res.Appname
		}
		if res.MsgID != nil {
			resMap["msgid"] = *res.MsgID
		}

		return resMap, nil
	}, nil
}

func getParseFormat(parser string, bestEffort, rfc3339 bool, defYear, defTZ string) (parserFormat, error) {
	switch parser {
	case "syslog_rfc5424":
		return parserRFC5424(bestEffort), nil
	case "syslog_rfc3164":
		return parserRFC3164(bestEffort, rfc3339, defYear, defTZ)
	}
	return nil, fmt.Errorf("format not recognised: %s", parser)
}

//------------------------------------------------------------------------------

type parseLogProc struct {
	format    parserFormat
	formatStr string
	log       log.Modular
}

func newParseLog(conf parseLogConfig, mgr bundle.NewManagement) (processor.AutoObserved, error) {
	s := &parseLogProc{
		formatStr: conf.Format,
		log:       mgr.Logger(),
	}
	var err error
	if s.format, err = getParseFormat(conf.Format, conf.BestEffort, conf.WithRFC3339,
		conf.WithYear, conf.WithTimezone); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *parseLogProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	dataMap, err := s.format(msg.AsBytes())
	if err != nil {
		s.log.Debug("Failed to parse message as %s: %v", s.formatStr, err)
		return nil, err
	}

	msg.SetStructuredMut(dataMap)
	return []*message.Part{msg}, nil
}

func (s *parseLogProc) Close(ctx context.Context) error {
	return nil
}

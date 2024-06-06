package pure

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/warpstreamlabs/bento/v1/internal/bloblang/field"
	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/component/interop"
	"github.com/warpstreamlabs/bento/v1/internal/component/metrics"
	"github.com/warpstreamlabs/bento/v1/internal/component/processor"
	"github.com/warpstreamlabs/bento/v1/internal/log"
	"github.com/warpstreamlabs/bento/v1/internal/message"
	"github.com/warpstreamlabs/bento/v1/public/service"
)

const (
	metProcFieldType   = "type"
	metProcFieldName   = "name"
	metProcFieldLabels = "labels"
	metProcFieldValue  = "value"
)

func metProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary("Emit custom metrics by extracting values from messages.").
		Description(`
This processor works by evaluating an [interpolated field `+"`value`"+`](/docs/configuration/interpolation#bloblang-queries) for each message and updating a emitted metric according to the [type](#types).

Custom metrics such as these are emitted along with Bento internal metrics, where you can customize where metrics are sent, which metric names are emitted and rename them as/when appropriate. For more information check out the [metrics docs here](/docs/components/metrics/about).`).
		Footnotes(`
## Types

### `+"`counter`"+`

Increments a counter by exactly 1, the contents of `+"`value`"+` are ignored
by this type.

### `+"`counter_by`"+`

If the contents of `+"`value`"+` can be parsed as a positive integer value
then the counter is incremented by this value.

For example, the following configuration will increment the value of the
`+"`count.custom.field` metric by the contents of `field.some.value`"+`:

`+"```yaml"+`
pipeline:
  processors:
    - metric:
        type: counter_by
        name: CountCustomField
        value: ${!json("field.some.value")}
`+"```"+`

### `+"`gauge`"+`

If the contents of `+"`value`"+` can be parsed as a positive integer value
then the gauge is set to this value.

For example, the following configuration will set the value of the
`+"`gauge.custom.field` metric to the contents of `field.some.value`"+`:

`+"```yaml"+`
pipeline:
  processors:
    - metric:
        type: gauge
        name: GaugeCustomField
        value: ${!json("field.some.value")}
`+"```"+`

### `+"`timing`"+`

Equivalent to `+"`gauge`"+` where instead the metric is a timing. It is recommended that timing values are recorded in nanoseconds in order to be consistent with standard Bento timing metrics, as in some cases these values are automatically converted into other units such as when exporting timings as histograms with Prometheus metrics.`).
		Example(
			"Counter",
			"In this example we emit a counter metric called `Foos`, which increments for every message processed, and we label the metric with some metadata about where the message came from and a field from the document that states what type it is. We also configure our metrics to emit to CloudWatch, and explicitly only allow our custom metric and some internal Bento metrics to emit.",
			`
pipeline:
  processors:
    - metric:
        name: Foos
        type: counter
        labels:
          topic: ${! meta("kafka_topic") }
          partition: ${! meta("kafka_partition") }
          type: ${! json("document.type").or("unknown") }

metrics:
  mapping: |
    root = if ![
      "Foos",
      "input_received",
      "output_sent"
    ].contains(this) { deleted() }
  aws_cloudwatch:
    namespace: ProdConsumer
`,
		).
		Example(
			"Gauge",
			"In this example we emit a gauge metric called `FooSize`, which is given a value extracted from JSON messages at the path `foo.size`. We then also configure our Prometheus metric exporter to only emit this custom metric and nothing else. We also label the metric with some metadata.",
			`
pipeline:
  processors:
    - metric:
        name: FooSize
        type: gauge
        labels:
          topic: ${! meta("kafka_topic") }
        value: ${! json("foo.size") }

metrics:
  mapping: 'if this != "FooSize" { deleted() }'
  prometheus: {}
`,
		).
		Fields(
			service.NewStringEnumField(metProcFieldType, "counter", "counter_by", "gauge", "timing").
				Description("The metric [type](#types) to create."),
			service.NewStringField(metProcFieldName).
				Description("The name of the metric to create, this must be unique across all Bento components otherwise it will overwrite those other metrics."),
			service.NewInterpolatedStringMapField(metProcFieldLabels).
				Description("A map of label names and values that can be used to enrich metrics. Labels are not supported by some metric destinations, in which case the metrics series are combined.").
				Example(map[string]any{
					"type":  "${! json(\"doc.type\") }",
					"topic": "${! meta(\"kafka_topic\") }",
				}).
				Optional(),
			service.NewInterpolatedStringField(metProcFieldValue).
				Description("For some metric types specifies a value to set, increment. Certain metrics exporters such as Prometheus support floating point values, but those that do not will cast a floating point value into an integer.").
				Default(""),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"metric", metProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			procTypeStr, err := conf.FieldString(metProcFieldType)
			if err != nil {
				return nil, err
			}

			procName, err := conf.FieldString(metProcFieldName)
			if err != nil {
				return nil, err
			}

			var labelMap map[string]string
			if conf.Contains(metProcFieldLabels) {
				if labelMap, err = conf.FieldStringMap(metProcFieldLabels); err != nil {
					return nil, err
				}
			}

			valueStr, err := conf.FieldString(metProcFieldValue)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newMetricProcessor(procTypeStr, procName, valueStr, labelMap, mgr)
			if err != nil {
				return nil, err
			}

			return interop.NewUnwrapInternalBatchProcessor(p), nil
		})
	if err != nil {
		panic(err)
	}
}

type metricProcessor struct {
	log log.Modular

	value  *field.Expression
	labels labels

	mCounter metrics.StatCounter
	mGauge   metrics.StatGauge
	mTimer   metrics.StatTimer

	mCounterVec metrics.StatCounterVec
	mGaugeVec   metrics.StatGaugeVec
	mTimerVec   metrics.StatTimerVec

	handler func(string, int, message.Batch) error
}

type (
	labels []label
	label  struct {
		name  string
		value *field.Expression
	}
)

func (l *label) val(index int, msg message.Batch) (string, error) {
	return l.value.String(index, msg)
}

func (l labels) names() []string {
	var names []string
	for i := range l {
		names = append(names, l[i].name)
	}
	return names
}

func (l labels) values(index int, msg message.Batch) ([]string, error) {
	var values []string
	for i := range l {
		vStr, err := l[i].val(index, msg)
		if err != nil {
			return nil, fmt.Errorf("label interpolation error: %w", err)
		}
		values = append(values, vStr)
	}
	return values, nil
}

func newMetricProcessor(typeStr, name, valueStr string, labels map[string]string, mgr bundle.NewManagement) (processor.V1, error) {
	value, err := mgr.BloblEnvironment().NewField(valueStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	m := &metricProcessor{
		log:   mgr.Logger(),
		value: value,
	}

	if name == "" {
		return nil, errors.New("metric name must not be empty")
	}

	labelNames := make([]string, 0, len(labels))
	for n := range labels {
		labelNames = append(labelNames, n)
	}
	sort.Strings(labelNames)

	for _, n := range labelNames {
		v, err := mgr.BloblEnvironment().NewField(labels[n])
		if err != nil {
			return nil, fmt.Errorf("failed to parse label '%v' expression: %v", n, err)
		}
		m.labels = append(m.labels, label{
			name:  n,
			value: v,
		})
	}

	stats := mgr.Metrics()
	switch strings.ToLower(typeStr) {
	case "counter":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(name, m.labels.names()...)
		} else {
			m.mCounter = stats.GetCounter(name)
		}
		m.handler = m.handleCounter
	case "counter_by":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(name, m.labels.names()...)
		} else {
			m.mCounter = stats.GetCounter(name)
		}
		m.handler = m.handleCounterBy
	case "gauge":
		if len(m.labels) > 0 {
			m.mGaugeVec = stats.GetGaugeVec(name, m.labels.names()...)
		} else {
			m.mGauge = stats.GetGauge(name)
		}
		m.handler = m.handleGauge
	case "timing":
		if len(m.labels) > 0 {
			m.mTimerVec = stats.GetTimerVec(name, m.labels.names()...)
		} else {
			m.mTimer = stats.GetTimer(name)
		}
		m.handler = m.handleTimer
	default:
		return nil, fmt.Errorf("metric type unrecognised: %v", typeStr)
	}

	return m, nil
}

func (m *metricProcessor) handleCounter(val string, index int, msg message.Batch) error {
	if len(m.labels) > 0 {
		labelValues, err := m.labels.values(index, msg)
		if err != nil {
			return err
		}
		m.mCounterVec.With(labelValues...).Incr(1)
	} else {
		m.mCounter.Incr(1)
	}
	return nil
}

func withNumberStr(val string, ifn func(i int64) error, ffn func(f float64) error) error {
	if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		if i < 0 {
			return fmt.Errorf("value %d is negative", i)
		}
		return ifn(i)
	}

	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return err
	}
	if f < 0 {
		return fmt.Errorf("value %f is negative", f)
	}
	return ffn(f)
}

func (m *metricProcessor) handleCounterBy(val string, index int, msg message.Batch) error {
	if len(m.labels) > 0 {
		labelValues, err := m.labels.values(index, msg)
		if err != nil {
			return err
		}
		return withNumberStr(val, func(i int64) error {
			m.mCounterVec.With(labelValues...).Incr(i)
			return nil
		}, func(f float64) error {
			m.mCounterVec.With(labelValues...).IncrFloat64(f)
			return nil
		})
	}
	return withNumberStr(val, func(i int64) error {
		m.mCounter.Incr(i)
		return nil
	}, func(f float64) error {
		m.mCounter.IncrFloat64(f)
		return nil
	})
}

func (m *metricProcessor) handleGauge(val string, index int, msg message.Batch) error {
	if len(m.labels) > 0 {
		labelValues, err := m.labels.values(index, msg)
		if err != nil {
			return err
		}
		return withNumberStr(val, func(i int64) error {
			m.mGaugeVec.With(labelValues...).Set(i)
			return nil
		}, func(f float64) error {
			m.mGaugeVec.With(labelValues...).SetFloat64(f)
			return nil
		})
	}
	return withNumberStr(val, func(i int64) error {
		m.mGauge.Set(i)
		return nil
	}, func(f float64) error {
		m.mGauge.SetFloat64(f)
		return nil
	})
}

func (m *metricProcessor) handleTimer(val string, index int, msg message.Batch) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	if i < 0 {
		return errors.New("value is negative")
	}
	if len(m.labels) > 0 {
		labelValues, err := m.labels.values(index, msg)
		if err != nil {
			return err
		}
		m.mTimerVec.With(labelValues...).Timing(i)
	} else {
		m.mTimer.Timing(i)
	}
	return nil
}

func (m *metricProcessor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	_ = msg.Iter(func(i int, p *message.Part) error {
		value, err := m.value.String(i, msg)
		if err != nil {
			m.log.Error("Value interpolation error: %v", err)
			return nil
		}
		if err := m.handler(value, i, msg); err != nil {
			m.log.Error("Handler error: %v", err)
		}
		return nil
	})
	return []message.Batch{msg}, nil
}

func (m *metricProcessor) Close(ctx context.Context) error {
	return nil
}

package service

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/warpstreamlabs/bento/v1/internal/component/metrics"
)

// Metrics allows plugin authors to emit custom metrics from components that are
// exported the same way as native Bento metrics. It's safe to pass around a
// nil pointer for testing components.
type Metrics struct {
	t metrics.Type
}

func newReverseAirGapMetrics(t metrics.Type) *Metrics {
	return &Metrics{t}
}

// NewCounter creates a new counter metric with a name and variant list of label
// keys.
func (m *Metrics) NewCounter(name string, labelKeys ...string) *MetricCounter {
	if m == nil {
		return nil
	}
	cv := m.t.GetCounterVec(name, labelKeys...)
	return &MetricCounter{cv}
}

// NewTimer creates a new timer metric with a name and variant list of label
// keys.
func (m *Metrics) NewTimer(name string, labelKeys ...string) *MetricTimer {
	if m == nil {
		return nil
	}
	tv := m.t.GetTimerVec(name, labelKeys...)
	return &MetricTimer{tv}
}

// NewGauge creates a new gauge metric with a name and variant list of label
// keys.
func (m *Metrics) NewGauge(name string, labelKeys ...string) *MetricGauge {
	if m == nil {
		return nil
	}
	gv := m.t.GetGaugeVec(name, labelKeys...)
	return &MetricGauge{gv}
}

//------------------------------------------------------------------------------

// MetricCounter represents a counter metric of a given name and labels.
type MetricCounter struct {
	cv metrics.StatCounterVec
}

// Incr increments a counter metric by an integer amount, the number of label values
// must match the number and order of labels specified when the counter was
// created.
func (c *MetricCounter) Incr(count int64, labelValues ...string) {
	if c == nil {
		return
	}
	c.cv.With(labelValues...).Incr(count)
}

// IncrFloat64 increments a counter metric by a decimal amount, the number of label values
// must match the number and order of labels specified when the counter was
// created.
func (c *MetricCounter) IncrFloat64(count float64, labelValues ...string) {
	if c == nil {
		return
	}
	c.cv.With(labelValues...).IncrFloat64(count)
}

// MetricTimer represents a timing metric of a given name and labels.
type MetricTimer struct {
	tv metrics.StatTimerVec
}

// Timing adds a delta to a timing metric. Delta should be measured in
// nanoseconds for consistency with other Bento timing metrics.
//
// The number of label values must match the number and order of labels
// specified when the timing was created.
func (t *MetricTimer) Timing(delta int64, labelValues ...string) {
	if t == nil {
		return
	}
	t.tv.With(labelValues...).Timing(delta)
}

// MetricGauge represents a gauge metric of a given name and labels.
type MetricGauge struct {
	gv metrics.StatGaugeVec
}

// Set a gauge metric, the number of label values must match the number and
// order of labels specified when the gauge was created.
func (g *MetricGauge) Set(value int64, labelValues ...string) {
	if g == nil {
		return
	}
	g.gv.With(labelValues...).Set(value)
}

// SetFloat64 sets a gauge metric to a float64 value. Not all metrics exporters
// support floats, in which case the value will be cast to an int64. The number
// of label values must match the number and order of labels specified when the
// gauge was created.
func (g *MetricGauge) SetFloat64(value float64, labelValues ...string) {
	if g == nil {
		return
	}
	g.gv.With(labelValues...).SetFloat64(value)
}

//------------------------------------------------------------------------------

// MetricsExporter is an interface implemented by Bento metrics exporters.
type MetricsExporter interface {
	NewCounterCtor(name string, labelKeys ...string) MetricsExporterCounterCtor
	NewTimerCtor(name string, labelKeys ...string) MetricsExporterTimerCtor
	NewGaugeCtor(name string, labelKeys ...string) MetricsExporterGaugeCtor
	Close(ctx context.Context) error
}

// MetricsExporterCounterCtor is a constructor for a MetricsExporterCounter that
// must be called with a variadic list of label values exactly matching the
// length and order of the label keys provided.
type MetricsExporterCounterCtor func(labelValues ...string) MetricsExporterCounter

// MetricsExporterTimerCtor is a constructor for a MetricsExporterTimer that
// must be called with a variadic list of label values exactly matching the
// length and order of the label keys provided.
type MetricsExporterTimerCtor func(labelValues ...string) MetricsExporterTimer

// MetricsExporterGaugeCtor is a constructor for a MetricsExporterGauge that
// must be called with a variadic list of label values exactly matching the
// length and order of the label keys provided.
type MetricsExporterGaugeCtor func(labelValues ...string) MetricsExporterGauge

// MetricsExporterCounter represents a counter metric of a given name and
// labels.
type MetricsExporterCounter interface {
	// Incr increments a counter metric by an integer amount, the number of label values
	// must match the number and order of labels specified when the counter was
	// created.
	Incr(count int64)

	// IncrFloat64 increments a counter metric by a decimal amount, the number of label values
	// must match the number and order of labels specified when the counter was
	// created.
	// TODO: V5 Add this (or replace the int based method)
	// IncrFloat64(count float64)
}

// MetricsExporterTimer represents a timing metric of a given name and labels.
type MetricsExporterTimer interface {
	// Timing adds a delta to a timing metric. Delta should be measured in
	// nanoseconds for consistency with other Bento timing metrics.
	//
	// The number of label values must match the number and order of labels
	// specified when the timing was created.
	Timing(delta int64)
}

// MetricsExporterGauge represents a gauge metric of a given name and labels.
type MetricsExporterGauge interface {
	// Set sets a gauge metric with an int64 value, the number of label values must match the number and
	// order of labels specified when the gauge was created.
	Set(value int64)

	// SetFloat64 sets a gauge metric with a float64 value, the number of label values must match the number and
	// order of labels specified when the gauge was created.
	// TODO: V5 Add this (or replace the int based method)
	// SetFloat64(value float64)
}

//------------------------------------------------------------------------------

// Implements internal metrics plugin interface.
type airGapMetrics struct {
	airGapped MetricsExporter
}

func newAirGapMetrics(m MetricsExporter) metrics.Type {
	return &airGapMetrics{m}
}

type airGapGauge struct {
	// TODO: This is a hack and we don't really use incr/decr internally in our
	// metrics. Can we ditch it?
	v         int64
	airGapped MetricsExporterGauge
}

func (a *airGapGauge) Incr(by int64) {
	value := atomic.AddInt64(&a.v, by)
	a.airGapped.Set(value)
}

func (a *airGapGauge) IncrFloat64(count float64) {
	a.Incr(int64(count))
}

func (a *airGapGauge) SetFloat64(value float64) {
	atomic.StoreInt64(&a.v, int64(value))
	if fer, ok := a.airGapped.(interface {
		SetFloat64(float64)
	}); ok {
		fer.SetFloat64(value)
	} else {
		a.airGapped.Set(int64(value))
	}
}

func (a *airGapGauge) Decr(by int64) {
	value := atomic.AddInt64(&a.v, -by)
	a.airGapped.Set(value)
}

func (a *airGapGauge) DecrFloat64(count float64) {
	a.Decr(int64(count))
}

func (a *airGapGauge) Set(value int64) {
	atomic.StoreInt64(&a.v, value)
	a.airGapped.Set(value)
}

type airGapCounter struct {
	airGapped MetricsExporterCounter
}

func (a *airGapCounter) Incr(count int64) {
	a.airGapped.Incr(count)
}

func (a *airGapCounter) IncrFloat64(count float64) {
	if fer, ok := a.airGapped.(interface {
		IncrFloat64(float64)
	}); ok {
		fer.IncrFloat64(count)
	} else {
		a.airGapped.Incr(int64(count))
	}
}

type airGapTiming struct {
	airGapped MetricsExporterTimer
}

func (a *airGapTiming) Timing(val int64) {
	a.airGapped.Timing(val)
}

type airGapCounterVec struct {
	ctor MetricsExporterCounterCtor
}

func (a *airGapCounterVec) With(labelValues ...string) metrics.StatCounter {
	return &airGapCounter{a.ctor(labelValues...)}
}

type airGapTimingVec struct {
	ctor MetricsExporterTimerCtor
}

func (a *airGapTimingVec) With(labelValues ...string) metrics.StatTimer {
	return &airGapTiming{a.ctor(labelValues...)}
}

type airGapGaugeVec struct {
	ctor MetricsExporterGaugeCtor
}

func (a *airGapGaugeVec) With(labelValues ...string) metrics.StatGauge {
	return &airGapGauge{airGapped: a.ctor(labelValues...)}
}

func (m *airGapMetrics) GetCounter(path string) metrics.StatCounter {
	return m.GetCounterVec(path).With()
}

func (m *airGapMetrics) GetCounterVec(path string, labelNames ...string) metrics.StatCounterVec {
	return &airGapCounterVec{m.airGapped.NewCounterCtor(path, labelNames...)}
}

func (m *airGapMetrics) GetTimer(path string) metrics.StatTimer {
	return m.GetTimerVec(path).With()
}

func (m *airGapMetrics) GetTimerVec(path string, labelNames ...string) metrics.StatTimerVec {
	return &airGapTimingVec{m.airGapped.NewTimerCtor(path, labelNames...)}
}

func (m *airGapMetrics) GetGauge(path string) metrics.StatGauge {
	return m.GetGaugeVec(path).With()
}

func (m *airGapMetrics) GetGaugeVec(path string, labelNames ...string) metrics.StatGaugeVec {
	return &airGapGaugeVec{m.airGapped.NewGaugeCtor(path, labelNames...)}
}

func (m *airGapMetrics) HandlerFunc() http.HandlerFunc {
	if hf, ok := m.airGapped.(interface {
		HandlerFunc() http.HandlerFunc
	}); ok {
		return hf.HandlerFunc()
	}
	return nil
}

func (m *airGapMetrics) Close() error {
	return m.airGapped.Close(context.Background())
}

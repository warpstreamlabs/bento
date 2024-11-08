package pure

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/warpstreamlabs/bento/internal/periodic"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterProcessor("benchmark", benchmarkSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			reporter := benchmarkLogReporter{logger: mgr.Logger()}
			return newBenchmarkProcFromConfig(conf, reporter, time.Now)
		},
	)
	if err != nil {
		panic(err)
	}
}

const (
	bmFieldInterval   = "interval"
	bmFieldCountBytes = "count_bytes"
)

func benchmarkSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Summary("Logs basic throughput statistics of messages that pass through this processor.").
		Description("Logs messages per second and bytes per second of messages that are processed at a regular interval. A summary of the amount of messages processed over the entire lifetime of the processor will also be printed when the processor shuts down.").
		Field(service.NewDurationField(bmFieldInterval).
			Description("How often to emit rolling statistics. If set to 0, only a summary will be logged when the processor shuts down.").
			Default("5s"),
		).
		Field(service.NewBoolField(bmFieldCountBytes).
			Description("Whether or not to measure the number of bytes per second of throughput. Counting the number of bytes requires serializing structured data, which can cause an unnecessary performance hit if serialization is not required elsewhere in the pipeline.").
			Default(true),
		)
}

func newBenchmarkProcFromConfig(conf *service.ParsedConfig, reporter benchmarkReporter, now func() time.Time) (service.Processor, error) {
	interval, err := conf.FieldDuration(bmFieldInterval)
	if err != nil {
		return nil, err
	}
	countBytes, err := conf.FieldBool(bmFieldCountBytes)
	if err != nil {
		return nil, err
	}

	b := &benchmarkProc{
		startTime:  now(),
		countBytes: countBytes,
		reporter:   reporter,
		now:        now,
	}

	if interval.String() != "0s" {
		b.periodic = periodic.New(interval, func() {
			stats := b.sampleRolling()
			b.printStats("rolling", stats, interval)
		})
		b.periodic.Start()
	}

	return b, nil
}

type benchmarkProc struct {
	startTime  time.Time
	countBytes bool
	reporter   benchmarkReporter

	lock         sync.Mutex
	rollingStats benchmarkStats
	totalStats   benchmarkStats
	closed       bool

	periodic *periodic.Periodic
	now      func() time.Time
}

func (b *benchmarkProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var bytesCount float64
	if b.countBytes {
		bytes, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("getting message bytes: %w", err)
		}
		bytesCount = float64(len(bytes))
	}

	b.lock.Lock()
	b.rollingStats.msgCount++
	b.totalStats.msgCount++
	if b.countBytes {
		b.rollingStats.msgBytesCount += bytesCount
		b.totalStats.msgBytesCount += bytesCount
	}
	b.lock.Unlock()

	return service.MessageBatch{msg}, nil
}

func (b *benchmarkProc) Close(ctx context.Context) error {
	// 2024-11-05: We have to guard against Close being from multiple goroutines
	// at the same time.
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.closed {
		return nil
	}
	if b.periodic != nil {
		b.periodic.Stop()
	}
	b.printStats("total", b.totalStats, b.now().Sub(b.startTime))
	b.closed = true
	return nil
}

func (b *benchmarkProc) sampleRolling() benchmarkStats {
	b.lock.Lock()
	defer b.lock.Unlock()

	s := b.rollingStats
	b.rollingStats.msgCount = 0
	b.rollingStats.msgBytesCount = 0
	return s
}

func (b *benchmarkProc) printStats(window string, stats benchmarkStats, interval time.Duration) {
	secs := interval.Seconds()
	msgsPerSec := stats.msgCount / secs

	if b.countBytes {
		bytesPerSec := stats.msgBytesCount / secs
		b.reporter.reportStats(window, msgsPerSec, bytesPerSec)
	} else {
		b.reporter.reportStatsNoBytes(window, msgsPerSec)
	}
}

type benchmarkStats struct {
	msgCount      float64
	msgBytesCount float64
}

type benchmarkReporter interface {
	reportStats(window string, msgsPerSec, bytesPerSec float64)
	reportStatsNoBytes(window string, msgsPerSec float64)
}

type benchmarkLogReporter struct {
	logger *service.Logger
}

func (l benchmarkLogReporter) reportStats(window string, msgsPerSec, bytesPerSec float64) {
	l.logger.
		With(
			"msg/sec", msgsPerSec,
			"bytes/sec", bytesPerSec,
		).
		Infof(
			"%s stats: %s msg/sec, %s/sec",
			window,
			humanize.Ftoa(msgsPerSec),
			humanize.Bytes(uint64(bytesPerSec)),
		)
}

func (l benchmarkLogReporter) reportStatsNoBytes(window string, msgsPerSec float64) {
	l.logger.
		With("msg/sec", msgsPerSec).
		Infof(
			"%s stats: %s msg/sec",
			window,
			humanize.Ftoa(msgsPerSec),
		)
}

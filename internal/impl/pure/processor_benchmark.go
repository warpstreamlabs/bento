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
	err := service.RegisterProcessor("benchmark", benchmarkSpec(), newBenchmarkProcFromConfig)
	if err != nil {
		panic(err)
	}
}

const (
	bmFieldInterval = "interval"
)

func benchmarkSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Summary("Logs basic throughput statistics of message that pass through this processor.").
		Description("Logs messages per second and bytes per second of messages that are processed at a regular interval. A summary of the amount of messages processed over the entire lifetime of the processor will also be printed when the processor shuts down.").
		Field(service.NewDurationField(bmFieldInterval).
			Description("How often to emit rolling statistics. If set to 0, only a summary will be logged when the processor shuts down.").
			Default("5s").
			Description("How often to emit rolling statistics."),
		)
}

func newBenchmarkProcFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	interval, err := conf.FieldDuration(bmFieldInterval)
	if err != nil {
		return nil, err
	}

	b := &benchmarkProc{
		startTime:       time.Now(),
		rollingInterval: interval,
		logger:          mgr.Logger(),
	}

	if interval.String() != "0s" {
		b.periodic = periodic.New(interval, func() {
			stats := b.sampleRolling()
			b.printStats("rolling", stats, b.rollingInterval)
		})
		b.periodic.Start()
	}

	return b, nil
}

type benchmarkProc struct {
	startTime       time.Time
	rollingInterval time.Duration
	logger          *service.Logger

	lock         sync.Mutex
	rollingStats benchmarkStats
	totalStats   benchmarkStats
	closed       bool

	periodic *periodic.Periodic
}

func (b *benchmarkProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	bytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("getting message bytes: %w", err)
	}

	bytesCount := float64(len(bytes))

	b.lock.Lock()
	b.rollingStats.recordMessage(bytesCount)
	b.totalStats.recordMessage(bytesCount)
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
	b.printStats("total", b.totalStats, time.Since(b.startTime))
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
	bytesPerSec := stats.msgBytesCount / secs
	b.logger.
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

type benchmarkStats struct {
	msgCount      float64
	msgBytesCount float64
}

func (s *benchmarkStats) recordMessage(bytesCount float64) {
	s.msgCount++
	s.msgCount++
	s.msgBytesCount += float64(bytesCount)
}

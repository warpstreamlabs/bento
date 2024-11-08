package pure

import (
	"context"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestBenchmarkProcessor(t *testing.T) {
	conf, err := benchmarkSpec().ParseYAML(`
interval: 0
`, nil)
	require.NoError(t, err)

	currentTime := time.Now()
	now := func() time.Time { return currentTime }
	reporter := &mockBenchmarkReporter{}

	benchmarkProc, err := newBenchmarkProcFromConfig(conf, reporter, now)
	require.NoError(t, err)

	msg := service.NewMessage([]byte("hello"))
	for i := 0; i < 5; i++ {
		batch, err := benchmarkProc.Process(context.Background(), msg)
		require.NoError(t, err)
		require.Equal(t, service.MessageBatch{msg}, batch)
	}

	currentTime = currentTime.Add(time.Second)
	benchmarkProc.Close(context.Background())

	require.Equal(t, []mockBenchmarkReport{
		{
			window:      "total",
			msgsPerSec:  5,
			bytesPerSec: 25,
		},
	}, reporter.reports)
}

func TestBenchmarkProcessorNoBytes(t *testing.T) {
	conf, err := benchmarkSpec().ParseYAML(`
interval: 0
count_bytes: false
`, nil)
	require.NoError(t, err)

	currentTime := time.Now()
	now := func() time.Time { return currentTime }
	reporter := &mockBenchmarkReporter{}

	benchmarkProc, err := newBenchmarkProcFromConfig(conf, reporter, now)
	require.NoError(t, err)

	msg := service.NewMessage([]byte("hello"))
	for i := 0; i < 5; i++ {
		batch, err := benchmarkProc.Process(context.Background(), msg)
		require.NoError(t, err)
		require.Equal(t, service.MessageBatch{msg}, batch)
	}

	currentTime = currentTime.Add(time.Second)
	benchmarkProc.Close(context.Background())

	require.Equal(t, []mockBenchmarkReport{
		{
			window:      "total",
			msgsPerSec:  5,
			bytesPerSec: -1,
		},
	}, reporter.reports)
}

type mockBenchmarkReporter struct {
	reports []mockBenchmarkReport
}

type mockBenchmarkReport struct {
	window      string
	msgsPerSec  float64
	bytesPerSec float64
}

func (l *mockBenchmarkReporter) reportStats(window string, msgsPerSec, bytesPerSec float64) {
	l.reports = append(l.reports, mockBenchmarkReport{
		window:      window,
		msgsPerSec:  msgsPerSec,
		bytesPerSec: bytesPerSec,
	})
}

func (l *mockBenchmarkReporter) reportStatsNoBytes(window string, msgsPerSec float64) {
	l.reports = append(l.reports, mockBenchmarkReport{
		window:      window,
		msgsPerSec:  msgsPerSec,
		bytesPerSec: -1,
	})
}

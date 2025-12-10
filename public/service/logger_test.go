package service

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
)

func TestReverseAirGapLogger(t *testing.T) {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	agLogger := newReverseAirGapLogger(logger)
	agLogger2 := agLogger.With("field1", "value1", "field2", "value2")

	agLogger.Debugf("foo: %v", "bar1")
	agLogger.Infof("foo: %v", "bar2")

	agLogger2.Debugf("foo2: %v", "bar1")
	agLogger2.Infof("foo2: %v", "bar2")

	agLogger.Warnf("foo: %v", "bar3")
	agLogger.Errorf("foo: %v", "bar4")

	agLogger2.Warnf("foo2: %v", "bar3")
	agLogger2.Errorf("foo2: %v", "bar4")

	assert.Equal(t, `{"@service":"bento","level":"info","msg":"foo: bar2"}
{"@service":"bento","field1":"value1","field2":"value2","level":"info","msg":"foo2: bar2"}
{"@service":"bento","level":"warning","msg":"foo: bar3"}
{"@service":"bento","level":"error","msg":"foo: bar4"}
{"@service":"bento","field1":"value1","field2":"value2","level":"warning","msg":"foo2: bar3"}
{"@service":"bento","field1":"value1","field2":"value2","level":"error","msg":"foo2: bar4"}
`, buf.String())
}

func TestReverseAirGapLoggerDodgyFields(t *testing.T) {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	agLogger := newReverseAirGapLogger(logger)

	agLogger.With("field1", "value1", "field2").Infof("foo1")
	agLogger.With(10, 20).Infof("foo2")
	agLogger.With("field3", 30).Infof("foo3")
	agLogger.With("field4", "value4").With("field5", "value5").Infof("foo4")

	assert.Equal(t, `{"@service":"bento","field1":"value1","level":"info","msg":"foo1"}
{"10":"20","@service":"bento","level":"info","msg":"foo2"}
{"@service":"bento","field3":"30","level":"info","msg":"foo3"}
{"@service":"bento","field4":"value4","field5":"value5","level":"info","msg":"foo4"}
`, buf.String())
}

func TestAirGapLogger(t *testing.T) {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	agLogger := newAirGapLogger(logger)

	agLogger.Error("foo: %v", "bar1")
	agLogger.Warn("foo: %v", "bar2")
	agLogger.Info("foo: %v", "bar3")
	agLogger.Debug("foo: %v", "bar4")

	agLogger.With("key", "value").Info("log")

	assert.Equal(t, `{"@service":"bento","level":"error","msg":"foo: bar1"}
{"@service":"bento","level":"warning","msg":"foo: bar2"}
{"@service":"bento","level":"info","msg":"foo: bar3"}
{"@service":"bento","key":"value","level":"info","msg":"log"}
`, buf.String())
}

func TestAirGapSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	slogLogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))

	airGap := newAirGapLogger(slogLogger)
	airGap.Info("test message")

	assert.Equal(t, `{"level":"INFO","msg":"test message"}
`, buf.String())
}

func TestAirGapLoggerChaining(t *testing.T) {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	agLogger := newAirGapLogger(logger)

	agLogger.WithFields(map[string]string{"service": "test", "version": "1.0"}).Info("with fields")
	agLogger.With("key1", "value1", "key2", "value2").Info("with context")
	agLogger.WithFields(map[string]string{"service": "api"}).With("request_id", "123").Info("chained")

	chainedLogger := agLogger.WithFields(map[string]string{"service": "api", "version": "1.0"})
	chainedLogger2 := chainedLogger.WithFields(map[string]string{"env": "prod"})
	chainedLogger2.Info("multiple withfields")

	chainedLogger3 := agLogger.With("request_id", "123", "user_id", "456")
	chainedLogger4 := chainedLogger3.With("action", "login")
	chainedLogger4.Info("multiple with")

	chain1 := agLogger.WithFields(map[string]string{"chain": "1"})
	chain2 := agLogger.WithFields(map[string]string{"chain": "2"})
	chain1.Info("from chain 1")
	chain2.Info("from chain 2")

	assert.Equal(t, `{"@service":"bento","level":"info","msg":"with fields","service":"test","version":"1.0"}
{"@service":"bento","key1":"value1","key2":"value2","level":"info","msg":"with context"}
{"@service":"bento","level":"info","msg":"chained","request_id":"123","service":"api"}
{"@service":"bento","env":"prod","level":"info","msg":"multiple withfields","service":"api","version":"1.0"}
{"@service":"bento","action":"login","level":"info","msg":"multiple with","request_id":"123","user_id":"456"}
{"@service":"bento","chain":"1","level":"info","msg":"from chain 1"}
{"@service":"bento","chain":"2","level":"info","msg":"from chain 2"}
`, buf.String())
}

func TestAirGapLoggerTrace(t *testing.T) {
	lConf := log.NewConfig()
	lConf.AddTimeStamp = false
	lConf.Format = "json"
	lConf.LogLevel = "trace"

	var buf bytes.Buffer
	logger, err := log.New(&buf, ifs.OS(), lConf)
	require.NoError(t, err)

	agLogger := newAirGapLogger(logger)
	agLogger.Trace("trace message: %s", "test")

	assert.Equal(t, `{"@service":"bento","level":"trace","msg":"trace message: test"}
`, buf.String())
}

type mockBasicLogger struct {
	logs *[]string
}

func (m *mockBasicLogger) Error(format string, v ...any) {
	*m.logs = append(*m.logs, fmt.Sprintf("ERROR: "+format, v...))
}

func (m *mockBasicLogger) Warn(format string, v ...any) {
	*m.logs = append(*m.logs, fmt.Sprintf("WARN: "+format, v...))
}

func (m *mockBasicLogger) Info(format string, v ...any) {
	*m.logs = append(*m.logs, fmt.Sprintf("INFO: "+format, v...))
}

func (m *mockBasicLogger) Debug(format string, v ...any) {
	*m.logs = append(*m.logs, fmt.Sprintf("DEBUG: "+format, v...))
}

func TestAirGapLoggerNonChainingLogger(t *testing.T) {
	var logs []string
	basicLogger := &struct {
		LeveledLogger
	}{
		LeveledLogger: &mockBasicLogger{logs: &logs},
	}

	agLogger := newAirGapLogger(basicLogger)

	chainedLogger := agLogger.WithFields(map[string]string{"ignored": "field"})
	chainedLogger2 := chainedLogger.With("ignored", "context")
	chainedLogger2.Info("test message")

	expected := []string{"INFO: test message"}
	assert.Equal(t, expected, logs)
}

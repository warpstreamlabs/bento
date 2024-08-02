package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

func TestLogger(t *testing.T) {
	tests := []struct {
		name        string
		fields      map[string]string
		message     string
		level       string
		format      string
		levelName   string
		messageName string
		expected    string
	}{
		{
			name:    "logger with default format and level emits INFO logs",
			message: "Info message",
			expected: `level=info msg="Info message" @service=benthos_service @system=foo
`,
		},
		{
			name:    "logger with DEBUG level emits DEBUG logs",
			level:   "DEBUG",
			message: "Info message",
			expected: `level=debug msg="debug message" @service=benthos_service @system=foo
level=info msg="Info message" @service=benthos_service @system=foo
`,
		},
		{
			name: "logger with WARN level and custom fields doesn't emit INFO logs",
			fields: map[string]string{
				"foo": "bar",
			},
			level:   "WARN",
			message: "Warning message",
			expected: `level=warning msg="Warning message" @service=benthos_service @system=foo foo=bar
`,
		},
		{
			name: "logger with custom fields",
			fields: map[string]string{
				"foo":    "bar",
				"count":  "10",
				"thing":  "is a string",
				"iscool": "true",
			},
			level:   "WARN",
			message: "Warning message foo fields",
			expected: `level=warning msg="Warning message foo fields" @service=benthos_service @system=foo count=10 foo=bar iscool=true thing="is a string"
`,
		},
		{
			name:    "logger with fmt strings",
			message: "foo%22bar",
			expected: `level=info msg="foo%22bar" @service=benthos_service @system=foo
`,
		},
		{
			name:        "logger with non-default LevelName and MessageName",
			message:     "Info message foo fields",
			levelName:   "severity",
			messageName: "message",
			expected: `severity=info message="Info message foo fields" @service=benthos_service @system=foo
`,
		},
		{
			name: "logger with json format",
			fields: map[string]string{
				"foo": "bar",
			},
			level:   "WARN",
			format:  "json",
			message: "Warning message foo fields",
			expected: `{"@service":"benthos_service","@system":"foo","foo":"bar","level":"warning","msg":"Warning message foo fields"}
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			loggerConfig := NewConfig()
			loggerConfig.AddTimeStamp = false
			loggerConfig.StaticFields = map[string]string{
				"@service": "benthos_service",
				"@system":  "foo",
			}

			if test.level != "" {
				loggerConfig.LogLevel = test.level
			}
			if test.format != "" {
				loggerConfig.Format = test.format
			}
			if test.levelName != "" {
				loggerConfig.LevelName = test.levelName
			}
			if test.messageName != "" {
				loggerConfig.MessageName = test.messageName
			}

			var buf bytes.Buffer

			logger, err := New(&buf, ifs.OS(), loggerConfig)
			require.NoError(t, err)

			logger.Debug("debug message")

			if test.fields != nil {
				logger = logger.WithFields(test.fields)
				require.NoError(t, err)

				logger.Warn(test.message)
			}

			logger.Info(test.message)

			assert.Equal(t, test.expected, buf.String())
		})
	}
}

type logCounter struct {
	count int
}

func (l *logCounter) Write(p []byte) (n int, err error) {
	l.count++
	return len(p), nil
}

func TestLogLevels(t *testing.T) {
	for i, lvl := range []string{
		"FATAL",
		"ERROR",
		"WARN",
		"INFO",
		"DEBUG",
		"TRACE",
	} {
		loggerConfig := NewConfig()
		loggerConfig.LogLevel = lvl

		buf := logCounter{}

		logger, err := New(&buf, ifs.OS(), loggerConfig)
		require.NoError(t, err)

		logger.Error("error test")
		logger.Warn("warn test")
		logger.Info("info test")
		logger.Debug("info test")
		logger.Trace("trace test")

		if i != buf.count {
			t.Errorf("Wrong log count for [%v], %v != %v", loggerConfig.LogLevel, i, buf.count)
		}
	}
}

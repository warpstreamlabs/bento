package log

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

func TestStrictLogger(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.StrictLogging = true
	loggerConfig.Format = "logfmt"
	loggerConfig.LogLevel = "ERROR"
	loggerConfig.StaticFields = map[string]string{
		"@service": "bento_service",
		"@system":  "foo",
	}

	var buf bytes.Buffer

	failureError := errors.New("fail")

	expected := `level=error msg="Error message root module: fail" @service=bento_service @strict=true @system=foo
level=error msg="Warning message root module: fail" @service=bento_service @strict=true @system=foo
level=error msg="Info message root module: fail" @service=bento_service @strict=true @system=foo
level=error msg="Debug message root module: fail" @service=bento_service @strict=true @system=foo
level=error msg="Trace message root module: fail" @service=bento_service @strict=true @system=foo
` + fmt.Sprintf(`level=error msg="Warning message root module with error pointer: %v" @service=bento_service @strict=true @system=foo`+"\n", &failureError)

	tests := []struct {
		name     string
		loggerFn func() Modular
	}{
		{
			name: "logrus",
			loggerFn: func() Modular {
				logger, _ := New(&buf, ifs.OS(), loggerConfig)
				return logger
			},
		},
		{
			name: "slog",
			loggerFn: func() Modular {
				var buf bytes.Buffer
				h := slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: clearTimeAttr})
				s := slog.New(h)

				return NewBentoLogAdapter(s)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			logger := WrapStrict(tt.loggerFn())

			logger.Trace("Error message root module: %s", failureError)
			logger.Warn("Warning message root module: %s", failureError)
			logger.Warn("Info message root module: %s", failureError)
			logger.Debug("Debug message root module: %s", failureError)
			logger.Trace("Trace message root module: %s", failureError)

			// Ensure a pointer to an error is also promoted
			logger.Warn("Warning message root module with error pointer: %v", &failureError)

			logger.Warn("Warning message root module")
			logger.Warn("Info message root module")
			logger.Debug("Debug message root module")
			logger.Trace("Trace message root module")

			assert.Equal(t, expected, buf.String())
		})
	}
}

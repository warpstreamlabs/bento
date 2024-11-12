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

type testErr struct {
}

func (t testErr) Error() string {
	return "custom error"
}
func (t testErr) Foo() string {
	return "bar"
}

func TestErrorPromotionLogger(t *testing.T) {
	loggerConfig := NewConfig()
	loggerConfig.AddTimeStamp = false
	loggerConfig.LogAllErrors = true
	loggerConfig.Format = "logfmt"
	loggerConfig.LogLevel = "ERROR"
	loggerConfig.StaticFields = map[string]string{
		"@service": "bento_service",
		"@system":  "foo",
	}

	var buf bytes.Buffer

	failureError := errors.New("fail")
	customErr := &testErr{}

	expected := `level=error msg="Error message root module: fail" @log_all_errors=true @service=bento_service @system=foo
level=error msg="Warning message root module: fail" @log_all_errors=true @service=bento_service @system=foo
level=error msg="Info message root module: fail" @log_all_errors=true @service=bento_service @system=foo
level=error msg="Debug message root module: fail" @log_all_errors=true @service=bento_service @system=foo
level=error msg="Trace message root module: fail" @log_all_errors=true @service=bento_service @system=foo
` + fmt.Sprintf(`level=error msg="Warning message root module with error pointer: %v" @log_all_errors=true @service=bento_service @system=foo`+"\n", &failureError) +
		`level=error msg="Trace message root module with custom error: custom error" @log_all_errors=true @service=bento_service @system=foo` + "\n"

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

			logger := WrapErrPromoter(tt.loggerFn())

			logger.Trace("Error message root module: %s", failureError)
			logger.Warn("Warning message root module: %s", failureError)
			logger.Warn("Info message root module: %s", failureError)
			logger.Debug("Debug message root module: %s", failureError)
			logger.Trace("Trace message root module: %s", failureError)

			// Ensure a pointer to an error is also promoted
			logger.Warn("Warning message root module with error pointer: %v", &failureError)

			// Ensure a custom error type is promoted
			logger.Trace("Trace message root module with custom error: %s", customErr)

			logger.Warn("Warning message root module")
			logger.Warn("Info message root module")
			logger.Debug("Debug message root module")
			logger.Trace("Trace message root module")

			assert.Equal(t, expected, buf.String())
		})
	}
}

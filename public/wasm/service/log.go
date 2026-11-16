package service

import logv1 "github.com/warpstreamlabs/bento/internal/plugin/components/log_v1"

// Logger returns a logger that writes to the logger of the processor running
// the plugin. A plugin that logs must enable logging in its manifest.
func (r *Resources) Logger() *Logger {
	return &Logger{}
}

// Logger writes plugin logs to Bento's logger.
type Logger struct{}

func (*Logger) Trace(message string) { logv1.Trace(message) }
func (*Logger) Debug(message string) { logv1.Debug(message) }
func (*Logger) Info(message string)  { logv1.Info(message) }
func (*Logger) Warn(message string)  { logv1.Warn(message) }
func (*Logger) Error(message string) { logv1.Error(message) }

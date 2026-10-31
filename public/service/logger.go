package service

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/warpstreamlabs/bento/internal/log"
)

// airGapLogger adapts a LeveledLogger to implement log.Modular for use in Bento streams.
type airGapLogger struct {
	l LeveledLogger
}

func newAirGapLogger(logger LeveledLogger) log.Modular {
	return &airGapLogger{l: logger}
}

func (a *airGapLogger) WithFields(fields map[string]string) log.Modular {
	if a.l == nil {
		return nil
	}

	switch t := a.l.(type) {
	case interface {
		WithFields(fields map[string]string) log.Modular
	}:
		return &airGapLogger{l: t.WithFields(fields)}
	case *slog.Logger:
		args := make([]any, 0, len(fields)*2)
		for k, v := range fields {
			args = append(args, slog.String(k, v))
		}
		return &airGapLogger{l: t.With(args...)}
	}

	return a.clone()
}

func (a *airGapLogger) With(keyValues ...any) log.Modular {
	if a.l == nil {
		return nil
	}

	switch t := a.l.(type) {
	case interface {
		With(keyValues ...any) log.Modular
	}:
		return &airGapLogger{l: t.With(keyValues...)}
	case *slog.Logger:
		return &airGapLogger{l: t.With(keyValues...)}
	}

	return a.clone()
}

// printf formats format/v the same way fmt.Sprintf does when there are
// arguments, but leaves a no-arg message untouched so a literal '%' in it
// isn't mangled into %!(NOVERB).
func printf(format string, v ...any) string {
	if len(v) == 0 {
		return format
	}
	return fmt.Sprintf(format, v...)
}

// slogForward formats format/v and logs the result through fn, used for the
// *slog.Logger case of each level below: slog.Logger.Error etc. treat v as
// key/value attrs rather than printf args, so the message must be formatted
// before forwarding. Calling the concrete *slog.Logger method (rather than
// through the LeveledLogger interface) also keeps go vet from flagging the
// resulting non-constant message as a bad format string.
func slogForward(fn func(string, ...any), format string, v ...any) {
	fn(printf(format, v...))
}

func (a *airGapLogger) Error(format string, v ...any) {
	if a.l == nil {
		return
	}
	if sl, ok := a.l.(*slog.Logger); ok {
		slogForward(sl.Error, format, v...)
		return
	}
	a.l.Error(format, v...)
}
func (a *airGapLogger) Warn(format string, v ...any) {
	if a.l == nil {
		return
	}
	if sl, ok := a.l.(*slog.Logger); ok {
		slogForward(sl.Warn, format, v...)
		return
	}
	a.l.Warn(format, v...)
}
func (a *airGapLogger) Info(format string, v ...any) {
	if a.l == nil {
		return
	}
	if sl, ok := a.l.(*slog.Logger); ok {
		slogForward(sl.Info, format, v...)
		return
	}
	a.l.Info(format, v...)
}
func (a *airGapLogger) Debug(format string, v ...any) {
	if a.l == nil {
		return
	}
	if sl, ok := a.l.(*slog.Logger); ok {
		slogForward(sl.Debug, format, v...)
		return
	}
	a.l.Debug(format, v...)
}
func (a *airGapLogger) Trace(format string, v ...any) {
	if a.l == nil {
		return
	}

	switch fl := a.l.(type) {
	case interface {
		Trace(format string, v ...any)
	}:
		fl.Trace(format, v...)
		return
	}
	// Logger does not implement Trace, so fallback to Debug.
	a.Debug(format, v...)
}

func (a *airGapLogger) Fatal(format string, v ...any) {
	if a.l == nil {
		return
	}

	switch fl := a.l.(type) {
	case interface {
		Fatal(format string, v ...any)
	}:
		fl.Fatal(format, v...)
		return
	}
	// Logger does not implement Fatal, so fallback to
	// Error and exit with a status code 1.
	a.Error(format, v...)
	os.Exit(1)
}

func (a *airGapLogger) clone() *airGapLogger {
	if a.l == nil {
		return nil
	}
	l := *a
	return &l
}

// Logger allows plugin authors to write custom logs from components that are
// exported the same way as native Bento logs. It's safe to pass around a nil
// pointer for testing components.
type Logger struct {
	m log.Modular
}

func newReverseAirGapLogger(l log.Modular) *Logger {
	return &Logger{l}
}

// Tracef logs a trace message using fmt.Sprintf when args are specified.
func (l *Logger) Tracef(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Trace(template, args...)
}

// Trace logs a trace message.
func (l *Logger) Trace(message string) {
	if l == nil {
		return
	}
	l.m.Trace("%s", message)
}

// Debugf logs a debug message using fmt.Sprintf when args are specified.
func (l *Logger) Debugf(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Debug(template, args...)
}

// Debug logs a debug message.
func (l *Logger) Debug(message string) {
	if l == nil {
		return
	}
	l.m.Debug("%s", message)
}

// Infof logs an info message using fmt.Sprintf when args are specified.
func (l *Logger) Infof(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Info(template, args...)
}

// Info logs an info message.
func (l *Logger) Info(message string) {
	if l == nil {
		return
	}
	l.m.Info("%s", message)
}

// Warnf logs a warning message using fmt.Sprintf when args are specified.
func (l *Logger) Warnf(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Warn(template, args...)
}

// Warn logs a warning message.
func (l *Logger) Warn(message string) {
	if l == nil {
		return
	}
	l.m.Warn("%s", message)
}

// Errorf logs an error message using fmt.Sprintf when args are specified.
func (l *Logger) Errorf(template string, args ...any) {
	if l == nil {
		return
	}
	l.m.Error(template, args...)
}

// Error logs an error message.
func (l *Logger) Error(message string) {
	if l == nil {
		return
	}
	l.m.Error("%s", message)
}

// With adds a variadic set of fields to a logger. Each field must consist
// of a string key and a value of any type. An odd number of key/value pairs
// will therefore result in malformed log messages, but should never panic.
func (l *Logger) With(keyValuePairs ...any) *Logger {
	if l == nil {
		return nil
	}
	fields := map[string]string{}
	for i := 0; i < (len(keyValuePairs) - 1); i += 2 {
		key, ok := keyValuePairs[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyValuePairs[i])
		}
		value, ok := keyValuePairs[i+1].(string)
		if !ok {
			value = fmt.Sprintf("%v", keyValuePairs[i+1])
		}
		fields[key] = value
	}
	lg := l.m.WithFields(fields)
	return &Logger{lg}
}

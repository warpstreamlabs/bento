package log

type strictLogger struct {
	logger Modular
}

func WrapStrict(l Modular) Modular {
	wrappedLogger := &strictLogger{
		logger: l,
	}

	return wrappedLogger.With("@strict", true)
}

func (s *strictLogger) promoteOnError(promotedFrom, format string, v ...any) bool {
	for _, arg := range v {
		if arg == nil {
			continue
		}

		switch arg.(type) {
		case error, *error:
			s.logger.Trace("Error passed to strict logger: %s promoting to ERROR", promotedFrom)
			s.logger.Error(format, v...)
			return true
		}
	}

	return false

}

//------------------------------------------------------------------------------

func (s *strictLogger) WithFields(fields map[string]string) Modular {
	return &strictLogger{
		logger: s.logger.WithFields(fields),
	}
}

func (s *strictLogger) With(keyValues ...any) Modular {
	return &strictLogger{
		logger: s.logger.With(keyValues...),
	}
}

func (s *strictLogger) Fatal(format string, v ...any) {
	s.logger.Fatal(format, v...)
}

func (s *strictLogger) Error(format string, v ...any) {
	s.logger.Error(format, v...)
}

func (s *strictLogger) Warn(format string, v ...any) {
	if hasErr := s.promoteOnError("WARN", format, v...); !hasErr {
		s.logger.Warn(format, v...)
	}
}

func (s *strictLogger) Info(format string, v ...any) {
	if hasErr := s.promoteOnError("INFO", format, v...); !hasErr {
		s.logger.Info(format, v...)
	}
}

func (s *strictLogger) Debug(format string, v ...any) {
	if hasErr := s.promoteOnError("DEBUG", format, v...); !hasErr {
		s.logger.Debug(format, v...)
	}
}

func (s *strictLogger) Trace(format string, v ...any) {
	if hasErr := s.promoteOnError("TRACE", format, v...); !hasErr {
		s.logger.Trace(format, v...)
	}
}

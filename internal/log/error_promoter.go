package log

type errPromotionLogger struct {
	logger Modular
}

func WrapErrPromoter(l Modular) Modular {
	wrappedLogger := &errPromotionLogger{
		logger: l,
	}

	return wrappedLogger.With("@log_all_errors", true)
}

func (s *errPromotionLogger) promoteOnError(promotedFrom, format string, v ...any) bool {
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

func (s *errPromotionLogger) WithFields(fields map[string]string) Modular {
	return &errPromotionLogger{
		logger: s.logger.WithFields(fields),
	}
}

func (s *errPromotionLogger) With(keyValues ...any) Modular {
	return &errPromotionLogger{
		logger: s.logger.With(keyValues...),
	}
}

func (s *errPromotionLogger) Fatal(format string, v ...any) {
	s.logger.Fatal(format, v...)
}

func (s *errPromotionLogger) Error(format string, v ...any) {
	s.logger.Error(format, v...)
}

func (s *errPromotionLogger) Warn(format string, v ...any) {
	if hasErr := s.promoteOnError("WARN", format, v...); !hasErr {
		s.logger.Warn(format, v...)
	}
}

func (s *errPromotionLogger) Info(format string, v ...any) {
	if hasErr := s.promoteOnError("INFO", format, v...); !hasErr {
		s.logger.Info(format, v...)
	}
}

func (s *errPromotionLogger) Debug(format string, v ...any) {
	if hasErr := s.promoteOnError("DEBUG", format, v...); !hasErr {
		s.logger.Debug(format, v...)
	}
}

func (s *errPromotionLogger) Trace(format string, v ...any) {
	if hasErr := s.promoteOnError("TRACE", format, v...); !hasErr {
		s.logger.Trace(format, v...)
	}
}

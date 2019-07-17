package internal

import (
	"github.com/google/logger"
	"io"
	"os"
	"path"
)

type loggerCloser struct {
	logio  io.Closer
	logger *logger.Logger
}

func (c *loggerCloser) Close() {
	c.logger.Close()
	if c.logio != nil {
		_ = c.logio.Close()
	}
}

func SetupLogger(cfg *Config) *loggerCloser {
	var err error
	var logio io.Writer = os.Stderr

	if cfg.Log.WriterTo != "" {
		logdir := path.Dir(cfg.Log.WriterTo)
		if logdir != "" && logdir != "." && logdir != ".." {
			_ = os.MkdirAll(logdir, 0700)
		}
		logio, err = os.OpenFile(cfg.Log.WriterTo, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
		if err != nil {
			logger.Fatalf("Failed to open log file: %v", err)
		}
	}

	if cfg.Sentry.Dsn != "" {
		if err := logger.ConnectSentry(cfg.Sentry.Dsn); err != nil {
			logger.Errorf("failed to connect sentry")
		}
	}

	return &loggerCloser{
		logio.(io.Closer),
		logger.Init("con-logger", cfg.Log.Debug, false, logio),
	}
}

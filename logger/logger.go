package logger

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/config"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
)

const LogFieldTraceId = "trace_id"

func GetTraceId(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	return sc.TraceID().String()
}

type LoggerWrapper struct {
	*slog.Logger
}

func NewBaseLogger(w io.Writer, cfg *config.AppConfig) *slog.Logger {
	var baseLogger *slog.Logger
	if cfg.LoggerConfig.Json {
		baseLogger = slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
			Level: cfg.LoggerConfig.GetLevel(),
		}))
	} else {
		baseLogger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
			Level: cfg.LoggerConfig.GetLevel(),
		}))
	}
	return baseLogger
}

func NewLogger(base *slog.Logger) *LoggerWrapper {
	return &LoggerWrapper{
		Logger: base,
	}
}

func (lw *LoggerWrapper) WithTrace(ctx context.Context) *slog.Logger {
	return lw.Logger.With(LogFieldTraceId, GetTraceId(ctx))
}

func (lw *LoggerWrapper) Printf(s string, args ...interface{}) {
	lw.Info(fmt.Sprintf(s, args...))
}

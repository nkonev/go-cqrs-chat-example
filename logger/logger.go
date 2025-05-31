package logger

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel/trace"
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

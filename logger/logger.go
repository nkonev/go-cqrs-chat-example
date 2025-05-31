package logger

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"strings"
	"time"
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

	replaceFunc := func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == "msg" {
			return slog.Attr{
				Key:   "message",
				Value: a.Value,
			}
		} else if a.Key == "time" {
			utcTime := time.Now().UTC()
			utcFormattedTime := utcTime.Format("2006-01-02T15:04:05.000000000Z")
			return slog.Attr{
				Key:   "@timestamp",
				Value: slog.AnyValue(utcFormattedTime),
			}
		} else if a.Key == "level" {
			return slog.Attr{
				Key:   "level",
				Value: slog.StringValue(strings.ToLower(a.Value.String())),
			}
		} else if a.Key == "file" || a.Key == "source" {
			return slog.Attr{
				Key:   "caller",
				Value: a.Value,
			}
		} else {
			return a
		}
	}

	if cfg.LoggerConfig.Json {
		baseLogger = slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
			Level:       cfg.LoggerConfig.GetLevel(),
			ReplaceAttr: replaceFunc,
			AddSource:   true,
		}))
	} else {
		baseLogger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
			Level:       cfg.LoggerConfig.GetLevel(),
			ReplaceAttr: replaceFunc,
			AddSource:   true,
		}))
	}

	return baseLogger.With(slog.String("service", app.TRACE_RESOURCE))
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

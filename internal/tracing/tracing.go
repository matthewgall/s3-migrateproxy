package tracing

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("s3-migrateproxy")

// Span creates a span if tracing is configured; otherwise a no-op span.
func Span(ctx context.Context, name string, attrs ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, attrs...)
}

// Logger returns a debug logger that can be wired to OpenTelemetry later.
func Logger() *slog.Logger { return slog.Default() }

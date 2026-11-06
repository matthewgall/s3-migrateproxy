package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_gateway_requests_total",
		Help: "Total S3 gateway requests by operation and status.",
	}, []string{"operation", "status"})

	RequestLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3_gateway_request_duration_seconds",
		Help:    "S3 gateway request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	SourceFallbacks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_gateway_source_fallbacks_total",
		Help: "Total source fallback reads.",
	})

	ProviderReads = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_gateway_provider_reads_total",
		Help: "Provider read operations.",
	}, []string{"provider"})

	ProviderWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_gateway_provider_writes_total",
		Help: "Provider write operations.",
	}, []string{"provider"})

	BytesRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_gateway_bytes_read_total",
		Help: "Bytes read per provider.",
	}, []string{"provider"})

	BytesWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_gateway_bytes_written_total",
		Help: "Bytes written per provider.",
	}, []string{"provider"})

	ProviderErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3_gateway_provider_errors_total",
		Help: "Provider errors by provider and code.",
	}, []string{"provider", "code"})

	MigrationQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "s3_migration_queue_depth",
		Help: "Current migration queue depth.",
	})

	ObjectsMigrated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_migration_objects_migrated_total",
		Help: "Objects migrated.",
	})

	BytesMigrated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_migration_bytes_migrated_total",
		Help: "Bytes migrated.",
	})

	MigrationFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_migration_failures_total",
		Help: "Migration failures.",
	})

	MigrationRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3_migration_retries_total",
		Help: "Migration retries.",
	})
)

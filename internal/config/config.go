package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	mitchellhmapstructure "github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	EnvPrefix = "S3MIGRATE"
)

// ProviderKind enumerates supported upstream providers.
type ProviderKind string

const (
	ProviderAWS     ProviderKind = "aws"
	ProviderR2      ProviderKind = "r2"
	ProviderB2      ProviderKind = "b2"
	ProviderWasabi  ProviderKind = "wasabi"
	ProviderGeneric ProviderKind = "generic"
)

// Provider is the configuration for one upstream bucket.
type Provider struct {
	Kind             ProviderKind  `mapstructure:"provider"`
	Endpoint         string        `mapstructure:"endpoint"`
	Region           string        `mapstructure:"region"`
	Bucket           string        `mapstructure:"bucket"`
	AccessKeyID      string        `mapstructure:"access_key_id"`
	SecretAccessKey  string        `mapstructure:"secret_access_key"`
	SessionToken     string        `mapstructure:"session_token"`
	PathStyle        bool          `mapstructure:"path_style"`
	TLSSkipVerify    bool          `mapstructure:"tls_skip_verify"`
	MaxConcurrency   int           `mapstructure:"max_concurrency"`
	RequestTimeout   time.Duration `mapstructure:"request_timeout"`
	RetryMaxAttempts int           `mapstructure:"retry_max_attempts"`
}

// GatewayConfig controls the public S3-compatible listener.
type GatewayConfig struct {
	ListenAddr          string        `mapstructure:"listen_addr"`
	ReadHeaderTimeout   time.Duration `mapstructure:"read_header_timeout"`
	ReadTimeout         time.Duration `mapstructure:"read_timeout"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout"`
	MaxRequestBodyBytes int64         `mapstructure:"max_request_body_bytes"`
	StreamingChunkSize  int           `mapstructure:"streaming_chunk_size"`
	DefaultMaxKeys      int32         `mapstructure:"default_max_keys"`
}

// AdminConfig controls the operator/admin HTTP listener.
type AdminConfig struct {
	ListenAddr        string        `mapstructure:"listen_addr"`
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout"`
	APIKeyHash        string        `mapstructure:"api_key_hash"`
}

// WorkerConfig controls background migration workers.
type WorkerConfig struct {
	Enable             bool          `mapstructure:"enable"`
	WorkerID           string        `mapstructure:"worker_id"`
	LeaseDuration      time.Duration `mapstructure:"lease_duration"`
	LeaseRenewInterval time.Duration `mapstructure:"lease_renew_interval"`
	PollInterval       time.Duration `mapstructure:"poll_interval"`
	DiscoveryPageSize  int32         `mapstructure:"discovery_page_size"`
	TransferPartSize   int64         `mapstructure:"transfer_part_size"`
	MaxConcurrentTasks int           `mapstructure:"max_concurrent_tasks"`
	VerifyChecksum     bool          `mapstructure:"verify_checksum"`
}

// MetricsConfig controls Prometheus/OpenTelemetry.
type MetricsConfig struct {
	Enable       bool   `mapstructure:"enable"`
	MetricsAddr  string `mapstructure:"metrics_addr"`
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`
}

// DatabaseConfig is the PostgreSQL connection pool.
type DatabaseConfig struct {
	URL             string        `mapstructure:"url"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// Config is the whole application configuration.
type Config struct {
	MigrationName         string         `mapstructure:"migration_name"`
	MasterKey             string         `mapstructure:"master_key"`
	AllowPrivateEndpoints bool           `mapstructure:"allow_private_endpoints"`
	Database              DatabaseConfig `mapstructure:"database"`
	Source                Provider       `mapstructure:"source"`
	Destination           Provider       `mapstructure:"destination"`
	Gateway               GatewayConfig  `mapstructure:"gateway"`
	Admin                 AdminConfig    `mapstructure:"admin"`
	Worker                WorkerConfig   `mapstructure:"worker"`
	Metrics               MetricsConfig  `mapstructure:"metrics"`
	LogLevel              string         `mapstructure:"log_level"`
}

// BindFlags registers command-line flags and annotates them with their viper keys.
func BindFlags(cmd *cobra.Command) {
	register(cmd, "config", "config", "", "config file path")
	register(cmd, "migration-name", "migration_name", "default", "migration name")
	register(cmd, "master-key", "master_key", "", "master key used to encrypt stored credentials")
	register(cmd, "allow-private-endpoints", "allow_private_endpoints", false, "allow loopback/private provider endpoints")

	register(cmd, "database-url", "database.url", "", "PostgreSQL URL")
	register(cmd, "database-max-open-conns", "database.max_open_conns", 25, "PostgreSQL max open connections")
	register(cmd, "database-max-idle-conns", "database.max_idle_conns", 5, "PostgreSQL max idle connections")
	register(cmd, "database-conn-max-lifetime", "database.conn_max_lifetime", 30*time.Minute, "PostgreSQL connection max lifetime")

	bindProviderFlags(cmd, "source", "src")
	bindProviderFlags(cmd, "destination", "dst")

	register(cmd, "gateway-listen-addr", "gateway.listen_addr", ":8080", "gateway S3 listener address")
	register(cmd, "gateway-read-timeout", "gateway.read_timeout", 30*time.Second, "gateway read timeout")
	register(cmd, "gateway-write-timeout", "gateway.write_timeout", 30*time.Second, "gateway write timeout")
	register(cmd, "gateway-read-header-timeout", "gateway.read_header_timeout", 10*time.Second, "gateway read header timeout")
	register(cmd, "gateway-max-request-body-bytes", "gateway.max_request_body_bytes", int64(50<<30), "maximum request body size")
	register(cmd, "gateway-streaming-chunk-size", "gateway.streaming_chunk_size", 512<<10, "gateway streaming chunk size")
	register(cmd, "gateway-default-max-keys", "gateway.default_max_keys", int32(1000), "default ListObjectsV2 max keys")

	register(cmd, "admin-listen-addr", "admin.listen_addr", "127.0.0.1:8081", "admin API listener address")
	register(cmd, "admin-read-header-timeout", "admin.read_header_timeout", 10*time.Second, "admin read header timeout")
	register(cmd, "admin-api-key-hash", "admin.api_key_hash", "", "admin API bearer token")

	register(cmd, "worker-enable", "worker.enable", true, "enable background migration worker behaviour")
	register(cmd, "worker-id", "worker.worker_id", "", "worker unique id")
	register(cmd, "worker-lease-duration", "worker.lease_duration", 60*time.Second, "task lease duration")
	register(cmd, "worker-lease-renew-interval", "worker.lease_renew_interval", 30*time.Second, "lease renewal interval")
	register(cmd, "worker-poll-interval", "worker.poll_interval", 5*time.Second, "task poll interval")
	register(cmd, "worker-discovery-page-size", "worker.discovery_page_size", int32(1000), "discovery list page size")
	register(cmd, "worker-transfer-part-size", "worker.transfer_part_size", int64(64<<20), "multipart part size for transfers")
	register(cmd, "worker-max-concurrent-tasks", "worker.max_concurrent_tasks", 10, "max concurrent transfer tasks per worker")
	register(cmd, "worker-verify-checksum", "worker.verify_checksum", true, "verify objects after transfer")

	register(cmd, "metrics-enable", "metrics.enable", true, "enable Prometheus metrics")
	register(cmd, "metrics-addr", "metrics.metrics_addr", "0.0.0.0:9090", "Prometheus listen address")
	register(cmd, "metrics-otlp-endpoint", "metrics.otlp_endpoint", "", "OpenTelemetry OTLP endpoint")

	register(cmd, "log-level", "log_level", "info", "log level")
}

func register[T any](cmd *cobra.Command, name, key string, value T, usage string) {
	switch v := any(value).(type) {
	case string:
		cmd.Flags().String(name, v, usage)
	case bool:
		cmd.Flags().Bool(name, v, usage)
	case int:
		cmd.Flags().Int(name, v, usage)
	case int32:
		cmd.Flags().Int32(name, v, usage)
	case int64:
		cmd.Flags().Int64(name, v, usage)
	case time.Duration:
		cmd.Flags().Duration(name, v, usage)
	default:
		panic(fmt.Sprintf("unsupported flag type %T for %s", value, name))
	}
	_ = cmd.Flags().SetAnnotation(name, "viper_key", []string{key})
}

func bindProviderFlags(cmd *cobra.Command, role, short string) {
	prefix := role
	if short != "" {
		prefix += ", " + short
	}
	register(cmd, role+"-provider", role+".provider", "generic", "["+prefix+"] provider kind")
	register(cmd, role+"-endpoint", role+".endpoint", "", "["+prefix+"] S3 endpoint URL")
	register(cmd, role+"-region", role+".region", "us-east-1", "["+prefix+"] region")
	register(cmd, role+"-bucket", role+".bucket", "", "["+prefix+"] bucket name")
	register(cmd, role+"-access-key-id", role+".access_key_id", "", "["+prefix+"] access key")
	register(cmd, role+"-secret-access-key", role+".secret_access_key", "", "["+prefix+"] secret key")
	register(cmd, role+"-session-token", role+".session_token", "", "["+prefix+"] session token")
	register(cmd, role+"-path-style", role+".path_style", false, "["+prefix+"] use path-style addressing")
	register(cmd, role+"-tls-skip-verify", role+".tls_skip_verify", false, "["+prefix+"] skip TLS verification")
	register(cmd, role+"-max-concurrency", role+".max_concurrency", 100, "["+prefix+"] connection pool size")
	register(cmd, role+"-request-timeout", role+".request_timeout", 30*time.Second, "["+prefix+"] request timeout")
	register(cmd, role+"-retry-max-attempts", role+".retry_max_attempts", 3, "["+prefix+"] SDK retry attempts")
}

// Load reads configuration from flags, environment variables, and a config file.
func Load(cmd *cobra.Command) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix(EnvPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	bindEnvAliases(v)

	setDefaults(v)

	if err := bindFlagAnnotations(v, cmd); err != nil {
		return nil, err
	}

	if cfgFile, _ := cmd.Flags().GetString("config"); cfgFile != "" {
		v.SetConfigFile(cfgFile)
		_ = v.ReadInConfig()
	} else if cfgEnv := v.GetString("config"); cfgEnv != "" {
		v.SetConfigFile(cfgEnv)
		_ = v.ReadInConfig()
	}

	expandEnvInSettings(v.AllSettings(), v)

	var cfg Config
	hooks := mitchellhmapstructure.ComposeDecodeHookFunc(
		mitchellhmapstructure.StringToTimeDurationHookFunc(),
		mitchellhmapstructure.StringToSliceHookFunc(","),
	)
	if err := v.Unmarshal(&cfg, viper.DecodeHook(hooks)); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if cfg.Worker.WorkerID == "" {
		cfg.Worker.WorkerID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}

	return &cfg, nil
}

// bindEnvAliases binds S3MIGRATE_SRC_* to source.* and S3MIGRATE_DEST_* to
// destination.* so the shorter prefixes work alongside the canonical keys.
func bindEnvAliases(v *viper.Viper) {
	for _, e := range os.Environ() {
		idx := strings.IndexByte(e, '=')
		if idx < 0 {
			continue
		}
		name := e[:idx]
		switch {
		case strings.HasPrefix(name, EnvPrefix+"_SRC_"):
			suffix := strings.TrimPrefix(name, EnvPrefix+"_SRC_")
			key := "source." + strings.ToLower(strings.ReplaceAll(suffix, "_", "."))
			_ = v.BindEnv(key, name, EnvPrefix+"_SOURCE_"+suffix)
		case strings.HasPrefix(name, EnvPrefix+"_DEST_"):
			suffix := strings.TrimPrefix(name, EnvPrefix+"_DEST_")
			key := "destination." + strings.ToLower(strings.ReplaceAll(suffix, "_", "."))
			_ = v.BindEnv(key, name, EnvPrefix+"_DESTINATION_"+suffix)
		}
	}
}

func bindFlagAnnotations(v *viper.Viper, cmd *cobra.Command) error {
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		if keys, ok := flag.Annotations["viper_key"]; ok && len(keys) > 0 {
			_ = v.BindPFlag(keys[0], flag)
		}
	})
	return nil
}

// LoadDirect loads configuration from environment only (useful in tests).
func LoadDirect() (*Config, error) {
	root := &cobra.Command{}
	BindFlags(root)
	return Load(root)
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("migration_name", "default")
	v.SetDefault("log_level", "info")
	v.SetDefault("allow_private_endpoints", false)

	for role := range []string{"source", "destination"} {
		_ = role // mapstructure defaults are set via struct field defaults during unmarshal
	}
	v.SetDefault("source.region", "us-east-1")
	v.SetDefault("source.path_style", false)
	v.SetDefault("source.tls_skip_verify", false)
	v.SetDefault("source.max_concurrency", 100)
	v.SetDefault("source.request_timeout", 30*time.Second)
	v.SetDefault("source.retry_max_attempts", 3)
	v.SetDefault("destination.region", "us-east-1")
	v.SetDefault("destination.path_style", false)
	v.SetDefault("destination.tls_skip_verify", false)
	v.SetDefault("destination.max_concurrency", 100)
	v.SetDefault("destination.request_timeout", 30*time.Second)
	v.SetDefault("destination.retry_max_attempts", 3)

	v.SetDefault("gateway.listen_addr", ":8080")
	v.SetDefault("gateway.read_header_timeout", 10*time.Second)
	v.SetDefault("gateway.read_timeout", 30*time.Second)
	v.SetDefault("gateway.write_timeout", 30*time.Second)
	v.SetDefault("gateway.max_request_body_bytes", 50<<30)
	v.SetDefault("gateway.streaming_chunk_size", 512<<10)
	v.SetDefault("gateway.default_max_keys", 1000)

	v.SetDefault("admin.listen_addr", "127.0.0.1:8081")
	v.SetDefault("admin.read_header_timeout", 10*time.Second)

	v.SetDefault("worker.enable", true)
	v.SetDefault("worker.lease_duration", 60*time.Second)
	v.SetDefault("worker.lease_renew_interval", 30*time.Second)
	v.SetDefault("worker.poll_interval", 5*time.Second)
	v.SetDefault("worker.discovery_page_size", 1000)
	v.SetDefault("worker.transfer_part_size", 64<<20)
	v.SetDefault("worker.max_concurrent_tasks", 10)
	v.SetDefault("worker.verify_checksum", true)

	v.SetDefault("metrics.enable", true)
	v.SetDefault("metrics.metrics_addr", "0.0.0.0:9090")

	v.SetDefault("database.max_open_conns", 25)
	v.SetDefault("database.max_idle_conns", 5)
	v.SetDefault("database.conn_max_lifetime", 30*time.Minute)
}

// expandEnvInSettings recursively replaces $VAR and ${VAR} in string values.
// It also accepts env defaults of the form ${VAR:-default}.
func expandEnvInSettings(settings map[string]any, v *viper.Viper) {
	expanded := expandMap(settings)
	for key, val := range expanded {
		v.Set(key, val)
	}
}

func expandMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, val := range m {
		switch x := val.(type) {
		case string:
			out[k] = os.Expand(x, shellGetenv)
		case map[string]any:
			out[k] = expandMap(x)
		case map[any]any:
			out[k] = expandAnyMap(x)
		default:
			out[k] = x
		}
	}
	return out
}

func expandAnyMap(m map[any]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, val := range m {
		switch x := val.(type) {
		case string:
			out[fmt.Sprintf("%v", k)] = os.Expand(x, shellGetenv)
		case map[string]any:
			out[fmt.Sprintf("%v", k)] = expandMap(x)
		case map[any]any:
			out[fmt.Sprintf("%v", k)] = expandAnyMap(x)
		default:
			out[fmt.Sprintf("%v", k)] = x
		}
	}
	return out
}

// shellGetenv returns the env value or an empty string.
// os.Expand does not support defaults, so we simulate ${VAR:-default} here.
func shellGetenv(key string) string {
	if idx := strings.Index(key, ":-"); idx > 0 {
		name := key[:idx]
		def := key[idx+2:]
		if v := os.Getenv(name); v != "" {
			return v
		}
		return def
	}
	return os.Getenv(key)
}

// Redacted returns a JSON-safe copy of the config with secrets removed.
func (c *Config) Redacted() map[string]any {
	b, _ := json.Marshal(c)
	var raw map[string]any
	_ = json.Unmarshal(b, &raw)
	redact(raw)
	return raw
}

func redact(m map[string]any) {
	for k, v := range m {
		lower := strings.ToLower(k)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "key") || strings.Contains(lower, "password") || strings.Contains(lower, "token") || strings.Contains(lower, "url") || strings.Contains(lower, "dsn") {
			if _, ok := v.(string); ok {
				m[k] = "[redacted]"
			}
		} else if sub, ok := v.(map[string]any); ok {
			redact(sub)
		}
	}
}

// String returns a compact, non-secret description.
func (c *Config) String() string {
	return fmt.Sprintf("migration=%s source=%s/%s dest=%s/%s log_level=%s",
		c.MigrationName,
		c.Source.Kind, c.Source.Bucket,
		c.Destination.Kind, c.Destination.Bucket,
		c.LogLevel,
	)
}

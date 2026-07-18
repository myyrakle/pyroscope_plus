package clickhouse

import (
	"errors"
	"flag"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/grafana/dskit/flagext"
)

const (
	defaultChunkSize              = 4 * 1024 * 1024
	maxChunkSize                  = 64 * 1024 * 1024
	maxInsertBatchSize            = 64
	maxBufferedChunkBytes         = 256 * 1024 * 1024
	defaultReadPrefetchChunks     = 8
	maxReadPrefetchChunks         = 64
	defaultMaxReadPrefetchBytes   = 32 * 1024 * 1024
	maxReadPrefetchBytes          = 256 * 1024 * 1024
	objectStorePartitionCount     = 64
	maxClickHouseIdentifierLength = 255
	maxCleanupMutationBatchSize   = 1000
)

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var supportedCompressionMethods = map[string]struct{}{
	"none":  {},
	"lz4":   {},
	"lz4hc": {},
	"zstd":  {},
}

// Config holds the configuration for a ClickHouse object store backend.
type Config struct {
	Addresses            flagext.StringSliceCSV `yaml:"addresses"`
	Database             string                 `yaml:"database"`
	ObjectsTable         string                 `yaml:"objects_table"`
	ChunksTable          string                 `yaml:"chunks_table"`
	Username             string                 `yaml:"username"`
	Password             flagext.Secret         `yaml:"password"`
	Secure               bool                   `yaml:"secure"`
	SkipVerify           bool                   `yaml:"skip_verify" category:"advanced"`
	TLSServerName        string                 `yaml:"tls_server_name" category:"advanced"`
	TLSCAPath            string                 `yaml:"tls_ca_path" category:"advanced"`
	DialTimeout          time.Duration          `yaml:"dial_timeout" category:"advanced"`
	QueryTimeout         time.Duration          `yaml:"query_timeout" category:"advanced"`
	MaxOpenConns         int                    `yaml:"max_open_connections" category:"advanced"`
	MaxIdleConns         int                    `yaml:"max_idle_connections" category:"advanced"`
	ConnMaxLifetime      time.Duration          `yaml:"connection_lifetime" category:"advanced"`
	Compression          string                 `yaml:"compression" category:"advanced"`
	ChunkSize            int                    `yaml:"chunk_size" category:"advanced"`
	InsertBatchSize      int                    `yaml:"insert_batch_size" category:"advanced"`
	ReadPrefetchChunks   int                    `yaml:"read_prefetch_chunks" category:"advanced"`
	MaxReadPrefetchBytes int                    `yaml:"max_read_prefetch_bytes" category:"advanced"`
	PartitionCount       int                    `yaml:"partition_count" category:"advanced"`
	MaxUploadDuration    time.Duration          `yaml:"max_upload_duration" category:"advanced"`
	AutoCreateTables     bool                   `yaml:"auto_create_tables"`
	Cleanup              CleanupConfig          `yaml:"cleanup"`
}

// CleanupConfig controls removal of incomplete and obsolete object generations.
type CleanupConfig struct {
	Enabled           bool          `yaml:"enabled"`
	Interval          time.Duration `yaml:"interval"`
	Grace             time.Duration `yaml:"grace"`
	MutationBatchSize int           `yaml:"mutation_batch_size"`
}

// RegisterFlags registers ClickHouse object store flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix registers ClickHouse object store flags with the provided prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	if len(cfg.Addresses) == 0 {
		cfg.Addresses = flagext.StringSliceCSV{"localhost:9000"}
	}
	f.Var(&cfg.Addresses, prefix+"clickhouse.addresses", "ClickHouse native protocol endpoint. Exactly one endpoint is required because the current node-local MergeTree schema requires a consistent endpoint. For HA, use a stable endpoint pinned to one ClickHouse storage node until replicated schema and consistency support is available.")
	f.StringVar(&cfg.Database, prefix+"clickhouse.database", "default", "ClickHouse database name.")
	f.StringVar(&cfg.ObjectsTable, prefix+"clickhouse.objects-table", "pyroscope_objects", "ClickHouse object manifests table name.")
	f.StringVar(&cfg.ChunksTable, prefix+"clickhouse.chunks-table", "pyroscope_object_chunks", "ClickHouse object chunks table name.")
	f.StringVar(&cfg.Username, prefix+"clickhouse.username", "default", "ClickHouse username.")
	f.Var(&cfg.Password, prefix+"clickhouse.password", "ClickHouse password.")
	f.BoolVar(&cfg.Secure, prefix+"clickhouse.secure", false, "Use TLS for ClickHouse connections.")
	f.BoolVar(&cfg.SkipVerify, prefix+"clickhouse.skip-verify", false, "Skip ClickHouse TLS certificate and hostname verification.")
	f.StringVar(&cfg.TLSServerName, prefix+"clickhouse.tls-server-name", "", "Server name used to verify the ClickHouse TLS certificate.")
	f.StringVar(&cfg.TLSCAPath, prefix+"clickhouse.tls-ca-path", "", "Path to a PEM-encoded CA certificate for ClickHouse TLS connections.")
	f.DurationVar(&cfg.DialTimeout, prefix+"clickhouse.dial-timeout", 5*time.Second, "Maximum duration for establishing a ClickHouse connection.")
	f.DurationVar(&cfg.QueryTimeout, prefix+"clickhouse.query-timeout", 30*time.Second, "Maximum duration for a ClickHouse query.")
	f.IntVar(&cfg.MaxOpenConns, prefix+"clickhouse.max-open-connections", 16, "Maximum number of open ClickHouse connections.")
	f.IntVar(&cfg.MaxIdleConns, prefix+"clickhouse.max-idle-connections", 8, "Maximum number of idle ClickHouse connections.")
	f.DurationVar(&cfg.ConnMaxLifetime, prefix+"clickhouse.connection-lifetime", time.Hour, "Maximum lifetime of a ClickHouse connection.")
	f.StringVar(&cfg.Compression, prefix+"clickhouse.compression", "lz4", "Compression used for ClickHouse connections.")
	f.IntVar(&cfg.ChunkSize, prefix+"clickhouse.chunk-size", defaultChunkSize, "Maximum object chunk size in bytes.")
	f.IntVar(&cfg.InsertBatchSize, prefix+"clickhouse.insert-batch-size", 8, "Maximum number of chunks per ClickHouse insert batch.")
	f.IntVar(&cfg.ReadPrefetchChunks, prefix+"clickhouse.read-prefetch-chunks", defaultReadPrefetchChunks, "Maximum number of object chunks prefetched by a reader.")
	f.IntVar(&cfg.MaxReadPrefetchBytes, prefix+"clickhouse.max-read-prefetch-bytes", defaultMaxReadPrefetchBytes, "Maximum bytes prefetched by an object reader.")
	f.IntVar(&cfg.PartitionCount, prefix+"clickhouse.partition-count", objectStorePartitionCount, "Fixed ClickHouse object-store schema partition count.")
	f.DurationVar(&cfg.MaxUploadDuration, prefix+"clickhouse.max-upload-duration", 30*time.Minute, "Maximum duration allowed for an object upload.")
	f.BoolVar(&cfg.AutoCreateTables, prefix+"clickhouse.auto-create-tables", true, "Create required ClickHouse tables when they do not exist.")
	cfg.Cleanup.RegisterFlagsWithPrefix(prefix+"clickhouse.cleanup.", f)
}

// Validate validates the ClickHouse object store configuration.
func (cfg *Config) Validate() error {
	if len(cfg.Addresses) != 1 {
		return errors.New("ClickHouse addresses must contain exactly one native endpoint: current node-local MergeTree tables require a consistent endpoint pinned to one ClickHouse storage node")
	}
	for _, address := range cfg.Addresses {
		if strings.TrimSpace(address) == "" {
			return errors.New("ClickHouse addresses must not contain empty values")
		}
	}
	if err := validIdentifier("database", cfg.Database); err != nil {
		return err
	}
	if err := validIdentifier("objects table", cfg.ObjectsTable); err != nil {
		return err
	}
	if err := validIdentifier("chunks table", cfg.ChunksTable); err != nil {
		return err
	}
	if _, ok := supportedCompressionMethods[cfg.Compression]; !ok {
		return fmt.Errorf("unsupported ClickHouse compression %q", cfg.Compression)
	}
	if err := cfg.validateUploadAllocations(); err != nil {
		return err
	}
	if err := cfg.validateReadPrefetch(); err != nil {
		return err
	}
	if cfg.PartitionCount != objectStorePartitionCount {
		return fmt.Errorf("ClickHouse partition count is immutable for the ClickHouse object store schema: must be %d; restore partition_count or recreate all ClickHouse object store tables", objectStorePartitionCount)
	}
	if _, err := deriveSchemaIdentifiers(cfg.ObjectsTable); err != nil {
		return err
	}
	if cfg.MaxOpenConns <= 0 {
		return errors.New("ClickHouse maximum open connections must be positive")
	}
	if cfg.MaxIdleConns <= 0 {
		return errors.New("ClickHouse maximum idle connections must be positive")
	}
	if cfg.MaxIdleConns > cfg.MaxOpenConns {
		return errors.New("ClickHouse maximum idle connections must not exceed maximum open connections")
	}
	if cfg.DialTimeout <= 0 {
		return errors.New("ClickHouse dial timeout must be positive")
	}
	if cfg.QueryTimeout <= 0 {
		return errors.New("ClickHouse query timeout must be positive")
	}
	if cfg.ConnMaxLifetime <= 0 {
		return errors.New("ClickHouse connection lifetime must be positive")
	}
	if cfg.MaxUploadDuration <= 0 {
		return errors.New("ClickHouse maximum upload duration must be positive and bounded")
	}
	if cfg.SkipVerify && !cfg.Secure {
		return errors.New("ClickHouse skip verify requires secure TLS")
	}
	return cfg.Cleanup.Validate()
}

func (cfg *Config) validateReadPrefetch() error {
	if cfg.ReadPrefetchChunks <= 0 {
		return errors.New("ClickHouse read prefetch chunks must be positive")
	}
	if cfg.ReadPrefetchChunks > maxReadPrefetchChunks {
		return fmt.Errorf("ClickHouse read prefetch chunks must not exceed %d", maxReadPrefetchChunks)
	}
	if cfg.MaxReadPrefetchBytes <= 0 {
		return errors.New("ClickHouse maximum read prefetch bytes must be positive")
	}
	if cfg.MaxReadPrefetchBytes > maxReadPrefetchBytes {
		return fmt.Errorf("ClickHouse maximum read prefetch bytes must not exceed %d", maxReadPrefetchBytes)
	}
	if uint64(cfg.ChunkSize) > uint64(cfg.MaxReadPrefetchBytes)/uint64(cfg.ReadPrefetchChunks) {
		return errors.New("ClickHouse chunk size times read prefetch chunks must not exceed maximum read prefetch bytes")
	}
	return nil
}

func (cfg *Config) validateUploadAllocations() error {
	if cfg.ChunkSize <= 0 {
		return errors.New("ClickHouse chunk size must be positive")
	}
	if cfg.ChunkSize > maxChunkSize {
		return fmt.Errorf("ClickHouse chunk size must not exceed %d bytes", maxChunkSize)
	}
	if cfg.InsertBatchSize <= 0 {
		return errors.New("ClickHouse insert batch size must be positive")
	}
	if cfg.InsertBatchSize > maxInsertBatchSize {
		return fmt.Errorf("ClickHouse insert batch size must not exceed %d", maxInsertBatchSize)
	}
	if uint64(cfg.ChunkSize) > uint64(maxBufferedChunkBytes)/uint64(cfg.InsertBatchSize) {
		return fmt.Errorf("ClickHouse buffered chunk bytes must not exceed %d", maxBufferedChunkBytes)
	}
	return nil
}

// RegisterFlags registers ClickHouse cleanup flags.
func (cfg *CleanupConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("clickhouse.cleanup.", f)
}

// RegisterFlagsWithPrefix registers ClickHouse cleanup flags with the provided prefix.
func (cfg *CleanupConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable cleanup of incomplete and obsolete ClickHouse object generations. Enable this on one designated process only.")
	f.DurationVar(&cfg.Interval, prefix+"interval", 24*time.Hour, "Interval between ClickHouse object cleanup passes.")
	f.DurationVar(&cfg.Grace, prefix+"grace", 24*time.Hour, "Minimum age of a ClickHouse object generation before cleanup.")
	f.IntVar(&cfg.MutationBatchSize, prefix+"mutation-batch-size", 100, fmt.Sprintf("Maximum number of ClickHouse object generations per cleanup mutation (up to %d).", maxCleanupMutationBatchSize))
}

// Validate validates the ClickHouse cleanup configuration.
func (cfg *CleanupConfig) Validate() error {
	if cfg.Interval <= 0 {
		return errors.New("ClickHouse cleanup interval must be positive")
	}
	if cfg.Grace <= 0 {
		return errors.New("ClickHouse cleanup grace must be positive")
	}
	if cfg.Grace < cfg.Interval {
		return errors.New("ClickHouse cleanup grace must not be shorter than cleanup interval")
	}
	if cfg.MutationBatchSize <= 0 {
		return errors.New("ClickHouse cleanup mutation batch size must be positive")
	}
	if cfg.MutationBatchSize > maxCleanupMutationBatchSize {
		return fmt.Errorf("ClickHouse cleanup mutation batch size must not exceed %d", maxCleanupMutationBatchSize)
	}
	return nil
}

func validIdentifier(field, value string) error {
	if !identifierRE.MatchString(value) {
		return fmt.Errorf("invalid ClickHouse %s %q", field, value)
	}
	if len(value) > maxClickHouseIdentifierLength {
		return fmt.Errorf("invalid ClickHouse %s %q: maximum identifier length is %d bytes", field, value, maxClickHouseIdentifierLength)
	}
	return nil
}

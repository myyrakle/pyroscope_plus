package clickhouse

import (
	"flag"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)

	require.Equal(t, flagext.StringSliceCSV{"localhost:9000"}, cfg.Addresses)
	require.Equal(t, "default", cfg.Database)
	require.Equal(t, "pyroscope_objects", cfg.ObjectsTable)
	require.Equal(t, "pyroscope_object_chunks", cfg.ChunksTable)
	require.Equal(t, 4*1024*1024, cfg.ChunkSize)
	require.Equal(t, 8, cfg.InsertBatchSize)
	require.Equal(t, 8, cfg.ReadPrefetchChunks)
	require.Equal(t, 32*1024*1024, cfg.MaxReadPrefetchBytes)
	require.Equal(t, 32*1024*1024, cfg.ChunkSize*cfg.InsertBatchSize)
	require.True(t, cfg.AutoCreateTables)
	require.False(t, cfg.Cleanup.Enabled)
	require.Equal(t, 24*time.Hour, cfg.Cleanup.Interval)
	require.Equal(t, 24*time.Hour, cfg.Cleanup.Grace)
	require.NoError(t, cfg.Validate())
}

func TestConfigRegisterFlagsWithPrefix(t *testing.T) {
	var cfg Config
	fs := flag.NewFlagSet("clickhouse", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix("storage.", fs)

	err := fs.Parse([]string{
		"-storage.clickhouse.addresses=one:9000",
		"-storage.clickhouse.password=secret",
		"-storage.clickhouse.read-prefetch-chunks=4",
		"-storage.clickhouse.max-read-prefetch-bytes=67108864",
		"-storage.clickhouse.cleanup.enabled=false",
	})
	require.NoError(t, err)
	require.Equal(t, flagext.StringSliceCSV{"one:9000"}, cfg.Addresses)
	require.Equal(t, "secret", cfg.Password.String())
	require.Equal(t, 4, cfg.ReadPrefetchChunks)
	require.Equal(t, 64*1024*1024, cfg.MaxReadPrefetchBytes)
	require.False(t, cfg.Cleanup.Enabled)
	require.Contains(t, fs.Lookup("storage.clickhouse.addresses").Usage, "node-local MergeTree")
	require.Contains(t, fs.Lookup("storage.clickhouse.addresses").Usage, "pinned to one ClickHouse storage node")
	require.NoError(t, cfg.Validate())
}

func TestConfigValidateAcceptsExactlyOneAddress(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)
	cfg.Addresses = flagext.StringSliceCSV{"clickhouse-storage:9000"}

	require.NoError(t, cfg.Validate())
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name:   "empty addresses",
			mutate: func(cfg *Config) { cfg.Addresses = nil },
		},
		{
			name:   "empty address",
			mutate: func(cfg *Config) { cfg.Addresses = flagext.StringSliceCSV{""} },
		},
		{
			name:   "two addresses",
			mutate: func(cfg *Config) { cfg.Addresses = flagext.StringSliceCSV{"one:9000", "two:9000"} },
		},
		{
			name: "three addresses",
			mutate: func(cfg *Config) {
				cfg.Addresses = flagext.StringSliceCSV{"one:9000", "two:9000", "three:9000"}
			},
		},
		{
			name:   "invalid database identifier",
			mutate: func(cfg *Config) { cfg.Database = "profile-data" },
		},
		{
			name:   "invalid objects table identifier",
			mutate: func(cfg *Config) { cfg.ObjectsTable = "objects;drop" },
		},
		{
			name:   "invalid chunks table identifier",
			mutate: func(cfg *Config) { cfg.ChunksTable = "1_chunks" },
		},
		{
			name:   "non-positive chunk size",
			mutate: func(cfg *Config) { cfg.ChunkSize = 0 },
		},
		{
			name:   "non-positive insert batch size",
			mutate: func(cfg *Config) { cfg.InsertBatchSize = 0 },
		},
		{
			name:   "non-positive max open connections",
			mutate: func(cfg *Config) { cfg.MaxOpenConns = 0 },
		},
		{
			name:   "non-positive max idle connections",
			mutate: func(cfg *Config) { cfg.MaxIdleConns = 0 },
		},
		{
			name:   "idle connections exceed open connections",
			mutate: func(cfg *Config) { cfg.MaxIdleConns = cfg.MaxOpenConns + 1 },
		},
		{
			name:   "non-positive dial timeout",
			mutate: func(cfg *Config) { cfg.DialTimeout = 0 },
		},
		{
			name:   "non-positive query timeout",
			mutate: func(cfg *Config) { cfg.QueryTimeout = 0 },
		},
		{
			name:   "non-positive connection lifetime",
			mutate: func(cfg *Config) { cfg.ConnMaxLifetime = 0 },
		},
		{
			name:   "unbounded max upload duration",
			mutate: func(cfg *Config) { cfg.MaxUploadDuration = 0 },
		},
		{
			name:   "non-positive cleanup interval",
			mutate: func(cfg *Config) { cfg.Cleanup.Interval = 0 },
		},
		{
			name:   "non-positive cleanup grace",
			mutate: func(cfg *Config) { cfg.Cleanup.Grace = 0 },
		},
		{
			name: "cleanup grace shorter than interval",
			mutate: func(cfg *Config) {
				cfg.Cleanup.Interval = time.Hour
				cfg.Cleanup.Grace = time.Minute
			},
		},
		{
			name:   "non-positive cleanup mutation batch size",
			mutate: func(cfg *Config) { cfg.Cleanup.MutationBatchSize = 0 },
		},
		{
			name: "skip verify without secure TLS",
			mutate: func(cfg *Config) {
				cfg.Secure = false
				cfg.SkipVerify = true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)
			tt.mutate(&cfg)

			require.Error(t, cfg.Validate())
		})
	}
}

func TestConfigValidateCompression(t *testing.T) {
	for _, compression := range []string{"none", "lz4", "lz4hc", "zstd"} {
		t.Run(compression, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)
			cfg.Compression = compression

			require.NoError(t, cfg.Validate())
		})
	}

	t.Run("unsupported", func(t *testing.T) {
		var cfg Config
		flagext.DefaultValues(&cfg)
		cfg.Compression = "gzip"

		require.Error(t, cfg.Validate())
	})
}

func TestConfigValidateUploadAllocations(t *testing.T) {
	tests := []struct {
		name             string
		chunkSize        int
		insertBatchSize  int
		prefetchChunks   int
		prefetchBytes    int
		validatePrefetch bool
		wantErr          bool
	}{
		{name: "limits", chunkSize: maxChunkSize, insertBatchSize: 4, prefetchChunks: 4, prefetchBytes: maxReadPrefetchBytes},
		{name: "chunk size exceeds limit", chunkSize: maxChunkSize + 1, insertBatchSize: 1, wantErr: true},
		{name: "insert batch size exceeds limit", chunkSize: 1, insertBatchSize: maxInsertBatchSize + 1, wantErr: true},
		{name: "buffered chunk bytes exceed limit", chunkSize: maxChunkSize, insertBatchSize: 5, wantErr: true},
		{name: "prefetch product does not overflow", chunkSize: math.MaxInt, insertBatchSize: 1, prefetchChunks: maxReadPrefetchChunks, prefetchBytes: maxReadPrefetchBytes, validatePrefetch: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.ChunkSize = tt.chunkSize
			cfg.InsertBatchSize = tt.insertBatchSize
			if tt.prefetchChunks != 0 {
				cfg.ReadPrefetchChunks = tt.prefetchChunks
			}
			if tt.prefetchBytes != 0 {
				cfg.MaxReadPrefetchBytes = tt.prefetchBytes
			}

			err := cfg.Validate()
			if tt.validatePrefetch {
				err = cfg.validateReadPrefetch()
			}
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigValidateReadPrefetch(t *testing.T) {
	tests := []struct {
		name                 string
		chunkSize            int
		readPrefetchChunks   int
		maxReadPrefetchBytes int
		wantErr              string
	}{
		{name: "limits", chunkSize: 4 * 1024 * 1024, readPrefetchChunks: 64, maxReadPrefetchBytes: 256 * 1024 * 1024},
		{name: "non-positive chunks", chunkSize: 1, readPrefetchChunks: 0, maxReadPrefetchBytes: 1, wantErr: "read prefetch chunks must be positive"},
		{name: "chunks exceed limit", chunkSize: 1, readPrefetchChunks: 65, maxReadPrefetchBytes: 65, wantErr: "read prefetch chunks must not exceed 64"},
		{name: "non-positive bytes", chunkSize: 1, readPrefetchChunks: 1, maxReadPrefetchBytes: 0, wantErr: "maximum read prefetch bytes must be positive"},
		{name: "bytes exceed limit", chunkSize: 1, readPrefetchChunks: 1, maxReadPrefetchBytes: 256*1024*1024 + 1, wantErr: "maximum read prefetch bytes must not exceed 268435456"},
		{name: "window exceeds bytes", chunkSize: 4 * 1024 * 1024, readPrefetchChunks: 9, maxReadPrefetchBytes: 32 * 1024 * 1024, wantErr: "chunk size times read prefetch chunks must not exceed maximum read prefetch bytes"},
		{name: "overflow safe", chunkSize: math.MaxInt, readPrefetchChunks: 64, maxReadPrefetchBytes: 256 * 1024 * 1024, wantErr: "chunk size must not exceed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.ChunkSize = tt.chunkSize
			cfg.ReadPrefetchChunks = tt.readPrefetchChunks
			cfg.MaxReadPrefetchBytes = tt.maxReadPrefetchBytes

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestConfigValidateDerivedTableIdentifiers(t *testing.T) {
	cfg := testConfig(t)
	cfg.ObjectsTable = strings.Repeat("o", maxClickHouseIdentifierLength)

	err := cfg.Validate()
	require.ErrorContains(t, err, "latest aggregate table")
	require.ErrorContains(t, err, "derived from objects table")
	require.ErrorContains(t, err, "maximum identifier length")
}

func TestCleanupConfigValidateMutationBatchSizeBounds(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr string
	}{
		{name: "minimum accepted", size: 1},
		{name: "maximum accepted", size: 20000},
		{name: "non-positive rejected", size: 0, wantErr: "ClickHouse cleanup mutation batch size must be positive"},
		{name: "above maximum rejected", size: 20001, wantErr: "ClickHouse cleanup mutation batch size must not exceed 20000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := CleanupConfig{
				Interval:          time.Hour,
				Grace:             2 * time.Hour,
				MutationBatchSize: tt.size,
			}

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

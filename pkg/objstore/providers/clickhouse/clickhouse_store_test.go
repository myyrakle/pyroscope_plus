package clickhouse

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"
)

const testDatabase = "profiles"

func TestClickHouseOptions(t *testing.T) {
	methods := map[string]ch.CompressionMethod{
		"none":  ch.CompressionNone,
		"lz4":   ch.CompressionLZ4,
		"lz4hc": ch.CompressionLZ4HC,
		"zstd":  ch.CompressionZSTD,
	}

	for name, method := range methods {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Addresses = flagext.StringSliceCSV{"one:9000"}
			cfg.Database = testDatabase
			cfg.Username = "reader"
			require.NoError(t, cfg.Password.Set("top-secret"))
			cfg.DialTimeout = 7 * time.Second
			cfg.QueryTimeout = 11 * time.Second
			cfg.MaxOpenConns = 23
			cfg.MaxIdleConns = 13
			cfg.ConnMaxLifetime = 17 * time.Minute
			cfg.Compression = name

			options, err := clickHouseOptions(cfg)
			require.NoError(t, err)
			require.Equal(t, []string{"one:9000"}, options.Addr)
			cfg.Addresses[0] = "changed:9000"
			require.Equal(t, []string{"one:9000"}, options.Addr)
			require.Equal(t, ch.Native, options.Protocol)
			require.Equal(t, testDatabase, options.Auth.Database)
			require.Equal(t, "reader", options.Auth.Username)
			require.Equal(t, "top-secret", options.Auth.Password)
			require.Equal(t, 7*time.Second, options.DialTimeout)
			require.Equal(t, 11*time.Second, options.ReadTimeout)
			require.Equal(t, 23, options.MaxOpenConns)
			require.Equal(t, 13, options.MaxIdleConns)
			require.Equal(t, 17*time.Minute, options.ConnMaxLifetime)
			require.NotNil(t, options.Compression)
			require.Equal(t, method, options.Compression.Method)
			require.Nil(t, options.TLS)
		})
	}
}

func TestNewClickHouseStoreValidatesBeforeOpening(t *testing.T) {
	cfg := testConfig(t)
	cfg.Addresses = nil
	opened := false

	_, err := newClickHouseStoreWithOpener(cfg, func(*ch.Options) (clickHouseConnection, error) {
		opened = true
		return &fakeClickHouseConnection{}, nil
	})
	require.ErrorContains(t, err, "addresses")
	require.False(t, opened)
}

func TestNewClickHouseStoreOpensNativeConnection(t *testing.T) {
	cfg := testConfig(t)
	cfg.Database = testDatabase
	cfg.ObjectsTable = "objects"
	cfg.ChunksTable = "chunks"
	conn := &fakeClickHouseConnection{}
	var openedOptions *ch.Options

	store, err := newClickHouseStoreWithOpener(cfg, func(options *ch.Options) (clickHouseConnection, error) {
		openedOptions = options
		return conn, nil
	})
	require.NoError(t, err)
	require.Same(t, conn, store.conn)
	require.Equal(t, ch.Native, openedOptions.Protocol)
	require.Equal(t, "`profiles`.`objects`", store.objectsTable)
	require.Equal(t, "`profiles`.`chunks`", store.chunksTable)
}

func TestNewClickHouseStoreDoesNotLeakPasswordInOpenError(t *testing.T) {
	cfg := testConfig(t)
	require.NoError(t, cfg.Password.Set("do-not-leak"))

	_, err := newClickHouseStoreWithOpener(cfg, func(*ch.Options) (clickHouseConnection, error) {
		return nil, errors.New("dial rejected for do-not-leak")
	})
	require.ErrorContains(t, err, "open ClickHouse connection")
	require.ErrorContains(t, err, "dial rejected")
	require.NotContains(t, err.Error(), "do-not-leak")
}

func TestClickHouseOptionsTLS(t *testing.T) {
	cfg := testConfig(t)
	cfg.Secure = true
	cfg.SkipVerify = true
	cfg.TLSServerName = "clickhouse.internal"

	options, err := clickHouseOptions(cfg)
	require.NoError(t, err)
	require.NotNil(t, options.TLS)
	require.Equal(t, uint16(0x0303), options.TLS.MinVersion)
	require.Equal(t, "clickhouse.internal", options.TLS.ServerName)
	require.True(t, options.TLS.InsecureSkipVerify)
}

func TestClickHouseOptionsLoadsTLSCA(t *testing.T) {
	caPath := writeTestCertificate(t)
	originalSystemCertPool := systemCertPool
	systemCertPool = func() (*x509.CertPool, error) { return x509.NewCertPool(), nil }
	t.Cleanup(func() { systemCertPool = originalSystemCertPool })
	cfg := testConfig(t)
	cfg.Secure = true
	cfg.TLSCAPath = caPath

	options, err := clickHouseOptions(cfg)
	require.NoError(t, err)
	require.NotNil(t, options.TLS.RootCAs)
	require.True(t, options.TLS.RootCAs.Equal(certPoolFromFile(t, caPath)))
}

func TestClickHouseOptionsLoadsTLSCAWhenSystemPoolFails(t *testing.T) {
	originalSystemCertPool := systemCertPool
	systemCertPool = func() (*x509.CertPool, error) {
		return nil, errors.New("system certificate pool unavailable")
	}
	t.Cleanup(func() { systemCertPool = originalSystemCertPool })
	cfg := testConfig(t)
	cfg.Secure = true
	cfg.TLSCAPath = writeTestCertificate(t)

	options, err := clickHouseOptions(cfg)
	require.NoError(t, err)
	require.NotNil(t, options.TLS.RootCAs)
	require.True(t, options.TLS.RootCAs.Equal(certPoolFromFile(t, cfg.TLSCAPath)))
}

func TestClickHouseOptionsRejectsInvalidTLSCA(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content []byte
		path    string
	}{
		{name: "invalid PEM", content: []byte("not a certificate")},
		{name: "unreadable", path: filepath.Join(t.TempDir(), "missing.pem")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.path
			if path == "" {
				path = filepath.Join(t.TempDir(), "ca.pem")
				require.NoError(t, os.WriteFile(path, tc.content, 0o600))
			}
			cfg := testConfig(t)
			cfg.Secure = true
			cfg.TLSCAPath = path
			require.NoError(t, cfg.Password.Set("do-not-leak"))

			_, err := clickHouseOptions(cfg)
			require.Error(t, err)
			require.ErrorContains(t, err, "ClickHouse TLS CA")
			require.ErrorContains(t, err, filepath.Base(path))
			require.NotContains(t, err.Error(), "do-not-leak")
		})
	}
}

func TestClickHouseStoreInitCreatesAndValidatesTables(t *testing.T) {
	conn := &fakeClickHouseConnection{columns: map[string][]columnInfo{
		"objects": columnsFromSchema(objectsSchema),
		"chunks":  columnsFromSchema(chunksSchema),
	}}
	store := testClickHouseStore(t, conn)

	require.NoError(t, store.Init(context.Background()))
	require.Equal(t, 1, conn.pingCalls)
	require.Len(t, conn.execQueries, 4)
	require.Contains(t, conn.execQueries[0], "CREATE TABLE IF NOT EXISTS `profiles`.`objects`")
	require.Contains(t, conn.execQueries[1], "CREATE TABLE IF NOT EXISTS `profiles`.`chunks`")
	require.Contains(t, conn.execQueries[2], "CREATE TABLE IF NOT EXISTS `profiles`.`objects_latest`")
	require.Contains(t, conn.execQueries[3], "CREATE MATERIALIZED VIEW IF NOT EXISTS `profiles`.`objects_latest_mv`")
	require.Equal(t, []selectCall{
		{query: systemColumnsQuery, args: []any{testDatabase, "objects"}},
		{query: systemColumnsQuery, args: []any{testDatabase, "chunks"}},
		{query: systemColumnsQuery, args: []any{testDatabase, "objects_latest"}},
		{query: systemTablesQuery, args: []any{testDatabase, "objects", "chunks", "objects_latest", "objects_latest_mv"}},
	}, conn.selectCalls)
}

func TestClickHouseStoreInitCreatesLatestAggregateTableAndMaterializedView(t *testing.T) {
	conn := &fakeClickHouseConnection{columns: map[string][]columnInfo{
		"objects": columnsFromSchema(objectsSchema),
		"chunks":  columnsFromSchema(chunksSchema),
	}}
	store := testClickHouseStore(t, conn)

	require.NoError(t, store.Init(context.Background()))
	require.Len(t, conn.execQueries, 4)
	require.Contains(t, conn.execQueries[2], "CREATE TABLE IF NOT EXISTS `profiles`.`objects_latest`")
	require.Contains(t, conn.execQueries[2], "ENGINE = AggregatingMergeTree")
	require.Contains(t, conn.execQueries[3], "CREATE MATERIALIZED VIEW IF NOT EXISTS `profiles`.`objects_latest_mv`")
	require.Contains(t, conn.execQueries[3], "TO `profiles`.`objects_latest`")
}

func TestClickHouseStoreInitValidatesLatestSchemaDefinition(t *testing.T) {
	conn := &fakeClickHouseConnection{columns: map[string][]columnInfo{
		"objects":        columnsFromSchema(objectsSchema),
		"chunks":         columnsFromSchema(chunksSchema),
		"objects_latest": latestManifestColumnsForTest(),
	}}
	store := testClickHouseStore(t, conn)
	store.cfg.AutoCreateTables = false

	require.NoError(t, store.Init(context.Background()))
	require.Len(t, conn.selectCalls, 4)
	require.Equal(t, "objects_latest", conn.selectCalls[2].args[1])
	require.Contains(t, conn.selectCalls[3].query, "FROM system.tables")
}

func TestClickHouseStoreInitRejectsLegacyV1Schema(t *testing.T) {
	conn := &fakeClickHouseConnection{
		columns: map[string][]columnInfo{
			"objects":        columnsFromSchema(objectsSchema),
			"chunks":         columnsFromSchema(chunksSchema),
			"objects_latest": latestManifestColumnsForTest(),
		},
		tables: map[string]fakeSystemTable{
			"objects": {
				Engine:     "MergeTree",
				SortingKey: "object_key, version, generation, state",
			},
			"chunks": {
				Engine:     "MergeTree",
				SortingKey: "object_key, generation, chunk_index",
			},
			"objects_latest": {
				Engine:       "AggregatingMergeTree",
				SortingKey:   "object_key",
				PartitionKey: "cityHash64(object_key) % 64",
			},
			"objects_latest_mv": validLatestMaterializedViewForTest(t),
		},
	}
	store := testClickHouseStore(t, conn)
	store.cfg.AutoCreateTables = false

	err := store.Init(context.Background())
	require.ErrorContains(t, err, "legacy ClickHouse object store V1 schema")
	require.ErrorContains(t, err, "recreate all ClickHouse object-store tables")
}

func TestClickHouseStoreInitRejectsInvalidLatestMaterializedView(t *testing.T) {
	conn := &fakeClickHouseConnection{
		columns: map[string][]columnInfo{
			"objects":        columnsFromSchema(objectsSchema),
			"chunks":         columnsFromSchema(chunksSchema),
			"objects_latest": latestManifestColumnsForTest(),
		},
		tables: map[string]fakeSystemTable{
			"objects":           validObjectsTableForTest(),
			"chunks":            validChunksTableForTest(),
			"objects_latest":    validLatestTableForTest(),
			"objects_latest_mv": {Engine: "MaterializedView", CreateTableQuery: "CREATE MATERIALIZED VIEW objects_latest_mv AS SELECT 1"},
		},
	}
	store := testClickHouseStore(t, conn)
	store.cfg.AutoCreateTables = false

	err := store.Init(context.Background())
	require.ErrorContains(t, err, "materialized view definition")
	require.ErrorContains(t, err, "recreate all ClickHouse object-store tables")
}

func TestClickHouseStoreInitSkipsDDLWhenDisabled(t *testing.T) {
	conn := &fakeClickHouseConnection{columns: map[string][]columnInfo{
		"objects": columnsFromSchema(objectsSchema),
		"chunks":  columnsFromSchema(chunksSchema),
	}}
	store := testClickHouseStore(t, conn)
	store.cfg.AutoCreateTables = false

	require.NoError(t, store.Init(context.Background()))
	require.Empty(t, conn.execQueries)
	require.Len(t, conn.selectCalls, 4)
}

func TestClickHouseStoreInitReportsConnectionAndSchemaErrors(t *testing.T) {
	t.Run("ping", func(t *testing.T) {
		conn := &fakeClickHouseConnection{pingErr: errors.New("unavailable")}
		store := testClickHouseStore(t, conn)

		err := store.Init(context.Background())
		require.ErrorContains(t, err, "ping ClickHouse")
		require.Empty(t, conn.execQueries)
		require.Empty(t, conn.selectCalls)
	})

	t.Run("missing column", func(t *testing.T) {
		conn := &fakeClickHouseConnection{columns: map[string][]columnInfo{
			"objects": removeColumn(columnsFromSchema(objectsSchema), "generation"),
			"chunks":  columnsFromSchema(chunksSchema),
		}}
		store := testClickHouseStore(t, conn)

		err := store.Init(context.Background())
		require.ErrorContains(t, err, "profiles.objects")
		require.ErrorContains(t, err, "missing column generation")
	})

	t.Run("incompatible column", func(t *testing.T) {
		chunks := columnsFromSchema(chunksSchema)
		for i := range chunks {
			if chunks[i].Name == "data" {
				chunks[i].Type = "Array(UInt8)"
			}
		}
		conn := &fakeClickHouseConnection{columns: map[string][]columnInfo{
			"objects": columnsFromSchema(objectsSchema),
			"chunks":  chunks,
		}}
		store := testClickHouseStore(t, conn)

		err := store.Init(context.Background())
		require.ErrorContains(t, err, "profiles.chunks")
		require.ErrorContains(t, err, "incompatible column data")
	})
}

func TestClickHouseStoreConnectionErrorsPreserveSafeContext(t *testing.T) {
	const password = "configured-password"
	tests := []struct {
		name     string
		sentinel error
		invoke   func(*clickhouseStore, *fakeClickHouseConnection, error) error
	}{
		{
			name:     "ping canceled",
			sentinel: context.Canceled,
			invoke: func(store *clickhouseStore, conn *fakeClickHouseConnection, driverErr error) error {
				conn.pingErr = driverErr
				return store.Init(context.Background())
			},
		},
		{
			name:     "DDL deadline exceeded",
			sentinel: context.DeadlineExceeded,
			invoke: func(store *clickhouseStore, conn *fakeClickHouseConnection, driverErr error) error {
				conn.execErr = driverErr
				return store.Init(context.Background())
			},
		},
		{
			name:     "select canceled",
			sentinel: context.Canceled,
			invoke: func(store *clickhouseStore, conn *fakeClickHouseConnection, driverErr error) error {
				store.cfg.AutoCreateTables = false
				conn.selectErr = driverErr
				return store.Init(context.Background())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &fakeClickHouseConnection{columns: map[string][]columnInfo{
				"objects": columnsFromSchema(objectsSchema),
				"chunks":  columnsFromSchema(chunksSchema),
			}}
			store := testClickHouseStore(t, conn)
			require.NoError(t, store.cfg.Password.Set(password))
			driverErr := fmt.Errorf("driver rejected %s: %w", password, tt.sentinel)

			err := tt.invoke(store, conn, driverErr)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.sentinel)
			require.False(t, errors.Is(err, driverErr))
			require.Contains(t, err.Error(), "[REDACTED]")
			require.NotContains(t, err.Error(), password)
		})
	}
}

func TestQueryContextUsesEarlierDeadline(t *testing.T) {
	store := &clickhouseStore{cfg: Config{QueryTimeout: time.Hour}}
	callerDeadline := time.Now().Add(time.Minute)
	caller, callerCancel := context.WithDeadline(context.Background(), callerDeadline)
	defer callerCancel()

	ctx, cancel := store.queryContext(caller)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, callerDeadline, deadline, time.Millisecond)
}

func TestQueryContextAddsConfiguredTimeout(t *testing.T) {
	store := &clickhouseStore{cfg: Config{QueryTimeout: time.Minute}}
	started := time.Now()

	ctx, cancel := store.queryContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, started.Add(time.Minute), deadline, time.Second)
}

func TestClickHouseStoreCloseDelegatesOnce(t *testing.T) {
	conn := &fakeClickHouseConnection{}
	store := &clickhouseStore{conn: conn}

	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
	require.Equal(t, 1, conn.closeCalls)
}

func TestClickHouseStoreClosePreservesSafeContextAndRedactsPassword(t *testing.T) {
	const password = "configured-password"
	driverErr := fmt.Errorf("close failed for %s: %w", password, context.DeadlineExceeded)
	conn := &fakeClickHouseConnection{closeErr: driverErr}
	store := testClickHouseStore(t, conn)
	require.NoError(t, store.cfg.Password.Set(password))

	err := store.Close()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, errors.Is(err, driverErr))
	require.Contains(t, err.Error(), "[REDACTED]")
	require.NotContains(t, err.Error(), password)
}

func TestClickHouseStoreBeginUploadUsesServerTimeAndInsertsPendingManifest(t *testing.T) {
	serverNow := time.Date(2026, 7, 16, 1, 2, 3, 456000000, time.UTC)
	batch := &fakeInsertBatch{}
	conn := &fakeClickHouseConnection{insertBatch: batch}
	conn.selectFn = selectServerTime(serverNow)
	store := testClickHouseStore(t, conn)
	store.cfg.MaxUploadDuration = 17 * time.Minute
	upload := manifest{
		Key: "tenant/object", Generation: uuid.New(), Size: 42, ChunkCount: 3,
		ChunkSize: 16, State: deleted, Version: 99,
	}

	got, err := store.BeginUpload(context.Background(), upload)
	require.NoError(t, err)
	require.Equal(t, pending, got.State)
	require.Equal(t, serverNow, got.EventAt)
	require.Equal(t, serverNow.Add(17*time.Minute), got.LeaseExpiresAt)
	require.Equal(t, upload.Key, got.Key)
	require.Equal(t, upload.Generation, got.Generation)
	require.Equal(t, upload.Version, got.Version)
	require.Equal(t, []selectCall{{query: "SELECT now64(3) AS now", args: nil}}, conn.selectCalls)
	require.Equal(t, "INSERT INTO `profiles`.`objects` (object_key, generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at)", conn.prepareQueries[0])
	require.Equal(t, [][]any{{got.Key, got.Generation, got.Size, got.ChunkCount, got.ChunkSize, got.State.String(), got.Version, got.LeaseExpiresAt, got.EventAt}}, batch.rows)
	require.Equal(t, []string{"append", "send", "close"}, batch.operations)
	require.NotContains(t, conn.prepareQueries[0], upload.Key)
}

func TestClickHouseStoreInsertChunksUsesOneOrderedBatch(t *testing.T) {
	batch := &fakeInsertBatch{}
	conn := &fakeClickHouseConnection{insertBatch: batch}
	store := testClickHouseStore(t, conn)
	generation := uuid.New()
	chunks := []chunk{
		{Key: "key", Generation: generation, Index: 4, Data: []byte("first-secret"), CreatedAt: time.Unix(1, 0).UTC()},
		{Key: "key", Generation: generation, Index: 5, Data: []byte("second-secret"), CreatedAt: time.Unix(2, 0).UTC()},
	}

	require.NoError(t, store.InsertChunks(context.Background(), chunks))
	require.Equal(t, []string{"INSERT INTO `profiles`.`chunks` (object_key, generation, chunk_index, data)"}, conn.prepareQueries)
	require.Equal(t, [][]any{
		{chunks[0].Key, generation, chunks[0].Index, chunks[0].Data},
		{chunks[1].Key, generation, chunks[1].Index, chunks[1].Data},
	}, batch.rows)
	require.Equal(t, []string{"append", "append", "send", "close"}, batch.operations)
}

func TestClickHouseStoreInsertChunksEmptyIsNoOp(t *testing.T) {
	conn := &fakeClickHouseConnection{}
	store := testClickHouseStore(t, conn)
	require.NoError(t, store.InsertChunks(context.Background(), nil))
	require.Empty(t, conn.prepareQueries)
}

func TestClickHouseStoreInsertChunksSanitizesBatchErrors(t *testing.T) {
	const marker = "large-profile-payload-must-not-leak"
	payload := bytes.Repeat([]byte(marker), 1<<15)
	for _, tc := range []struct {
		name  string
		batch *fakeInsertBatch
	}{
		{name: "append", batch: &fakeInsertBatch{appendErr: fmt.Errorf("bad %s: %w", marker, context.Canceled)}},
		{name: "send", batch: &fakeInsertBatch{sendErr: fmt.Errorf("bad %s: %w", marker, context.DeadlineExceeded)}},
		{name: "close", batch: &fakeInsertBatch{closeErr: fmt.Errorf("bad %s: %w", marker, context.Canceled)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := testClickHouseStore(t, &fakeClickHouseConnection{insertBatch: tc.batch})
			err := store.InsertChunks(context.Background(), []chunk{{Key: "key", Data: payload}})
			require.Error(t, err)
			require.NotContains(t, err.Error(), marker)
			require.Contains(t, err.Error(), "payload operation failed")
			require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
		})
	}
}

func TestClickHouseStoreCommitUploadValidatesExactChunksInOneExec(t *testing.T) {
	for _, tc := range []struct {
		name   string
		upload manifest
	}{
		{
			name:   "non-empty",
			upload: manifest{Key: "sensitive/value", Generation: uuid.New(), Size: 91, ChunkCount: 7, ChunkSize: 13, State: pending, Version: 8, LeaseExpiresAt: time.Unix(1, 0), EventAt: time.Unix(2, 0)},
		},
		{
			name:   "empty",
			upload: manifest{Key: "empty/value", Generation: uuid.New(), Size: 0, ChunkCount: 0, ChunkSize: 13, State: pending, Version: 9, LeaseExpiresAt: time.Unix(3, 0), EventAt: time.Unix(4, 0)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &fakeClickHouseConnection{}
			store := testClickHouseStore(t, conn)

			require.NoError(t, store.CommitUpload(context.Background(), tc.upload))
			require.Empty(t, conn.prepareQueries)
			require.Empty(t, conn.selectCalls)
			require.Len(t, conn.execCalls, 1)
			insert := conn.execCalls[0]
			require.Contains(t, insert.query, "INSERT INTO `profiles`.`objects` (object_key, generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at)")
			require.Contains(t, insert.query, "now64(3) AS commit_at")
			require.Contains(t, insert.query, "AS committed_count")
			require.Contains(t, insert.query, "AS pending_count")
			require.Contains(t, insert.query, "argMax(tuple(chunk_size, lease_expires_at), event_at)")
			require.Contains(t, insert.query, "FROM `profiles`.`chunks`")
			require.Contains(t, insert.query, "count() AS chunk_row_count")
			require.Contains(t, insert.query, "countDistinct(chunk_index) AS chunk_distinct_count")
			require.Contains(t, insert.query, "sum(length(data)) AS chunk_object_size")
			require.Contains(t, insert.query, "min(chunk_index) AS chunk_min_index")
			require.Contains(t, insert.query, "max(chunk_index) AS chunk_max_index")
			require.Contains(t, insert.query, "SELECT target_key, target_generation, chunk_object_size, chunk_row_count")
			require.Contains(t, insert.query, "committed_count = 0 AND pending_count = 0")
			require.Contains(t, insert.query, "committed_count = 0 AND pending_count > 0 AND commit_at > pending_lease")
			require.Contains(t, insert.query, "caller_object_size != chunk_object_size")
			require.Contains(t, insert.query, "caller_chunk_count != chunk_row_count")
			require.Contains(t, insert.query, "caller_chunk_size != pending_chunk_size")
			require.Contains(t, insert.query, "chunk_row_count != chunk_distinct_count")
			require.Contains(t, insert.query, "chunk_row_count > 0 AND (")
			require.Contains(t, insert.query, "chunk_min_index != 0")
			require.Contains(t, insert.query, "chunk_max_index != chunk_row_count - 1")
			require.Contains(t, insert.query, "WHERE committed_count = 0")
			require.NotContains(t, insert.query, "pending_object_size")
			require.NotContains(t, insert.query, "pending_chunk_count")
			require.NotContains(t, insert.query, tc.upload.Key)
			require.NotContains(t, insert.args, tc.upload.LeaseExpiresAt)
			require.Equal(t, []any{
				tc.upload.Key, tc.upload.Generation, tc.upload.Version,
				uint8(committed), uint8(pending),
				tc.upload.Size, tc.upload.ChunkCount, tc.upload.ChunkSize,
				committed.String(),
			}, insert.args)
		})
	}
}

func TestClickHouseStoreCommitUploadReturnsServerValidationErrors(t *testing.T) {
	for _, message := range []string{
		"missing pending manifest",
		"upload lease expired",
		"upload metadata mismatch",
	} {
		t.Run(message, func(t *testing.T) {
			conn := &fakeClickHouseConnection{execErr: errors.New("ClickHouse commit " + message)}
			store := testClickHouseStore(t, conn)

			err := store.CommitUpload(context.Background(), manifest{Generation: uuid.New()})
			require.ErrorContains(t, err, message)
			require.Len(t, conn.execCalls, 1)
			require.Empty(t, conn.selectCalls)
		})
	}
}

func TestClickHouseStoreInsertDeleteUsesServerTimeAndDeletedState(t *testing.T) {
	serverNow := time.Date(2026, 7, 16, 3, 0, 0, 0, time.UTC)
	batch := &fakeInsertBatch{}
	conn := &fakeClickHouseConnection{insertBatch: batch, selectFn: selectServerTime(serverNow)}
	store := testClickHouseStore(t, conn)
	tombstone := manifest{Key: "key", Generation: uuid.New(), Version: 101, State: committed}

	got, err := store.InsertDelete(context.Background(), tombstone)
	require.NoError(t, err)
	require.Equal(t, deleted, got.State)
	require.Equal(t, serverNow, got.EventAt)
	require.Equal(t, serverNow, got.LeaseExpiresAt)
	require.Equal(t, tombstone.Generation, got.Generation)
	require.Equal(t, tombstone.Version, got.Version)
	require.Equal(t, deleted.String(), batch.rows[0][5])
	require.Equal(t, serverNow, batch.rows[0][7])
	require.Equal(t, serverNow, batch.rows[0][8])
}

func TestClickHouseStoreServerTimeRequiresExactlyOneRow(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows []time.Time
	}{
		{name: "no rows"},
		{name: "multiple rows", rows: []time.Time{time.Unix(1, 0), time.Unix(2, 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &fakeClickHouseConnection{selectFn: selectServerTimes(tc.rows...)}
			store := testClickHouseStore(t, conn)
			_, err := store.BeginUpload(context.Background(), manifest{})
			require.ErrorContains(t, err, "expected exactly one row")
			require.ErrorContains(t, err, fmt.Sprintf("got %d", len(tc.rows)))
			require.Empty(t, conn.prepareQueries)
		})
	}
}

func TestClickHouseStoreLatestManifestSelectsNewestValidOperation(t *testing.T) {
	want := manifest{Key: "prefix/key", Generation: uuid.New(), Size: 12, ChunkCount: 2, ChunkSize: 6, State: deleted, Version: 9, LeaseExpiresAt: time.Unix(10, 0).UTC(), EventAt: time.Unix(9, 0).UTC()}
	conn := &fakeClickHouseConnection{selectFn: selectManifests(want)}
	store := testClickHouseStore(t, conn)

	got, err := store.LatestManifest(context.Background(), want.Key)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Len(t, conn.selectCalls, 1)
	call := conn.selectCalls[0]
	require.Contains(t, call.query, "argMaxMerge(manifest) AS latest_manifest")
	require.Contains(t, call.query, "FROM `profiles`.`objects_latest`")
	require.Contains(t, call.query, "GROUP BY object_key")
	require.Contains(t, call.query, "tupleElement(latest_manifest, 1) AS generation")
	require.Contains(t, call.query, "toUInt8(tupleElement(latest_manifest, 5)) AS state")
	require.NotContains(t, call.query, "`profiles`.`objects`")
	require.NotContains(t, call.query, "row_number()")
	require.NotContains(t, call.query, "UNION ALL")
	require.NotContains(t, call.query, "FINAL")
	require.NotContains(t, call.query, want.Key)
	require.Equal(t, []any{want.Key}, call.args)
}

func TestClickHouseStoreLatestManifestReturnsNotFoundForEmptyResult(t *testing.T) {
	store := testClickHouseStore(t, &fakeClickHouseConnection{selectFn: selectManifests()})
	_, err := store.LatestManifest(context.Background(), "missing")
	require.ErrorIs(t, err, ErrObjectNotFound)
}

func TestClickHouseStoreChunksBindsRangeAndOrdersByIndex(t *testing.T) {
	generation := uuid.New()
	want := []chunk{{Key: "key", Generation: generation, Index: 2, Data: []byte("data"), FullLength: 4}}
	conn := &fakeClickHouseConnection{selectFn: selectChunks(want...)}
	store := testClickHouseStore(t, conn)

	object := manifest{Key: "key", Generation: generation, ChunkSize: 4}
	got, err := store.Chunks(context.Background(), object, 2, 5, 9, 21)
	require.NoError(t, err)
	require.Equal(t, want, got)
	call := conn.selectCalls[0]
	require.Contains(t, call.query, "chunk_index >= ? AND chunk_index <= ?")
	require.Contains(t, call.query, "ORDER BY chunk_index")
	require.Contains(t, call.query, "substring(data, slice_from, slice_len) AS data")
	require.Contains(t, call.query, "toUInt64(length(data)) AS full_length")
	require.Equal(t, []any{int64(9), int64(4), int64(21), int64(4), "key", generation, uint32(2), uint32(5)}, call.args)
	require.NotContains(t, call.query, "'key'")
}

func TestClickHouseStoreListLatestUsesNewestLiveOperationPerKey(t *testing.T) {
	oldGeneration := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	newGeneration := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	deletedGeneration := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	oldCommitAt := time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC)
	newCommitAt := oldCommitAt.Add(time.Hour)
	leaseExpiresAt := newCommitAt.Add(time.Hour)
	want := []manifest{{Key: "pre/overwritten", Generation: newGeneration, State: committed, Version: 2, EventAt: newCommitAt}}
	conn := &fakeClickHouseConnection{selectFn: selectLatestManifestsFromHistory(t,
		manifest{Key: "pre/deleted", Generation: oldGeneration, State: pending, Version: 1, LeaseExpiresAt: leaseExpiresAt},
		manifest{Key: "pre/deleted", Generation: oldGeneration, State: committed, Version: 1, EventAt: oldCommitAt},
		manifest{Key: "pre/deleted", Generation: deletedGeneration, State: deleted, Version: 2, EventAt: newCommitAt},
		manifest{Key: "pre/overwritten", Generation: oldGeneration, State: pending, Version: 1, LeaseExpiresAt: leaseExpiresAt},
		manifest{Key: "pre/overwritten", Generation: oldGeneration, State: committed, Version: 1, EventAt: oldCommitAt},
		manifest{Key: "pre/overwritten", Generation: newGeneration, State: pending, Version: 2, LeaseExpiresAt: leaseExpiresAt},
		want[0],
		manifest{Key: "other/prefix", Generation: newGeneration, State: pending, Version: 3, LeaseExpiresAt: leaseExpiresAt},
		manifest{Key: "other/prefix", Generation: newGeneration, State: committed, Version: 3, EventAt: newCommitAt},
	)}
	store := testClickHouseStore(t, conn)

	got, err := store.ListLatest(context.Background(), "pre/", "pre/a", 7)
	require.NoError(t, err)
	require.Equal(t, want, got)
	call := conn.selectCalls[0]
	require.Contains(t, call.query, "startsWith(object_key, ?)")
	require.Contains(t, call.query, "object_key > ?")
	require.Contains(t, call.query, "ORDER BY object_key")
	require.Contains(t, call.query, "LIMIT ?")
	require.Contains(t, call.query, "toUInt8(tupleElement(latest_manifest, 5)) != ?")
	require.Contains(t, call.query, "argMaxMerge(manifest) AS latest_manifest")
	require.Contains(t, call.query, "FROM `profiles`.`objects_latest`")
	require.Contains(t, call.query, "GROUP BY object_key")
	require.NotContains(t, call.query, "`profiles`.`objects`")
	require.NotContains(t, call.query, "row_number()")
	require.NotContains(t, call.query, "UNION ALL")
	require.NotContains(t, call.query, "FINAL")
	require.Equal(t, []any{"pre/", "pre/a", uint8(deleted), 7}, call.args)
	require.NotContains(t, call.query, "pre/")
}

func TestClickHouseStoreOperationsRespectCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn := &fakeClickHouseConnection{selectFn: func(ctx context.Context, _ any, _ string, _ ...any) error { return ctx.Err() }}
	store := testClickHouseStore(t, conn)
	_, err := store.LatestManifest(ctx, "key")
	require.ErrorIs(t, err, context.Canceled)
}

func TestClickHouseStoreCleanupCandidatesUsesSafeBoundedQuery(t *testing.T) {
	grace := 2*time.Hour + 1500*time.Microsecond
	generation := uuid.New()
	conn := &fakeClickHouseConnection{selectFn: func(_ context.Context, dest any, query string, args ...any) error {
		rows := dest.(*[]cleanupCandidateRow)
		*rows = append(*rows, cleanupCandidateRow{Key: "key", Generation: generation})
		return nil
	}}
	store := testClickHouseStore(t, conn)

	candidates, err := store.CleanupCandidates(context.Background(), 17, grace, 7)
	require.NoError(t, err)
	require.Equal(t, []cleanupCandidate{{Key: "key", Generation: generation}}, candidates)
	require.Len(t, conn.selectCalls, 1)
	call := conn.selectCalls[0]
	require.Contains(t, call.query, "partition_operations AS")
	require.Contains(t, call.query, "now64(3) AS cleanup_now")
	require.Contains(t, call.query, "toIntervalMillisecond(?) AS cleanup_grace")
	require.Contains(t, call.query, "WHERE _partition_id = ?")
	require.NotContains(t, call.query, "PREWHERE cityHash64")
	require.Contains(t, call.query, "SETTINGS max_threads = 2")
	require.Contains(t, call.query, "valid_generations AS")
	require.Contains(t, call.query, "GROUP BY object_key, generation")
	require.Contains(t, call.query, "latest_generations AS")
	require.Contains(t, call.query, "tupleElement(argMaxMerge(manifest), 1) AS generation")
	require.Contains(t, call.query, "FROM `profiles`.`objects_latest`")
	require.Contains(t, call.query, "pending_generations AS")
	require.Contains(t, call.query, "abandoned_pending AS")
	require.Contains(t, call.query, "max(lease_expires_at) AS lease_expires_at")
	require.Contains(t, call.query, "greatest(lease_expires_at, event_at)")
	require.Contains(t, call.query, "LEFT ANTI JOIN valid_generations")
	require.Contains(t, call.query, "source.lease_expires_at + cleanup_grace < cleanup_now")
	require.Contains(t, call.query, "superseded_generations AS")
	require.Contains(t, call.query, "LEFT ANTI JOIN latest_generations")
	require.Contains(t, call.query, "operation_safety_at + cleanup_grace < cleanup_now")
	require.Contains(t, call.query, "SELECT DISTINCT object_key, generation")
	require.Contains(t, call.query, "ORDER BY object_key, generation")
	require.Contains(t, call.query, "LIMIT ?")
	require.NotContains(t, call.query, "row_number()")
	require.Equal(t, []any{
		int64(7_200_002),
		"17", uint8(pending), uint8(committed), uint8(deleted),
		"17", 7,
	}, call.args)
}

func TestClickHouseStoreCleanupCandidatesProtectsValidLatestGenerations(t *testing.T) {
	serverNow := time.Unix(100, 0)
	grace := 20 * time.Second
	expired := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	committedGeneration := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	oldLive := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	latestLive := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	oldBeforeDelete := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	latestTombstone := uuid.MustParse("00000000-0000-0000-0000-000000000006")
	delayedByLease := uuid.MustParse("00000000-0000-0000-0000-000000000007")
	afterDelayed := uuid.MustParse("00000000-0000-0000-0000-000000000008")
	history := []manifest{
		{Key: "expired", Generation: expired, State: pending, Version: 1, LeaseExpiresAt: time.Unix(50, 0)},
		{Key: "committed", Generation: committedGeneration, State: pending, Version: 1, LeaseExpiresAt: time.Unix(50, 0)},
		{Key: "committed", Generation: committedGeneration, State: committed, Version: 1, EventAt: time.Unix(40, 0)},
		{Key: "live", Generation: oldLive, State: pending, Version: 1, LeaseExpiresAt: time.Unix(40, 0)},
		{Key: "live", Generation: oldLive, State: committed, Version: 1, EventAt: time.Unix(30, 0)},
		{Key: "live", Generation: latestLive, State: pending, Version: 2, LeaseExpiresAt: time.Unix(200, 0)},
		{Key: "live", Generation: latestLive, State: committed, Version: 2, EventAt: time.Unix(90, 0)},
		{Key: "deleted", Generation: oldBeforeDelete, State: pending, Version: 1, LeaseExpiresAt: time.Unix(40, 0)},
		{Key: "deleted", Generation: oldBeforeDelete, State: committed, Version: 1, EventAt: time.Unix(30, 0)},
		{Key: "deleted", Generation: latestTombstone, State: deleted, Version: 2, EventAt: time.Unix(90, 0)},
		{Key: "delayed", Generation: delayedByLease, State: pending, Version: 1, LeaseExpiresAt: time.Unix(90, 0)},
		{Key: "delayed", Generation: delayedByLease, State: committed, Version: 1, EventAt: time.Unix(30, 0), LeaseExpiresAt: time.Unix(90, 0)},
		{Key: "delayed", Generation: afterDelayed, State: deleted, Version: 2, EventAt: time.Unix(95, 0)},
	}
	conn := &fakeClickHouseConnection{selectFn: selectCleanupCandidatesFromHistory(t, serverNow, grace, history...)}
	store := testClickHouseStore(t, conn)

	candidates, err := store.CleanupCandidates(context.Background(), 7, grace, 10)
	require.NoError(t, err)
	require.Equal(t, []cleanupCandidate{
		{Key: "deleted", Generation: oldBeforeDelete},
		{Key: "expired", Generation: expired},
		{Key: "live", Generation: oldLive},
	}, candidates)
}

func TestClickHouseStoreDeleteGenerationsUsesTwoParameterizedLightweightDeletes(t *testing.T) {
	candidates := []cleanupCandidate{
		{Key: "quoted' key", Generation: uuid.New()},
		{Key: "second", Generation: uuid.New()},
	}
	conn := &fakeClickHouseConnection{}
	store := testClickHouseStore(t, conn)

	require.NoError(t, store.DeleteGenerations(context.Background(), 17, candidates))
	require.Len(t, conn.execCalls, 2)
	for _, call := range conn.execCalls {
		require.Contains(t, call.query, "IN PARTITION ? WHERE (object_key, generation) IN ((?, ?), (?, ?))")
		require.Contains(t, call.query, "DELETE FROM")
		require.Contains(t, call.query, "SETTINGS lightweight_deletes_sync = 2")
		require.NotContains(t, call.query, "ALTER TABLE")
		for _, candidate := range candidates {
			require.NotContains(t, call.query, candidate.Key)
			require.NotContains(t, call.query, candidate.Generation.String())
		}
		require.Equal(t, []any{
			uint64(17),
			candidates[0].Key, candidates[0].Generation,
			candidates[1].Key, candidates[1].Generation,
		}, call.args)
	}
	require.Contains(t, conn.execCalls[0].query, "`profiles`.`chunks`")
	require.Contains(t, conn.execCalls[1].query, "`profiles`.`objects`")
}

func TestClickHouseStoreDeleteGenerationsEmptyIsNoop(t *testing.T) {
	conn := &fakeClickHouseConnection{}
	store := testClickHouseStore(t, conn)

	require.NoError(t, store.DeleteGenerations(context.Background(), 17, nil))
	require.Empty(t, conn.execCalls)
}

func TestClickHouseStoreDeleteGenerationsRetriesChunksAfterPartialFailure(t *testing.T) {
	candidates := []cleanupCandidate{{Key: "key", Generation: uuid.New()}}
	manifestFailures := 1
	conn := &fakeClickHouseConnection{execFn: func(_ context.Context, query string, _ ...any) error {
		if strings.Contains(query, "`profiles`.`objects`") && manifestFailures > 0 {
			manifestFailures--
			return errors.New("manifest mutation failed")
		}
		return nil
	}}
	store := testClickHouseStore(t, conn)

	require.Error(t, store.DeleteGenerations(context.Background(), 17, candidates))
	require.NoError(t, store.DeleteGenerations(context.Background(), 17, candidates))
	require.Len(t, conn.execCalls, 4)
	require.Contains(t, conn.execCalls[0].query, "`profiles`.`chunks`")
	require.Contains(t, conn.execCalls[1].query, "`profiles`.`objects`")
	require.Contains(t, conn.execCalls[2].query, "`profiles`.`chunks`")
	require.Contains(t, conn.execCalls[3].query, "`profiles`.`objects`")
}

func testConfig(t *testing.T) Config {
	t.Helper()
	var cfg Config
	flagext.DefaultValues(&cfg)
	return cfg
}

func testClickHouseStore(t *testing.T, conn clickHouseConnection) *clickhouseStore {
	t.Helper()
	cfg := testConfig(t)
	cfg.Database = testDatabase
	cfg.ObjectsTable = "objects"
	cfg.ChunksTable = "chunks"
	objectsTable, err := qualifiedTable(cfg.Database, cfg.ObjectsTable)
	require.NoError(t, err)
	chunksTable, err := qualifiedTable(cfg.Database, cfg.ChunksTable)
	require.NoError(t, err)
	identifiers, err := deriveSchemaIdentifiers(cfg.ObjectsTable)
	require.NoError(t, err)
	latestTable, err := qualifiedTable(cfg.Database, identifiers.LatestTable)
	require.NoError(t, err)
	return &clickhouseStore{
		cfg:          cfg,
		objectsTable: objectsTable,
		chunksTable:  chunksTable,
		latestTable:  latestTable,
		conn:         conn,
	}
}

func writeTestCertificate(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	return path
}

func certPoolFromFile(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(contents))
	return pool
}

type selectCall struct {
	query string
	args  []any
}

type fakeSystemTable struct {
	Engine           string
	SortingKey       string
	PartitionKey     string
	CreateTableQuery string
}

func latestManifestColumnsForTest() []columnInfo {
	return []columnInfo{
		{Name: "object_key", Type: "String"},
		{Name: "manifest", Type: latestManifestAggregateType},
	}
}

func validObjectsTableForTest() fakeSystemTable {
	return fakeSystemTable{
		Engine:       "MergeTree",
		SortingKey:   "object_key, version, generation, state",
		PartitionKey: "cityHash64(object_key) % 64",
	}
}

func validChunksTableForTest() fakeSystemTable {
	return fakeSystemTable{
		Engine:       "MergeTree",
		SortingKey:   "object_key, generation, chunk_index",
		PartitionKey: "cityHash64(object_key) % 64",
	}
}

func validLatestTableForTest() fakeSystemTable {
	return fakeSystemTable{
		Engine:       "AggregatingMergeTree",
		SortingKey:   "object_key",
		PartitionKey: "cityHash64(object_key) % 64",
	}
}

func validLatestMaterializedViewForTest(t *testing.T) fakeSystemTable {
	t.Helper()
	definition, err := latestMaterializedViewDDL(testDatabase, "objects", "objects_latest", "objects_latest_mv")
	require.NoError(t, err)
	return fakeSystemTable{Engine: "MaterializedView", CreateTableQuery: strings.ReplaceAll(definition, "`", "")}
}

type execCall struct {
	query string
	args  []any
}

type fakeClickHouseConnection struct {
	pingCalls      int
	pingErr        error
	execQueries    []string
	execCalls      []execCall
	execErr        error
	execFn         func(context.Context, string, ...any) error
	selectCalls    []selectCall
	selectErr      error
	selectFn       func(context.Context, any, string, ...any) error
	columns        map[string][]columnInfo
	tables         map[string]fakeSystemTable
	insertBatch    insertBatch
	prepareErr     error
	prepareQueries []string
	closeCalls     int
	closeErr       error
}

func (c *fakeClickHouseConnection) Ping(context.Context) error {
	c.pingCalls++
	return c.pingErr
}

func (c *fakeClickHouseConnection) Exec(ctx context.Context, query string, args ...any) error {
	c.execQueries = append(c.execQueries, query)
	c.execCalls = append(c.execCalls, execCall{query: query, args: append([]any(nil), args...)})
	if c.execFn != nil {
		return c.execFn(ctx, query, args...)
	}
	return c.execErr
}

func (c *fakeClickHouseConnection) Select(ctx context.Context, dest any, query string, args ...any) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr || value.IsNil() || value.Elem().Kind() != reflect.Slice {
		return errors.New("fake ClickHouse Select destination must be a pointer to a slice")
	}
	c.selectCalls = append(c.selectCalls, selectCall{query: query, args: args})
	if c.selectErr != nil {
		return c.selectErr
	}
	if c.selectFn != nil {
		return c.selectFn(ctx, dest, query, args...)
	}
	switch rows := dest.(type) {
	case *[]systemColumn:
		columns := c.columns[args[1].(string)]
		if columns == nil && args[1].(string) == "objects_latest" {
			columns = latestManifestColumnsForTest()
		}
		for _, column := range columns {
			*rows = append(*rows, systemColumn(column))
		}
	case *[]tableInfo:
		tables := c.tables
		if tables == nil {
			tables = defaultV2TablesForTest(args)
		}
		for _, arg := range args[1:] {
			name := arg.(string)
			table, ok := tables[name]
			if !ok {
				continue
			}
			*rows = append(*rows, tableInfo{
				Name:             name,
				Engine:           table.Engine,
				SortingKey:       table.SortingKey,
				PartitionKey:     table.PartitionKey,
				CreateTableQuery: table.CreateTableQuery,
			})
		}
	default:
		return fmt.Errorf("fake ClickHouse Select does not support destination %T", dest)
	}
	return nil
}

func defaultV2TablesForTest(args []any) map[string]fakeSystemTable {
	database := args[0].(string)
	objectsTable := args[1].(string)
	chunksTable := args[2].(string)
	latestTable := args[3].(string)
	latestView := args[4].(string)
	definition, _ := latestMaterializedViewDDL(database, objectsTable, latestTable, latestView)
	definition = strings.ReplaceAll(definition, "`", "")
	return map[string]fakeSystemTable{
		objectsTable: validObjectsTableForTest(),
		chunksTable:  validChunksTableForTest(),
		latestTable:  validLatestTableForTest(),
		latestView:   {Engine: "MaterializedView", CreateTableQuery: definition},
	}
}

func (c *fakeClickHouseConnection) PrepareInsert(_ context.Context, query string) (insertBatch, error) {
	c.prepareQueries = append(c.prepareQueries, query)
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if c.insertBatch == nil {
		return nil, errors.New("fake ClickHouse insert batch is not configured")
	}
	return c.insertBatch, nil
}

func (c *fakeClickHouseConnection) Close() error {
	c.closeCalls++
	return c.closeErr
}

type fakeInsertBatch struct {
	rows       [][]any
	operations []string
	appendErr  error
	sendErr    error
	closeErr   error
}

func (b *fakeInsertBatch) Append(values ...any) error {
	b.operations = append(b.operations, "append")
	b.rows = append(b.rows, append([]any(nil), values...))
	return b.appendErr
}
func (b *fakeInsertBatch) Send() error {
	b.operations = append(b.operations, "send")
	return b.sendErr
}
func (b *fakeInsertBatch) Close() error {
	b.operations = append(b.operations, "close")
	return b.closeErr
}

func selectServerTime(now time.Time) func(context.Context, any, string, ...any) error {
	return selectServerTimes(now)
}

func selectServerTimes(times ...time.Time) func(context.Context, any, string, ...any) error {
	return func(_ context.Context, dest any, _ string, _ ...any) error {
		rows := dest.(*[]serverTimeRow)
		for _, now := range times {
			*rows = append(*rows, serverTimeRow{Now: now})
		}
		return nil
	}
}

func selectManifests(rows ...manifest) func(context.Context, any, string, ...any) error {
	return func(_ context.Context, dest any, _ string, _ ...any) error {
		result := dest.(*[]manifestRow)
		for _, row := range rows {
			*result = append(*result, manifestRow{
				Key: row.Key, Generation: row.Generation, Size: row.Size,
				ChunkCount: row.ChunkCount, ChunkSize: row.ChunkSize,
				State: uint8(row.State), Version: row.Version,
				LeaseExpiresAt: row.LeaseExpiresAt, EventAt: row.EventAt,
			})
		}
		return nil
	}
}

func selectLatestManifestsFromHistory(t *testing.T, history ...manifest) func(context.Context, any, string, ...any) error {
	t.Helper()
	return func(ctx context.Context, dest any, query string, args ...any) error {
		require.Contains(t, query, "argMaxMerge(manifest)")
		require.NotContains(t, query, "row_number()")
		require.Len(t, args, 4)

		prefix := args[0].(string)
		afterKey := args[1].(string)
		filteredState := manifestState(args[2].(uint8))
		limit := args[3].(int)
		require.Equal(t, deleted, filteredState)

		latest := make(map[string]manifest)
		for _, operation := range history {
			if !strings.HasPrefix(operation.Key, prefix) || operation.Key <= afterKey {
				continue
			}
			if operation.State != committed && operation.State != deleted {
				continue
			}
			current, ok := latest[operation.Key]
			if !ok || operation.Version > current.Version ||
				(operation.Version == current.Version && bytes.Compare(operation.Generation[:], current.Generation[:]) > 0) {
				latest[operation.Key] = operation
			}
		}

		keys := make([]string, 0, len(latest))
		for key, operation := range latest {
			if operation.State != filteredState {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		if len(keys) > limit {
			keys = keys[:limit]
		}
		rows := make([]manifest, 0, len(keys))
		for _, key := range keys {
			rows = append(rows, latest[key])
		}
		return selectManifests(rows...)(ctx, dest, query, args...)
	}
}

func selectCleanupCandidatesFromHistory(t *testing.T, serverNow time.Time, grace time.Duration, history ...manifest) func(context.Context, any, string, ...any) error {
	t.Helper()
	return func(_ context.Context, dest any, query string, args ...any) error {
		require.Contains(t, query, "LEFT ANTI JOIN valid_generations")
		require.Contains(t, query, "LEFT ANTI JOIN latest_generations")
		require.NotContains(t, query, "row_number()")
		require.Equal(t, grace.Milliseconds(), args[0])
		require.Equal(t, []any{
			grace.Milliseconds(), "7", uint8(pending), uint8(committed), uint8(deleted), "7", 10,
		}, args)

		type operationID struct {
			key        string
			generation uuid.UUID
			version    uint64
		}
		leases := make(map[operationID]time.Time)
		for _, operation := range history {
			if operation.State != pending {
				continue
			}
			id := operationID{operation.Key, operation.Generation, operation.Version}
			if operation.LeaseExpiresAt.After(leases[id]) {
				leases[id] = operation.LeaseExpiresAt
			}
		}
		valid := make([]manifest, 0, len(history))
		validGeneration := make(map[cleanupCandidate]bool)
		for _, operation := range history {
			switch operation.State {
			case committed:
				lease, ok := leases[operationID{operation.Key, operation.Generation, operation.Version}]
				if !ok || operation.EventAt.After(lease) {
					continue
				}
				operation.LeaseExpiresAt = lease
			case deleted:
			default:
				continue
			}
			valid = append(valid, operation)
			validGeneration[cleanupCandidate{operation.Key, operation.Generation}] = true
		}
		latest := make(map[string]manifest)
		for _, operation := range valid {
			current, ok := latest[operation.Key]
			if !ok || operation.Version > current.Version ||
				(operation.Version == current.Version && bytes.Compare(operation.Generation[:], current.Generation[:]) > 0) {
				latest[operation.Key] = operation
			}
		}
		set := make(map[cleanupCandidate]struct{})
		for id, lease := range leases {
			candidate := cleanupCandidate{id.key, id.generation}
			if lease.Add(grace).Before(serverNow) && !validGeneration[candidate] {
				set[candidate] = struct{}{}
			}
		}
		for _, operation := range valid {
			safetyAt := operation.EventAt
			if operation.LeaseExpiresAt.After(safetyAt) {
				safetyAt = operation.LeaseExpiresAt
			}
			if operation.Generation != latest[operation.Key].Generation && safetyAt.Add(grace).Before(serverNow) {
				set[cleanupCandidate{operation.Key, operation.Generation}] = struct{}{}
			}
		}
		candidates := make([]cleanupCandidate, 0, len(set))
		for candidate := range set {
			candidates = append(candidates, candidate)
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Key != candidates[j].Key {
				return candidates[i].Key < candidates[j].Key
			}
			return bytes.Compare(candidates[i].Generation[:], candidates[j].Generation[:]) < 0
		})
		rows := dest.(*[]cleanupCandidateRow)
		for _, candidate := range candidates {
			*rows = append(*rows, cleanupCandidateRow(candidate))
		}
		*rows = append(*rows, cleanupCandidateRow{Key: candidates[0].Key, Generation: candidates[0].Generation})
		return nil
	}
}

func selectChunks(rows ...chunk) func(context.Context, any, string, ...any) error {
	return func(_ context.Context, dest any, _ string, _ ...any) error {
		result := dest.(*[]chunkRow)
		for _, row := range rows {
			*result = append(*result, chunkRow(row))
		}
		return nil
	}
}

var _ insertBatch = (*fakeInsertBatch)(nil)

//go:build integration

package clickhouse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestClickHouseIntegrationObjectLifecycle(t *testing.T) {
	cfg := integrationConfig(t)
	dropIntegrationTables(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	bucket, err := NewBucketClient(ctx, cfg, "clickhouse-integration", log.NewNopLogger(), nil)
	require.NoError(t, err, "initialize integration bucket")
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Errorf("cleanup integration bucket: %v", err)
		}
	})

	payload := append(bytes.Repeat([]byte{0x00, 0x01, 0xfe, 0xff}, 1024), []byte("binary-tail")...)
	require.NoError(t, bucket.Upload(ctx, "dir/object.bin", bytes.NewReader(payload)), "upload binary object")
	requireObjectHash(t, ctx, bucket, "dir/object.bin", payload)

	ranged, err := bucket.GetRange(ctx, "dir/object.bin", 3, 29)
	require.NoError(t, err, "open binary object range")
	rangeData, readErr := io.ReadAll(ranged)
	closeErr := ranged.Close()
	require.NoError(t, readErr, "read binary object range")
	require.NoError(t, closeErr, "close binary object range")
	require.Equal(t, sha256.Sum256(payload[3:32]), sha256.Sum256(rangeData), "binary object range hash mismatch")

	attrs, err := bucket.Attributes(ctx, "dir/object.bin")
	require.NoError(t, err, "read binary object attributes")
	require.Equal(t, int64(len(payload)), attrs.Size)
	exists, err := bucket.Exists(ctx, "dir/object.bin")
	require.NoError(t, err, "check binary object existence")
	require.True(t, exists)

	require.NoError(t, bucket.Upload(ctx, "dir/nested/child", bytes.NewReader([]byte("child"))))
	require.NoError(t, bucket.Upload(ctx, "dir/sibling", bytes.NewReader([]byte("sibling"))))

	var nonRecursive []string
	require.NoError(t, bucket.Iter(ctx, "dir", func(name string) error {
		nonRecursive = append(nonRecursive, name)
		return nil
	}))
	require.Equal(t, []string{"dir/nested/", "dir/object.bin", "dir/sibling"}, nonRecursive)

	var recursive []string
	require.NoError(t, bucket.Iter(ctx, "dir", func(name string) error {
		recursive = append(recursive, name)
		return nil
	}, objstore.WithRecursiveIter()))
	require.Equal(t, []string{"dir/nested/child", "dir/object.bin", "dir/sibling"}, recursive)

	replacement := []byte{0xff, 0x00, 0x10, 0x00, 0x20}
	require.NoError(t, bucket.Upload(ctx, "dir/object.bin", bytes.NewReader(replacement)), "overwrite binary object")
	requireObjectHash(t, ctx, bucket, "dir/object.bin", replacement)
	require.NoError(t, bucket.Close(), "close auto-created integration bucket")

	cfg.AutoCreateTables = false
	reopened, err := NewBucketClient(ctx, cfg, "clickhouse-integration-reopen", log.NewNopLogger(), nil)
	require.NoError(t, err, "reopen integration bucket without auto-create")
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("cleanup reopened integration bucket: %v", err)
		}
	})
	requireObjectHash(t, ctx, reopened, "dir/object.bin", replacement)
	require.NoError(t, reopened.Delete(ctx, "dir/object.bin"), "delete overwritten object")
	exists, err = reopened.Exists(ctx, "dir/object.bin")
	require.NoError(t, err, "check deleted object existence")
	require.False(t, exists)
	reader, err := reopened.Get(ctx, "dir/object.bin")
	require.Error(t, err, "get deleted object")
	require.Nil(t, reader)
	require.True(t, reopened.IsObjNotFoundErr(err), "deleted object error must be not-found")
	require.NoError(t, reopened.Close(), "close reopened integration bucket")
}

func TestClickHouseIntegrationAcceptance(t *testing.T) {
	cfg := integrationConfig(t)
	dropIntegrationTables(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	bucket, err := NewBucketClient(ctx, cfg, "clickhouse-integration-acceptance", log.NewNopLogger(), nil)
	require.NoError(t, err, "initialize acceptance integration bucket")
	t.Cleanup(func() { require.NoError(t, bucket.Close(), "close acceptance integration bucket") })

	objstore.AcceptanceTest(t, bucket)
}

func TestClickHouseIntegrationAutoCreateDisabledRejectsMissingTables(t *testing.T) {
	cfg := integrationConfig(t)
	cfg.AutoCreateTables = false
	dropIntegrationTables(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bucket, err := NewBucketClient(ctx, cfg, "clickhouse-integration-missing", log.NewNopLogger(), nil)
	require.Error(t, err, "initialization with missing tables must fail")
	require.Nil(t, bucket)
}

func TestClickHouseIntegrationLatestAggregateDeterministicBeforeMerge(t *testing.T) {
	cfg := integrationConfig(t)
	dropIntegrationTables(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bucket, err := NewBucketClient(ctx, cfg, "clickhouse-integration-latest", log.NewNopLogger(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	store := bucket.store.(*clickhouseStore)

	low := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	high := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	for _, generation := range []uuid.UUID{high, low} {
		require.NoError(t, store.insertManifest(ctx, manifest{
			Key:            "aggregate/equal-version",
			Generation:     generation,
			State:          committed,
			Version:        42,
			LeaseExpiresAt: time.Now().Add(time.Hour).UTC(),
			EventAt:        time.Now().UTC(),
		}))
	}

	latest, err := store.LatestManifest(ctx, "aggregate/equal-version")
	require.NoError(t, err)
	require.Equal(t, high, latest.Generation)
	require.Equal(t, uint64(42), latest.Version)
}

func TestClickHouseIntegrationCleanupSQL(t *testing.T) {
	cfg := integrationConfig(t)
	dropIntegrationTables(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	bucket, err := NewBucketClient(ctx, cfg, "clickhouse-integration-cleanup", log.NewNopLogger(), nil)
	require.NoError(t, err, "initialize cleanup integration bucket")
	t.Cleanup(func() { require.NoError(t, bucket.Close(), "close cleanup integration bucket") })
	store, ok := bucket.store.(*clickhouseStore)
	require.True(t, ok, "integration bucket must use the native ClickHouse store")

	serverNow, err := store.serverTime(ctx)
	require.NoError(t, err, "read ClickHouse server time")
	eventAt := serverNow.Add(-2 * time.Hour)
	leaseExpiresAt := serverNow.Add(-90 * time.Minute)
	abandoned := cleanupCandidate{Key: "cleanup/abandoned", Generation: uuid.New()}
	superseded := cleanupCandidate{Key: "cleanup/live", Generation: uuid.New()}
	latest := cleanupCandidate{Key: "cleanup/live", Generation: uuid.New()}

	insertIntegrationGeneration(t, ctx, store, abandoned, 1, eventAt, leaseExpiresAt, []byte("abandoned"), false)
	insertIntegrationGeneration(t, ctx, store, superseded, 1, eventAt, leaseExpiresAt, []byte("superseded"), true)
	latestPayload := []byte{0x00, 0xff, 0x10, 0x00, 0x20}
	insertIntegrationGeneration(t, ctx, store, latest, 2, eventAt, leaseExpiresAt, latestPayload, true)

	var candidates []cleanupCandidate
	candidatesByPartition := make(map[uint32][]cleanupCandidate)
	for partition := uint32(0); partition < objectStorePartitionCount; partition++ {
		partitionCandidates, err := store.CleanupCandidates(ctx, partition, time.Millisecond, 10)
		require.NoError(t, err, "select cleanup candidates in partition %d", partition)
		if len(partitionCandidates) == 0 {
			continue
		}
		candidates = append(candidates, partitionCandidates...)
		candidatesByPartition[partition] = partitionCandidates
	}
	require.ElementsMatch(t, []cleanupCandidate{abandoned, superseded}, candidates)
	require.NotContains(t, candidates, latest)

	for partition, partitionCandidates := range candidatesByPartition {
		require.NoError(t, store.DeleteGenerations(ctx, partition, partitionCandidates), "delete cleanup candidates in partition %d", partition)
	}
	for _, candidate := range candidates {
		require.Zero(t, integrationGenerationRows(t, ctx, store, store.objectsTable, candidate), "candidate manifest rows remain")
		require.Zero(t, integrationGenerationRows(t, ctx, store, store.chunksTable, candidate), "candidate chunk rows remain")
	}
	require.Equal(t, uint64(2), integrationGenerationRows(t, ctx, store, store.objectsTable, latest), "latest manifest history must remain")
	require.Equal(t, uint64(1), integrationGenerationRows(t, ctx, store, store.chunksTable, latest), "latest chunk must remain")
	requireObjectHash(t, ctx, bucket, latest.Key, latestPayload)
}

func integrationConfig(t testing.TB) Config {
	t.Helper()
	address := os.Getenv("CLICKHOUSE_ADDR")
	if address == "" {
		t.Skip("CLICKHOUSE_ADDR is not set")
	}

	var cfg Config
	flagext.DefaultValues(&cfg)
	require.NoError(t, cfg.Addresses.Set(address), "parse CLICKHOUSE_ADDR")
	if database := os.Getenv("CLICKHOUSE_DATABASE"); database != "" {
		cfg.Database = database
	}
	if username := os.Getenv("CLICKHOUSE_USERNAME"); username != "" {
		cfg.Username = username
	}
	if password := os.Getenv("CLICKHOUSE_PASSWORD"); password != "" {
		cfg.Password = flagext.SecretWithValue(password)
	}
	suffix := uuid.NewString()
	suffix = "integration_" + suffix[:8]
	cfg.ObjectsTable = "pyroscope_objects_" + suffix
	cfg.ChunksTable = "pyroscope_chunks_" + suffix
	cfg.Cleanup.Enabled = false
	return cfg
}

func dropIntegrationTables(t testing.TB, cfg Config) {
	t.Helper()
	options, err := clickHouseOptions(cfg)
	require.NoError(t, err, "build ClickHouse integration connection options")
	conn, err := ch.Open(options)
	require.NoError(t, err, "open ClickHouse integration cleanup connection")

	objectsTable, err := qualifiedTable(cfg.Database, cfg.ObjectsTable)
	require.NoError(t, err)
	chunksTable, err := qualifiedTable(cfg.Database, cfg.ChunksTable)
	require.NoError(t, err)
	identifiers, err := deriveSchemaIdentifiers(cfg.ObjectsTable)
	require.NoError(t, err)
	latestTable, err := qualifiedTable(cfg.Database, identifiers.LatestTable)
	require.NoError(t, err)
	latestView, err := qualifiedTable(cfg.Database, identifiers.LatestView)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+latestView), "drop integration latest materialized view")
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+latestTable), "drop integration latest table")
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+chunksTable), "drop integration chunks table")
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+objectsTable), "drop integration objects table")
		require.NoError(t, conn.Close(), "close ClickHouse integration cleanup connection")
	})
}

func requireObjectHash(t *testing.T, ctx context.Context, bucket *Bucket, key string, expected []byte) {
	t.Helper()
	reader, err := bucket.Get(ctx, key)
	require.NoError(t, err, "open object")
	actual, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	require.NoError(t, readErr, "read object")
	require.NoError(t, closeErr, "close object reader")
	require.Equal(t, sha256.Sum256(expected), sha256.Sum256(actual), "object hash mismatch")
}

func insertIntegrationGeneration(
	t *testing.T,
	ctx context.Context,
	store *clickhouseStore,
	candidate cleanupCandidate,
	version uint64,
	eventAt time.Time,
	leaseExpiresAt time.Time,
	payload []byte,
	commit bool,
) {
	t.Helper()
	value := manifest{
		Key:            candidate.Key,
		Generation:     candidate.Generation,
		Size:           uint64(len(payload)),
		ChunkCount:     1,
		ChunkSize:      uint32(len(payload)),
		State:          pending,
		Version:        version,
		LeaseExpiresAt: leaseExpiresAt,
		EventAt:        eventAt,
	}
	require.NoError(t, store.insertManifest(ctx, value), "insert pending integration manifest")
	require.NoError(t, store.InsertChunks(ctx, []chunk{{
		Key:        candidate.Key,
		Generation: candidate.Generation,
		Index:      0,
		Data:       append([]byte(nil), payload...),
	}}), "insert integration chunk")
	if !commit {
		return
	}
	value.State = committed
	value.EventAt = eventAt.Add(time.Minute)
	require.NoError(t, store.insertManifest(ctx, value), "insert committed integration manifest")
}

func integrationGenerationRows(t *testing.T, ctx context.Context, store *clickhouseStore, table string, candidate cleanupCandidate) uint64 {
	t.Helper()
	var rows []struct {
		Count uint64 `ch:"count"`
	}
	err := store.conn.Select(ctx, &rows, fmt.Sprintf(
		"SELECT count() AS count FROM %s WHERE object_key = ? AND generation = ?",
		table,
	), candidate.Key, candidate.Generation)
	require.NoError(t, err, "count integration generation rows")
	require.Len(t, rows, 1)
	return rows[0].Count
}

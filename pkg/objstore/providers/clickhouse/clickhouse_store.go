package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

const (
	systemColumnsQuery = `SELECT name, type, default_kind, default_expression
FROM system.columns
WHERE database = ? AND table = ?
ORDER BY position`
	systemTablesQuery = `SELECT name, engine, sorting_key, partition_key, create_table_query
FROM system.tables
WHERE database = ? AND name IN (?, ?, ?, ?)
ORDER BY name`
)

var systemCertPool = x509.SystemCertPool

type clickHouseConnection interface {
	Ping(context.Context) error
	Exec(context.Context, string, ...any) error
	Select(context.Context, any, string, ...any) error
	PrepareInsert(context.Context, string) (insertBatch, error)
	Close() error
}

type insertBatch interface {
	Append(...any) error
	Send() error
	Close() error
}

type clickHouseOpener func(*ch.Options) (clickHouseConnection, error)

type nativeClickHouseConnection struct {
	conn chdriver.Conn
}

type nativeInsertBatch struct {
	batch chdriver.Batch
}

func (c *nativeClickHouseConnection) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *nativeClickHouseConnection) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c *nativeClickHouseConnection) Select(ctx context.Context, dest any, query string, args ...any) error {
	return c.conn.Select(ctx, dest, query, args...)
}

func (c *nativeClickHouseConnection) PrepareInsert(ctx context.Context, query string) (insertBatch, error) {
	batch, err := c.conn.PrepareBatch(ctx, query)
	if err != nil {
		return nil, err
	}
	return &nativeInsertBatch{batch: batch}, nil
}

func (c *nativeClickHouseConnection) Close() error {
	return c.conn.Close()
}

func (b *nativeInsertBatch) Append(values ...any) error {
	return b.batch.Append(values...)
}

func (b *nativeInsertBatch) Send() error {
	return b.batch.Send()
}

func (b *nativeInsertBatch) Close() error {
	return b.batch.Close()
}

type clickhouseStore struct {
	cfg          Config
	objectsTable string
	chunksTable  string
	latestTable  string
	conn         clickHouseConnection
	closeOnce    sync.Once
	closeErr     error
}

var _ store = (*clickhouseStore)(nil)

const manifestColumns = "object_key, generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at"

const latestManifestProjection = `object_key,
       tupleElement(latest_manifest, 1) AS generation,
       tupleElement(latest_manifest, 2) AS object_size,
       tupleElement(latest_manifest, 3) AS chunk_count,
       tupleElement(latest_manifest, 4) AS chunk_size,
       toUInt8(tupleElement(latest_manifest, 5)) AS state,
       tupleElement(latest_manifest, 6) AS version,
       tupleElement(latest_manifest, 7) AS lease_expires_at,
       tupleElement(latest_manifest, 8) AS event_at`

type manifestRow struct {
	Key            string    `ch:"object_key"`
	Generation     uuid.UUID `ch:"generation"`
	Size           uint64    `ch:"object_size"`
	ChunkCount     uint32    `ch:"chunk_count"`
	ChunkSize      uint32    `ch:"chunk_size"`
	State          uint8     `ch:"state"`
	Version        uint64    `ch:"version"`
	LeaseExpiresAt time.Time `ch:"lease_expires_at"`
	EventAt        time.Time `ch:"event_at"`
}

type chunkRow struct {
	Key        string    `ch:"object_key"`
	Generation uuid.UUID `ch:"generation"`
	Index      uint32    `ch:"chunk_index"`
	Data       []byte    `ch:"data"`
	CreatedAt  time.Time `ch:"created_at"`
}

type cleanupCandidateRow struct {
	Key        string    `ch:"object_key"`
	Generation uuid.UUID `ch:"generation"`
}

type serverTimeRow struct {
	Now time.Time `ch:"now"`
}

type systemColumn struct {
	Name              string `ch:"name"`
	Type              string `ch:"type"`
	DefaultKind       string `ch:"default_kind"`
	DefaultExpression string `ch:"default_expression"`
}

func newClickhouseStore(cfg Config) (*clickhouseStore, error) {
	return newClickHouseStoreWithOpener(cfg, openNativeClickHouse)
}

func newClickHouseStoreWithOpener(cfg Config, open clickHouseOpener) (*clickhouseStore, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate ClickHouse configuration: %w", err)
	}
	options, err := clickHouseOptions(cfg)
	if err != nil {
		return nil, err
	}
	objectsTable, err := qualifiedTable(cfg.Database, cfg.ObjectsTable)
	if err != nil {
		return nil, err
	}
	chunksTable, err := qualifiedTable(cfg.Database, cfg.ChunksTable)
	if err != nil {
		return nil, err
	}
	identifiers, err := deriveSchemaIdentifiers(cfg.ObjectsTable)
	if err != nil {
		return nil, err
	}
	latestTable, err := qualifiedTable(cfg.Database, identifiers.LatestTable)
	if err != nil {
		return nil, err
	}
	conn, err := open(options)
	if err != nil {
		return nil, clickHouseConnectionError("open ClickHouse connection", err, cfg.Password.String())
	}
	return &clickhouseStore{
		cfg:          cfg,
		objectsTable: objectsTable,
		chunksTable:  chunksTable,
		latestTable:  latestTable,
		conn:         conn,
	}, nil
}

func openNativeClickHouse(options *ch.Options) (clickHouseConnection, error) {
	conn, err := ch.Open(options)
	if err != nil {
		return nil, err
	}
	return &nativeClickHouseConnection{conn: conn}, nil
}

func clickHouseOptions(cfg Config) (*ch.Options, error) {
	compression, err := clickHouseCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := clickHouseTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &ch.Options{
		Protocol: ch.Native,
		Addr:     append([]string(nil), cfg.Addresses...),
		Auth: ch.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password.String(),
		},
		TLS:             tlsConfig,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.QueryTimeout,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		Compression:     &ch.Compression{Method: compression},
	}, nil
}

func clickHouseCompression(name string) (ch.CompressionMethod, error) {
	switch name {
	case "none":
		return ch.CompressionNone, nil
	case "lz4":
		return ch.CompressionLZ4, nil
	case "lz4hc":
		return ch.CompressionLZ4HC, nil
	case "zstd":
		return ch.CompressionZSTD, nil
	default:
		return 0, fmt.Errorf("unsupported ClickHouse compression %q", name)
	}
}

func clickHouseTLSConfig(cfg Config) (*tls.Config, error) {
	if !cfg.Secure {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.TLSServerName,
		InsecureSkipVerify: cfg.SkipVerify,
	}
	if cfg.TLSCAPath == "" {
		return tlsConfig, nil
	}
	caPath := filepath.Clean(cfg.TLSCAPath)
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read ClickHouse TLS CA %q: %w", caPath, err)
	}
	rootCAs, err := systemCertPool()
	if err != nil || rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ClickHouse TLS CA %q does not contain a valid PEM certificate", caPath)
	}
	tlsConfig.RootCAs = rootCAs
	return tlsConfig, nil
}

func (s *clickhouseStore) Init(ctx context.Context) error {
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Ping(queryCtx)
	cancel()
	if err != nil {
		return clickHouseConnectionError("ping ClickHouse", err, s.cfg.Password.String())
	}

	identifiers, err := deriveSchemaIdentifiers(s.cfg.ObjectsTable)
	if err != nil {
		return err
	}

	if s.cfg.AutoCreateTables {
		objectsStatement, err := objectsDDL(s.cfg.Database, s.cfg.ObjectsTable)
		if err != nil {
			return err
		}
		chunksStatement, err := chunksDDL(s.cfg.Database, s.cfg.ChunksTable)
		if err != nil {
			return err
		}
		latestStatement, err := latestAggregateDDL(s.cfg.Database, identifiers.LatestTable)
		if err != nil {
			return err
		}
		viewStatement, err := latestMaterializedViewDDL(s.cfg.Database, s.cfg.ObjectsTable, identifiers.LatestTable, identifiers.LatestView)
		if err != nil {
			return err
		}
		for _, statement := range []string{objectsStatement, chunksStatement, latestStatement, viewStatement} {
			queryCtx, cancel = s.queryContext(ctx)
			err = s.conn.Exec(queryCtx, statement)
			cancel()
			if err != nil {
				return clickHouseConnectionError("create ClickHouse table", err, s.cfg.Password.String())
			}
		}
	}

	objectsColumns, err := s.tableColumns(ctx, s.cfg.ObjectsTable)
	if err != nil {
		return err
	}
	chunksColumns, err := s.tableColumns(ctx, s.cfg.ChunksTable)
	if err != nil {
		return err
	}
	latestColumns, err := s.tableColumns(ctx, identifiers.LatestTable)
	if err != nil {
		return err
	}
	tables, err := s.tableDefinitions(ctx, s.cfg.ObjectsTable, s.cfg.ChunksTable, identifiers.LatestTable, identifiers.LatestView)
	if err != nil {
		return err
	}
	if err := validateSchema(s.cfg.Database+"."+s.cfg.ObjectsTable, objectsColumns, objectsSchema); err != nil {
		return incompatibleV2SchemaError(err)
	}
	if err := validateSchema(s.cfg.Database+"."+s.cfg.ChunksTable, chunksColumns, chunksSchema); err != nil {
		return incompatibleV2SchemaError(err)
	}
	if err := validateSchema(s.cfg.Database+"."+identifiers.LatestTable, latestColumns, latestSchema); err != nil {
		return incompatibleV2SchemaError(err)
	}
	if err := validateTableDefinition(s.cfg.Database+"."+s.cfg.ObjectsTable, tables, s.cfg.ObjectsTable, expectedTable{
		Engine: "MergeTree", SortingKey: objectsSortingKey, PartitionKey: objectStorePartitionKey,
	}); err != nil {
		return incompatibleV2SchemaError(err)
	}
	if err := validateTableDefinition(s.cfg.Database+"."+s.cfg.ChunksTable, tables, s.cfg.ChunksTable, expectedTable{
		Engine: "MergeTree", SortingKey: chunksSortingKey, PartitionKey: objectStorePartitionKey,
	}); err != nil {
		return incompatibleV2SchemaError(err)
	}
	if err := validateTableDefinition(s.cfg.Database+"."+identifiers.LatestTable, tables, identifiers.LatestTable, expectedTable{
		Engine: "AggregatingMergeTree", SortingKey: latestSortingKey, PartitionKey: objectStorePartitionKey,
	}); err != nil {
		return incompatibleV2SchemaError(err)
	}
	view, ok := tables[identifiers.LatestView]
	if !ok {
		return incompatibleV2SchemaError(fmt.Errorf("ClickHouse materialized view %s is missing", s.cfg.Database+"."+identifiers.LatestView))
	}
	if err := validateLatestMaterializedView(s.cfg.Database+"."+identifiers.LatestView, view, s.cfg.Database, s.cfg.ObjectsTable, identifiers.LatestTable, identifiers.LatestView); err != nil {
		return incompatibleV2SchemaError(err)
	}
	return nil
}

func (s *clickhouseStore) BeginUpload(ctx context.Context, upload manifest) (manifest, error) {
	now, err := s.serverTime(ctx)
	if err != nil {
		return manifest{}, err
	}
	upload.State = pending
	upload.EventAt = now
	upload.LeaseExpiresAt = now.Add(s.cfg.MaxUploadDuration)
	if err := s.insertManifest(ctx, upload); err != nil {
		return manifest{}, err
	}
	return upload, nil
}

func (s *clickhouseStore) InsertChunks(ctx context.Context, chunks []chunk) error {
	if len(chunks) == 0 {
		return contextError(ctx)
	}
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	batch, err := s.conn.PrepareInsert(queryCtx, fmt.Sprintf(
		"INSERT INTO %s (object_key, generation, chunk_index, data)",
		s.chunksTable,
	))
	if err != nil {
		return s.operationError("prepare ClickHouse chunk insert", err)
	}
	var operationErr error
	for _, value := range chunks {
		if err := queryCtx.Err(); err != nil {
			operationErr = s.operationError("append ClickHouse chunk", err)
			break
		}
		if err := batch.Append(value.Key, value.Generation, value.Index, value.Data); err != nil {
			operationErr = chunkPayloadError("append ClickHouse chunk", err)
			break
		}
	}
	if operationErr == nil {
		if err := queryCtx.Err(); err != nil {
			operationErr = s.operationError("send ClickHouse chunks", err)
		} else if err := batch.Send(); err != nil {
			operationErr = chunkPayloadError("send ClickHouse chunks", err)
		}
	}
	if err := batch.Close(); operationErr == nil && err != nil {
		operationErr = chunkPayloadError("close ClickHouse chunk insert", err)
	}
	return operationErr
}

func (s *clickhouseStore) CommitUpload(ctx context.Context, upload manifest) error {
	query := commitUploadQuery(s.objectsTable, s.chunksTable)
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Exec(
		queryCtx,
		query,
		upload.Key,
		upload.Generation,
		upload.Version,
		uint8(committed),
		uint8(pending),
		upload.Size,
		upload.ChunkCount,
		upload.ChunkSize,
		committed.String(),
	)
	cancel()
	if err != nil {
		return s.operationError("validate and insert ClickHouse committed manifest", err)
	}
	return nil
}

func commitUploadQuery(objectsTable, chunksTable string) string {
	return fmt.Sprintf(`INSERT INTO %s (%s)
WITH
    ? AS target_key,
    ? AS target_generation,
    ? AS target_version,
    ? AS committed_state,
    ? AS pending_state,
    ? AS caller_object_size,
    ? AS caller_chunk_count,
    ? AS caller_chunk_size,
    ? AS committed_state_name,
    now64(3) AS commit_at,
    (
        SELECT count()
        FROM %s
        WHERE object_key = target_key
          AND generation = target_generation
          AND version = target_version
          AND toUInt8(state) = committed_state
    ) AS committed_count,
    (
        SELECT count()
        FROM %s
        WHERE object_key = target_key
          AND generation = target_generation
          AND version = target_version
          AND toUInt8(state) = pending_state
    ) AS pending_count,
    (
        SELECT argMax(tuple(chunk_size, lease_expires_at), event_at)
        FROM %s
        WHERE object_key = target_key
          AND generation = target_generation
          AND version = target_version
          AND toUInt8(state) = pending_state
    ) AS pending_metadata,
    tupleElement(pending_metadata, 1) AS pending_chunk_size,
    tupleElement(pending_metadata, 2) AS pending_lease,
    chunk_metadata AS (
        SELECT count() AS chunk_row_count,
               countDistinct(chunk_index) AS chunk_distinct_count,
               sum(length(data)) AS chunk_object_size,
               min(chunk_index) AS chunk_min_index,
               max(chunk_index) AS chunk_max_index
        FROM %s
        WHERE object_key = target_key
          AND generation = target_generation
    )
SELECT target_key, target_generation, chunk_object_size, chunk_row_count,
       pending_chunk_size, committed_state_name, target_version, pending_lease, commit_at
FROM chunk_metadata
WHERE committed_count = 0
  AND throwIf(
      committed_count = 0 AND pending_count = 0,
      'ClickHouse commit missing pending manifest'
  ) = 0
  AND throwIf(
      committed_count = 0 AND pending_count > 0 AND commit_at > pending_lease,
      'ClickHouse upload lease expired before commit'
  ) = 0
  AND throwIf(
      committed_count = 0 AND pending_count > 0 AND (
          caller_object_size != chunk_object_size
          OR caller_chunk_count != chunk_row_count
          OR caller_chunk_size != pending_chunk_size
      ),
      'ClickHouse upload metadata mismatch'
  ) = 0
  AND throwIf(
      committed_count = 0 AND pending_count > 0
          AND chunk_row_count != chunk_distinct_count,
      'ClickHouse upload contains duplicate chunk indexes'
  ) = 0
  AND throwIf(
      committed_count = 0 AND pending_count > 0 AND chunk_row_count > 0 AND (
          chunk_min_index != 0
          OR chunk_max_index != chunk_row_count - 1
      ),
      'ClickHouse upload chunk indexes are not contiguous from zero'
  ) = 0`, objectsTable, manifestColumns, objectsTable, objectsTable, objectsTable, chunksTable)
}

func (s *clickhouseStore) InsertDelete(ctx context.Context, tombstone manifest) (manifest, error) {
	now, err := s.serverTime(ctx)
	if err != nil {
		return manifest{}, err
	}
	tombstone.State = deleted
	tombstone.EventAt = now
	tombstone.LeaseExpiresAt = now
	if err := s.insertManifest(ctx, tombstone); err != nil {
		return manifest{}, err
	}
	return tombstone, nil
}

func (s *clickhouseStore) LatestManifest(ctx context.Context, key string) (manifest, error) {
	query := fmt.Sprintf(`WITH merged AS (
    SELECT object_key, argMaxMerge(manifest) AS latest_manifest
    FROM %s
    WHERE object_key = ?
    GROUP BY object_key
)
SELECT %s
FROM merged`, s.latestTable, latestManifestProjection)
	rows, err := s.selectManifests(ctx, "select latest ClickHouse manifest", query, key)
	if err != nil {
		return manifest{}, err
	}
	if len(rows) == 0 {
		return manifest{}, ErrObjectNotFound
	}
	return rows[0], nil
}

func (s *clickhouseStore) Chunks(ctx context.Context, key string, generation uuid.UUID, first, last uint32) ([]chunk, error) {
	query := fmt.Sprintf(`SELECT object_key, generation, chunk_index, data, created_at
FROM %s
WHERE object_key = ? AND generation = ? AND chunk_index >= ? AND chunk_index <= ?
ORDER BY chunk_index`, s.chunksTable)
	var rows []chunkRow
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Select(queryCtx, &rows, query, key, generation, first, last)
	cancel()
	if err != nil {
		return nil, s.operationError("select ClickHouse chunks", err)
	}
	result := make([]chunk, 0, len(rows))
	for _, row := range rows {
		result = append(result, chunk(row))
	}
	return result, nil
}

func (s *clickhouseStore) ListLatest(ctx context.Context, prefix, afterKey string, limit int) ([]manifest, error) {
	query := fmt.Sprintf(`WITH merged AS (
    SELECT object_key, argMaxMerge(manifest) AS latest_manifest
    FROM %s
    WHERE startsWith(object_key, ?) AND object_key > ?
    GROUP BY object_key
)
SELECT %s
FROM merged
WHERE toUInt8(tupleElement(latest_manifest, 5)) != ?
ORDER BY object_key
LIMIT ?`, s.latestTable, latestManifestProjection)
	return s.selectManifests(ctx, "list latest ClickHouse manifests", query,
		prefix, afterKey, uint8(deleted), limit,
	)
}

func (s *clickhouseStore) CleanupCandidates(ctx context.Context, partition uint32, grace time.Duration, limit int) ([]cleanupCandidate, error) {
	query := fmt.Sprintf(`WITH
	now64(3) AS cleanup_now,
	toIntervalMillisecond(?) AS cleanup_grace,
	partition_operations AS (
	SELECT object_key, generation, state, version, lease_expires_at, event_at
	FROM %s
	PREWHERE cityHash64(object_key) %% 64 = ?
), pending_versions AS (
	SELECT object_key, generation, version, max(lease_expires_at) AS lease_expires_at
	FROM partition_operations
	WHERE toUInt8(state) = ?
	GROUP BY object_key, generation, version
), valid_operations AS (
	SELECT committed.object_key, committed.generation,
	       greatest(pending.lease_expires_at, committed.event_at) AS operation_safety_at
	FROM partition_operations AS committed
	INNER JOIN pending_versions AS pending
	  ON committed.object_key = pending.object_key
	 AND committed.generation = pending.generation
	 AND committed.version = pending.version
	WHERE toUInt8(committed.state) = ?
	  AND committed.event_at <= pending.lease_expires_at
	UNION ALL
	SELECT object_key, generation, greatest(lease_expires_at, event_at) AS operation_safety_at
	FROM partition_operations
	WHERE toUInt8(state) = ?
), valid_generations AS (
	SELECT object_key, generation, max(operation_safety_at) AS operation_safety_at
	FROM valid_operations
	GROUP BY object_key, generation
), pending_generations AS (
	SELECT object_key, generation, max(lease_expires_at) AS lease_expires_at
	FROM pending_versions
	GROUP BY object_key, generation
), latest_generations AS (
	SELECT object_key, tupleElement(argMaxMerge(manifest), 1) AS generation
	FROM %s
	PREWHERE cityHash64(object_key) %% 64 = ?
	GROUP BY object_key
), abandoned_pending AS (
	SELECT source.object_key, source.generation
	FROM pending_generations AS source
	LEFT ANTI JOIN valid_generations AS valid
	  ON valid.object_key = source.object_key
	 AND valid.generation = source.generation
	WHERE source.lease_expires_at + cleanup_grace < cleanup_now
), superseded_generations AS (
	SELECT source.object_key, source.generation
	FROM valid_generations AS source
	LEFT ANTI JOIN latest_generations AS latest
	  ON latest.object_key = source.object_key
	 AND latest.generation = source.generation
	WHERE source.operation_safety_at + cleanup_grace < cleanup_now
), candidates AS (
	SELECT object_key, generation FROM abandoned_pending
	UNION DISTINCT
	SELECT object_key, generation FROM superseded_generations
)
SELECT DISTINCT object_key, generation
FROM candidates
ORDER BY object_key, generation
LIMIT ?`, s.objectsTable, s.latestTable)
	var rows []cleanupCandidateRow
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Select(queryCtx, &rows, query,
		cleanupGraceMilliseconds(grace),
		uint64(partition), uint8(pending), uint8(committed), uint8(deleted),
		uint64(partition), limit,
	)
	cancel()
	if err != nil {
		return nil, s.operationError("select ClickHouse cleanup candidates", err)
	}
	candidates := make([]cleanupCandidate, 0, len(rows))
	seen := make(map[cleanupCandidate]struct{}, len(rows))
	for _, row := range rows {
		candidate := cleanupCandidate(row)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func cleanupGraceMilliseconds(grace time.Duration) int64 {
	milliseconds := int64(grace / time.Millisecond)
	if grace%time.Millisecond != 0 {
		milliseconds++
	}
	return milliseconds
}

func (s *clickhouseStore) DeleteGenerations(ctx context.Context, partition uint32, candidates []cleanupCandidate) error {
	if len(candidates) == 0 {
		return nil
	}
	placeholders := make([]string, len(candidates))
	args := make([]any, 0, 1+len(candidates)*2)
	args = append(args, uint64(partition))
	for i, candidate := range candidates {
		placeholders[i] = "(?, ?)"
		args = append(args, candidate.Key, candidate.Generation)
	}
	for _, table := range []string{s.chunksTable, s.objectsTable} {
		queryCtx, cancel := s.queryContext(ctx)
		err := s.conn.Exec(queryCtx, fmt.Sprintf(
			"ALTER TABLE %s DELETE IN PARTITION ? WHERE (object_key, generation) IN (%s) SETTINGS mutations_sync = 2",
			table, strings.Join(placeholders, ", "),
		), args...)
		cancel()
		if err != nil {
			return s.operationError("delete ClickHouse object generation", err)
		}
	}
	return nil
}

func (s *clickhouseStore) serverTime(ctx context.Context) (time.Time, error) {
	var rows []serverTimeRow
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Select(queryCtx, &rows, "SELECT now64(3) AS now")
	cancel()
	if err != nil {
		return time.Time{}, s.operationError("select ClickHouse server time", err)
	}
	if len(rows) != 1 {
		return time.Time{}, fmt.Errorf("select ClickHouse server time: expected exactly one row, got %d", len(rows))
	}
	return rows[0].Now, nil
}

func (s *clickhouseStore) insertManifest(ctx context.Context, value manifest) error {
	queryCtx, cancel := s.queryContext(ctx)
	defer cancel()
	batch, err := s.conn.PrepareInsert(queryCtx, fmt.Sprintf("INSERT INTO %s (%s)", s.objectsTable, manifestColumns))
	if err != nil {
		return s.operationError("prepare ClickHouse manifest insert", err)
	}
	operationErr := contextError(queryCtx)
	if operationErr == nil {
		operationErr = batch.Append(
			value.Key, value.Generation, value.Size, value.ChunkCount, value.ChunkSize,
			value.State.String(), value.Version, value.LeaseExpiresAt, value.EventAt,
		)
		if operationErr != nil {
			operationErr = s.operationError("append ClickHouse manifest", operationErr)
		}
	}
	if operationErr == nil {
		if err := queryCtx.Err(); err != nil {
			operationErr = s.operationError("send ClickHouse manifest", err)
		} else if err := batch.Send(); err != nil {
			operationErr = s.operationError("send ClickHouse manifest", err)
		}
	}
	if err := batch.Close(); operationErr == nil && err != nil {
		operationErr = s.operationError("close ClickHouse manifest insert", err)
	}
	return operationErr
}

func (s *clickhouseStore) selectManifests(ctx context.Context, action, query string, args ...any) ([]manifest, error) {
	var rows []manifestRow
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Select(queryCtx, &rows, query, args...)
	cancel()
	if err != nil {
		return nil, s.operationError(action, err)
	}
	result := make([]manifest, 0, len(rows))
	for _, row := range rows {
		result = append(result, manifest{
			Key: row.Key, Generation: row.Generation, Size: row.Size,
			ChunkCount: row.ChunkCount, ChunkSize: row.ChunkSize,
			State: manifestState(row.State), Version: row.Version,
			LeaseExpiresAt: row.LeaseExpiresAt, EventAt: row.EventAt,
		})
	}
	return result, nil
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return clickHouseConnectionError("ClickHouse operation canceled", err, "")
	}
	return nil
}

func (s *clickhouseStore) operationError(action string, err error) error {
	return clickHouseConnectionError(action, err, s.cfg.Password.String())
}

func chunkPayloadError(action string, err error) error {
	var cause error
	switch {
	case errors.Is(err, context.Canceled):
		cause = context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		cause = context.DeadlineExceeded
	}
	return &safeClickHouseError{
		message: action + ": payload operation failed",
		cause:   cause,
	}
}

func (s *clickhouseStore) tableColumns(ctx context.Context, table string) ([]columnInfo, error) {
	var rows []systemColumn
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Select(queryCtx, &rows, systemColumnsQuery, s.cfg.Database, table)
	cancel()
	if err != nil {
		return nil, clickHouseConnectionError(
			fmt.Sprintf("select ClickHouse schema for %s.%s", s.cfg.Database, table),
			err,
			s.cfg.Password.String(),
		)
	}
	columns := make([]columnInfo, 0, len(rows))
	for _, row := range rows {
		columns = append(columns, columnInfo(row))
	}
	return columns, nil
}

func (s *clickhouseStore) tableDefinitions(ctx context.Context, objectsTable, chunksTable, latestTable, latestView string) (map[string]tableInfo, error) {
	var rows []tableInfo
	queryCtx, cancel := s.queryContext(ctx)
	err := s.conn.Select(queryCtx, &rows, systemTablesQuery, s.cfg.Database, objectsTable, chunksTable, latestTable, latestView)
	cancel()
	if err != nil {
		return nil, clickHouseConnectionError("select ClickHouse table definitions", err, s.cfg.Password.String())
	}
	tables := make(map[string]tableInfo, len(rows))
	for _, row := range rows {
		tables[row.Name] = row
	}
	return tables, nil
}

func validateTableDefinition(table string, tables map[string]tableInfo, name string, expected expectedTable) error {
	actual, ok := tables[name]
	if !ok {
		return fmt.Errorf("ClickHouse table %s is missing", table)
	}
	return validateTable(table, actual, expected)
}

func incompatibleV2SchemaError(err error) error {
	return fmt.Errorf("legacy ClickHouse object store V1 schema is incompatible with V2: %w; recreate all ClickHouse object-store tables", err)
}

func (s *clickhouseStore) queryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.cfg.QueryTimeout)
}

func (s *clickhouseStore) Close() error {
	s.closeOnce.Do(func() {
		if err := s.conn.Close(); err != nil {
			s.closeErr = clickHouseConnectionError("close ClickHouse connection", err, s.cfg.Password.String())
		}
	})
	return s.closeErr
}

func clickHouseConnectionError(action string, err error, password string) error {
	detail := err.Error()
	if password != "" {
		detail = strings.ReplaceAll(detail, password, "[REDACTED]")
	}
	var cause error
	switch {
	case errors.Is(err, context.Canceled):
		cause = context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		cause = context.DeadlineExceeded
	}
	return &safeClickHouseError{
		message: fmt.Sprintf("%s: %s", action, detail),
		cause:   cause,
	}
}

type safeClickHouseError struct {
	message string
	cause   error
}

func (e *safeClickHouseError) Error() string {
	return e.message
}

func (e *safeClickHouseError) Unwrap() error {
	return e.cause
}

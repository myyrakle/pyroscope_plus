package clickhouse

import (
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestManifestStateString(t *testing.T) {
	require.Equal(t, "pending", pending.String())
	require.Equal(t, "committed", committed.String())
	require.Equal(t, "deleted", deleted.String())
	require.Equal(t, "manifestState(0)", manifestState(0).String())
}

func TestVersionClockStartsAtWallTimeAndAlwaysIncreases(t *testing.T) {
	times := []time.Time{
		time.Unix(0, 0),
		time.Unix(0, 0),
		time.Unix(0, 100),
		time.Unix(0, 99),
	}
	index := 0
	clock := newVersionClock(func() time.Time {
		value := times[index]
		index++
		return value
	})

	value, err := clock.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(0), value)
	value, err = clock.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), value)
	value, err = clock.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(100), value)
	value, err = clock.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(101), value)
}

func TestVersionClockRejectsPreEpochTime(t *testing.T) {
	clock := newVersionClock(func() time.Time { return time.Unix(0, -1) })

	_, err := clock.Next()
	require.ErrorContains(t, err, "before Unix epoch")
}

func TestVersionClockRejectsOverflow(t *testing.T) {
	clock := newVersionClock(func() time.Time { return time.Unix(0, 100) })
	clock.last.Store(math.MaxUint64)

	_, err := clock.Next()
	require.ErrorContains(t, err, "exhausted")
}

func TestVersionClockConcurrentCallsAreUniqueAndIncreasing(t *testing.T) {
	const calls = 1000

	clock := newVersionClock(func() time.Time { return time.Unix(0, 100) })
	values := make([]uint64, calls)
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range values {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			values[i], errs[i] = clock.Next()
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	for i, value := range values {
		require.Equal(t, uint64(100+i), value)
	}
}

func TestObjectsDDL(t *testing.T) {
	ddl, err := objectsDDL("profile_db", "objects")
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(`
CREATE TABLE IF NOT EXISTS `+"`profile_db`.`objects`"+`
(
    object_key String,
    generation UUID,
    object_size UInt64,
    chunk_count UInt32,
    chunk_size UInt32,
    state Enum8('pending' = 1, 'committed' = 2, 'deleted' = 3),
    version UInt64,
    lease_expires_at DateTime64(3, 'UTC'),
    event_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (object_key, version, generation, state)
SETTINGS old_parts_lifetime = 60`), ddl)
}

func TestChunksDDL(t *testing.T) {
	ddl, err := chunksDDL("profile_db", "chunks")
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(`
CREATE TABLE IF NOT EXISTS `+"`profile_db`.`chunks`"+`
(
    object_key String CODEC(ZSTD(1)),
    generation UUID,
    chunk_index UInt32 CODEC(ZSTD(1)),
    data String CODEC(ZSTD(1)),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (object_key, generation, chunk_index)
SETTINGS old_parts_lifetime = 60`), ddl)
}

func TestLatestAggregateDDL(t *testing.T) {
	ddl, err := latestAggregateDDL("profile_db", "objects_latest")
	require.NoError(t, err)
	require.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS `profile_db`.`objects_latest`")
	require.Contains(t, ddl, "manifest AggregateFunction(argMax, Tuple(")
	require.Contains(t, ddl, "Enum8('pending' = 1, 'committed' = 2, 'deleted' = 3)")
	require.Contains(t, ddl, "Tuple(UInt64, UUID))")
	require.Contains(t, ddl, "ENGINE = AggregatingMergeTree")
	require.NotContains(t, ddl, "PARTITION BY")
	require.Contains(t, ddl, "ORDER BY object_key")
}

func TestLatestMaterializedViewDDL(t *testing.T) {
	ddl, err := latestMaterializedViewDDL("profile_db", "objects", "objects_latest", "objects_latest_mv")
	require.NoError(t, err)
	require.Contains(t, ddl, "CREATE MATERIALIZED VIEW IF NOT EXISTS `profile_db`.`objects_latest_mv`")
	require.Contains(t, ddl, "TO `profile_db`.`objects_latest`")
	require.Contains(t, ddl, "FROM `profile_db`.`objects`")
	require.Contains(t, ddl, "WHERE state IN ('committed', 'deleted')")
	require.Contains(t, ddl, "argMaxState(")
	require.Contains(t, ddl, "tuple(version, generation)")
	require.Contains(t, ddl, ") AS manifest")
	require.Contains(t, ddl, "GROUP BY object_key")
}

func TestValidateLatestMaterializedViewAcceptsClickHouseCanonicalFormatting(t *testing.T) {
	definition, err := latestMaterializedViewDDL("profile_db", "objects", "objects_latest", "objects_latest_mv")
	require.NoError(t, err)
	definition = strings.ReplaceAll(definition, " IF NOT EXISTS", "")
	definition = strings.ReplaceAll(definition, "`", "")
	definition = strings.ReplaceAll(definition, "\n", "\n\t")

	err = validateLatestMaterializedView("profile_db.objects_latest_mv", tableInfo{
		Engine:           "MaterializedView",
		CreateTableQuery: definition,
	}, "profile_db", "objects", "objects_latest", "objects_latest_mv")
	require.NoError(t, err)
}

func TestValidateLatestMaterializedViewAcceptsClickHouse253CanonicalDefinition(t *testing.T) {
	definition := "CREATE MATERIALIZED VIEW default.objects_latest_mv TO default.objects_latest " +
		"(`object_key` String, `manifest` AggregateFunction(argMax, Tuple(UUID, UInt64, UInt32, UInt32, " +
		"Enum8('pending' = 1, 'committed' = 2, 'deleted' = 3), UInt64, DateTime64(3, 'UTC'), " +
		"DateTime64(3, 'UTC')), Tuple(UInt64, UUID))) AS SELECT object_key, " +
		"argMaxState((generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at), " +
		"(version, generation)) AS manifest FROM default.objects " +
		"WHERE state IN ('committed', 'deleted') GROUP BY object_key"

	err := validateLatestMaterializedView("default.objects_latest_mv", tableInfo{
		Engine:           "MaterializedView",
		CreateTableQuery: definition,
	}, "default", "objects", "objects_latest", "objects_latest_mv")
	require.NoError(t, err)
}

func TestValidateLatestMaterializedViewAcceptsClickHouse26TupleCanonicalDefinition(t *testing.T) {
	definition := "CREATE MATERIALIZED VIEW default.objects_latest_mv TO default.objects_latest " +
		"(`object_key` String, `manifest` AggregateFunction(argMax, Tuple(UUID, UInt64, UInt32, UInt32, " +
		"Enum8('pending' = 1, 'committed' = 2, 'deleted' = 3), UInt64, DateTime64(3, 'UTC'), " +
		"DateTime64(3, 'UTC')), Tuple(UInt64, UUID))) AS SELECT object_key, " +
		"argMaxState(tuple(generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at), " +
		"tuple(version, generation)) AS manifest FROM default.objects " +
		"WHERE state IN ('committed', 'deleted') GROUP BY object_key"

	err := validateLatestMaterializedView("default.objects_latest_mv", tableInfo{
		Engine:           "MaterializedView",
		CreateTableQuery: definition,
	}, "default", "objects", "objects_latest", "objects_latest_mv")
	require.NoError(t, err)
}

func TestValidateLatestMaterializedViewRejectsSemanticChanges(t *testing.T) {
	definition, err := latestMaterializedViewDDL("profile_db", "objects", "objects_latest", "objects_latest_mv")
	require.NoError(t, err)

	tests := []struct {
		name       string
		definition string
	}{
		{
			name:       "additional pending state",
			definition: strings.Replace(definition, "WHERE state IN ('committed', 'deleted')", "WHERE state IN ('committed', 'deleted') OR state = 'pending'", 1),
		},
		{
			name:       "additional filter",
			definition: strings.Replace(definition, "WHERE state IN ('committed', 'deleted')", "WHERE state IN ('committed', 'deleted') AND object_key != ''", 1),
		},
		{
			name:       "additional projection",
			definition: strings.Replace(definition, "    object_key,", "    object_key, 1 AS extra,", 1),
		},
		{
			name:       "additional group key",
			definition: strings.Replace(definition, "GROUP BY object_key", "GROUP BY object_key, generation", 1),
		},
		{
			name:       "different target",
			definition: strings.Replace(definition, "TO `profile_db`.`objects_latest`", "TO `profile_db`.`other_latest`", 1),
		},
		{
			name:       "different source",
			definition: strings.Replace(definition, "FROM `profile_db`.`objects`", "FROM `profile_db`.`other_objects`", 1),
		},
		{
			name:       "different argmax ordering",
			definition: strings.Replace(definition, "tuple(version, generation)", "tuple(generation, version)", 1),
		},
		{
			name: "different canonical target columns",
			definition: strings.Replace(
				definition,
				"TO `profile_db`.`objects_latest`",
				"TO `profile_db`.`objects_latest` (`object_key` String, `extra` UInt8)",
				1,
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLatestMaterializedView("profile_db.objects_latest_mv", tableInfo{
				Engine:           "MaterializedView",
				CreateTableQuery: tt.definition,
			}, "profile_db", "objects", "objects_latest", "objects_latest_mv")
			require.ErrorContains(t, err, "incompatible materialized view definition")
		})
	}
}

func TestDerivedSchemaIdentifiers(t *testing.T) {
	names, err := deriveSchemaIdentifiers("objects")
	require.NoError(t, err)
	require.Equal(t, "objects_latest", names.LatestTable)
	require.Equal(t, "objects_latest_mv", names.LatestView)

	_, err = deriveSchemaIdentifiers(strings.Repeat("o", maxClickHouseIdentifierLength))
	require.ErrorContains(t, err, "latest aggregate table")
	require.ErrorContains(t, err, "maximum identifier length")
}

func TestDDLRejectsInvalidIdentifiers(t *testing.T) {
	_, err := objectsDDL("profile-db", "objects")
	require.ErrorContains(t, err, "database")

	_, err = chunksDDL("profile_db", "chunks; DROP TABLE chunks")
	require.ErrorContains(t, err, "table")

	_, err = latestAggregateDDL("profile_db", "latest; DROP TABLE latest")
	require.ErrorContains(t, err, "table")
}

func TestValidateSchemaAcceptsRequiredColumns(t *testing.T) {
	columns := columnsFromSchema(objectsSchema)
	columns = append(columns, columnInfo{Name: "operator_metadata", Type: "String"})

	require.NoError(t, validateSchema("profile_db.objects", columns, objectsSchema))
	require.NoError(t, validateSchema("profile_db.chunks", columnsFromSchema(chunksSchema), chunksSchema))
}

func TestValidateSchemaReportsMissingColumn(t *testing.T) {
	columns := columnsFromSchema(objectsSchema)
	columns = removeColumn(columns, "generation")

	err := validateSchema("profile_db.objects", columns, objectsSchema)
	require.ErrorContains(t, err, "profile_db.objects")
	require.ErrorContains(t, err, "missing column generation")
}

func TestValidateSchemaReportsIncompatibleColumn(t *testing.T) {
	columns := columnsFromSchema(chunksSchema)
	for i := range columns {
		if columns[i].Name == "data" {
			columns[i].Type = "Array(UInt8)"
		}
	}

	err := validateSchema("profile_db.chunks", columns, chunksSchema)
	require.ErrorContains(t, err, "profile_db.chunks")
	require.ErrorContains(t, err, "incompatible column data")
	require.ErrorContains(t, err, "expected String")
	require.ErrorContains(t, err, "got Array(UInt8)")
}

func TestValidateSchemaReportsMissingDefault(t *testing.T) {
	columns := columnsFromSchema(objectsSchema)
	for i := range columns {
		if columns[i].Name == "event_at" {
			columns[i].DefaultKind = ""
			columns[i].DefaultExpression = ""
		}
	}

	err := validateSchema("profile_db.objects", columns, objectsSchema)
	require.ErrorContains(t, err, "profile_db.objects")
	require.ErrorContains(t, err, "event_at")
	require.ErrorContains(t, err, "expected DEFAULT now64(3)")
	require.ErrorContains(t, err, "got no default")
}

func TestValidateSchemaRejectsUnexpectedDefault(t *testing.T) {
	columns := columnsFromSchema(objectsSchema)
	for i := range columns {
		if columns[i].Name == "object_key" {
			columns[i].DefaultKind = "DEFAULT"
			columns[i].DefaultExpression = "'fallback'"
		}
	}

	err := validateSchema("profile_db.objects", columns, objectsSchema)
	require.EqualError(t, err, "ClickHouse table profile_db.objects has incompatible default for column object_key: expected no default, got DEFAULT 'fallback'")
}

func TestValidateSchemaReportsWrongDefault(t *testing.T) {
	tests := []struct {
		name              string
		defaultKind       string
		defaultExpression string
	}{
		{name: "kind", defaultKind: "MATERIALIZED", defaultExpression: "now64(3)"},
		{name: "expression", defaultKind: "DEFAULT", defaultExpression: "now64(6)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := columnsFromSchema(chunksSchema)
			for i := range columns {
				if columns[i].Name == "created_at" {
					columns[i].DefaultKind = tt.defaultKind
					columns[i].DefaultExpression = tt.defaultExpression
				}
			}

			err := validateSchema("profile_db.chunks", columns, chunksSchema)
			require.ErrorContains(t, err, "profile_db.chunks")
			require.ErrorContains(t, err, "created_at")
			require.ErrorContains(t, err, "expected DEFAULT now64(3)")
			require.ErrorContains(t, err, "got "+tt.defaultKind+" "+tt.defaultExpression)
		})
	}
}

func TestValidateSchemaReportsMissingColumnsDeterministically(t *testing.T) {
	err := validateSchema("profile_db.objects", nil, objectsSchema)

	require.EqualError(t, err, "ClickHouse table profile_db.objects is missing column chunk_count")
}

func TestCorruptionErrorContainsMetadataButNotData(t *testing.T) {
	generation := uuid.MustParse("3d41a140-d15b-4cfa-a626-87bba82fa784")
	err := (&CorruptionError{
		Key:        "blocks/tenant/object",
		Generation: generation,
		Reason:     "expected 3 chunks, got 2",
	}).Error()

	require.Contains(t, err, "blocks/tenant/object")
	require.Contains(t, err, generation.String())
	require.Contains(t, err, "expected 3 chunks, got 2")

	_, hasData := reflect.TypeOf(CorruptionError{}).FieldByName("Data")
	require.False(t, hasData)
}

func columnsFromSchema(schema map[string]expectedColumn) []columnInfo {
	columns := make([]columnInfo, 0, len(schema))
	for name, expected := range schema {
		columns = append(columns, columnInfo{
			Name:              name,
			Type:              expected.Type,
			DefaultKind:       expected.DefaultKind,
			DefaultExpression: expected.DefaultExpression,
		})
	}
	return columns
}

func removeColumn(columns []columnInfo, name string) []columnInfo {
	filtered := columns[:0]
	for _, column := range columns {
		if column.Name != name {
			filtered = append(filtered, column)
		}
	}
	return filtered
}

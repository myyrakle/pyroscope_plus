package clickhouse

import (
	"fmt"
	"sort"
	"strings"
)

var objectsSchema = map[string]expectedColumn{
	"object_key":       {Type: "String"},
	"generation":       {Type: "UUID"},
	"object_size":      {Type: "UInt64"},
	"chunk_count":      {Type: "UInt32"},
	"chunk_size":       {Type: "UInt32"},
	"state":            {Type: "Enum8('pending' = 1, 'committed' = 2, 'deleted' = 3)"},
	"version":          {Type: "UInt64"},
	"lease_expires_at": {Type: "DateTime64(3, 'UTC')"},
	"event_at": {
		Type:              "DateTime64(3, 'UTC')",
		DefaultKind:       "DEFAULT",
		DefaultExpression: "now64(3)",
	},
}

var chunksSchema = map[string]expectedColumn{
	"object_key":  {Type: "String"},
	"generation":  {Type: "UUID"},
	"chunk_index": {Type: "UInt32"},
	"data":        {Type: "String"},
	"created_at": {
		Type:              "DateTime64(3, 'UTC')",
		DefaultKind:       "DEFAULT",
		DefaultExpression: "now64(3)",
	},
}

const latestManifestAggregateType = "AggregateFunction(argMax, Tuple(UUID, UInt64, UInt32, UInt32, Enum8('pending' = 1, 'committed' = 2, 'deleted' = 3), UInt64, DateTime64(3, 'UTC'), DateTime64(3, 'UTC')), Tuple(UInt64, UUID))"

var latestSchema = map[string]expectedColumn{
	"object_key": {Type: "String"},
	"manifest":   {Type: latestManifestAggregateType},
}

const (
	objectStorePartitionKey = "cityHash64(object_key) % 64"
	objectsSortingKey       = "object_key, version, generation, state"
	chunksSortingKey        = "object_key, generation, chunk_index"
	latestSortingKey        = "object_key"
)

type expectedColumn struct {
	Type              string
	DefaultKind       string
	DefaultExpression string
}

type columnInfo struct {
	Name              string
	Type              string
	DefaultKind       string
	DefaultExpression string
}

type tableInfo struct {
	Name             string `ch:"name"`
	Engine           string `ch:"engine"`
	SortingKey       string `ch:"sorting_key"`
	PartitionKey     string `ch:"partition_key"`
	CreateTableQuery string `ch:"create_table_query"`
}

type expectedTable struct {
	Engine       string
	SortingKey   string
	PartitionKey string
}

type schemaIdentifiers struct {
	LatestTable string
	LatestView  string
}

func deriveSchemaIdentifiers(objectsTable string) (schemaIdentifiers, error) {
	if err := validIdentifier("objects table", objectsTable); err != nil {
		return schemaIdentifiers{}, err
	}
	names := schemaIdentifiers{
		LatestTable: objectsTable + "_latest",
		LatestView:  objectsTable + "_latest_mv",
	}
	if err := validIdentifier("latest aggregate table derived from objects table", names.LatestTable); err != nil {
		return schemaIdentifiers{}, err
	}
	if err := validIdentifier("latest materialized view derived from objects table", names.LatestView); err != nil {
		return schemaIdentifiers{}, err
	}
	return names, nil
}

func objectsDDL(database, table string) (string, error) {
	name, err := qualifiedTable(database, table)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(strings.TrimSpace(`
CREATE TABLE IF NOT EXISTS %s
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
PARTITION BY cityHash64(object_key) %% 64
ORDER BY (object_key, version, generation, state)`), name), nil
}

func chunksDDL(database, table string) (string, error) {
	name, err := qualifiedTable(database, table)
	if err != nil {
		return "", err
	}
	// Chunk payloads dominate storage volume; ZSTD(1) measurably reduces
	// their size over the default LZ4 at a small CPU cost. The generation
	// UUID is random and incompressible, so it keeps the default codec.
	// Schema validation ignores codecs, so tables created before these
	// defaults remain compatible.
	return fmt.Sprintf(strings.TrimSpace(`
CREATE TABLE IF NOT EXISTS %s
(
    object_key String CODEC(ZSTD(1)),
    generation UUID,
    chunk_index UInt32 CODEC(ZSTD(1)),
    data String CODEC(ZSTD(1)),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY cityHash64(object_key) %% 64
ORDER BY (object_key, generation, chunk_index)`), name), nil
}

func latestAggregateDDL(database, table string) (string, error) {
	name, err := qualifiedTable(database, table)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(strings.TrimSpace(`
CREATE TABLE IF NOT EXISTS %s
(
    object_key String,
    manifest %s
)
ENGINE = AggregatingMergeTree
PARTITION BY cityHash64(object_key) %% 64
ORDER BY object_key`), name, latestManifestAggregateType), nil
}

func latestMaterializedViewDDL(database, objectsTable, latestTable, view string) (string, error) {
	objectsName, err := qualifiedTable(database, objectsTable)
	if err != nil {
		return "", err
	}
	latestName, err := qualifiedTable(database, latestTable)
	if err != nil {
		return "", err
	}
	viewName, err := qualifiedTable(database, view)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(strings.TrimSpace(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s
TO %s
AS SELECT
    object_key,
    argMaxState(
        tuple(generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at),
        tuple(version, generation)
    ) AS manifest
FROM %s
WHERE state IN ('committed', 'deleted')
GROUP BY object_key`), viewName, latestName, objectsName), nil
}

func latestMaterializedViewCanonicalDDL(database, objectsTable, latestTable, view string) (string, error) {
	objectsName, err := qualifiedTable(database, objectsTable)
	if err != nil {
		return "", err
	}
	latestName, err := qualifiedTable(database, latestTable)
	if err != nil {
		return "", err
	}
	viewName, err := qualifiedTable(database, view)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s TO %s (`object_key` String, `manifest` %s) AS SELECT "+
			"object_key, argMaxState((generation, object_size, chunk_count, chunk_size, state, version, "+
			"lease_expires_at, event_at), (version, generation)) AS manifest FROM %s "+
			"WHERE state IN ('committed', 'deleted') GROUP BY object_key",
		viewName,
		latestName,
		latestManifestAggregateType,
		objectsName,
	), nil
}

func qualifiedTable(database, table string) (string, error) {
	if err := validIdentifier("database", database); err != nil {
		return "", err
	}
	if err := validIdentifier("table", table); err != nil {
		return "", err
	}
	return fmt.Sprintf("`%s`.`%s`", database, table), nil
}

func validateSchema(table string, columns []columnInfo, expected map[string]expectedColumn) error {
	actual := make(map[string]columnInfo, len(columns))
	for _, column := range columns {
		actual[column.Name] = column
	}
	expectedNames := make([]string, 0, len(expected))
	for name := range expected {
		expectedNames = append(expectedNames, name)
	}
	sort.Strings(expectedNames)
	for _, name := range expectedNames {
		expectedColumn := expected[name]
		actualColumn, ok := actual[name]
		if !ok {
			return fmt.Errorf("ClickHouse table %s is missing column %s", table, name)
		}
		if actualColumn.Type != expectedColumn.Type {
			return fmt.Errorf("ClickHouse table %s has incompatible column %s: expected %s, got %s", table, name, expectedColumn.Type, actualColumn.Type)
		}
		if actualColumn.DefaultKind != expectedColumn.DefaultKind || actualColumn.DefaultExpression != expectedColumn.DefaultExpression {
			return fmt.Errorf(
				"ClickHouse table %s has incompatible default for column %s: expected %s, got %s",
				table,
				name,
				formatColumnDefault(expectedColumn.DefaultKind, expectedColumn.DefaultExpression),
				formatColumnDefault(actualColumn.DefaultKind, actualColumn.DefaultExpression),
			)
		}
	}
	return nil
}

func formatColumnDefault(kind, expression string) string {
	value := strings.TrimSpace(kind + " " + expression)
	if value == "" {
		return "no default"
	}
	return value
}

func validateTable(table string, actual tableInfo, expected expectedTable) error {
	if actual.Engine != expected.Engine {
		return fmt.Errorf("ClickHouse table %s has incompatible engine: expected %s, got %s", table, expected.Engine, actual.Engine)
	}
	if normalizeSchemaExpression(actual.SortingKey) != normalizeSchemaExpression(expected.SortingKey) {
		return fmt.Errorf("ClickHouse table %s has incompatible sorting key: expected %s, got %s", table, expected.SortingKey, actual.SortingKey)
	}
	if normalizeSchemaExpression(actual.PartitionKey) != normalizeSchemaExpression(expected.PartitionKey) {
		return fmt.Errorf("ClickHouse table %s has incompatible partition key: expected %s, got %s", table, expected.PartitionKey, actual.PartitionKey)
	}
	return nil
}

func validateLatestMaterializedView(table string, actual tableInfo, database, objectsTable, latestTable, view string) error {
	if actual.Engine != "MaterializedView" {
		return fmt.Errorf("ClickHouse materialized view %s has incompatible engine: expected MaterializedView, got %s", table, actual.Engine)
	}
	expectedDefinition, err := latestMaterializedViewDDL(database, objectsTable, latestTable, view)
	if err != nil {
		return err
	}
	canonicalDefinition, err := latestMaterializedViewCanonicalDDL(database, objectsTable, latestTable, view)
	if err != nil {
		return err
	}
	actualDefinition := normalizeMaterializedViewDefinition(actual.CreateTableQuery)
	if actualDefinition != normalizeMaterializedViewDefinition(expectedDefinition) &&
		actualDefinition != normalizeMaterializedViewDefinition(canonicalDefinition) {
		return fmt.Errorf("ClickHouse materialized view %s has incompatible materialized view definition", table)
	}
	return nil
}

func normalizeSchemaExpression(value string) string {
	return strings.ReplaceAll(strings.Join(strings.Fields(value), ""), "`", "")
}

func normalizeMaterializedViewDefinition(value string) string {
	value = strings.Replace(normalizeSchemaExpression(value), "IFNOTEXISTS", "", 1)
	// ClickHouse 26 canonicalizes tuple expressions in SHOW CREATE TABLE from
	// `(a, b)` to `tuple(a, b)`. Both forms are semantically identical.
	return strings.ReplaceAll(value, "tuple(", "(")
}

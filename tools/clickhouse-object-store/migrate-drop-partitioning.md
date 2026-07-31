# Migrating ClickHouse object-store tables to the unpartitioned layout

New deployments create unpartitioned tables. Existing tables created with the
legacy `PARTITION BY cityHash64(object_key) % 64` layout keep working — the
schema validation accepts both — so migrating is optional. Migrate when you
want the ~64x lower part counts (and correspondingly cheaper mutations and
merges) on an existing deployment.

Two options:

## Option 1: reset (simplest, loses profiling history)

```sql
DROP TABLE IF EXISTS <db>.pyroscope_objects_latest_mv;
DROP TABLE IF EXISTS <db>.pyroscope_objects_latest;
DROP TABLE IF EXISTS <db>.pyroscope_object_chunks;
DROP TABLE IF EXISTS <db>.pyroscope_objects;
```

Also wipe the metastore state (it references the deleted blocks), then restart
Pyroscope with `auto_create_tables: true`. In the docker-compose deployment:

```bash
docker compose down pyroscope
docker volume rm pyroscope-clickhouse_pyroscope-metastore-data
# run the DROP TABLE statements above, then:
docker compose up -d pyroscope
```

## Option 2: copy (keeps history, ~10 minutes of write downtime)

The `_latest` aggregate table and materialized view names are derived from the
objects table name, so the copy has to finish with the original names.

1. **Stop Pyroscope** (stops writers and the cleaner):

   ```bash
   docker compose stop pyroscope
   ```

2. **Create the new tables** (unpartitioned; adjust `<db>` and table names to
   your config):

   ```sql
   CREATE TABLE <db>.objects_new
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
   SETTINGS old_parts_lifetime = 60;

   CREATE TABLE <db>.chunks_new
   (
       object_key String CODEC(ZSTD(1)),
       generation UUID,
       chunk_index UInt32 CODEC(ZSTD(1)),
       data String CODEC(ZSTD(1)),
       created_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
   )
   ENGINE = MergeTree
   ORDER BY (object_key, generation, chunk_index)
   SETTINGS old_parts_lifetime = 60;
   ```

3. **Copy the visible rows** (lightweight-deleted rows are skipped
   automatically):

   ```sql
   INSERT INTO <db>.objects_new SELECT * FROM <db>.pyroscope_objects;
   INSERT INTO <db>.chunks_new  SELECT * FROM <db>.pyroscope_object_chunks;
   ```

4. **Swap names and drop the old aggregate objects** (the `_latest` table and
   view are rebuilt from the copied manifests in the next step):

   ```sql
   DROP TABLE <db>.pyroscope_objects_latest_mv;
   DROP TABLE <db>.pyroscope_objects_latest;
   RENAME TABLE <db>.pyroscope_objects        TO <db>.objects_old,
                <db>.pyroscope_object_chunks TO <db>.chunks_old,
                <db>.objects_new             TO <db>.pyroscope_objects,
                <db>.chunks_new              TO <db>.pyroscope_object_chunks;
   ```

5. **Recreate the `_latest` aggregate table and view, and backfill it** —
   the materialized view only sees new inserts, so the aggregate state must
   be seeded from the copied manifests before Pyroscope starts serving:

   ```sql
   CREATE TABLE <db>.pyroscope_objects_latest
   (
       object_key String,
       manifest AggregateFunction(argMax, Tuple(UUID, UInt64, UInt32, UInt32, Enum8('pending' = 1, 'committed' = 2, 'deleted' = 3), UInt64, DateTime64(3, 'UTC'), DateTime64(3, 'UTC')), Tuple(UInt64, UUID))
   )
   ENGINE = AggregatingMergeTree
   ORDER BY object_key
   SETTINGS old_parts_lifetime = 60;

   CREATE MATERIALIZED VIEW <db>.pyroscope_objects_latest_mv
   TO <db>.pyroscope_objects_latest
   AS SELECT
       object_key,
       argMaxState(
           tuple(generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at),
           tuple(version, generation)
       ) AS manifest
   FROM <db>.pyroscope_objects
   WHERE state IN ('committed', 'deleted')
   GROUP BY object_key;

   INSERT INTO <db>.pyroscope_objects_latest
   SELECT object_key,
          argMaxState(
              tuple(generation, object_size, chunk_count, chunk_size, state, version, lease_expires_at, event_at),
              tuple(version, generation)
          ) AS manifest
   FROM <db>.pyroscope_objects
   WHERE state IN ('committed', 'deleted')
   GROUP BY object_key;
   ```

6. **Start Pyroscope and verify, then drop the old tables:**

   ```bash
   docker compose up -d pyroscope
   ```

   ```sql
   -- both should return the same manifest for a sample of keys
   SELECT count() FROM <db>.pyroscope_objects;
   SELECT count() FROM <db>.objects_old;

   DROP TABLE <db>.objects_old;
   DROP TABLE <db>.chunks_old;
   ```

# ClickHouse object-store test stack

This directory contains a dedicated local and integration test topology for
Pyroscope's ClickHouse object-store backend. It builds Pyroscope from the
current checkout, starts ClickHouse, and runs a profiled Echo workload. This is
explicitly not a production deployment topology: it does not provide the
authentication, TLS, replication, resource sizing, backup, or upgrade controls
needed in production.

## Prerequisites

- Docker Engine with Docker Compose v2 (`docker compose`)
- Bash (the bounded polling loops use Bash's `$SECONDS` variable)
- `curl`
- Enough local CPU, memory, and disk to build Pyroscope and run three containers

Run every command below from the repository root in the same Bash shell. If the
current shell is not Bash, start one with `bash` first. Export the Compose file
and host ports once so every later Compose and HTTP command uses the same
settings:

```bash
export COMPOSE_FILE=tools/clickhouse-object-store/docker-compose.yml
export PYROSCOPE_HTTP_PORT=4040
export ECHO_HTTP_PORT=18080
export CLICKHOUSE_NATIVE_PORT=19000
export CLICKHOUSE_HTTP_PORT=18123
```

The defaults expose only loopback endpoints:

- Pyroscope: `http://127.0.0.1:4040`
- Echo: `http://127.0.0.1:18080`
- ClickHouse native protocol: `127.0.0.1:19000`
- ClickHouse HTTP: `http://127.0.0.1:18123`

Override a port before starting the stack when its default is occupied. For
example:

```bash
export ECHO_HTTP_PORT=18081
```

Keep these exports for the entire workflow, including teardown. The top-level
Compose project name `clickhouse-object-store` scopes commands to this stack;
it does not make destructive volume removal reversible.

## Clean start

Warning: the first command deletes this stack's dedicated ClickHouse and
Pyroscope volumes and all data in them. It does not remove resources belonging
to other Compose projects.

```bash
docker compose down -v --remove-orphans
docker compose up --build -d
```

Wait up to three minutes for all three services to become healthy, then inspect
their status:

```bash
deadline=$((SECONDS + 180))
while :; do
  healthy=0
  for service in clickhouse pyroscope echo-server; do
    container_id=$(docker compose ps -q "$service")
    if [ -n "$container_id" ] &&
       [ "$(docker inspect --format '{{.State.Health.Status}}' "$container_id")" = healthy ]; then
      healthy=$((healthy + 1))
    fi
  done
  [ "$healthy" -eq 3 ] && break
  if [ "$SECONDS" -ge "$deadline" ]; then
    docker compose ps
    exit 1
  fi
  sleep 5
done
docker compose ps
```

## Generate and verify profiles

Check Echo and trigger each profiled workload route:

```bash
curl -fsS "http://127.0.0.1:${ECHO_HTTP_PORT}/health"
curl -fsS "http://127.0.0.1:${ECHO_HTTP_PORT}/fast"
curl -fsS "http://127.0.0.1:${ECHO_HTTP_PORT}/slow"
curl -fsS "http://127.0.0.1:${ECHO_HTTP_PORT}/alloc"
```

Echo also runs the same bounded CPU and allocation workloads automatically
every 750 milliseconds. The pyroscope-go upload interval is 15 seconds. A short
initial delay reduces noisy first attempts, but correctness is gated by the
bounded polling below:

```bash
sleep 5
```

Poll for up to two minutes, freezing a pre-restart window only after both
ClickHouse tables contain data, the Echo label is indexed, and an actual Echo CPU
profile has a nonempty `sample` array. On timeout, the last responses, service
status, and recent logs are printed for diagnosis:

```bash
POLL_STARTED=$SECONDS
deadline=$((SECONDS + 120))
OBJECT_COUNT=0
CHUNK_COUNT=0
LABEL_RESPONSE=
PROFILE_RESPONSE=
while :; do
  PROFILE_END_MS=$(($(date +%s) * 1000))
  PROFILE_START_MS=$((PROFILE_END_MS - 900000))
  OBJECT_COUNT=$(docker compose exec -T clickhouse \
    clickhouse-client --query "SELECT count() FROM default.pyroscope_objects" 2>&1)
  CHUNK_COUNT=$(docker compose exec -T clickhouse \
    clickhouse-client --query "SELECT count() FROM default.pyroscope_object_chunks" 2>&1)
  LABEL_RESPONSE=$(curl -fsS \
    -H 'Content-Type: application/json' \
    -H 'Connect-Protocol-Version: 1' \
    --data "{\"name\":\"service_name\",\"matchers\":[\"{}\"],\"start\":${PROFILE_START_MS},\"end\":${PROFILE_END_MS}}" \
    "http://127.0.0.1:${PYROSCOPE_HTTP_PORT}/querier.v1.QuerierService/LabelValues" 2>&1)
  PROFILE_RESPONSE=$(curl -fsS \
    -H 'Content-Type: application/json' \
    -H 'Connect-Protocol-Version: 1' \
    --data "{\"profileTypeID\":\"process_cpu:cpu:nanoseconds:cpu:nanoseconds\",\"labelSelector\":\"{service_name=\\\"clickhouse.echo.server\\\"}\",\"start\":${PROFILE_START_MS},\"end\":${PROFILE_END_MS}}" \
    "http://127.0.0.1:${PYROSCOPE_HTTP_PORT}/querier.v1.QuerierService/SelectMergeProfile" 2>&1)

  if printf '%s\n' "$OBJECT_COUNT" | grep -Eq '^[0-9]+$' &&
     printf '%s\n' "$CHUNK_COUNT" | grep -Eq '^[0-9]+$' &&
     [ "$OBJECT_COUNT" -gt 0 ] && [ "$CHUNK_COUNT" -gt 0 ] &&
     printf '%s' "$LABEL_RESPONSE" | grep -Fq '"clickhouse.echo.server"' &&
     printf '%s' "$PROFILE_RESPONSE" | grep -Eq '"sample"[[:space:]]*:[[:space:]]*\[\{'; then
    WAIT_SECONDS=$((SECONDS - POLL_STARTED))
    PROFILE_BYTES=$(printf '%s' "$PROFILE_RESPONSE" | wc -c | tr -d ' ')
    printf 'pyroscope_objects=%s\npyroscope_object_chunks=%s\nwait_seconds=%s\nprofile_bytes=%s\n' \
      "$OBJECT_COUNT" "$CHUNK_COUNT" "$WAIT_SECONDS" "$PROFILE_BYTES"
    break
  fi

  if [ "$SECONDS" -ge "$deadline" ]; then
    printf 'Timed out waiting for persisted Echo CPU profile.\n'
    printf 'pyroscope_objects=%s\npyroscope_object_chunks=%s\n' "$OBJECT_COUNT" "$CHUNK_COUNT"
    printf 'label_response=%s\nprofile_response=%s\n' "$LABEL_RESPONSE" "$PROFILE_RESPONSE"
    docker compose ps
    docker compose logs --tail=100 clickhouse pyroscope echo-server
    exit 1
  fi
  sleep 5
done
```

`PROFILE_START_MS` and `PROFILE_END_MS` now identify a known-good window containing
only profiles uploaded before the restart. Keep those values unchanged, restart
Pyroscope, and wait up to three minutes for it to become healthy:

```bash
docker compose restart pyroscope
deadline=$((SECONDS + 180))
while :; do
  container_id=$(docker compose ps -q pyroscope)
  if [ -n "$container_id" ] &&
     [ "$(docker inspect --format '{{.State.Health.Status}}' "$container_id")" = healthy ]; then
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    docker compose ps pyroscope
    exit 1
  fi
  sleep 5
done
```

Query that pre-restart window for an actual CPU profile. The assertion requires
a nonempty `sample` array, proving the restarted Pyroscope retrieved persisted
Echo profile data rather than merely listing its labels:

```bash
PROFILE_RESPONSE=$(curl -fsS \
  -H 'Content-Type: application/json' \
  -H 'Connect-Protocol-Version: 1' \
  --data "{\"profileTypeID\":\"process_cpu:cpu:nanoseconds:cpu:nanoseconds\",\"labelSelector\":\"{service_name=\\\"clickhouse.echo.server\\\"}\",\"start\":${PROFILE_START_MS},\"end\":${PROFILE_END_MS}}" \
  "http://127.0.0.1:${PYROSCOPE_HTTP_PORT}/querier.v1.QuerierService/SelectMergeProfile")
PROFILE_BYTES=$(printf '%s' "$PROFILE_RESPONSE" | wc -c | tr -d ' ')
printf '%s' "$PROFILE_RESPONSE" | grep -Eq '"sample"[[:space:]]*:[[:space:]]*\[\{' &&
  printf 'persisted_profile_query=nonempty_sample\npersisted_profile_bytes=%s\n' "$PROFILE_BYTES"
```

## Backend acceptance and benchmarks

The integration suite includes the shared Thanos object-store acceptance test,
including a 200 MiB object. Start only ClickHouse and run the suite from the
repository root:

```bash
docker compose up -d clickhouse
CLICKHOUSE_ADDR="127.0.0.1:${CLICKHOUSE_NATIVE_PORT}" \
  go test -tags=integration ./pkg/objstore/providers/clickhouse -count=1
```

Run the default 64 MiB payload and 10,000-key benchmarks once each:

```bash
CLICKHOUSE_ADDR="127.0.0.1:${CLICKHOUSE_NATIVE_PORT}" \
  go test -tags=integration ./pkg/objstore/providers/clickhouse \
  -run '^$' -bench '^BenchmarkClickHouse' -benchtime=1x -count=1
```

The large benchmark mode uses 1 GiB payloads and 100,000 metadata rows. It is
opt-in because it can consume substantial local disk, memory, and ClickHouse
merge capacity:

```bash
CLICKHOUSE_BENCHMARK_LARGE=1 \
CLICKHOUSE_ADDR="127.0.0.1:${CLICKHOUSE_NATIVE_PORT}" \
  go test -tags=integration ./pkg/objstore/providers/clickhouse \
  -run '^$' -bench '^BenchmarkClickHouse' -benchtime=1x -count=1
```

## Logs

Follow the complete stack (press Ctrl-C to stop following):

```bash
docker compose logs -f
```

Alternatively, follow only Echo (press Ctrl-C to stop following):

```bash
docker compose logs -f echo-server
```

## Stop and reset

Stop the project while retaining its named volumes:

```bash
docker compose down
```

For a destructive reset, remove this project's containers, network, named
volumes, and orphaned services:

```bash
docker compose down -v --remove-orphans
```

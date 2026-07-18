//go:build integration

package clickhouse

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/thanos-io/objstore"
)

const (
	benchmarkObjectSize = int64(64 * 1024 * 1024)
	largeBenchmarkSize  = int64(1024 * 1024 * 1024)
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func BenchmarkClickHouseUpload(b *testing.B) {
	bucket := benchmarkBucket(b)
	size := benchmarkPayloadSize()
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := io.LimitReader(zeroReader{}, size)
		if err := bucket.Upload(context.Background(), fmt.Sprintf("benchmark/upload/%d", i), reader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClickHouseRead(b *testing.B) {
	bucket := benchmarkBucket(b)
	size := benchmarkPayloadSize()
	ctx := context.Background()
	if err := bucket.Upload(ctx, "benchmark/read", io.LimitReader(zeroReader{}, size)); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := bucket.Get(ctx, "benchmark/read")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClickHouseConcurrentUpload(b *testing.B) {
	bucket := benchmarkBucket(b)
	const size = int64(4 * 1024 * 1024)
	var sequence atomic.Uint64
	b.SetBytes(size)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("benchmark/concurrent/%d", sequence.Add(1))
			if err := bucket.Upload(context.Background(), key, io.LimitReader(zeroReader{}, size)); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkClickHouseList(b *testing.B) {
	bucket := benchmarkBucket(b)
	count := benchmarkMetadataCount()
	store := bucket.store.(*clickhouseStore)
	insertBenchmarkManifests(b, store, count, committed)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seen := 0
		err := bucket.Iter(context.Background(), "benchmark/metadata/", func(string) error {
			seen++
			return nil
		}, objstore.WithRecursiveIter())
		if err != nil {
			b.Fatal(err)
		}
		if seen != count {
			b.Fatalf("listed %d objects, expected %d", seen, count)
		}
	}
}

func BenchmarkClickHouseCleanupCandidates(b *testing.B) {
	bucket := benchmarkBucket(b)
	count := benchmarkMetadataCount()
	store := bucket.store.(*clickhouseStore)
	insertBenchmarkManifests(b, store, count, pending)
	partition := busiestBenchmarkPartition(b, store)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		candidates, err := store.CleanupCandidates(context.Background(), partition, time.Millisecond, 100)
		if err != nil {
			b.Fatal(err)
		}
		if len(candidates) == 0 {
			b.Fatal("cleanup query returned no candidates")
		}
	}
}

func benchmarkBucket(b *testing.B) *Bucket {
	b.Helper()
	cfg := integrationConfig(b)
	dropIntegrationTables(b, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	b.Cleanup(cancel)
	bucket, err := NewBucketClient(ctx, cfg, "clickhouse-benchmark", log.NewNopLogger(), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			b.Error(err)
		}
	})
	return bucket
}

func insertBenchmarkManifests(b *testing.B, store *clickhouseStore, count int, state manifestState) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	batch, err := store.conn.PrepareInsert(ctx, fmt.Sprintf("INSERT INTO %s (%s)", store.objectsTable, manifestColumns))
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := batch.Close(); err != nil {
			b.Error(err)
		}
	}()
	now := time.Now().UTC()
	lease := now.Add(-time.Hour)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("benchmark/metadata/%09d", i)
		if err := batch.Append(key, uuid.New(), uint64(0), uint32(0), uint32(0), state.String(), uint64(i+1), lease, lease); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		b.Fatal(err)
	}
}

func busiestBenchmarkPartition(b *testing.B, store *clickhouseStore) uint32 {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var rows []struct {
		Partition uint32 `ch:"partition"`
	}
	query := fmt.Sprintf(`SELECT toUInt32(cityHash64(object_key) %% 64) AS partition
FROM %s
GROUP BY partition
ORDER BY count() DESC
LIMIT 1`, store.objectsTable)
	if err := store.conn.Select(ctx, &rows, query); err != nil {
		b.Fatal(err)
	}
	if len(rows) != 1 {
		b.Fatalf("expected one benchmark partition, got %d", len(rows))
	}
	return rows[0].Partition
}

func benchmarkPayloadSize() int64 {
	if os.Getenv("CLICKHOUSE_BENCHMARK_LARGE") == "1" {
		return largeBenchmarkSize
	}
	return benchmarkObjectSize
}

func benchmarkMetadataCount() int {
	if os.Getenv("CLICKHOUSE_BENCHMARK_LARGE") == "1" {
		return 100_000
	}
	return 10_000
}

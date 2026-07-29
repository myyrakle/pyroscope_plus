package clickhouse

import (
	"context"
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestBucketGetReadsBinaryAndEmptyObjects(t *testing.T) {
	t.Run("binary with multiple reads and size", func(t *testing.T) {
		store := fakeObjectStore([]byte{0, 1, 2, 0xff, 4}, 3)
		reader, err := testBucket(bucketTestConfig(), store).Get(context.Background(), "key")
		require.NoError(t, err)

		sizer, ok := reader.(objstore.ObjectSizer)
		require.True(t, ok)
		size, err := sizer.ObjectSize()
		require.NoError(t, err)
		require.Equal(t, int64(5), size)
		size, err = objstore.TryToGetSize(reader)
		require.NoError(t, err)
		require.Equal(t, int64(5), size)

		buf := make([]byte, 2)
		var got []byte
		for {
			n, readErr := reader.Read(buf)
			got = append(got, buf[:n]...)
			if errors.Is(readErr, io.EOF) {
				break
			}
			require.NoError(t, readErr)
		}
		require.Equal(t, []byte{0, 1, 2, 0xff, 4}, got)
		require.NoError(t, reader.Close())
		require.NoError(t, reader.Close())
		_, err = reader.Read(buf)
		require.ErrorIs(t, err, io.ErrClosedPipe)
	})

	t.Run("empty", func(t *testing.T) {
		store := fakeObjectStore(nil, 0)
		reader, err := testBucket(bucketTestConfig(), store).Get(context.Background(), "empty")
		require.NoError(t, err)
		got, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Empty(t, got)
		require.Empty(t, store.chunkCalls)
	})
}

func TestBucketGetRange(t *testing.T) {
	data := []byte("abcdefgh")
	tests := []struct {
		name   string
		offset int64
		length int64
		want   string
	}{
		{name: "same chunk", offset: 1, length: 2, want: "bc"},
		{name: "cross chunk", offset: 2, length: 4, want: "cdef"},
		{name: "to EOF", offset: 4, length: -1, want: "efgh"},
		{name: "oversized clamps", offset: 6, length: 99, want: "gh"},
		{name: "zero length", offset: 2, length: 0, want: ""},
		{name: "offset at EOF", offset: 8, length: 2, want: ""},
		{name: "offset beyond EOF", offset: 20, length: 2, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := fakeObjectStore(data, 3)
			reader, err := testBucket(bucketTestConfig(), store).GetRange(context.Background(), "key", tt.offset, tt.length)
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
			size, err := objstore.TryToGetSize(reader)
			require.NoError(t, err)
			require.Equal(t, int64(len(tt.want)), size)
		})
	}
}

func TestBucketGetRangeRejectsInvalidArguments(t *testing.T) {
	bucket := testBucket(bucketTestConfig(), fakeObjectStore([]byte("abc"), 3))
	for _, tc := range []struct {
		name   string
		offset int64
		length int64
	}{
		{name: "negative offset", offset: -1, length: 1},
		{name: "invalid negative length", offset: 0, length: -2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := bucket.GetRange(context.Background(), "key", tc.offset, tc.length)
			require.Error(t, err)
			require.False(t, bucket.IsObjNotFoundErr(err))
		})
	}
}

func TestBucketReadNotFoundAndKeyValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *fakeStore
	}{
		{name: "missing", store: &fakeStore{latestErr: ErrObjectNotFound}},
		{name: "deleted", store: &fakeStore{latest: manifest{Key: "key", State: deleted}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := testBucket(bucketTestConfig(), tc.store)
			_, err := bucket.Get(context.Background(), "key")
			require.Error(t, err)
			require.ErrorIs(t, err, ErrObjectNotFound)
			require.True(t, bucket.IsObjNotFoundErr(err))
		})
	}

	bucket := testBucket(bucketTestConfig(), &fakeStore{})
	_, err := bucket.Get(context.Background(), "")
	require.Error(t, err)
	require.False(t, bucket.IsObjNotFoundErr(err))
}

func TestBucketAttributesAndExists(t *testing.T) {
	eventAt := time.Date(2026, 7, 16, 4, 5, 6, 0, time.UTC)
	store := fakeObjectStore([]byte("hello"), 3)
	store.latest.EventAt = eventAt
	bucket := testBucket(bucketTestConfig(), store)

	attrs, err := bucket.Attributes(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, int64(5), attrs.Size)
	require.Equal(t, eventAt, attrs.LastModified)
	exists, err := bucket.Exists(context.Background(), "key")
	require.NoError(t, err)
	require.True(t, exists)

	store.latestErr = ErrObjectNotFound
	exists, err = bucket.Exists(context.Background(), "missing")
	require.NoError(t, err)
	require.False(t, exists)

	store.latestErr = nil
	store.latest.State = deleted
	exists, err = bucket.Exists(context.Background(), "deleted")
	require.NoError(t, err)
	require.False(t, exists)

	queryErr := errors.New("query failed")
	store.latestErr = queryErr
	exists, err = bucket.Exists(context.Background(), "key")
	require.False(t, exists)
	require.ErrorIs(t, err, queryErr)
	require.False(t, bucket.IsAccessDeniedErr(errors.New("access denied")))
	require.True(t, bucket.IsObjNotFoundErr(fmtWrapped(ErrObjectNotFound)))
}

func TestBucketRejectsCorruptManifest(t *testing.T) {
	tests := []struct {
		name     string
		manifest manifest
	}{
		{name: "nonempty zero chunk size", manifest: manifest{Key: "key", Generation: uuid.New(), Size: 1, ChunkCount: 1, State: committed}},
		{name: "chunk count too small", manifest: manifest{Key: "key", Generation: uuid.New(), Size: 7, ChunkSize: 3, ChunkCount: 2, State: committed}},
		{name: "chunk count too large", manifest: manifest{Key: "key", Generation: uuid.New(), Size: 6, ChunkSize: 3, ChunkCount: 3, State: committed}},
		{name: "empty has chunks", manifest: manifest{Key: "key", Generation: uuid.New(), Size: 0, ChunkSize: 3, ChunkCount: 1, State: committed}},
		{name: "size exceeds reader API", manifest: manifest{Key: "key", Generation: uuid.New(), Size: math.MaxUint64, ChunkSize: math.MaxUint32, ChunkCount: math.MaxUint32, State: committed}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testBucket(bucketTestConfig(), &fakeStore{latest: tt.manifest}).Get(context.Background(), "key")
			var corruption *CorruptionError
			require.ErrorAs(t, err, &corruption)
		})
	}
}

func TestBucketReportsManifestCorruptionButNotBackendErrors(t *testing.T) {
	registry := prometheus.NewRegistry()
	store := &fakeStore{latest: manifest{
		Key: "key", Generation: uuid.New(), Size: 1, ChunkCount: 1, State: committed,
	}}
	bucket, err := newBucketWithStore(bucketTestConfig(), "metrics", log.NewNopLogger(), store, registry)
	require.NoError(t, err)

	_, err = bucket.Get(context.Background(), "key")
	require.Error(t, err)
	require.Equal(t, float64(1), testutil.ToFloat64(bucket.metrics.corruptReads))
	_, err = bucket.Attributes(context.Background(), "key")
	require.Error(t, err)
	require.Equal(t, float64(2), testutil.ToFloat64(bucket.metrics.corruptReads))

	store.latestErr = errors.New("backend unavailable")
	_, err = bucket.Get(context.Background(), "key")
	require.Error(t, err)
	require.Equal(t, float64(2), testutil.ToFloat64(bucket.metrics.corruptReads))
}

func TestChunkReaderMakesCorruptionTerminal(t *testing.T) {
	registry := prometheus.NewRegistry()
	store := &fakeStore{latest: manifest{
		Key: "key", Generation: uuid.New(), Size: 3, ChunkSize: 3, ChunkCount: 1, State: committed,
	}}
	store.chunksFn = func(context.Context, string, uuid.UUID, uint32, uint32) ([]chunk, error) {
		return nil, nil
	}
	bucket, err := newBucketWithStore(bucketTestConfig(), "metrics", log.NewNopLogger(), store, registry)
	require.NoError(t, err)
	reader, err := bucket.Get(context.Background(), "key")
	require.NoError(t, err)

	_, firstErr := reader.Read(make([]byte, 1))
	var firstCorruption *CorruptionError
	require.ErrorAs(t, firstErr, &firstCorruption)
	_, secondErr := reader.Read(make([]byte, 1))
	var secondCorruption *CorruptionError
	require.ErrorAs(t, secondErr, &secondCorruption)
	require.Same(t, firstCorruption, secondCorruption)
	require.Len(t, store.chunkCalls, 1)
	require.Empty(t, reader.(*chunkReader).buffer)
	require.Equal(t, float64(1), testutil.ToFloat64(bucket.metrics.corruptReads))
}

func TestChunkReaderRejectsCorruptChunksBeforeExposure(t *testing.T) {
	generation := uuid.New()
	base := manifest{Key: "key", Generation: generation, Size: 8, ChunkSize: 3, ChunkCount: 3, State: committed}
	tests := []struct {
		name   string
		chunks func(uint32, uint32) []chunk
	}{
		{name: "missing index", chunks: func(uint32, uint32) []chunk { return nil }},
		{name: "duplicate index", chunks: func(uint32, uint32) []chunk {
			return []chunk{{Index: 0, Data: []byte("abc")}, {Index: 0, Data: []byte("abc")}, {Index: 2, Data: []byte("gh")}}
		}},
		{name: "out of order", chunks: func(uint32, uint32) []chunk {
			return []chunk{{Index: 1, Data: []byte("def")}, {Index: 0, Data: []byte("abc")}, {Index: 2, Data: []byte("gh")}}
		}},
		{name: "wrong nonfinal length", chunks: func(uint32, uint32) []chunk {
			return []chunk{{Index: 0, Data: []byte("ab")}, {Index: 1, Data: []byte("def")}, {Index: 2, Data: []byte("gh")}}
		}},
		{name: "wrong final length", chunks: func(uint32, uint32) []chunk {
			return []chunk{{Index: 0, Data: []byte("abc")}, {Index: 1, Data: []byte("def")}, {Index: 2, Data: []byte("g")}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{latest: base}
			store.chunksFn = func(_ context.Context, _ string, _ uuid.UUID, first, last uint32) ([]chunk, error) {
				return tt.chunks(first, last), nil
			}
			reader, err := testBucket(bucketTestConfig(), store).Get(context.Background(), "key")
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.Empty(t, got)
			var corruption *CorruptionError
			require.ErrorAs(t, err, &corruption)
			require.NotContains(t, err.Error(), "abc")
		})
	}
}

func TestChunkReaderRetainsDirectChunkRangeSlice(t *testing.T) {
	payload := []byte("abc")
	store := &fakeStore{latest: manifest{
		Key: "key", Generation: uuid.New(), Size: 3, ChunkSize: 3, ChunkCount: 1, State: committed,
	}}
	store.chunksFn = func(context.Context, string, uuid.UUID, uint32, uint32) ([]chunk, error) {
		return []chunk{{Index: 0, Data: payload}}, nil
	}
	reader, err := testBucket(bucketTestConfig(), store).GetRange(context.Background(), "key", 1, 2)
	require.NoError(t, err)

	buffer := make([]byte, 1)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, "b", string(buffer[:n]))
	chunkReader := reader.(*chunkReader)
	require.Same(t, &payload[2], &chunkReader.buffer[0])
}

func TestChunkReaderPinsManifestGeneration(t *testing.T) {
	oldGeneration := uuid.New()
	newGeneration := uuid.New()
	store := &fakeStore{latest: manifest{Key: "key", Generation: oldGeneration, Size: 3, ChunkSize: 3, ChunkCount: 1, State: committed}}
	store.chunksFn = func(_ context.Context, _ string, generation uuid.UUID, _, _ uint32) ([]chunk, error) {
		store.mu.Lock()
		store.latest.Generation = newGeneration
		store.mu.Unlock()
		require.Equal(t, oldGeneration, generation)
		return []chunk{{Index: 0, Data: []byte("old")}}, nil
	}

	reader, err := testBucket(bucketTestConfig(), store).Get(context.Background(), "key")
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "old", string(got))
	require.Equal(t, 1, store.latestCalls)
	require.Equal(t, oldGeneration, store.chunkCalls[0].generation)
}

func TestChunkReaderPreservesContextCancellationDuringRefill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := fakeObjectStore([]byte("abc"), 3)
	store.chunksFn = func(ctx context.Context, _ string, _ uuid.UUID, _, _ uint32) ([]chunk, error) {
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	reader, err := testBucket(bucketTestConfig(), store).Get(ctx, "key")
	require.NoError(t, err)
	_, err = io.ReadAll(reader)
	require.ErrorIs(t, err, context.Canceled)
}

func TestChunkReaderDoesNotReturnBufferedPayloadAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := fakeObjectStore([]byte("secret"), 3)
	reader, err := testBucket(bucketTestConfig(), store).Get(ctx, "key")
	require.NoError(t, err)

	buffer := make([]byte, 1)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "s", string(buffer[:n]))

	cancel()
	n, err = reader.Read(buffer)
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)
	require.NotContains(t, err.Error(), "ecret")
	require.Empty(t, reader.(*chunkReader).buffer)
	require.Empty(t, reader.(*chunkReader).batch)
}

func TestChunkReaderDoesNotRetainChunksReturnedDuringCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := fakeObjectStore([]byte("secret"), 3)
	store.chunksFn = func(context.Context, string, uuid.UUID, uint32, uint32) ([]chunk, error) {
		cancel()
		return []chunk{{Index: 0, Data: []byte("sec")}, {Index: 1, Data: []byte("ret")}}, nil
	}
	reader, err := testBucket(bucketTestConfig(), store).Get(ctx, "key")
	require.NoError(t, err)

	buffer := make([]byte, 6)
	n, err := reader.Read(buffer)
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)
	require.NotContains(t, err.Error(), "secret")
	require.Empty(t, reader.(*chunkReader).buffer)
	require.Empty(t, reader.(*chunkReader).batch)
}

func TestChunkReaderPrefetchesBoundedContiguousBatches(t *testing.T) {
	cfg := bucketTestConfig()
	cfg.ReadPrefetchChunks = 8
	cfg.MaxReadPrefetchBytes = cfg.ChunkSize * cfg.ReadPrefetchChunks
	data := make([]byte, 32*cfg.ChunkSize)
	for i := range data {
		data[i] = byte(i)
	}
	store := fakeObjectStore(data, uint32(cfg.ChunkSize))
	registry := prometheus.NewRegistry()
	bucket, err := newBucketWithStore(cfg, "metrics", log.NewNopLogger(), store, registry)
	require.NoError(t, err)
	reader, err := bucket.Get(context.Background(), "key")
	require.NoError(t, err)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Equal(t, []chunkRequest{
		{generation: store.latest.Generation, first: 0, last: 7},
		{generation: store.latest.Generation, first: 8, last: 15},
		{generation: store.latest.Generation, first: 16, last: 23},
		{generation: store.latest.Generation, first: 24, last: 31},
	}, store.chunkCalls)
	require.Equal(t, float64(4), testutil.ToFloat64(bucket.metrics.chunkQueries))
	require.Equal(t, float64(len(data)), testutil.ToFloat64(bucket.metrics.readPrefetchBytes))
	require.Equal(t, uint64(4), histogramSampleCount(t, registry, "pyroscope_objstore_clickhouse_operation_duration_seconds", "operation", "chunk_query"))
	require.Equal(t, float64(0), testutil.ToFloat64(bucket.metrics.operationFailures.WithLabelValues("chunk_query")))
}

func TestChunkReaderRangeFetchesOnlyIntersectingBatches(t *testing.T) {
	cfg := bucketTestConfig()
	cfg.ReadPrefetchChunks = 8
	cfg.MaxReadPrefetchBytes = cfg.ChunkSize * cfg.ReadPrefetchChunks
	data := []byte("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	store := fakeObjectStore(data, uint32(cfg.ChunkSize))
	reader, err := testBucket(cfg, store).GetRange(context.Background(), "key", 5*int64(cfg.ChunkSize)+1, 10*int64(cfg.ChunkSize)-2)
	require.NoError(t, err)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, data[16:44], got)
	require.Equal(t, []chunkRequest{
		{generation: store.latest.Generation, first: 5, last: 12},
		{generation: store.latest.Generation, first: 13, last: 14},
	}, store.chunkCalls)
}

func TestChunkReaderSmallRangeTransfersOnlyRequestedBytes(t *testing.T) {
	cfg := bucketTestConfig()
	data := make([]byte, 3*cfg.ChunkSize)
	for i := range data {
		data[i] = byte(i)
	}
	store := fakeObjectStore(data, uint32(cfg.ChunkSize))
	registry := prometheus.NewRegistry()
	bucket, err := newBucketWithStore(cfg, "sliced", log.NewNopLogger(), store, registry)
	require.NoError(t, err)

	offset := int64(cfg.ChunkSize) + 1
	reader, err := bucket.GetRange(context.Background(), "key", offset, 1)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, data[offset:offset+1], got)
	require.Equal(t, []chunkRequest{
		{generation: store.latest.Generation, first: 1, last: 1},
	}, store.chunkCalls)
	// The store must slice chunk payloads server-side: a 1-byte read must not
	// transfer the whole chunk.
	require.Equal(t, float64(1), testutil.ToFloat64(bucket.metrics.readPrefetchBytes))
}

func TestChunkReaderConcurrentCloseCancelsInFlightQuery(t *testing.T) {
	queryStarted := make(chan struct{})
	queryCanceled := make(chan struct{})
	releaseQuery := make(chan struct{})

	store := fakeObjectStore([]byte("secret"), 3)
	store.chunksFn = func(ctx context.Context, _ string, _ uuid.UUID, _, _ uint32) ([]chunk, error) {
		close(queryStarted)
		select {
		case <-ctx.Done():
			close(queryCanceled)
			return []chunk{{Index: 0, Data: []byte("sec")}}, ctx.Err()
		case <-releaseQuery:
			return nil, errors.New("query was not canceled")
		}
	}
	reader, err := testBucket(bucketTestConfig(), store).Get(context.Background(), "key")
	require.NoError(t, err)

	type readResult struct {
		n   int
		err error
	}
	readDone := make(chan readResult, 1)
	go func() {
		buffer := make([]byte, 3)
		n, readErr := reader.Read(buffer)
		readDone <- readResult{n: n, err: readErr}
	}()
	<-queryStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- reader.Close() }()

	canceled := false
	select {
	case <-queryCanceled:
		canceled = true
	case <-time.After(time.Second):
	}
	close(releaseQuery)
	result := <-readDone
	require.True(t, canceled, "Close did not cancel the in-flight chunk query")
	require.Zero(t, result.n)
	require.ErrorIs(t, result.err, context.Canceled)
	require.NotContains(t, result.err.Error(), "secret")
	require.NoError(t, <-closeDone)

	buffer := make([]byte, 1)
	n, err := reader.Read(buffer)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func fakeObjectStore(data []byte, chunkSize uint32) *fakeStore {
	generation := uuid.New()
	chunkCount := uint32(0)
	if len(data) > 0 {
		chunkCount = uint32((uint64(len(data)) + uint64(chunkSize) - 1) / uint64(chunkSize))
	}
	store := &fakeStore{latest: manifest{
		Key: "key", Generation: generation, Size: uint64(len(data)), ChunkSize: chunkSize, ChunkCount: chunkCount, State: committed,
	}}
	store.chunksFn = func(_ context.Context, key string, gotGeneration uuid.UUID, first, last uint32) ([]chunk, error) {
		if gotGeneration != generation {
			return nil, errors.New("unexpected generation")
		}
		chunks := make([]chunk, 0, last-first+1)
		for index := first; index <= last; index++ {
			start := uint64(index) * uint64(chunkSize)
			end := min(start+uint64(chunkSize), uint64(len(data)))
			chunks = append(chunks, chunk{Key: key, Generation: generation, Index: index, Data: append([]byte(nil), data[start:end]...)})
		}
		return chunks, nil
	}
	return store
}

func fmtWrapped(err error) error {
	return errors.Join(errors.New("wrapped"), err)
}

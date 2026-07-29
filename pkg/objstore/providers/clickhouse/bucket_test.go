package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

type fakeStore struct {
	mu sync.Mutex

	events      []string
	pending     []manifest
	batches     [][]chunk
	commits     []manifest
	initErr     error
	beginErr    error
	chunkErr    error
	commitErr   error
	closeErr    error
	initCalls   int
	closeCalls  int
	latest      manifest
	latestErr   error
	latestCalls int
	chunkCalls  []chunkRequest
	lists       []string
	listCalls   []listRequest
	listResult  []manifest
	listErr     error
	deletes     []manifest
	deleteErr   error
	cleanupErr  error
	cleanupFn   func(context.Context, uint32, time.Duration, int) ([]cleanupCandidate, error)
	cleanupDel  func(context.Context, uint32, []cleanupCandidate) error

	begin    func(context.Context, manifest) (manifest, error)
	commit   func(context.Context, manifest) error
	chunksFn func(context.Context, string, uuid.UUID, uint32, uint32) ([]chunk, error)
	listFn   func(context.Context, string, string, int) ([]manifest, error)
	deleteFn func(context.Context, manifest) (manifest, error)
	latestFn func(context.Context, string) (manifest, error)
}

type chunkRequest struct {
	generation uuid.UUID
	first      uint32
	last       uint32
}

type listRequest struct {
	prefix   string
	afterKey string
	limit    int
}

func (s *fakeStore) Init(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initCalls++
	return s.initErr
}

func (s *fakeStore) BeginUpload(ctx context.Context, upload manifest) (manifest, error) {
	if s.begin != nil {
		return s.begin(ctx, upload)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "pending")
	s.pending = append(s.pending, upload)
	return upload, s.beginErr
}

func (s *fakeStore) InsertChunks(_ context.Context, chunks []chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "chunks:"+strconv.Itoa(len(chunks)))
	s.batches = append(s.batches, append([]chunk(nil), chunks...))
	return s.chunkErr
}

func (s *fakeStore) CommitUpload(ctx context.Context, upload manifest) error {
	s.mu.Lock()
	s.events = append(s.events, "commit")
	s.commits = append(s.commits, upload)
	commit := s.commit
	commitErr := s.commitErr
	s.mu.Unlock()
	if commit != nil {
		return commit(ctx, upload)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if commitErr == nil {
		upload.State = committed
		s.mu.Lock()
		s.latest = upload
		s.mu.Unlock()
	}
	return commitErr
}

func (s *fakeStore) InsertDelete(ctx context.Context, tombstone manifest) (manifest, error) {
	s.mu.Lock()
	s.deletes = append(s.deletes, tombstone)
	deleteFn := s.deleteFn
	deleteErr := s.deleteErr
	s.mu.Unlock()
	if deleteFn != nil {
		return deleteFn(ctx, tombstone)
	}
	tombstone.State = deleted
	if deleteErr == nil {
		s.mu.Lock()
		s.latest = tombstone
		s.mu.Unlock()
	}
	return tombstone, deleteErr
}

func (s *fakeStore) LatestManifest(ctx context.Context, key string) (manifest, error) {
	s.mu.Lock()
	s.latestCalls++
	latest := s.latest
	latestErr := s.latestErr
	latestFn := s.latestFn
	s.mu.Unlock()
	if latestFn != nil {
		return latestFn(ctx, key)
	}
	return latest, latestErr
}

func (s *fakeStore) Chunks(ctx context.Context, object manifest, first, last uint32, start, end uint64) ([]chunk, error) {
	s.mu.Lock()
	s.chunkCalls = append(s.chunkCalls, chunkRequest{generation: object.Generation, first: first, last: last})
	chunksFn := s.chunksFn
	s.mu.Unlock()
	if chunksFn == nil {
		return nil, nil
	}
	full, err := chunksFn(ctx, object.Key, object.Generation, first, last)
	if err != nil {
		return nil, err
	}
	return sliceChunksForTest(full, object.ChunkSize, start, end), nil
}

// sliceChunksForTest mirrors the server-side chunk payload slicing of the real
// store: FullLength reports the raw payload size and Data is trimmed to the
// absolute object byte range [start, end).
func sliceChunksForTest(full []chunk, chunkSize uint32, start, end uint64) []chunk {
	result := make([]chunk, 0, len(full))
	for _, value := range full {
		length := int64(len(value.Data))
		chunkStart := int64(value.Index) * int64(chunkSize)
		from := min(length, max(int64(start)-chunkStart, 0))
		to := min(length, max(int64(end)-chunkStart, 0))
		if to < from {
			to = from
		}
		value.FullLength = uint64(length)
		value.Data = value.Data[from:to]
		result = append(result, value)
	}
	return result
}

func (s *fakeStore) ListLatest(ctx context.Context, prefix, afterKey string, limit int) ([]manifest, error) {
	s.mu.Lock()
	s.lists = append(s.lists, prefix)
	s.listCalls = append(s.listCalls, listRequest{prefix: prefix, afterKey: afterKey, limit: limit})
	listFn := s.listFn
	result := make([]manifest, 0, min(limit, len(s.listResult)))
	for _, value := range s.listResult {
		if value.State == committed && strings.HasPrefix(value.Key, prefix) && value.Key > afterKey {
			result = append(result, value)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	if len(result) > limit {
		result = result[:limit]
	}
	listErr := s.listErr
	s.mu.Unlock()
	if listFn != nil {
		return listFn(ctx, prefix, afterKey, limit)
	}
	return result, listErr
}

func (s *fakeStore) CleanupCandidates(ctx context.Context, partition uint32, grace time.Duration, limit int) ([]cleanupCandidate, error) {
	if s.cleanupFn != nil {
		return s.cleanupFn(ctx, partition, grace, limit)
	}
	return nil, s.cleanupErr
}

func (s *fakeStore) DeleteGenerations(ctx context.Context, partition uint32, candidates []cleanupCandidate) error {
	if s.cleanupDel != nil {
		return s.cleanupDel(ctx, partition, candidates)
	}
	return nil
}

func (s *fakeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return s.closeErr
}

func bucketTestConfig() Config {
	return Config{
		ChunkSize:            3,
		InsertBatchSize:      2,
		ReadPrefetchChunks:   8,
		MaxReadPrefetchBytes: 24,
		MaxUploadDuration:    time.Minute,
	}
}

func testBucket(cfg Config, store store) *Bucket {
	bucket, err := newBucketWithStore(cfg, "test", log.NewNopLogger(), store)
	if err != nil {
		panic(err)
	}
	return bucket
}

func TestBucketUploadChunksUnknownSizeAtomically(t *testing.T) {
	store := &fakeStore{}
	bucket := testBucket(bucketTestConfig(), store)

	require.NoError(t, bucket.Upload(context.Background(), "key", bytes.NewBufferString("abcdefgh")))
	require.Equal(t, []string{"pending", "chunks:2", "chunks:1", "commit"}, store.events)
	require.Len(t, store.pending, 1)
	require.Len(t, store.commits, 1)
	require.Equal(t, uint64(8), store.commits[0].Size)
	require.Equal(t, uint32(3), store.commits[0].ChunkCount)
	require.Equal(t, uint32(3), store.commits[0].ChunkSize)
	require.Equal(t, store.pending[0].Generation, store.commits[0].Generation)
	require.Equal(t, store.pending[0].Version, store.commits[0].Version)

	var gotData [][]byte
	var gotIndexes []uint32
	for _, batch := range store.batches {
		for _, value := range batch {
			gotData = append(gotData, value.Data)
			gotIndexes = append(gotIndexes, value.Index)
			require.Equal(t, "key", value.Key)
			require.Equal(t, store.pending[0].Generation, value.Generation)
		}
	}
	require.Equal(t, [][]byte{[]byte("abc"), []byte("def"), []byte("gh")}, gotData)
	require.Equal(t, []uint32{0, 1, 2}, gotIndexes)
}

func TestBucketUploadMakesGenerationVisibleAcrossDecreasingClocksAndConcurrentCommit(t *testing.T) {
	var persistedMu sync.Mutex
	persistedLatest := manifest{
		Key:        "key",
		Generation: uuid.New(),
		State:      committed,
		Version:    100,
	}
	commitCalls := 0
	store := &fakeStore{}
	store.latestFn = func(context.Context, string) (manifest, error) {
		persistedMu.Lock()
		defer persistedMu.Unlock()
		return persistedLatest, nil
	}
	store.commit = func(_ context.Context, upload manifest) error {
		persistedMu.Lock()
		defer persistedMu.Unlock()
		commitCalls++
		if commitCalls == 1 {
			persistedLatest = manifest{
				Key:        upload.Key,
				Generation: uuid.New(),
				State:      committed,
				Version:    max(persistedLatest.Version, upload.Version) + 1,
			}
			return nil
		}
		upload.State = committed
		persistedLatest = upload
		return nil
	}

	cfg := bucketTestConfig()
	bucket, err := newBucketWithStoreOptions(cfg, "test", log.NewNopLogger(), store, bucketOptions{
		now: func() time.Time { return time.Unix(0, 10) },
	})
	require.NoError(t, err)

	require.NoError(t, bucket.Upload(context.Background(), "key", bytes.NewBufferString("payload")))
	require.Equal(t, store.commits[len(store.commits)-1].Generation, persistedLatest.Generation,
		"Upload returned with a stale generation instead of the persisted latest generation")
	require.Len(t, store.pending, 2, "only manifest metadata should be retried")
	require.Len(t, store.batches, 2, "payload chunks should be written once in bounded batches")
	require.Len(t, store.commits, 2)
	require.Equal(t, store.commits[0].Generation, store.commits[1].Generation)
	require.Greater(t, store.commits[0].Version, uint64(100))
	require.Greater(t, store.commits[1].Version, store.commits[0].Version)
}

func TestBucketUploadStopsAfterMaximumCommitAttempts(t *testing.T) {
	const expectedMaxAttempts = 8

	var persistedMu sync.Mutex
	persistedLatest := manifest{
		Key:        "key",
		Generation: uuid.New(),
		State:      committed,
		Version:    100,
	}
	commitCalls := 0
	store := &fakeStore{}
	store.latestFn = func(context.Context, string) (manifest, error) {
		persistedMu.Lock()
		defer persistedMu.Unlock()
		return persistedLatest, nil
	}
	store.commit = func(_ context.Context, upload manifest) error {
		persistedMu.Lock()
		defer persistedMu.Unlock()
		commitCalls++
		if commitCalls > expectedMaxAttempts {
			return errors.New("test observed unrestricted ninth commit attempt")
		}
		persistedLatest = manifest{
			Key:        upload.Key,
			Generation: uuid.New(),
			State:      committed,
			Version:    max(persistedLatest.Version, upload.Version) + 1,
		}
		return nil
	}

	registry := prometheus.NewRegistry()
	bucket, bucketErr := newBucketWithStore(bucketTestConfig(), "metrics", log.NewNopLogger(), store, registry)
	require.NoError(t, bucketErr)
	err := bucket.Upload(context.Background(), "key", bytes.NewBufferString("payload"))

	require.Equal(t, expectedMaxAttempts, commitCalls, "commit attempts must be explicitly bounded")
	require.ErrorIs(t, err, errUploadCommitAttemptsExhausted)
	require.ErrorContains(t, err, "ClickHouse upload commit attempts exhausted after 8 attempts")
	require.Equal(t, float64(expectedMaxAttempts-1), testutil.ToFloat64(bucket.metrics.commitRetries))
	require.Equal(t, float64(1), testutil.ToFloat64(bucket.metrics.operationFailures.WithLabelValues("upload")))
	require.Equal(t, float64(0), testutil.ToFloat64(bucket.metrics.operationsInflight.WithLabelValues("upload")))
}

func TestBucketUploadReportsDurableChunkMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	store := &fakeStore{}
	bucket, err := newBucketWithStore(bucketTestConfig(), "metrics", log.NewNopLogger(), store, registry)
	require.NoError(t, err)

	require.NoError(t, bucket.Upload(context.Background(), "key", bytes.NewBufferString("abcdefgh")))
	require.Equal(t, float64(3), testutil.ToFloat64(bucket.metrics.uploadChunks))
	require.Equal(t, float64(8), testutil.ToFloat64(bucket.metrics.uploadBytes))
	require.Equal(t, float64(0), testutil.ToFloat64(bucket.metrics.operationFailures.WithLabelValues("upload")))

	store.chunkErr = errors.New("insert failed")
	require.Error(t, bucket.Upload(context.Background(), "failed", bytes.NewBufferString("abc")))
	require.Equal(t, float64(3), testutil.ToFloat64(bucket.metrics.uploadChunks))
	require.Equal(t, float64(8), testutil.ToFloat64(bucket.metrics.uploadBytes))
	require.Equal(t, float64(1), testutil.ToFloat64(bucket.metrics.operationFailures.WithLabelValues("upload")))
	require.Equal(t, float64(0), testutil.ToFloat64(bucket.metrics.operationsInflight.WithLabelValues("upload")))
	require.Equal(t, uint64(2), histogramSampleCount(t, registry, "pyroscope_objstore_clickhouse_operation_duration_seconds", "operation", "upload"))
}

func histogramSampleCount(t *testing.T, registry *prometheus.Registry, metricName, labelName, labelValue string) uint64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					return metric.Histogram.GetSampleCount()
				}
			}
		}
	}
	return 0
}

func TestBucketMetricsAllowNilAndDuplicateRegisterers(t *testing.T) {
	require.NotPanics(t, func() {
		_, err := newBucketWithStore(bucketTestConfig(), "nil", log.NewNopLogger(), &fakeStore{}, nil)
		require.NoError(t, err)
	})
	registry := prometheus.NewRegistry()
	first, err := newBucketWithStore(bucketTestConfig(), "same", log.NewNopLogger(), &fakeStore{}, registry)
	require.NoError(t, err)
	second, err := newBucketWithStore(bucketTestConfig(), "same", log.NewNopLogger(), &fakeStore{}, registry)
	require.NoError(t, err)
	first.metrics.uploadChunks.Inc()
	require.Equal(t, float64(1), testutil.ToFloat64(second.metrics.uploadChunks))
}

func TestBucketUploadCopiesChunksBeforeBufferReuse(t *testing.T) {
	store := &fakeStore{}
	bucket := testBucket(bucketTestConfig(), store)

	require.NoError(t, bucket.Upload(context.Background(), "key", bytes.NewBufferString("abcdef")))
	require.Equal(t, []byte("abc"), store.batches[0][0].Data)
	require.Equal(t, []byte("def"), store.batches[0][1].Data)
}

func TestBucketUploadEmptyObject(t *testing.T) {
	store := &fakeStore{}
	bucket := testBucket(bucketTestConfig(), store)

	require.NoError(t, bucket.Upload(context.Background(), "empty", bytes.NewReader(nil)))
	require.Equal(t, []string{"pending", "commit"}, store.events)
	require.Empty(t, store.batches)
	require.Equal(t, uint64(0), store.commits[0].Size)
	require.Equal(t, uint32(0), store.commits[0].ChunkCount)
}

func TestBucketUploadDoesNotCommitOnFailure(t *testing.T) {
	readErr := errors.New("read failed")
	tests := []struct {
		name   string
		store  *fakeStore
		ctx    context.Context
		reader io.Reader
	}{
		{name: "read error", store: &fakeStore{}, ctx: context.Background(), reader: errorReader{err: readErr}},
		{name: "begin error", store: &fakeStore{beginErr: errors.New("begin failed")}, ctx: context.Background(), reader: bytes.NewBufferString("abc")},
		{name: "chunk error", store: &fakeStore{chunkErr: errors.New("chunk failed")}, ctx: context.Background(), reader: bytes.NewBufferString("abc")},
		{name: "caller canceled", store: &fakeStore{}, ctx: canceledContext(), reader: bytes.NewBufferString("abc")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := testBucket(bucketTestConfig(), tt.store).Upload(tt.ctx, "key", tt.reader)
			require.Error(t, err)
			require.Empty(t, tt.store.commits)
		})
	}
}

func TestBucketUploadDoesNotFlushPartialReadWithError(t *testing.T) {
	readErr := errors.New("read failed after data")
	store := &fakeStore{}

	err := testBucket(bucketTestConfig(), store).Upload(context.Background(), "key", &partialErrorReader{
		data: []byte("ab"),
		err:  readErr,
	})

	require.ErrorIs(t, err, readErr)
	require.Equal(t, []string{"pending"}, store.events)
	require.Empty(t, store.batches)
	require.Empty(t, store.commits)
}

func TestBucketUploadReturnsCommitErrorAfterChunks(t *testing.T) {
	commitErr := errors.New("commit failed")
	store := &fakeStore{commitErr: commitErr}

	err := testBucket(bucketTestConfig(), store).Upload(context.Background(), "key", bytes.NewBufferString("abc"))

	require.ErrorIs(t, err, commitErr)
	require.Equal(t, []string{"pending", "chunks:1", "commit"}, store.events)
	require.Len(t, store.commits, 1)
}

func TestBucketUploadPreservesCommitContextErrors(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		cfg     func() Config
		commit  func(context.CancelFunc) func(context.Context, manifest) error
		wantErr error
	}{
		{
			name: "canceled",
			ctx:  func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			cfg:  bucketTestConfig,
			commit: func(cancel context.CancelFunc) func(context.Context, manifest) error {
				return func(ctx context.Context, _ manifest) error {
					cancel()
					<-ctx.Done()
					return ctx.Err()
				}
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline exceeded",
			ctx:  func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			cfg: func() Config {
				cfg := bucketTestConfig()
				cfg.MaxUploadDuration = 20 * time.Millisecond
				return cfg
			},
			commit: func(context.CancelFunc) func(context.Context, manifest) error {
				return func(ctx context.Context, _ manifest) error {
					<-ctx.Done()
					return ctx.Err()
				}
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.ctx()
			defer cancel()
			store := &fakeStore{}
			store.commit = tt.commit(cancel)

			err := testBucket(tt.cfg(), store).Upload(ctx, "key", bytes.NewBufferString("abc"))

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, []string{"pending", "chunks:1", "commit"}, store.events)
			require.Len(t, store.commits, 1)
		})
	}
}

func TestBucketUploadValidatesAllocationsBeforeUse(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "chunk size", cfg: Config{ChunkSize: maxChunkSize + 1, InsertBatchSize: 1, MaxUploadDuration: time.Minute}},
		{name: "insert batch size", cfg: Config{ChunkSize: 1, InsertBatchSize: maxInsertBatchSize + 1, MaxUploadDuration: time.Minute}},
		{name: "buffered chunk bytes", cfg: Config{ChunkSize: maxChunkSize, InsertBatchSize: 5, MaxUploadDuration: time.Minute}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{}
			err := testBucket(tt.cfg, store).Upload(context.Background(), "key", panicReader{})

			require.Error(t, err)
			require.Empty(t, store.events)
		})
	}
}

func TestBucketUploadUsesHardLocalTimeout(t *testing.T) {
	cfg := bucketTestConfig()
	cfg.MaxUploadDuration = 20 * time.Millisecond
	store := &fakeStore{}
	store.begin = func(ctx context.Context, upload manifest) (manifest, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(cfg.MaxUploadDuration), deadline, 50*time.Millisecond)
		<-ctx.Done()
		return manifest{}, ctx.Err()
	}

	err := testBucket(cfg, store).Upload(context.Background(), "key", bytes.NewBufferString("abc"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, store.commits)
}

func TestBucketUploadPreservesEarlierCallerDeadline(t *testing.T) {
	cfg := bucketTestConfig()
	cfg.MaxUploadDuration = time.Minute
	callerCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	callerDeadline, ok := callerCtx.Deadline()
	require.True(t, ok)

	store := &fakeStore{}
	store.begin = func(ctx context.Context, upload manifest) (manifest, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, callerDeadline, deadline)
		<-ctx.Done()
		return manifest{}, ctx.Err()
	}

	err := testBucket(cfg, store).Upload(callerCtx, "key", bytes.NewBufferString("abc"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, store.commits)
}

func TestBucketUploadRejectsEmptyKey(t *testing.T) {
	store := &fakeStore{}
	err := testBucket(bucketTestConfig(), store).Upload(context.Background(), "", bytes.NewBufferString("payload"))

	require.Error(t, err)
	require.Contains(t, err.Error(), "object key")
	require.Empty(t, store.pending)
}

func TestBucketUploadPropagatesVersionClockError(t *testing.T) {
	store := &fakeStore{latestErr: ErrObjectNotFound}
	bucket := testBucket(bucketTestConfig(), store)
	bucket.versionClock = newVersionClock(func() time.Time { return time.Unix(0, -1) })

	err := bucket.Upload(context.Background(), "key", bytes.NewBufferString("payload"))
	require.ErrorContains(t, err, "version")
	require.Empty(t, store.pending)
}

func TestCheckedUploadProgressRejectsOverflow(t *testing.T) {
	t.Run("size", func(t *testing.T) {
		upload := manifest{Size: math.MaxUint64}
		require.Error(t, checkedUploadProgress(&upload, 1))
	})

	t.Run("chunk count", func(t *testing.T) {
		upload := manifest{ChunkCount: math.MaxUint32}
		require.Error(t, checkedUploadProgress(&upload, 1))
	})
}

func TestNewBucketClientWithConstructor(t *testing.T) {
	t.Run("validates before opening", func(t *testing.T) {
		called := false
		_, err := newBucketClient(context.Background(), Config{}, "test", log.NewNopLogger(), func(Config) (store, error) {
			called = true
			return &fakeStore{}, nil
		})
		require.Error(t, err)
		require.False(t, called)
	})

	t.Run("open error", func(t *testing.T) {
		openErr := errors.New("open failed")
		_, err := newBucketClient(context.Background(), testConfig(t), "test", log.NewNopLogger(), func(Config) (store, error) {
			return nil, openErr
		})
		require.ErrorIs(t, err, openErr)
	})

	t.Run("init error closes store", func(t *testing.T) {
		fake := &fakeStore{initErr: errors.New("init failed")}
		_, err := newBucketClient(context.Background(), testConfig(t), "test", log.NewNopLogger(), func(Config) (store, error) {
			return fake, nil
		})
		require.ErrorIs(t, err, fake.initErr)
		require.Equal(t, 1, fake.initCalls)
		require.Equal(t, 1, fake.closeCalls)
	})

	t.Run("success", func(t *testing.T) {
		fake := &fakeStore{}
		registry := prometheus.NewRegistry()
		bucket, err := newBucketClient(context.Background(), testConfig(t), "test", log.NewNopLogger(), func(Config) (store, error) {
			return fake, nil
		}, registry)
		require.NoError(t, err)
		require.Equal(t, 1, fake.initCalls)
		require.Same(t, fake, bucket.store)
		require.NoError(t, bucket.Close())
	})

	t.Run("metrics error closes initialized store", func(t *testing.T) {
		fake := &fakeStore{}
		registry := prometheus.NewRegistry()
		registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "pyroscope_objstore_clickhouse_upload_chunks_total",
			Help:        "Total ClickHouse object chunks durably inserted.",
			ConstLabels: prometheus.Labels{"bucket": "test"},
		}))

		bucket, err := newBucketClient(context.Background(), testConfig(t), "test", log.NewNopLogger(), func(Config) (store, error) {
			return fake, nil
		}, registry)

		require.Nil(t, bucket)
		require.ErrorContains(t, err, "metrics")
		require.Equal(t, 1, fake.initCalls)
		require.Equal(t, 1, fake.closeCalls)
	})
}

func TestBucketIdentityAndCloseAreStable(t *testing.T) {
	closeErr := errors.New("close failed")
	store := &fakeStore{closeErr: closeErr}
	bucket := testBucket(bucketTestConfig(), store)

	require.Equal(t, "test", bucket.Name())
	require.Equal(t, objstore.ObjProvider("CLICKHOUSE"), bucket.Provider())
	require.ErrorIs(t, bucket.Close(), closeErr)
	require.ErrorIs(t, bucket.Close(), closeErr)
	require.Equal(t, 1, store.closeCalls)
}

func TestDeleteAppendsFreshHigherVersionTombstone(t *testing.T) {
	oldGeneration := uuid.New()
	latest := manifest{
		Key:            "key",
		Generation:     oldGeneration,
		Size:           17,
		ChunkCount:     3,
		ChunkSize:      8,
		State:          committed,
		Version:        100,
		LeaseExpiresAt: time.Unix(20, 0),
		EventAt:        time.Unix(10, 0),
	}
	store := &fakeStore{latest: latest}
	bucket := testBucket(bucketTestConfig(), store)
	bucket.versionClock = newVersionClock(func() time.Time { return time.Unix(0, 10) })

	require.NoError(t, bucket.Delete(context.Background(), latest.Key))
	require.Equal(t, 2, store.latestCalls)
	require.Len(t, store.deletes, 1)
	tombstone := store.deletes[0]
	require.Equal(t, latest.Key, tombstone.Key)
	require.NotEqual(t, oldGeneration, tombstone.Generation)
	require.Greater(t, tombstone.Version, latest.Version)
	require.Equal(t, deleted, tombstone.State)
	require.Equal(t, latest.Size, tombstone.Size)
	require.Equal(t, latest.ChunkCount, tombstone.ChunkCount)
	require.Equal(t, latest.ChunkSize, tombstone.ChunkSize)
	require.True(t, tombstone.LeaseExpiresAt.IsZero())
	require.True(t, tombstone.EventAt.IsZero())
}

func TestDeleteReturnsConcurrentMutationWhenAnotherGenerationWins(t *testing.T) {
	live := manifest{Key: "key", Generation: uuid.New(), State: committed, Version: 100}
	winner := manifest{Key: "key", Generation: uuid.New(), State: committed, Version: 102}
	store := &fakeStore{latest: live}
	store.deleteFn = func(_ context.Context, tombstone manifest) (manifest, error) {
		tombstone.State = deleted
		store.mu.Lock()
		store.latest = winner
		store.mu.Unlock()
		return tombstone, nil
	}

	err := testBucket(bucketTestConfig(), store).Delete(context.Background(), live.Key)

	require.ErrorIs(t, err, ErrConcurrentMutation)
	require.ErrorContains(t, err, winner.Generation.String())
	require.Equal(t, 2, store.latestCalls)
	require.Len(t, store.deletes, 1)
}

func TestDeleteReturnsConcurrentMutationWhenTombstoneStateDidNotWin(t *testing.T) {
	live := manifest{Key: "key", Generation: uuid.New(), State: committed, Version: 100}
	store := &fakeStore{latest: live}
	store.deleteFn = func(_ context.Context, tombstone manifest) (manifest, error) {
		store.mu.Lock()
		store.latest = tombstone
		store.latest.State = committed
		store.mu.Unlock()
		return tombstone, nil
	}

	err := testBucket(bucketTestConfig(), store).Delete(context.Background(), live.Key)

	require.ErrorIs(t, err, ErrConcurrentMutation)
	require.ErrorContains(t, err, committed.String())
}

func TestDeletePropagatesPostconditionReadError(t *testing.T) {
	postconditionErr := errors.New("postcondition read failed")
	live := manifest{Key: "key", Generation: uuid.New(), State: committed, Version: 100}
	store := &fakeStore{}
	reads := 0
	store.latestFn = func(context.Context, string) (manifest, error) {
		reads++
		if reads == 1 {
			return live, nil
		}
		return manifest{}, postconditionErr
	}

	err := testBucket(bucketTestConfig(), store).Delete(context.Background(), live.Key)

	require.ErrorIs(t, err, postconditionErr)
	require.ErrorContains(t, err, "verify tombstone")
	require.Equal(t, 2, store.latestCalls)
}

func TestDeleteRejectsEmptyKey(t *testing.T) {
	store := &fakeStore{}
	bucket := testBucket(bucketTestConfig(), store)
	err := bucket.Delete(context.Background(), "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "object key")
	require.False(t, bucket.IsObjNotFoundErr(err))
	require.Empty(t, store.deletes)
	require.Zero(t, store.latestCalls)
}

func TestDeleteReturnsNotFoundForAbsentOrDeletedObject(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *fakeStore
	}{
		{name: "absent", store: &fakeStore{latestErr: ErrObjectNotFound}},
		{name: "deleted", store: &fakeStore{latest: manifest{Key: "key", State: deleted}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := testBucket(bucketTestConfig(), tc.store).Delete(context.Background(), "key")
			require.ErrorIs(t, err, ErrObjectNotFound)
			require.Empty(t, tc.store.deletes)
		})
	}
}

func TestDeletePropagatesStoreAndVersionErrors(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		insertErr := errors.New("insert delete failed")
		store := &fakeStore{latest: manifest{Key: "key", State: committed}, deleteErr: insertErr}
		err := testBucket(bucketTestConfig(), store).Delete(context.Background(), "key")
		require.ErrorIs(t, err, insertErr)
	})

	t.Run("version", func(t *testing.T) {
		store := &fakeStore{latest: manifest{Key: "key", State: committed, Version: math.MaxUint64}}
		bucket := testBucket(bucketTestConfig(), store)

		err := bucket.Delete(context.Background(), "key")
		require.ErrorContains(t, err, "version")
		require.Empty(t, store.deletes)
	})
}

type errorReader struct {
	err error
}

type partialErrorReader struct {
	data []byte
	err  error
}

func (r *partialErrorReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = nil
	return n, r.err
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) {
	panic("reader used before upload allocation validation")
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
